use std::{future::Future, pin::Pin, sync::Arc, task::Poll};

use atomic_waker::AtomicWaker;

use super::{ipc, listener::TryExecuteError, Operation};

/// A Handle to a commited Instance
///
/// Through this you can execute the Operation in the referenced Instance and get the Value of the
/// Result of that Operation
pub struct CommitHandle<Id, O, T>
where
    O: Operation<T>,
{
    node: Id,
    instance: u64,
    op: O,
    ipc_sender: ipc::IPCSender<Id, O, T>,
}

/// A Handle to a running Execution Attempt, to get the result of the Execution, this handle needs
/// to be awaited
pub struct ExecutionHandle<Id, O, T>
where
    O: Operation<T>,
{
    fut: Pin<Box<dyn Future<Output = Result<O::ApplyResult, TryExecuteError<Id, O::ApplyResult>>>>>,
}

impl<Id, O, T> CommitHandle<Id, O, T>
where
    O: Operation<T> + 'static,
    Id: Clone + 'static,
    T: 'static,
{
    pub(super) fn committed(
        sender: ipc::IPCSender<Id, O, T>,
        node: Id,
        instance: u64,
        op: O,
    ) -> Self {
        Self {
            node,
            instance,
            op,
            ipc_sender: sender,
        }
    }

    /// Gets the Operation stored in that Instance
    pub fn op(&self) -> &O {
        &self.op
    }

    /// Starts the Execution of the Operation
    pub fn try_execute(&self) -> Result<ExecutionHandle<Id, O, T>, ()> {
        let waker = Arc::new(AtomicWaker::new());

        // Send Execution command to internal node
        let rx = match self.ipc_sender.send(ipc::Request::Execute {
            node: self.node.clone(),
            instance: self.instance,
            waker: Some(waker.clone()),
        }) {
            Ok(r) => r,
            Err(_e) => return Err(()),
        };

        Ok(ExecutionHandle {
            fut: Box::pin(async move {
                let tmp = rx.await.map_err(|_e| TryExecuteError::Other(""))?;

                match tmp {
                    ipc::Response::Executed(v) => v,
                    _other => Err(TryExecuteError::Other("")),
                }
            }),
        })
    }
}

impl<Id, O, T> Future for ExecutionHandle<Id, O, T>
where
    O: Operation<T>,
    O::ApplyResult: Clone,
    Id: Clone,
{
    type Output = Result<O::ApplyResult, TryExecuteError<Id, O::ApplyResult>>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        #[cfg(feature = "tracing")]
        tracing::trace!("Polled");

        self.get_mut().fut.as_mut().poll(cx)
    }
}
