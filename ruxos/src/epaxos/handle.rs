use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc, task::Poll};

use atomic_waker::AtomicWaker;

use super::{ipc, listener::TryExecuteError, Operation};

// TODO
// Currently the Commit handle also starts the execution when being awaited

/// TODO
pub struct CommitHandle<Id, O, T>
where
    O: Operation<T>,
{
    node: Id,
    instance: u64,
    op: O,
    ipc_sender: ipc::IPCSender<Id, O, T>,
}

/// TODO
pub struct ExecutionHandle<Id, O, T>
where
    O: Operation<T>,
{
    fut: Pin<Box<dyn Future<Output = Result<O::ApplyResult, TryExecuteError<Id, O::ApplyResult>>>>>,
}

enum ExecutionInner<Id, O, T>
where
    O: Operation<T>,
{
    Waiting {
        rx: tokio::sync::oneshot::Receiver<ipc::Response<Id, O, T>>,
        waker: Arc<AtomicWaker>,
        _marker: PhantomData<T>,
    },
    Done(Result<O::ApplyResult, TryExecuteError<Id, O::ApplyResult>>),
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

    pub fn op(&self) -> &O {
        &self.op
    }

    pub fn try_execute(&self) -> Result<ExecutionHandle<Id, O, T>, ()> {
        let waker = Arc::new(AtomicWaker::new());

        // Send Execution command to internal node
        let rx = match self.ipc_sender.send(ipc::Request::Execute {
            node: self.node.clone(),
            instance: self.instance,
            waker: Some(waker.clone()),
        }) {
            Ok(r) => r,
            Err(e) => return Err(()),
        };

        Ok(ExecutionHandle {
            fut: Box::pin(async move {
                let tmp = rx.await.map_err(|e| TryExecuteError::Other(""))?;

                match tmp {
                    ipc::Response::Executed(v) => v,
                    other => Err(TryExecuteError::Other("")),
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
