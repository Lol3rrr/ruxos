use std::{
    collections::BTreeSet,
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::tempo::{ipc, replica::OpId};

use super::{msgs, replica::InternalMessage, Operation};

#[derive(Clone)]
pub struct Handle<O, NodeId, T, V>
where
    NodeId: Ord,
{
    pub(super) node: NodeId,
    pub(super) counter: Arc<AtomicU64>,
    pub(super) tx: tokio::sync::mpsc::UnboundedSender<InternalMessage<O, NodeId, V>>,
    pub(super) _marker: PhantomData<T>,
}

#[derive(Debug)]
pub enum SubmitError {
    SendingMessageToCluster,
    ReceivingResult,
}

impl<O, NodeId, T> Handle<O, NodeId, T, O::Result>
where
    NodeId: Clone + Ord + PartialEq,
    O: Operation<T>,
{
    pub fn id(&self) -> &NodeId {
        &self.node
    }

    pub fn try_execute(&self) {
        let _ = self
            .tx
            .send(InternalMessage::IPC(ipc::IPCRequest::TryExecute(
                ipc::TryExecute {},
            )));
    }

    pub fn promises(&self) {
        let _ = self
            .tx
            .send(InternalMessage::IPC(ipc::IPCRequest::Promises));
    }

    pub fn message(&self, msg: msgs::Message<O, NodeId>) -> Result<(), ()> {
        self.tx.send(InternalMessage::Message(msg)).map_err(|e| ())
    }

    pub async fn submit(&self, op: O, nodes: BTreeSet<NodeId>) -> Result<O::Result, SubmitError> {
        let op_id = OpId {
            node: self.node.clone(),
            counter: self.counter.fetch_add(1, Ordering::Relaxed),
        };

        let (wait_tx, wait_rx) = tokio::sync::oneshot::channel();

        // TODO
        // Determine the quorum
        let quorum = nodes;

        let submit = ipc::Submit {
            id: op_id,
            operation: op,
            quorum: quorum.clone(),
            listeners: vec![wait_tx],
        };

        // Send Messages to cluster
        self.tx
            .send(InternalMessage::IPC(ipc::IPCRequest::Submit(submit)))
            .map_err(|e| SubmitError::SendingMessageToCluster)?;

        wait_rx.await.map_err(|e| SubmitError::ReceivingResult)
    }
}
