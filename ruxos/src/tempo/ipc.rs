use std::collections::BTreeSet;

use super::replica::OpId;

#[derive(Debug)]
pub enum IPCRequest<O, NodeId, V> {
    Submit(Submit<O, NodeId, V>),
    TryExecute(TryExecute),
    LivenessCheck(LivenessCheck),
    Promises(Promises),
}

#[derive(Debug)]
pub struct Submit<O, NodeId, V> {
    pub id: OpId<NodeId>,
    pub operation: O,
    pub quorum: BTreeSet<NodeId>,
    pub listeners: Vec<tokio::sync::oneshot::Sender<V>>,
    #[cfg(feature = "tracing")]
    pub span: tracing::Span,
}

impl<O, NodeId, V> PartialEq for Submit<O, NodeId, V>
where
    NodeId: PartialEq,
    O: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id) && self.operation.eq(&other.operation) && self.quorum.eq(&self.quorum)
    }
}
impl<O, NodeId, V> Clone for Submit<O, NodeId, V>
where
    NodeId: Clone,
    O: Clone,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            operation: self.operation.clone(),
            quorum: self.quorum.clone(),
            listeners: Vec::new(),
            #[cfg(feature = "tracing")]
            span: self.span.clone(),
        }
    }
}

#[derive(Debug)]
pub struct TryExecute {}

#[derive(Debug)]
pub struct LivenessCheck {}

#[derive(Debug)]
pub struct Promises {
    #[cfg(feature = "tracing")]
    pub span: tracing::Span,
}
