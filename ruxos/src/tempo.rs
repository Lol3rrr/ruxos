//! # References
//! * [Paper](https://hdl.handle.net/1822/81307)

mod client;
mod commands;
mod failuredetector;
mod ipc;
pub mod msgs;
pub mod promises;
pub mod replica;

pub use client::Handle;

pub trait Operation<T> {
    type Result;

    fn apply(&self, state: &mut T) -> Self::Result;
}

pub struct Builder<NodeId> {
    id: Option<NodeId>,
    nodes: Vec<NodeId>,
    accepted_failures: Option<usize>,
}

impl<NodeId> Builder<NodeId> {
    pub fn new() -> Self {
        Self {
            id: None,
            nodes: Vec::new(),
            accepted_failures: None,
        }
    }

    pub fn id(mut self, id: NodeId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn nodes(mut self, nodes: impl IntoIterator<Item = NodeId>) -> Self {
        self.nodes = nodes.into_iter().collect();
        self
    }

    pub fn accepted_failures(mut self, failures: usize) -> Self {
        self.accepted_failures = Some(failures);
        self
    }

    pub fn finish<O, V, T>(self, state: T) -> replica::Replica<O, NodeId, V, T>
    where
        NodeId: Ord + Clone,
    {
        let replica_id = self.id.unwrap();

        assert!(!self.nodes.is_empty());

        let tolerated_failures = match self.accepted_failures {
            Some(fs) => {
                assert!(self.nodes.len() >= fs * 2 + 1);
                fs
            }
            None => self.nodes.len() / 2,
        };

        replica::Replica::new(replica_id, self.nodes.len(), state, tolerated_failures)
    }
}

#[cfg(test)]
impl Operation<()> for () {
    type Result = ();

    fn apply(&self, _: &mut ()) -> Self::Result {
        ()
    }
}
