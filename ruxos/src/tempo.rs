//! # Tempo
//! A leaderless replicated state machine protocol that provides linearizable operations
//!
//! # Notes on Usage
//! Altough `tempo` aims to be relatively easy to use, there are some easy to make mistakes that
//! users need to be aware of.
//!
//! ## Execution
//! Contrary to how one normally thinks of executing Operations, mostly immediately, in `tempo`
//! Operations are executed in batches.
//! One of these batch execution attempts can be triggered by calling
//! [`Handle::try_execute`]
//!
//! ## Liveness/Recovery
//!
//! ## Submitting Command
//!
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
pub use replica::Replica;

pub trait Operation<T> {
    type Result;

    fn apply(&self, state: &mut T) -> Self::Result;
}

/// A Builder to construct a [`Replica`] instance
pub struct Builder<NodeId> {
    id: Option<NodeId>,
    nodes: Vec<NodeId>,
    accepted_failures: Option<usize>,
}

impl<NodeId> Builder<NodeId> {
    /// Creates a new empty Builder
    pub fn new() -> Self {
        Self {
            id: None,
            nodes: Vec::new(),
            accepted_failures: None,
        }
    }

    /// Set the ID of the [`Replica`]
    pub fn id(mut self, id: NodeId) -> Self {
        self.id = Some(id);
        self
    }

    /// Set the Nodes in the system
    pub fn nodes(mut self, nodes: impl IntoIterator<Item = NodeId>) -> Self {
        self.nodes = nodes.into_iter().collect();
        self
    }

    /// Set the number of accptable failures in the system before it is not able to make anymore
    /// progress
    pub fn accepted_failures(mut self, failures: usize) -> Self {
        self.accepted_failures = Some(failures);
        self
    }

    /// Attempts to finish the construction of the [`Replica`]
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
