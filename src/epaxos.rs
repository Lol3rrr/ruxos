//! An EPaxos implementation
//!
//! # References:
//! * [EPaxos Paper](https://dl.acm.org/doi/pdf/10.1145/2517349.2517350)

// Ideas
// * left-right for read handles?
// * when commiting an operation, get handle to notify of execution of operation

// TODO
// * Ballot numbers
// * Explicit Prepare

use std::{
    fmt::Debug,
    hash::Hash,
    sync::{atomic::AtomicU64, Arc},
};

#[doc(hidden)]
pub mod testing;

mod handle;
mod ipc;
mod listener;
mod node;

pub mod msgs;

pub use handle::{CommitHandle, ExecutionHandle};

pub use listener::{NodeListener, TryExecuteError};
pub use node::NodeHandle;

/// Defines the behaviour of the Operations used in the State-Machine
pub trait Operation<T> {
    #[cfg(feature = "serde")]
    type ApplyResult: Clone + serde::Serialize + serde::de::DeserializeOwned;
    #[cfg(not(feature = "serde"))]
    type ApplyResult: Clone;

    fn interfere(&self, other: &Self) -> bool;

    fn apply(&mut self, state: &mut T) -> Self::ApplyResult;
}

/// Defines the interaction of the entire Cluster, especially the communication by sending messages
/// to other nodes and receiving responses
pub trait Cluster<Id, O> {
    type Error;
    type Receiver<'r>: ResponseReceiver<Id, O>
    where
        Self: 'r;

    fn size(&self) -> usize;

    async fn send<'s, 'r>(
        &'s mut self,
        msg: msgs::Request<Id, O>,
        count: usize,
        local: &Id,
    ) -> Result<Self::Receiver<'r>, Self::Error>
    where
        's: 'r;
}

/// Defines how responses, to previously send requests, should be received
pub trait ResponseReceiver<Id, O> {
    async fn recv(&mut self) -> Result<msgs::Response<Id, O>, ()>;
}

/// Create a new Node, this consists of a Listener, that handles all the data and the node itself,
/// which is what clients should interact with/what you use to start operations
pub fn new<Id, O, T>(id: Id, state: T) -> (NodeListener<Id, O, T>, NodeHandle<Id, O, T>)
where
    Id: Hash + Clone + Eq + Ord + Debug,
    O: Operation<T> + Clone,
{
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let ipc_tx = ipc::IPCSender::new(tx.clone());

    let epoch = Arc::new(AtomicU64::new(0));

    (
        NodeListener::new(id.clone(), rx, tx, state),
        NodeHandle::new(id, ipc_tx, epoch),
    )
}
