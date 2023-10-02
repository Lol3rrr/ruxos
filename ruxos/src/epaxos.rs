//! An EPaxos implementation, allow for leaderless State-Machien replication
//!
//! # General Structure
//! ## The `NodeListener`
//! The [`NodeListener`] contains the inner workings of the protocol and also contains the actual
//! State-Machine and State. The [`NodeListener`] needs to be "driven" to do any work, this is done
//! by calling [`NodeListener::poll`], which is usually done in a dedicated thread/async task to
//! ensure that this part can always process any work.
//!
//! ## The `NodeHandle`
//! The [`NodeHandle`] represents the main way to interact with the State-Machine, by "enqueuing"
//! Operations to be executed using [`NodeHandle::request`].
//!
//! ## The `ListenerHandle`
//! The [`ListenerHandle`] is the interface through which messages send from other nodes in the
//! system are forwarded to the local [`NodeListener`].
//!
//! # References:
//! * [EPaxos Paper](https://dl.acm.org/doi/pdf/10.1145/2517349.2517350)
//! * [EPaxos Go Impl](https://github.com/efficient/epaxos/blob/master/src/epaxos/epaxos.go)

// Ideas
// * left-right for read handles?
// * when commiting an operation, get handle to notify of execution of operation

// TODO
// * Ballot numbers
// * Explicit Prepare
// * Dynamic Cluster membership
// * Snapshots of the state machine for more efficient joins

use std::{
    fmt::Debug,
    hash::Hash,
    sync::{atomic::AtomicU64, Arc},
};

#[doc(hidden)]
pub mod testing;

mod cmd_storage;
mod dependencies;
mod handle;
mod ipc;
mod listener;
mod node;

pub mod msgs;

pub use handle::{CommitHandle, ExecutionHandle};

pub use listener::{ListenerHandle, NodeListener, TryExecuteError};
pub use node::NodeHandle;

/// Defines the behaviour of the Operations used in the State-Machine
pub trait Operation<T> {
    #[cfg(feature = "serde")]
    type ApplyResult: Clone + serde::Serialize + serde::de::DeserializeOwned;
    #[cfg(not(feature = "serde"))]
    type ApplyResult: Clone;

    const TRANSITIVE: bool;

    fn noop() -> Self;

    /// Determine if two operations interefere and need to be ordered relative to one another
    ///
    /// # Examples
    /// * If two Operations interact with the same key, they likely interefere
    /// * If two Operations work on seperate keys, they likely *dont* interefere
    fn interfere(&self, other: &Self) -> bool;

    /// Applies the Operation to the given state, mutating it in place
    fn apply(&mut self, state: &mut T) -> Self::ApplyResult;
}

/// Defines the interaction of the entire Cluster, especially the communication by sending messages
/// to other nodes and receiving responses
pub trait Cluster<Id, O> {
    type Error;
    type Receiver<'r>: ResponseReceiver<Id, O>
    where
        Self: 'r;

    /// The Size of the entire Cluster, including the current Node
    fn size(&self) -> usize;

    /// Send the provided `msg` to `count` other nodes in the cluster, excluding the node we are
    /// on, provided through the `local` parameter
    async fn send<'s, 'r>(
        &'s mut self,
        msg: msgs::Request<Id, O>,
        count: usize,
        local: &Id,
    ) -> Result<Self::Receiver<'r>, Self::Error>
    where
        's: 'r;
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperationInstance<Id> {
    node: Id,
    instance: u64,
}

/// Defines how responses, to previously send requests, should be received
pub trait ResponseReceiver<Id, O> {
    /// Recv a new response
    async fn recv(&mut self) -> Result<msgs::Response<Id, O>, ()>;
}

/// Create a new Node, this consists of a Listener, that handles all the data and the node itself,
/// which is what clients should interact with/what you use to start operations
pub fn new<Id, O, T>(id: Id, state: T) -> (NodeListener<Id, O, T>, NodeHandle<Id, O, T>)
where
    Id: Hash + Clone + Eq + Ord + Debug,
    O: Operation<T> + Clone + PartialEq,
{
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let ipc_tx = ipc::IPCSender::new(tx.clone());

    let epoch = Arc::new(AtomicU64::new(0));

    (
        NodeListener::new(id.clone(), rx, tx, state),
        NodeHandle::new(id, ipc_tx, epoch),
    )
}
