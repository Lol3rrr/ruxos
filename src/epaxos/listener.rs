use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    sync::Arc,
};

use atomic_waker::AtomicWaker;

use crate::epaxos::msgs;

use super::{
    ipc::{self},
    Operation,
};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub(crate) struct Ballot<Id>(u64, u64, Id);

impl<Id> Ballot<Id> {
    pub fn initial(epoch: u64, id: Id) -> Self {
        Self(epoch, 0, id)
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
struct CmdOp<Id, O, T>
where
    O: Operation<T>,
{
    op: O,
    seq: u64,
    deps: Vec<Interference<Id>>,
    state: CmdState<O::ApplyResult>,
    #[cfg_attr(feature = "serde", serde(skip))]
    wakers: Vec<Arc<AtomicWaker>>,
    ballot: Ballot<Id>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum OpState {
    PreAccepted,
    Accepted,
    Committed,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum CmdState<V> {
    PreAccepted,
    Accepted,
    Commited,
    Executed(V),
}

impl<V> CmdState<V> {
    pub fn op_state(&self) -> OpState {
        match self {
            Self::PreAccepted => OpState::PreAccepted,
            Self::Accepted => OpState::Accepted,
            Self::Commited | Self::Executed(_) => OpState::Committed,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TryExecuteError<Id, V> {
    UnknownCommand(Id, u64),
    NotCommited(Id, u64, CmdState<V>),
    Other(&'static str),
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub(super) struct Interference<Id> {
    node: Id,
    instance: u64,
}

#[derive(Debug)]
pub enum FeedError {
    SendError,
    ReceiveError(tokio::sync::oneshot::error::RecvError),
}

pub struct ListenerHandle<Id, O, T>
where
    O: Operation<T>,
{
    tx: tokio::sync::mpsc::UnboundedSender<NodeMessage<Id, O, T>>,
    _marker: PhantomData<T>,
}

impl<Id, O, T> ListenerHandle<Id, O, T>
where
    O: Operation<T>,
{
    pub async fn feed(
        &self,
        msg: msgs::Request<Id, O>,
    ) -> Result<msgs::Response<Id, O>, FeedError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.tx
            .send(NodeMessage::External { req: msg, tx })
            .map_err(|e| FeedError::SendError)?;

        rx.await.map_err(FeedError::ReceiveError)
    }

    pub(crate) fn raw_feed(
        &self,
        msg: msgs::Request<Id, O>,
        tx: tokio::sync::oneshot::Sender<msgs::Response<Id, O>>,
    ) -> Result<(), FeedError> {
        self.tx
            .send(NodeMessage::External { req: msg, tx })
            .map_err(|e| FeedError::SendError)
    }
}

pub(super) enum NodeMessage<Id, O, T>
where
    O: Operation<T>,
{
    Ipc(ipc::Message<Id, O, T>),
    External {
        req: msgs::Request<Id, O>,
        tx: tokio::sync::oneshot::Sender<msgs::Response<Id, O>>,
    },
}

/// The internal stuff for a Node
///
/// # Usage
/// Needs to be polled in an event loop
pub struct NodeListener<Id, O, T>
where
    O: Operation<T>,
{
    id: Id,
    cmds: HashMap<Id, BTreeMap<u64, CmdOp<Id, O, T>>>,
    inputs: tokio::sync::mpsc::UnboundedReceiver<NodeMessage<Id, O, T>>,
    external_tx: tokio::sync::mpsc::UnboundedSender<NodeMessage<Id, O, T>>,
    state: T,
    _marker: PhantomData<T>,
}

impl<Id, O, T> NodeListener<Id, O, T>
where
    Id: Hash + Ord + Eq + Clone + Debug,
    O: Operation<T> + Clone,
{
    pub(super) fn new(
        id: Id,
        inputs: tokio::sync::mpsc::UnboundedReceiver<NodeMessage<Id, O, T>>,
        external_tx: tokio::sync::mpsc::UnboundedSender<NodeMessage<Id, O, T>>,
        state: T,
    ) -> Self {
        Self {
            id,
            cmds: HashMap::new(),
            inputs,
            external_tx,
            state,
            _marker: PhantomData,
        }
    }

    pub fn handle(&self) -> ListenerHandle<Id, O, T> {
        ListenerHandle {
            tx: self.external_tx.clone(),
            _marker: PhantomData,
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn poll(&mut self) {
        let msg = match self.inputs.try_recv() {
            Ok(m) => m,
            Err(e) => return,
        };

        match msg {
            NodeMessage::Ipc(ipcm) => {
                self.poll_ipc(ipcm);
            }
            NodeMessage::External { req, tx } => {
                self.poll_msgs(req, tx);
            }
        };
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, req, tx)))]
    fn poll_msgs(
        &mut self,
        req: msgs::Request<Id, O>,
        tx: tokio::sync::oneshot::Sender<msgs::Response<Id, O>>,
    ) {
        let resp = match req {
            msgs::Request::PreAccept(p) => {
                #[cfg(feature = "tracing")]
                tracing::trace!("PreAccept");

                let inner = self.receive_preaccept(p);
                msgs::Response::PreAcceptOk(inner)
            }
            msgs::Request::Accept(a) => {
                #[cfg(feature = "tracing")]
                tracing::trace!("Accept");

                let cmd = match self
                    .cmds
                    .get_mut(&a.node.0)
                    .map(|c| c.get_mut(&a.node.1))
                    .flatten()
                {
                    Some(c) => c,
                    None => todo!(),
                };

                cmd.state = CmdState::Accepted;

                msgs::Response::AcceptOk(msgs::AcceptOk { node: a.node })
            }
            msgs::Request::Commit(c) => {
                #[cfg(feature = "tracing")]
                tracing::trace!("Commit");

                let c_entry = self.cmds.entry(c.node.0.clone()).or_default();

                match c_entry.get_mut(&c.node.1) {
                    Some(t) => {
                        t.state = CmdState::Commited;
                    }
                    None => {
                        c_entry.insert(
                            c.node.1,
                            CmdOp {
                                op: c.op,
                                seq: c.seq,
                                deps: c.deps,
                                state: CmdState::Commited,
                                wakers: Vec::new(),
                                ballot: c.ballot,
                            },
                        );
                    }
                };

                msgs::Response::Commit
            }
            msgs::Request::Prepare(prep) => {
                let resp = self.recv_prepare(prep).unwrap();

                msgs::Response::PrepareResp(resp)
            }
        };

        if let Err(_e) = tx.send(resp) {
            #[cfg(feature = "tracing")]
            tracing::error!("Sending Response");
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, req)))]
    fn poll_ipc(&mut self, req: ipc::Message<Id, O, T>) {
        let resp = match req.req {
            ipc::Request::PreAccept {
                op: operation,
                instance,
                ballot,
            } => {
                #[cfg(feature = "tracing")]
                tracing::trace!("PreAccept");

                let interf: Vec<_> = self
                    .cmds
                    .iter()
                    .flat_map(|(i, cmds)| {
                        cmds.into_iter()
                            .map(move |(idx, cmd)| (idx, (i, cmd)))
                            .filter(|(_, c)| operation.interfere(&c.1.op))
                            .map(|(i, (id, _))| Interference {
                                node: id.clone(),
                                instance: *i,
                            })
                    })
                    .collect();

                // 2.
                let op_seq = interf
                    .iter()
                    .filter_map(|o| self.cmds.get(&o.node)?.get(&o.instance).map(|o| o.seq))
                    .max()
                    .unwrap_or(0)
                    + 1;

                // 3.
                let deps = interf;

                // 4.
                let node_entry = self.cmds.entry(self.id.clone()).or_default();
                node_entry.insert(
                    instance,
                    CmdOp {
                        op: operation.clone(),
                        seq: op_seq,
                        deps: deps.clone(),
                        state: CmdState::PreAccepted,
                        wakers: Vec::new(),
                        ballot: ballot.clone(),
                    },
                );

                // 5.
                let msg = msgs::PreAccept {
                    op: operation,
                    seq: op_seq,
                    deps: deps.clone(),
                    node: (self.id.clone(), instance),
                    ballot,
                };

                let resp = self.receive_preaccept(msg.clone());

                ipc::Response::PreAcceptOk(msg, resp)
            }
            ipc::Request::Accept {
                instance,
                n_deps,
                n_seq,
            } => {
                #[cfg(feature = "tracing")]
                tracing::trace!("Accept");

                let node = match self
                    .cmds
                    .get_mut(&self.id)
                    .map(|c| c.get_mut(&instance))
                    .flatten()
                {
                    Some(n) => n,
                    None => todo!(),
                };

                node.deps = n_deps;
                node.seq = n_seq;
                node.state = CmdState::Accepted;

                ipc::Response::Accepted(msgs::Accept {
                    op: node.op.clone(),
                    seq: node.seq,
                    deps: node.deps.clone(),
                    node: (self.id.clone(), instance),
                })
            }
            ipc::Request::Commit { instance } => {
                #[cfg(feature = "tracing")]
                tracing::trace!("Commit");

                // 21.
                let inner = match self
                    .cmds
                    .get_mut(&self.id)
                    .map(|l| l.get_mut(&instance))
                    .flatten()
                {
                    Some(t) => {
                        t.state = CmdState::Commited;
                        msgs::Commit {
                            op: t.op.clone(),
                            seq: t.seq.clone(),
                            deps: t.deps.clone(),
                            node: (self.id.clone(), instance),
                            ballot: t.ballot.clone(),
                        }
                    }
                    None => todo!(),
                };

                ipc::Response::Committed(inner)
            }
            ipc::Request::Execute { instance, waker } => {
                #[cfg(feature = "tracing")]
                tracing::trace!("Execute");

                if let Some(waker) = waker {
                    if let Some(cmd) = self
                        .cmds
                        .get_mut(&self.id)
                        .map(|c| c.get_mut(&instance))
                        .flatten()
                    {
                        cmd.wakers.push(waker);
                    }
                }

                let value = self.try_execute(self.id.clone(), instance);

                if let Some(cmd) = self.cmds.get(&self.id).map(|c| c.get(&instance)).flatten() {
                    for waker in cmd.wakers.iter() {
                        waker.wake();
                    }
                }

                #[cfg(feature = "tracing")]
                tracing::trace!("Attempted execute: {}", value.is_ok());

                ipc::Response::Executed(value)
            }
            ipc::Request::ExplicitPrepare { instance, node } => {
                let (msg, resp) = match self.cmds.get(&node).map(|c| c.get(&instance)).flatten() {
                    Some(cmd) => {
                        let n_ballot = Ballot(cmd.ballot.0, cmd.ballot.1 + 1, cmd.ballot.2.clone());
                        let msg = msgs::Prepare {
                            ballot: n_ballot,
                            node,
                            instance,
                        };

                        (msg.clone(), self.recv_prepare(msg).unwrap())
                    }
                    None => {
                        // TODO
                        // Figure out the epoch stuff?
                        let msg = msgs::Prepare {
                            ballot: Ballot(0, 1, self.id.clone()),
                            node,
                            instance,
                        };

                        (msg, msgs::PrepareResp::Nack)
                    }
                };

                ipc::Response::ExplicitPrepare(msg, resp)
            }
            ipc::Request::ExplicitCommit {
                instance,
                node,
                op,
                ballot,
                seq,
                deps,
            } => {
                let node_entries = self.cmds.entry(node.clone()).or_default();
                node_entries.insert(
                    instance,
                    CmdOp {
                        seq,
                        op: op.clone(),
                        ballot: ballot.clone(),
                        deps: deps.clone(),
                        state: CmdState::Commited,
                        wakers: Vec::new(),
                    },
                );

                ipc::Response::ExplicitCommitted(msgs::Commit {
                    seq,
                    op,
                    deps,
                    node: (node, instance),
                    ballot,
                })
            }
        };

        if let Err(_e) = req.resp.send(resp) {
            #[cfg(feature = "tracing")]
            tracing::error!("Sending Response");
        }
    }

    fn receive_preaccept(&mut self, msg: msgs::PreAccept<Id, O>) -> msgs::PreAcceptOk<Id, O> {
        // 6.
        // Update seq of the operation to the maximum of the seq of the msg and all the "local"
        // intererence operations
        let n_seq = core::iter::once(msg.seq).chain(
            self.cmds
                .iter()
                .flat_map(|(n, o)| o.values().map(move |v| (n, v)))
                .filter(|(node, cmd)| !(cmd.seq == msg.seq && *node == &self.id) && msg.op.interfere(&cmd.op))
                .map(|(_, cmd)| cmd.seq + 1),
        ).max().expect("We know that the iterator is not empty as we start it with the received sequence number");

        // 7.
        // Update dependencies of the operation by combining with the local dependencies
        let n_deps = {
            let mut tmp = msg.deps.clone();
            tmp.extend(
                self.cmds
                    .iter()
                    .flat_map(|(node, o)| o.into_iter().map(move |(i, cmd)| (i, node, cmd)))
                    .filter(|(_, node, cmd)| {
                        !(cmd.seq == msg.seq && *node == &msg.node.0) && msg.op.interfere(&cmd.op)
                    })
                    .map(|(i, node, _)| Interference {
                        node: node.clone(),
                        instance: *i,
                    }),
            );
            tmp.dedup_by(|f, s| f == s);
            tmp
        };

        // 8.
        // store the command
        let entry = self.cmds.entry(msg.node.0.clone()).or_default();
        entry.insert(
            msg.node.1,
            CmdOp {
                op: msg.op.clone(),
                seq: n_seq.clone(),
                deps: n_deps.clone(),
                state: CmdState::PreAccepted,
                wakers: Vec::new(),
                ballot: msg.ballot,
            },
        );

        // 9.
        // reply with a preaccept ok
        msgs::PreAcceptOk {
            op: msg.op,
            seq: n_seq,
            deps: n_deps,
            node: msg.node.0,
            instance: msg.node.1,
        }
    }

    /// Attempts to execute a given Command, and all the commands it depends on, but fails if not
    /// everything has been commited yet
    fn try_execute(
        &mut self,
        node: Id,
        instance: u64,
    ) -> Result<O::ApplyResult, TryExecuteError<Id, O::ApplyResult>> {
        let cmd = self
            .cmds
            .get(&node)
            .map(|c| c.get(&instance))
            .flatten()
            .ok_or(TryExecuteError::UnknownCommand(node.clone(), instance))?;
        if let CmdState::Executed(v) = &cmd.state {
            for waker in cmd.wakers.iter() {
                waker.wake();
            }

            return Ok(v.clone());
        }

        let dependency_graph = {
            let mut node_mapping: HashMap<(Id, u64), petgraph::stable_graph::NodeIndex> =
                HashMap::new();
            let mut dependency_graph = petgraph::graph::DiGraph::new();
            let mut pending = vec![(node.clone(), instance)];
            while let Some(pend) = pending.pop() {
                let node = match self.cmds.get(&pend.0).map(|c| c.get(&pend.1)).flatten() {
                    Some(c) => c,
                    None => {
                        return Err(TryExecuteError::UnknownCommand(pend.0, pend.1));
                    }
                };

                match &node.state {
                    CmdState::Commited => {}
                    CmdState::Executed(_) => {}
                    other => {
                        return Err(TryExecuteError::NotCommited(pend.0, pend.1, other.clone()));
                    }
                };

                let pend_idx = match node_mapping.get(&pend) {
                    Some(idx) => idx.to_owned(),
                    None => {
                        let pend_idx = dependency_graph.add_node(pend.clone());
                        node_mapping.insert(pend.clone(), pend_idx.clone());
                        pend_idx
                    }
                };

                for dep in node.deps.iter() {
                    let target = (dep.node.clone(), dep.instance);

                    match node_mapping.get(&target) {
                        Some(idx) => {
                            dependency_graph.add_edge(pend_idx.clone(), idx.clone(), target);
                        }
                        None => {
                            pending.push(target.clone());
                            let target_idx = dependency_graph.add_node(target.clone());
                            node_mapping.insert(target.clone(), target_idx);

                            dependency_graph.add_edge(pend_idx.clone(), target_idx, target);
                        }
                    };
                }
            }
            dependency_graph
        };

        let sccs = petgraph::algo::kosaraju_scc(&dependency_graph);

        for mut comp in sccs {
            comp.sort_unstable_by_key(|n| {
                let node = &dependency_graph.raw_nodes()[n.index()];
                self.cmds
                    .get(&node.weight.0)
                    .map(|c| c.get(&node.weight.1))
                    .flatten()
                    .map(|c| c.seq)
                    .unwrap_or(0)
            });

            for node_idx in comp {
                let node = &dependency_graph.raw_nodes()[node_idx.index()];

                let cmd = match self
                    .cmds
                    .get_mut(&node.weight.0)
                    .map(|c| c.get_mut(&node.weight.1))
                    .flatten()
                {
                    Some(c) => c,
                    None => {
                        return Err(TryExecuteError::UnknownCommand(
                            node.weight.0.clone(),
                            node.weight.1,
                        ))
                    }
                };

                match &cmd.state {
                    CmdState::Executed(_) => {}
                    CmdState::Commited => {
                        let res = cmd.op.apply(&mut self.state);
                        cmd.state = CmdState::Executed(res);

                        for waker in cmd.wakers.iter() {
                            waker.wake();
                        }

                        // TODO
                        // Somehow notify all the waiting handles
                    }
                    _ => unreachable!(),
                };
            }
        }

        let result = match self
            .cmds
            .get(&node)
            .map(|c| c.get(&instance))
            .flatten()
            .map(|c| match &c.state {
                CmdState::Executed(v) => Some(v.clone()),
                _ => None,
            })
            .flatten()
        {
            Some(r) => r,
            None => return Err(TryExecuteError::UnknownCommand(node, instance)),
        };

        Ok(result)
    }

    fn recv_prepare(&mut self, prepare: msgs::Prepare<Id>) -> Result<msgs::PrepareResp<Id, O>, ()> {
        let cmd = match self
            .cmds
            .get(&prepare.node)
            .map(|c| c.get(&prepare.instance))
            .flatten()
        {
            Some(c) => c,
            None => return Err(()),
        };

        if &prepare.ballot > &cmd.ballot {
            return Ok(msgs::PrepareResp::Ok(msgs::PrepareOk {
                op: cmd.op.clone(),
                state: cmd.state.op_state(),
                seq: cmd.seq,
                deps: cmd.deps.clone(),
                node: prepare.node.clone(),
                instance: prepare.instance,
                ballot: cmd.ballot.clone(),
            }));
        } else {
            return Ok(msgs::PrepareResp::Nack);
        }
    }
}
