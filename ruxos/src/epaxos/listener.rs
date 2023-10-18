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
    dependencies::{Dependencies, Interference},
    ipc::{self},
    Operation, OperationInstance,
};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub(crate) struct Ballot<Id>(u64, u64, Id);

impl<Id> Ballot<Id> {
    pub fn initial(epoch: u64, id: Id) -> Self {
        Self(epoch, 0, id)
    }
    pub(super) fn from_raw(epoch: u64, ballot: u64, node: Id) -> Self {
        Self(epoch, ballot, node)
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub(super) struct CmdOp<Id, O, T>
where
    O: Operation<T>,
{
    op: O,
    seq: u64,
    deps: Dependencies<Id>,
    state: CmdState<O::ApplyResult>,
    #[cfg_attr(feature = "serde", serde(skip))]
    wakers: Vec<Arc<AtomicWaker>>,
    ballot: Ballot<Id>,
    seen_ballot: Ballot<Id>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub enum OpState {
    PreAccepted,
    Accepted,
    Committed,
    Executed,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
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
            Self::Commited => OpState::Committed,
            Self::Executed(_) => OpState::Executed,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TryExecuteError<Id, V> {
    UnknownCommand(OperationInstance<Id>),
    NotCommited(OperationInstance<Id>, CmdState<V>),
    Other(&'static str),
}

#[derive(Debug)]
pub enum FeedError {
    SendError,
    ReceiveError(tokio::sync::oneshot::error::RecvError),
}

/// The Handle to forward messages from other Nodes in the cluster to the listener of the current
/// Node
pub struct ListenerHandle<Id, O, T>
where
    O: Operation<T>,
{
    tx: tokio::sync::mpsc::UnboundedSender<NodeMessage<Id, O, T>>,
    _marker: PhantomData<T>,
}

impl<Id, O, T> Clone for ListenerHandle<Id, O, T>
where
    O: Operation<T>,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _marker: PhantomData,
        }
    }
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
            .map_err(|_e| FeedError::SendError)?;

        rx.await.map_err(FeedError::ReceiveError)
    }

    pub fn raw_feed(
        &self,
        msg: msgs::Request<Id, O>,
        tx: tokio::sync::oneshot::Sender<msgs::Response<Id, O>>,
    ) -> Result<(), FeedError> {
        self.tx
            .send(NodeMessage::External { req: msg, tx })
            .map_err(|_e| FeedError::SendError)
    }
}

pub(super) enum NodeMessage<Id, O, T>
where
    O: Operation<T>,
{
    Ipc {
        #[cfg(feature = "tracing")]
        span: tracing::Span,
        req: ipc::Message<Id, O, T>,
    },
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
    O: Operation<T>,
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
}

impl<Id, O, T> NodeListener<Id, O, T>
where
    Id: Hash + Ord + Eq + Clone + Debug,
    O: Operation<T> + Clone + PartialEq + Debug,
    T: Clone,
{
    pub fn handle(&self) -> ListenerHandle<Id, O, T> {
        ListenerHandle {
            tx: self.external_tx.clone(),
            _marker: PhantomData,
        }
    }

    fn poll_handle(&mut self, msg: NodeMessage<Id, O, T>) {
        match msg {
            NodeMessage::Ipc {
                req,
                #[cfg(feature = "tracing")]
                span,
            } => {
                #[cfg(feature = "tracing")]
                let _guard = span.enter();

                let resp = match self.handle_ipc(req.req) {
                    Ok(r) => r,
                    Err(_e) => {
                        tracing::error!("Handling IPC");
                        return;
                    }
                };

                if let Err(_e) = req.resp.send(resp) {
                    #[cfg(feature = "tracing")]
                    tracing::error!("Sending Response");
                }
            }
            NodeMessage::External { req, tx } => {
                let resp = match self.handle_msg(req) {
                    Ok(r) => r,
                    Err(_e) => {
                        tracing::error!("Handling Message");
                        return;
                    }
                };

                if let Err(_e) = tx.send(resp) {
                    tracing::error!("Sending Response");
                }
            }
        };
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub async fn poll(&mut self) {
        while let Some(msg) = self.inputs.recv().await {
            self.poll_handle(msg);
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub fn try_poll(&mut self) {
        while let Ok(msg) = self.inputs.try_recv() {
            self.poll_handle(msg);
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, req)))]
    fn handle_msg(&mut self, req: msgs::Request<Id, O>) -> Result<msgs::Response<Id, O>, ()> {
        match req {
            msgs::Request::PreAccept(p) => {
                #[cfg(feature = "tracing")]
                let span = tracing::span!(tracing::Level::INFO, "preaccept", instance = ?p.node);
                #[cfg(feature = "tracing")]
                let _guard = span.enter();

                #[cfg(feature = "tracing")]
                tracing::trace!("PreAccept");

                let inner = match self.receive_preaccept(p) {
                    Ok(r) => r,
                    Err(_e) => {
                        tracing::error!("Could not Preaccept operation");

                        return Err(());
                    }
                };
                Ok(msgs::Response::PreAcceptOk(inner))
            }
            msgs::Request::Accept(a) => {
                #[cfg(feature = "tracing")]
                let span = tracing::span!(tracing::Level::INFO, "accept", instance = ?a.node);
                #[cfg(feature = "tracing")]
                let _guard = span.enter();

                #[cfg(feature = "tracing")]
                tracing::trace!("Accept");

                let node = self.cmds.entry(a.node.0.clone()).or_default();

                match node.get_mut(&a.node.1) {
                    Some(cmd) => {
                        if matches!(cmd.state, CmdState::Commited | CmdState::Executed(_)) {
                            tracing::error!("Accept for already commited/executed Operation");

                            Ok(msgs::Response::Nack)
                        } else {
                            if a.ballot < cmd.ballot {
                                tracing::error!(
                                    "Accept with out of date Ballot {:?} < {:?}",
                                    a.ballot,
                                    cmd.ballot
                                );

                                Ok(msgs::Response::Nack)
                            } else {
                                if cmd.op != a.op {
                                    tracing::warn!(prev = ?cmd.ballot, new=?a.ballot, "Updating Operation {:?} -> {:?}", cmd.op, a.op);
                                }

                                cmd.state = CmdState::Accepted;
                                cmd.ballot = a.ballot;
                                cmd.op = a.op;
                                cmd.seq = a.seq;
                                cmd.deps = a.deps;

                                Ok(msgs::Response::AcceptOk(msgs::AcceptOk { node: a.node }))
                            }
                        }
                    }
                    None => {
                        let cmd = CmdOp {
                            op: a.op,
                            seq: a.seq,
                            deps: a.deps,
                            state: CmdState::Accepted,
                            wakers: Vec::new(),
                            ballot: a.ballot.clone(),
                            seen_ballot: a.ballot,
                        };

                        node.insert(a.node.1.clone(), cmd);

                        Ok(msgs::Response::AcceptOk(msgs::AcceptOk { node: a.node }))
                    }
                }
            }
            msgs::Request::Commit(c) => {
                #[cfg(feature = "tracing")]
                let span = tracing::span!(tracing::Level::INFO, "commit", instance = ?c.node);
                #[cfg(feature = "tracing")]
                let _guard = span.enter();

                #[cfg(feature = "tracing")]
                tracing::trace!("Commit");

                let c_entry = self.cmds.entry(c.node.0.clone()).or_default();
                match c_entry.get_mut(&c.node.1) {
                    Some(t) => {
                        tracing::debug!(
                            existing_ballot = ?t.ballot,
                            new_ballot = ?c.ballot,
                            "Committing existing Command with commited status being {:?}",
                            matches!(t.state, CmdState::Commited | CmdState::Executed(_))
                        );
                        if matches!(t.state, CmdState::Commited) && t.op != c.op {
                            tracing::error!("Committing different Operations for same slot");
                        }

                        t.state = CmdState::Commited;
                        t.ballot = c.ballot;
                        t.deps = c.deps;
                        t.seq = c.seq;
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
                                ballot: c.ballot.clone(),
                                seen_ballot: c.ballot,
                            },
                        );
                    }
                };

                Ok(msgs::Response::Commit)
            }
            msgs::Request::Prepare(prep) => {
                #[cfg(feature = "tracing")]
                let span = tracing::span!(tracing::Level::INFO, "prepare", instance = ?(&prep.node, prep.instance));
                #[cfg(feature = "tracing")]
                let _guard = span.enter();

                #[cfg(feature = "tracing")]
                tracing::trace!(ballot = ?prep.ballot, "Prepare");

                let resp = match self.recv_prepare(prep) {
                    Ok(r) => r,
                    Err(_e) => {
                        #[cfg(feature = "tracing")]
                        tracing::error!("Receiving Prepare");
                        return Err(());
                    }
                };

                Ok(msgs::Response::PrepareResp(resp))
            }
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, req)))]
    fn handle_ipc(&mut self, req: ipc::Request<Id, O>) -> Result<ipc::Response<Id, O, T>, ()> {
        match req {
            ipc::Request::PreAccept {
                op: operation,
                node,
                instance,
                ballot,
            } => {
                #[cfg(feature = "tracing")]
                tracing::trace!("PreAccept");

                let interf = match self.cmds.get(&node).map(|c| c.get(&instance)).flatten() {
                    Some(c) => {
                        let mut tmp = c.deps.clone();
                        tmp.union_mut(self.dependencies(&node, instance, &operation));
                        tmp
                    }
                    None => Dependencies::from_raw(self.dependencies(&node, instance, &operation)),
                };

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
                // This is done by receive_preaccept already

                // 5.
                let msg = msgs::PreAccept {
                    op: operation,
                    seq: op_seq,
                    deps: deps.clone(),
                    node: (node, instance),
                    ballot,
                };

                match self.receive_preaccept(msg.clone()) {
                    Ok(resp) => Ok(ipc::Response::PreAcceptOk(msg, resp)),
                    Err(_e) => Ok(ipc::Response::Nack),
                }
            }
            ipc::Request::Accept {
                op,
                node: node_id,
                instance,
                n_deps,
                n_seq,
                ballot,
            } => {
                #[cfg(feature = "tracing")]
                tracing::trace!("Accept");

                let node = self.cmds.entry(node_id.clone()).or_default();

                let node = match node.get_mut(&instance) {
                    Some(n) => n,
                    None => {
                        tracing::error!("Cant accept Unknown Command");
                        return Err(());
                    }
                };

                if ballot < node.ballot {
                    tracing::trace!(?ballot, current = ?node.ballot, "Disregarding outdated ballot");
                    return Ok(ipc::Response::Nack);
                }

                // assert!(node.op == op);

                if matches!(
                    node.state,
                    CmdState::Accepted | CmdState::Commited | CmdState::Executed(_)
                ) {
                    tracing::warn!(
                        instance,
                        ?node_id,
                        stored_ballot = ?node.ballot,
                        new_ballot = ?ballot,
                        "Accepting command that was already accepted/committed/executed ({:?}) with different Op ? {:?}", node.state.op_state(), node.op != op,
                    );
                }

                node.op = op;
                node.deps = n_deps;
                node.seq = n_seq;
                node.state = CmdState::Accepted;

                Ok(ipc::Response::Accepted(msgs::Accept {
                    op: node.op.clone(),
                    seq: node.seq,
                    deps: node.deps.clone(),
                    node: (node_id, instance),
                    ballot: node.ballot.clone(),
                }))
            }
            ipc::Request::Commit {
                node,
                instance,
                ballot,
            } => {
                #[cfg(feature = "tracing")]
                tracing::trace!("Commit");

                // 21.
                let inner = match self
                    .cmds
                    .get_mut(&node)
                    .map(|l| l.get_mut(&instance))
                    .flatten()
                {
                    Some(t) => {
                        if ballot < t.ballot {
                            tracing::error!("Outdated Ballot");
                            return Ok(ipc::Response::Nack);
                        } else {
                            t.state = CmdState::Commited;
                            msgs::Commit {
                                op: t.op.clone(),
                                seq: t.seq.clone(),
                                deps: t.deps.clone(),
                                node: (node, instance),
                                ballot: t.ballot.clone(),
                            }
                        }
                    }
                    None => todo!(),
                };

                Ok(ipc::Response::Committed(inner))
            }
            ipc::Request::Execute {
                node,
                instance,
                waker,
            } => {
                #[cfg(feature = "tracing")]
                tracing::trace!("Execute");

                if let Some(waker) = waker {
                    if let Some(cmd) = self
                        .cmds
                        .get_mut(&node)
                        .map(|c| c.get_mut(&instance))
                        .flatten()
                    {
                        cmd.wakers.push(waker);
                    }
                }

                let value = self.try_execute(node.clone(), instance);

                if let Some(cmd) = self.cmds.get(&node).map(|c| c.get(&instance)).flatten() {
                    for waker in cmd.wakers.iter() {
                        waker.wake();
                    }
                }

                #[cfg(feature = "tracing")]
                tracing::trace!("Attempted execute: {}", value.is_ok());

                Ok(ipc::Response::Executed(value))
            }
            ipc::Request::ExplicitPrepare { instance, node } => {
                let (msg, resp) = match self.cmds.get(&node).map(|c| c.get(&instance)).flatten() {
                    Some(cmd) => {
                        let msg = msgs::Prepare {
                            ballot: Ballot::from_raw(0, cmd.ballot.1 + 1, self.id.clone()),
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

                Ok(ipc::Response::ExplicitPrepare(msg, resp))
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
                let prev = node_entries.insert(
                    instance,
                    CmdOp {
                        seq,
                        op: op.clone(),
                        ballot: ballot.clone(),
                        seen_ballot: ballot.clone(),
                        deps: deps.clone(),
                        state: CmdState::Commited,
                        wakers: Vec::new(),
                    },
                );

                match prev {
                    Some(prev_cmd) => {
                        if prev_cmd.op != op {
                            tracing::error!(
                                ?instance,
                                ?node,
                                "Overwrote previous Cmd Instance - Operation {:?} -> {:?}",
                                prev_cmd.op,
                                op
                            );
                        }
                        if prev_cmd.deps != deps {
                            tracing::error!(
                                ?instance,
                                ?node,
                                "Overwrote previous Deps - {:?} -> {:?}",
                                prev_cmd.deps,
                                deps
                            );
                        }
                    }
                    _ => {}
                };

                Ok(ipc::Response::ExplicitCommitted(msgs::Commit {
                    seq,
                    op,
                    deps,
                    node: (node, instance),
                    ballot,
                }))
            }
            ipc::Request::GetStorage => Ok(ipc::Response::Storage(self.cmds.clone())),
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, msg)))]
    fn receive_preaccept(
        &mut self,
        msg: msgs::PreAccept<Id, O>,
    ) -> Result<msgs::PreAcceptOk<Id, O>, ()> {
        let existing_cmd = self
            .cmds
            .get(&msg.node.0)
            .map(|c| c.get(&msg.node.1))
            .flatten();
        if existing_cmd
            .as_ref()
            .map(|b| &msg.ballot < &b.ballot)
            .unwrap_or(false)
        {
            tracing::error!(
                "Msg Ballot is older: {:?} <= {:?}",
                msg.ballot,
                existing_cmd.map(|c| &c.ballot),
            );
            tracing::debug!(
                "State of Operation {:?}",
                existing_cmd.map(|c| c.state.op_state())
            );
            return Err(());
        }

        // 7.
        // Update dependencies of the operation by combining with the local dependencies
        let n_deps = {
            let mut tmp = msg.deps.clone();

            match self
                .cmds
                .get(&msg.node.0)
                .map(|c| c.get(&msg.node.1))
                .flatten()
            {
                Some(c) => {
                    tmp.union_mut(c.deps.iter().cloned());
                }
                None => {
                    tmp.union_mut(self.dependencies(&msg.node.0, msg.node.1, &msg.op));
                }
            };

            tmp
        };

        // 6.
        // Update seq of the operation to the maximum of the seq of the msg and all the "local"
        // intererence operations
        let n_seq = core::iter::once(msg.seq)
            .chain(n_deps.iter().filter_map(|inter| self.cmds.get(&inter.node)?.get(&inter.instance).map(|cmd| cmd.seq + 1))
        ).max().expect("We know that the iterator is not empty as we start it with the received sequence number");

        // 8.
        // store the command
        let entry = self.cmds.entry(msg.node.0.clone()).or_default();
        let prev = entry.insert(
            msg.node.1,
            CmdOp {
                op: msg.op.clone(),
                seq: n_seq.clone(),
                deps: n_deps.clone(),
                state: CmdState::PreAccepted,
                wakers: Vec::new(),
                ballot: msg.ballot.clone(),
                seen_ballot: msg.ballot.clone(),
            },
        );

        if let Some(prev_cmd) = prev {
            tracing::error!(msg_ballot = ?msg.ballot, prev_ballot = ?prev_cmd.ballot, "PreAccept replaced command");
            if prev_cmd.deps != n_deps {
                tracing::error!("Deps changed - {:?} -> {:?}", prev_cmd.deps, n_deps);
            }
        }

        // 9.
        // reply with a preaccept ok
        Ok(msgs::PreAcceptOk {
            op: msg.op,
            seq: n_seq,
            deps: n_deps,
            node: msg.node.0,
            instance: msg.node.1,
        })
    }

    /// Attempts to execute a given Command, and all the commands it depends on, but fails if not
    /// everything has been commited yet
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    fn try_execute(
        &mut self,
        node: Id,
        instance: u64,
    ) -> Result<O::ApplyResult, TryExecuteError<Id, O::ApplyResult>> {
        #[cfg(feature = "tracing")]
        tracing::trace!("Attempting execute");

        let cmd = self
            .cmds
            .get(&node)
            .map(|c| c.get(&instance))
            .flatten()
            .ok_or(TryExecuteError::UnknownCommand(OperationInstance {
                node: node.clone(),
                instance,
            }))?;
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
                        return Err(TryExecuteError::UnknownCommand(OperationInstance {
                            node: pend.0,
                            instance: pend.1,
                        }));
                    }
                };

                match &node.state {
                    CmdState::Commited => {}
                    CmdState::Executed(_) => {
                        continue;
                    }
                    other => {
                        return Err(TryExecuteError::NotCommited(
                            OperationInstance {
                                node: pend.0,
                                instance: pend.1,
                            },
                            other.clone(),
                        ));
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

                let cmd_node = &node.weight.0;
                let cmd_instance = &node.weight.1;
                let cmd = match self
                    .cmds
                    .get_mut(cmd_node)
                    .map(|c| c.get_mut(cmd_instance))
                    .flatten()
                {
                    Some(c) => c,
                    None => {
                        tracing::error!("Needs to execute unknown Command");

                        return Err(TryExecuteError::UnknownCommand(OperationInstance {
                            node: node.weight.0.clone(),
                            instance: node.weight.1,
                        }));
                    }
                };

                match &cmd.state {
                    CmdState::Executed(_) => {}
                    CmdState::Commited => {
                        tracing::debug!(?cmd_node, cmd_instance, "Executing Command");

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
            None => {
                return Err(TryExecuteError::UnknownCommand(OperationInstance {
                    node,
                    instance,
                }))
            }
        };

        Ok(result)
    }

    fn recv_prepare(&mut self, prepare: msgs::Prepare<Id>) -> Result<msgs::PrepareResp<Id, O>, ()> {
        let node = self.cmds.entry(prepare.node.clone()).or_default();

        let cmd = match node.get_mut(&prepare.instance) {
            Some(c) => c,
            None => {
                #[cfg(feature = "tracing")]
                tracing::error!("Unknown Command Instance");

                return Ok(msgs::PrepareResp::Nack);
            }
        };

        if &prepare.ballot >= &cmd.ballot {
            // tracing::info!("Updating Ballot {:?} -> {:?}", cmd.ballot, prepare.ballot);
            cmd.ballot = prepare.ballot;

            return Ok(msgs::PrepareResp::Ok(msgs::PrepareOk {
                op: cmd.op.clone(),
                state: cmd.state.op_state(),
                seq: cmd.seq,
                deps: cmd.deps.clone(),
                node: prepare.node,
                instance: prepare.instance,
                ballot: cmd.ballot.clone(),
            }));
        } else {
            tracing::error!(msg_ballot = ?prepare.ballot, cmd_ballot = ?cmd.ballot, "Nacking Prepare");
            return Ok(msgs::PrepareResp::Nack);
        }
    }

    fn dependencies_iter<'s, 'o, 't>(
        &'s self,
        node: Id,
        instance: u64,
        op: &'o O,
    ) -> impl Iterator<Item = &CmdOp<Id, O, T>> + 't
    where
        's: 't,
        's: 'o,
        'o: 't,
    {
        if O::TRANSITIVE {
            itertools::Either::Left(self.cmds.iter().filter_map(move |(f_node, cmds)| {
                cmds.into_iter()
                    .rev()
                    .filter(|(i, cmd)| {
                        (**i != instance || &node != f_node) && op.interfere(&cmd.op)
                    })
                    .map(|(_, d)| d)
                    .next()
            }))
        } else {
            itertools::Either::Right(self.cmds.iter().flat_map(|(i, cmds)| {
                cmds.into_iter()
                    .map(move |(idx, cmd)| (idx, (i, cmd)))
                    .filter(|(_, c)| op.interfere(&c.1.op))
                    .map(|(_, (_, d))| d)
            }))
        }
    }
    fn dependencies(&self, node: &Id, instance: u64, op: &O) -> Vec<Interference<Id>> {
        if O::TRANSITIVE {
            self.cmds
                .iter()
                .filter_map(|(f_node, cmds)| {
                    cmds.into_iter()
                        .rev()
                        .filter(|(i, cmd)| {
                            (**i != instance || node != f_node) && op.interfere(&cmd.op)
                        })
                        .map(|(i, _)| Interference {
                            node: f_node.clone(),
                            instance: *i,
                        })
                        .next()
                })
                .collect()
        } else {
            self.cmds
                .iter()
                .flat_map(|(i, cmds)| {
                    cmds.into_iter()
                        .map(move |(idx, cmd)| (idx, (i, cmd)))
                        .filter(|(_, c)| op.interfere(&c.1.op))
                        .map(|(i, (id, _))| Interference {
                            node: id.clone(),
                            instance: *i,
                        })
                })
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::epaxos::testing::TestOp;

    macro_rules! ipc_request_response {
        ($listener:ident, $tx:ident, $req:expr) => {{
            let (req_tx, mut req_rx) = tokio::sync::oneshot::channel();
            $tx.send(NodeMessage::Ipc {
                req: ipc::Message {
                    req: $req,
                    resp: req_tx,
                },
                span: tracing::span!(tracing::Level::INFO, "Testing"),
            })
            .unwrap();

            $listener.try_poll();

            req_rx.try_recv().unwrap()
        }};
        ($listener:ident, $tx:ident, $req:expr, $expected:expr) => {{
            let resp = ipc_request_response!($listener, $tx, $req);

            assert_eq!($expected, resp);

            resp
        }};
    }

    macro_rules! msg_request_response {
        ($listener:ident, $tx:ident, $req:expr) => {{
            let (req_tx, mut req_rx) = tokio::sync::oneshot::channel();
            $tx.send(NodeMessage::External {
                req: $req,
                tx: req_tx,
            })
            .unwrap();

            $listener.try_poll();

            req_rx.try_recv().unwrap()
        }};
        ($listener:ident, $tx:ident, $req:expr, $expected:expr) => {{
            let resp = msg_request_response!($listener, $tx, $req);

            assert_eq!($expected, resp);

            resp
        }};
    }

    #[test]
    fn listener_ipc_preaccept() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut listener = NodeListener::<i32, TestOp, usize>::new(0, rx, tx.clone(), 0usize);

        ipc_request_response!(
            listener,
            tx,
            ipc::Request::PreAccept {
                op: TestOp::Read,
                node: 0,
                instance: 0,
                ballot: Ballot::initial(0, 0),
            },
            ipc::Response::PreAcceptOk(
                msgs::PreAccept {
                    op: TestOp::Read,
                    seq: 1,
                    deps: Dependencies::new(),
                    node: (0, 0),
                    ballot: Ballot::initial(0, 0),
                },
                msgs::PreAcceptOk {
                    op: TestOp::Read,
                    seq: 1,
                    instance: 0,
                    deps: Dependencies::new(),
                    node: 0,
                }
            )
        );

        assert!(!listener.cmds.is_empty());
        let entry = listener.cmds.get(&0).unwrap().get(&0);
        assert!(entry.is_some());
        let entry = entry.unwrap();

        assert_eq!(CmdState::PreAccepted, entry.state);
    }

    #[test]
    fn listener_ipc_preaccept_commit() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut listener = NodeListener::<i32, TestOp, usize>::new(0, rx, tx.clone(), 0usize);

        ipc_request_response!(
            listener,
            tx,
            ipc::Request::PreAccept {
                op: TestOp::Read,
                node: 0,
                instance: 0,
                ballot: Ballot::initial(0, 0),
            },
            ipc::Response::PreAcceptOk(
                msgs::PreAccept {
                    op: TestOp::Read,
                    seq: 1,
                    deps: Dependencies::new(),
                    node: (0, 0),
                    ballot: Ballot::initial(0, 0),
                },
                msgs::PreAcceptOk {
                    op: TestOp::Read,
                    seq: 1,
                    instance: 0,
                    deps: Dependencies::new(),
                    node: 0,
                }
            )
        );

        assert!(!listener.cmds.is_empty());
        let entry = listener.cmds.get(&0).unwrap().get(&0);
        assert!(entry.is_some());
        let entry = entry.unwrap();

        assert_eq!(CmdState::PreAccepted, entry.state);

        ipc_request_response!(
            listener,
            tx,
            ipc::Request::Commit {
                node: 0,
                instance: 0,
                ballot: Ballot::initial(0, 0),
            },
            ipc::Response::Committed(msgs::Commit {
                op: TestOp::Read,
                seq: 1,
                deps: Dependencies::new(),
                node: (0, 0),
                ballot: Ballot::initial(0, 0),
            })
        );
    }

    #[test]
    fn listener_ipc_preaccept_accepted_commit() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut listener = NodeListener::<i32, TestOp, usize>::new(0, rx, tx.clone(), 0usize);

        ipc_request_response!(
            listener,
            tx,
            ipc::Request::PreAccept {
                op: TestOp::Read,
                node: 0,
                instance: 0,
                ballot: Ballot::initial(0, 0),
            },
            ipc::Response::PreAcceptOk(
                msgs::PreAccept {
                    op: TestOp::Read,
                    seq: 1,
                    deps: Dependencies::new(),
                    node: (0, 0),
                    ballot: Ballot::initial(0, 0),
                },
                msgs::PreAcceptOk {
                    op: TestOp::Read,
                    seq: 1,
                    instance: 0,
                    deps: Dependencies::new(),
                    node: 0,
                }
            )
        );

        assert!(!listener.cmds.is_empty());
        let entry = listener.cmds.get(&0).unwrap().get(&0);
        assert!(entry.is_some());
        let entry = entry.unwrap();

        assert_eq!(CmdState::PreAccepted, entry.state);

        ipc_request_response!(
            listener,
            tx,
            ipc::Request::Accept {
                op: TestOp::Read,
                node: 0,
                instance: 0,
                n_deps: Dependencies::new(),
                n_seq: 1,
                ballot: Ballot::initial(0, 0),
            },
            ipc::Response::Accepted(msgs::Accept {
                op: TestOp::Read,
                seq: 1,
                deps: Dependencies::new(),
                node: (0, 0),
                ballot: Ballot::initial(0, 0)
            })
        );

        assert!(!listener.cmds.is_empty());
        let entry = listener.cmds.get(&0).unwrap().get(&0);
        assert!(entry.is_some());
        let entry = entry.unwrap();

        assert_eq!(CmdState::Accepted, entry.state);

        ipc_request_response!(
            listener,
            tx,
            ipc::Request::Commit {
                node: 0,
                instance: 0,
                ballot: Ballot::initial(0, 0),
            },
            ipc::Response::Committed(msgs::Commit {
                op: TestOp::Read,
                seq: 1,
                deps: Dependencies::new(),
                node: (0, 0),
                ballot: Ballot::initial(0, 0),
            })
        );
    }

    #[test]
    fn preaccept_prepare() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut listener = NodeListener::<i32, TestOp, usize>::new(0, rx, tx.clone(), 0usize);

        ipc_request_response!(
            listener,
            tx,
            ipc::Request::PreAccept {
                op: TestOp::Set(0),
                node: 0,
                instance: 0,
                ballot: Ballot::initial(0, 0)
            },
            ipc::Response::PreAcceptOk(
                msgs::PreAccept {
                    op: TestOp::Set(0),
                    seq: 1,
                    deps: Dependencies::new(),
                    node: (0, 0),
                    ballot: Ballot::initial(0, 0),
                },
                msgs::PreAcceptOk {
                    op: TestOp::Set(0),
                    seq: 1,
                    deps: Dependencies::new(),
                    node: 0,
                    instance: 0,
                }
            )
        );

        msg_request_response!(
            listener,
            tx,
            msgs::Request::Prepare(msgs::Prepare {
                node: 0,
                instance: 0,
                ballot: Ballot(0, 1, 1),
            }),
            msgs::Response::PrepareResp(msgs::PrepareResp::Ok(msgs::PrepareOk {
                op: TestOp::Set(0),
                state: OpState::PreAccepted,
                seq: 1,
                instance: 0,
                node: 0,
                deps: Dependencies::new(),
                ballot: Ballot(0, 1, 1),
            }))
        );

        // TODO
        // Is this the correct behaviour?
        ipc_request_response!(
            listener,
            tx,
            ipc::Request::Accept {
                op: TestOp::Set(0),
                node: 0,
                instance: 0,
                n_deps: Dependencies::new(),
                n_seq: 1,
                ballot: Ballot(0, 0, 0)
            },
            ipc::Response::Nack
        );
        ipc_request_response!(
            listener,
            tx,
            ipc::Request::Commit {
                node: 0,
                instance: 0,
                ballot: Ballot(0, 0, 0)
            },
            ipc::Response::Nack
        );
    }

    #[test]
    fn preaccept_prepare_preaccept() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut listener = NodeListener::<i32, TestOp, usize>::new(0, rx, tx.clone(), 0usize);

        ipc_request_response!(
            listener,
            tx,
            ipc::Request::PreAccept {
                op: TestOp::Read,
                node: 0,
                instance: 0,
                ballot: Ballot::initial(0, 0)
            }
        );

        msg_request_response!(
            listener,
            tx,
            msgs::Request::PreAccept(msgs::PreAccept {
                op: TestOp::Set(1),
                seq: 1,
                deps: Dependencies::new(),
                node: (1, 0),
                ballot: Ballot::initial(0, 1),
            }),
            msgs::Response::PreAcceptOk(msgs::PreAcceptOk {
                op: TestOp::Set(1),
                seq: 2,
                instance: 0,
                node: 1,
                deps: Dependencies::from_raw(vec![Interference {
                    node: 0,
                    instance: 0
                }]),
            })
        );

        msg_request_response!(
            listener,
            tx,
            msgs::Request::Prepare(msgs::Prepare {
                node: 0,
                instance: 0,
                ballot: Ballot(0, 1, 1),
            }),
            msgs::Response::PrepareResp(msgs::PrepareResp::Ok(msgs::PrepareOk {
                op: TestOp::Read,
                state: OpState::PreAccepted,
                seq: 1,
                node: 0,
                instance: 0,
                deps: Dependencies::new(),
                ballot: Ballot(0, 1, 1)
            }))
        );

        // TODO
        // Is this the correct behaviour?
        msg_request_response!(
            listener,
            tx,
            msgs::Request::PreAccept(msgs::PreAccept {
                op: TestOp::Read,
                seq: 1,
                deps: Dependencies::new(),
                node: (0, 0),
                ballot: Ballot(0, 1, 1),
            }),
            msgs::Response::PreAcceptOk(msgs::PreAcceptOk {
                op: TestOp::Read,
                seq: 1,
                instance: 0,
                node: 0,
                deps: Dependencies::new(),
            })
        );
    }
}
