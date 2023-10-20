use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    hash::Hash,
    sync::{atomic::AtomicU64, Arc},
};

use super::{
    client,
    failuredetector::Detector,
    ipc,
    msgs::{self},
    promises::{AllPromises, AttachedPromises, DetachedPromises, HighestContinuousPromise},
    Operation,
};

pub(super) enum InternalMessage<O, NodeId, V>
where
    NodeId: Ord,
{
    IPC(ipc::IPCRequest<O, NodeId, V>),
    Message(msgs::Message<O, NodeId>),
}

pub trait Broadcaster<O, NodeId>
where
    NodeId: Ord,
{
    fn send(&mut self, target: &NodeId, content: msgs::Message<O, NodeId>);

    fn send_nodes(
        &mut self,
        targets: &mut dyn Iterator<Item = NodeId>,
        content: msgs::Message<O, NodeId>,
    ) where
        NodeId: Clone,
        O: Clone,
    {
        for target in targets {
            self.send(&target, content.clone());
        }
    }
}

impl<O, NodeId> Broadcaster<O, NodeId> for HashMap<NodeId, VecDeque<msgs::Message<O, NodeId>>>
where
    NodeId: Clone + Eq + Hash + Ord,
    O: Clone,
{
    fn send(&mut self, target: &NodeId, content: msgs::Message<O, NodeId>) {
        let entry = self.get_mut(target).unwrap();
        entry.push_back(content);
    }
}

#[derive(Debug)]
#[must_use]
pub struct ResponseMsg<NodeId, M> {
    target: NodeId,
    pub msg: M,
}

#[derive(Debug)]
#[must_use]
pub struct AllNodeBroadcast<M> {
    pub msg: M,
}

#[derive(Debug)]
#[must_use]
pub struct QuorumBroadcast<NodeId, M> {
    quorum: BTreeSet<NodeId>,
    msg: M,
}

#[derive(Debug)]
#[must_use]
pub struct OutOfQuorumBroadcast<NodeId, M> {
    quorum: BTreeSet<NodeId>,
    msg: M,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CommandPhase<NodeId>
where
    NodeId: Ord,
{
    Start,
    Payload,
    Propose { responses: BTreeMap<NodeId, u64> },
    RecoverR,
    RecoverP,
    Commit,
    Execute,
}

impl<NodeId> Default for CommandPhase<NodeId>
where
    NodeId: Ord,
{
    fn default() -> Self {
        Self::Start
    }
}

impl<NodeId> PartialEq for CommandPhase<NodeId>
where
    NodeId: Eq + Ord,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Start, Self::Start) => true,
            (Self::Payload, Self::Payload) => true,
            (Self::Propose { responses: r1 }, Self::Propose { responses: r2 }) => r1 == r2,
            (Self::RecoverR, Self::RecoverR) => true,
            (Self::RecoverP, Self::RecoverP) => true,
            (Self::Commit, Self::Commit) => true,
            (Self::Execute, Self::Execute) => true,
            _ => false,
        }
    }
}

impl<NodeId> CommandPhase<NodeId>
where
    NodeId: Ord,
{
    /// Checks if a given State is considered `pending`
    pub fn pending(&self) -> bool {
        matches!(
            self,
            Self::Payload | Self::Propose { .. } | Self::RecoverP | Self::RecoverR
        )
    }
}

pub trait Receive<NodeId, M> {
    type Output;
    type Error;

    fn recv(&mut self, src: NodeId, payload: M) -> Result<Self::Output, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Ballot<NodeId>(u64, NodeId);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OpId<NodeId> {
    pub node: NodeId,
    pub counter: u64,
}

pub(super) struct Command<O, NodeId, V>
where
    NodeId: Ord,
{
    operation: O,
    timestamp: u64,
    phase: CommandPhase<NodeId>,
    /// The Fast Quoum used per Partition
    quroum: BTreeSet<NodeId>,
    initial: NodeId,
    /// The current Ballot
    bal: Ballot<NodeId>,
    /// The last accepted Ballot
    abal: Option<Ballot<NodeId>>,

    execute_channels: Vec<tokio::sync::oneshot::Sender<V>>,
    consensus_acks: BTreeMap<Ballot<NodeId>, usize>,
    rec_acks: BTreeMap<NodeId, msgs::RecAck<NodeId>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Promise<NodeId> {
    pub(super) node: NodeId,
    pub(super) timestamp: u64,
}

pub struct Replica<O, NodeId, V, T>
where
    NodeId: Ord,
{
    node: NodeId,
    tolerated_failures: usize,
    cluster: Vec<NodeId>,

    detector: Detector<NodeId>,

    clock: u64,
    detached: DetachedPromises<NodeId>,
    attached: AttachedPromises<NodeId>,
    promises: AllPromises<NodeId>,
    highest_continuous: HighestContinuousPromise<NodeId>,
    highest_acked: BTreeMap<NodeId, u64>,

    commands: HashMap<OpId<NodeId>, Command<O, NodeId, V>>,
    commands_pending_exec: BTreeMap<u64, Vec<OpId<NodeId>>>,

    state: T,
    counter: Arc<AtomicU64>,
    msg_rx: tokio::sync::mpsc::UnboundedReceiver<InternalMessage<O, NodeId, V>>,
    msg_tx: Option<tokio::sync::mpsc::UnboundedSender<InternalMessage<O, NodeId, V>>>,
}

impl<O, NodeId, V, T> Replica<O, NodeId, V, T>
where
    NodeId: Clone + Ord,
{
    pub(crate) fn new(
        id: NodeId,
        cluster: Vec<NodeId>,
        state: T,
        tolerated_failures: usize,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            node: id.clone(),
            tolerated_failures,
            cluster: cluster.clone(),

            detector: Detector::new(id.clone()),

            clock: 0,
            detached: DetachedPromises::new(id.clone()),
            attached: AttachedPromises::new(id),
            promises: AllPromises::new(),
            highest_continuous: HighestContinuousPromise::new(cluster),
            highest_acked: BTreeMap::new(),

            commands: HashMap::new(),
            commands_pending_exec: BTreeMap::new(),

            state,
            counter: Arc::new(AtomicU64::new(0)),
            msg_rx: rx,
            msg_tx: Some(tx),
        }
    }

    pub fn handle(&self) -> client::Handle<O, NodeId, T, V> {
        client::Handle {
            node: self.node.clone(),
            counter: self.counter.clone(),
            tx: self.msg_tx.clone().unwrap(),
            _marker: core::marker::PhantomData {},
        }
    }
}

impl<O, NodeId, V, T> Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
{
    fn bump(&mut self, timestamp: u64) {
        let t = core::cmp::max(timestamp, self.clock);

        self.detached.add(self.clock + 1..=t);

        self.clock = t;
    }

    fn proposal(&mut self, op_id: OpId<NodeId>, timestamp: u64) -> u64 {
        let t = core::cmp::max(timestamp, self.clock + 1);

        self.detached.add(self.clock + 1..=(t - 1));
        self.attached.attach(op_id, t);

        self.clock = t;

        return t;
    }
}

impl<O, NodeId, V, T> Replica<O, NodeId, V, T>
where
    NodeId: Clone + Ord,
{
    pub fn get_promises(&mut self) -> Option<msgs::Promises<NodeId>> {
        let attached = if self.highest_acked.len() == self.cluster.len() {
            self.attached.filtered(&self.highest_acked)
        } else {
            self.attached.clone()
        };

        let detached = if self.highest_acked.len() == self.cluster.len() {
            self.detached.filtered(&self.highest_acked)
        } else {
            self.detached.clone()
        };

        if detached.is_empty() && attached.len() == 0 {
            return None;
        }

        Some(msgs::Promises { detached, attached })
    }
}

#[derive(Debug)]
pub enum ReceiveIPCError {}

impl<O, NodeId, T> Replica<O, NodeId, O::Result, T>
where
    NodeId: Eq + Ord + Clone + Hash,
    O: Operation<T> + Clone,
    O::Result: Clone,
{
    #[tracing::instrument(skip(self, req, broadcaster))]
    pub fn recv_ipc(
        &mut self,
        req: ipc::IPCRequest<O, NodeId, O::Result>,
        broadcaster: &mut dyn Broadcaster<O, NodeId>,
    ) -> Result<(), ReceiveIPCError> {
        tracing::trace!("Handling IPC");

        match req {
            ipc::IPCRequest::Submit(payload) => {
                let t = self.clock + 1;

                self.commands.insert(
                    payload.id.clone(),
                    Command {
                        operation: payload.operation.clone(),
                        timestamp: 0,
                        phase: CommandPhase::Start,
                        quroum: payload.quorum.clone(),
                        initial: self.node.clone(),
                        bal: Ballot(0, self.node.clone()),
                        abal: None,

                        execute_channels: payload.listeners,
                        consensus_acks: BTreeMap::new(),
                        rec_acks: BTreeMap::new(),
                    },
                );

                let propose = QuorumBroadcast {
                    quorum: payload.quorum.clone(),
                    msg: msgs::Propose {
                        id: payload.id.clone(),
                        operation: payload.operation.clone(),
                        quroum: payload.quorum.clone(),
                        timestamp: t,
                    },
                };
                let payload = OutOfQuorumBroadcast {
                    quorum: payload.quorum.clone(),
                    msg: msgs::Payload {
                        id: payload.id,
                        operation: payload.operation,
                        quroum: payload.quorum,
                    },
                };

                broadcaster.send_nodes(
                    &mut self
                        .cluster
                        .iter()
                        .filter(|cn| propose.quorum.contains(cn))
                        .cloned(),
                    msgs::Message {
                        src: self.node.clone(),
                        msg: propose.msg.into(),
                    },
                );

                broadcaster.send_nodes(
                    &mut self
                        .cluster
                        .iter()
                        .filter(|cn| !payload.quorum.contains(cn))
                        .cloned(),
                    msgs::Message {
                        src: self.node.clone(),
                        msg: payload.msg.into(),
                    },
                );

                Ok(())
            }
            ipc::IPCRequest::TryExecute(_execute) => {
                let mut nodes: Vec<_> = self.cluster.iter().cloned().collect();
                nodes.sort();

                self.try_execute(&nodes);

                Ok(())
            }
            ipc::IPCRequest::LivenessCheck(_check) => {
                // TODO
                // We should perform a liveness check on the system and recover any commands that
                // we might think, that their initial coordinator has crashed

                todo!()
            }
            ipc::IPCRequest::Promises => {
                let msg = match self.get_promises() {
                    Some(m) => m,
                    None => return Ok(()),
                };

                broadcaster.send_nodes(
                    &mut self.cluster.iter().cloned(),
                    msgs::Message {
                        src: self.node.clone(),
                        msg: msgs::MessageContent::Promises(msg),
                    },
                );

                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub enum ReceiveMessageError {
    Other(&'static str),
}

impl<O, NodeId, V, T> Replica<O, NodeId, V, T>
where
    NodeId: Eq + Ord + Clone + Hash,
    O: Clone,
    V: Clone,
{
    #[tracing::instrument(skip(self, msg, broadcaster))]
    pub fn recv_msg(
        &mut self,
        msg: msgs::Message<O, NodeId>,
        broadcaster: &mut dyn Broadcaster<O, NodeId>,
    ) -> Result<(), ReceiveMessageError> {
        tracing::trace!("Handling Message");

        match msg.msg {
            msgs::MessageContent::Propose(propose) => {
                match self
                    .recv(msg.src, propose)
                    .map_err(|_e| ReceiveMessageError::Other("Propose Message"))?
                {
                    Some((ack, bump)) => {
                        broadcaster.send(
                            &ack.target,
                            msgs::Message {
                                src: self.node.clone(),
                                msg: ack.msg.into(),
                            },
                        );

                        broadcaster.send_nodes(
                            &mut self.cluster.iter().cloned(),
                            msgs::Message {
                                src: self.node.clone(),
                                msg: bump.msg.into(),
                            },
                        );
                    }
                    None => {}
                };
            }
            msgs::MessageContent::Payload(payload) => {
                // We dont send any responses to payload messages so there is no nothing else to do
                // here
                self.recv(msg.src, payload)
                    .map_err(|_e| ReceiveMessageError::Other("Payload"))?;
            }
            msgs::MessageContent::ProposeAck(pack) => {
                match self
                    .recv(msg.src, pack)
                    .map_err(|_e| ReceiveMessageError::Other("ProposeAck"))?
                {
                    Some(propose_resp) => match propose_resp {
                        ProposeResult::Wait => {}
                        ProposeResult::Commit(commit) => {
                            broadcaster.send_nodes(
                                &mut self.cluster.iter().cloned(),
                                msgs::Message {
                                    src: self.node.clone(),
                                    msg: msgs::MessageContent::Commit(commit.msg),
                                },
                            );
                        }
                        ProposeResult::Consensus(consensus) => {
                            broadcaster.send_nodes(
                                &mut self.cluster.iter().cloned(),
                                msgs::Message {
                                    src: self.node.clone(),
                                    msg: consensus.msg.into(),
                                },
                            );
                        }
                    },
                    None => {}
                };
            }
            msgs::MessageContent::Bump(bump) => {
                return Ok(());

                // We dont send any responses to bump messages so there is no nothing else to do
                // here
                self.recv(msg.src, bump)
                    .map_err(|_e| ReceiveMessageError::Other("Bump"))?;
            }
            msgs::MessageContent::Commit(commit) => {
                // We dont send any responses to commit messages so there is no nothing else to do
                // here
                self.recv(msg.src, commit)
                    .map_err(|_e| ReceiveMessageError::Other("Commit"))?;
            }
            msgs::MessageContent::Consensus(consensus) => {
                let consensus_resp = self
                    .recv(msg.src, consensus)
                    .map_err(|_e| ReceiveMessageError::Other("Consensus"))?;

                match consensus_resp {
                    ConsensusResponse::Empty => {
                        // We dont have anything to do with this
                    }
                    ConsensusResponse::Ack(ack) => {
                        broadcaster.send(
                            &ack.target,
                            msgs::Message {
                                src: self.node.clone(),
                                msg: ack.msg.into(),
                            },
                        );
                    }
                    ConsensusResponse::RecNack(nack) => {
                        broadcaster.send(
                            &nack.target,
                            msgs::Message {
                                src: self.node.clone(),
                                msg: nack.msg.into(),
                            },
                        );
                    }
                };
            }
            msgs::MessageContent::ConsensusAck(cack) => {
                match self
                    .recv(msg.src, cack)
                    .map_err(|_e| ReceiveMessageError::Other("ConsensusAck"))?
                {
                    Some(commit) => {
                        broadcaster.send(
                            &commit.target,
                            msgs::Message {
                                src: self.node.clone(),
                                msg: commit.msg.into(),
                            },
                        );
                    }
                    None => {
                        // We did not make any decisions based on the ConsensusAck yet, so we dont
                        // send any messages and just "wait"
                    }
                };
            }
            msgs::MessageContent::Promises(promises) => {
                tracing::debug!("Got Promises Message");

                let (_commit_requests, promise_ok) = self
                    .recv(msg.src, promises)
                    .map_err(|_e| ReceiveMessageError::Other("Promises"))?;

                /*
                 * TODO
                 * This is only needed for multiple partitions, which is currently not supported
                for entry in commit_requests.msg {
                    broadcaster.send_nodes(
                        &mut cluster.iter().cloned(),
                        msgs::Message {
                            src: self.node.clone(),
                            msg: entry.into(),
                        },
                    );
                }
                */
                if let Some(promise_ok) = promise_ok {
                    broadcaster.send(
                        &promise_ok.target,
                        msgs::Message {
                            src: self.node.clone(),
                            msg: promise_ok.msg.into(),
                        },
                    );
                }
            }
            msgs::MessageContent::PromisesOk(promisesok) => {
                let acked = self.highest_acked.entry(msg.src).or_insert(0);
                *acked = core::cmp::max(*acked, promisesok.highest);
            }
            msgs::MessageContent::CommitRequest(creq) => {
                let (payload, commit) = self
                    .recv(msg.src, creq)
                    .map_err(|_e| ReceiveMessageError::Other("CommitRequest"))?;

                broadcaster.send(
                    &payload.target,
                    msgs::Message {
                        src: self.node.clone(),
                        msg: payload.msg.into(),
                    },
                );

                broadcaster.send(
                    &commit.target,
                    msgs::Message {
                        src: self.node.clone(),
                        msg: commit.msg.into(),
                    },
                );
            }
            msgs::MessageContent::Stable(stable) => {
                // self.recv(msg.src, stable);
                // TODO
                // How do we handle this
                let _ = stable;
                todo!("Handle Stable message")
            }
            msgs::MessageContent::Rec(rec) => {
                let tmp = self
                    .recv(msg.src, rec)
                    .map_err(|_e| ReceiveMessageError::Other("Rec"))?;

                match tmp {
                    RecResponse::Ack(ack) => {
                        broadcaster.send(
                            &ack.target,
                            msgs::Message {
                                src: self.node.clone(),
                                msg: ack.msg.into(),
                            },
                        );
                    }
                    RecResponse::Nack(nack) => {
                        broadcaster.send(
                            &nack.target,
                            msgs::Message {
                                src: self.node.clone(),
                                msg: nack.msg.into(),
                            },
                        );
                    }
                };
            }
            msgs::MessageContent::RecAck(recack) => {
                let consensus = self
                    .recv(msg.src, recack)
                    .map_err(|_e| ReceiveMessageError::Other("RecAck"))?;

                broadcaster.send_nodes(
                    &mut self.cluster.iter().cloned(),
                    msgs::Message {
                        src: self.node.clone(),
                        msg: consensus.msg.into(),
                    },
                );
            }
            msgs::MessageContent::RecNAck(recnack) => {
                // We dont send any responses to RecNack messages so there is no nothing else to do
                // here
                self.recv(msg.src, recnack)
                    .map_err(|_e| ReceiveMessageError::Other("RecNAck"))?;
            }
        };

        Ok(())
    }
}

impl<O, NodeId, T> Replica<O, NodeId, O::Result, T>
where
    NodeId: PartialEq + Ord + Clone + Hash + Eq,
    O: Operation<T> + Clone,
    O::Result: Clone,
{
    #[tracing::instrument(skip(self, nodes))]
    pub fn try_execute(&mut self, nodes: &[NodeId]) {
        tracing::trace!("Attemping Execute");

        let h = self.highest_continuous.sorted();
        tracing::trace!("H {:?}", h);

        let limit = match h.get(nodes.len() / 2).copied() {
            Some(l) => l,
            None => return,
        };

        let lower_bound = self
            .commands_pending_exec
            .first_entry()
            .map(|k| *k.key())
            .unwrap_or(limit);
        for ts in lower_bound..=limit {
            // Remove the commands from the pending execution stage
            let elems = match self.commands_pending_exec.remove(&ts) {
                Some(e) => e.into_iter(),
                None => continue,
            };

            for id in elems {
                let cmd = self.commands.get_mut(&id).expect("We got the IDs by iterating over all the commands so every ID is also contained in the commands");

                // Execute command
                let cmd_res = cmd.operation.apply(&mut self.state);

                for channel in cmd.execute_channels.drain(..) {
                    // We can ignore the result here, because this is just to notify anyone waiting for
                    // this command to be executed and this is just best effort based.
                    //
                    // There is nothing we can do to recover here anyway
                    let _ = channel.send(cmd_res.clone());
                }

                cmd.phase = CommandPhase::Execute;
            }
        }
    }

    pub fn liveness_check(&mut self) {
        let pending: Vec<OpId<NodeId>> = self
            .commands
            .iter()
            .filter_map(|(id, cmd)| {
                if cmd.phase.pending() {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();

        for id in pending {
            let cmd = match self.commands.get(&id) {
                Some(c) => c,
                None => continue,
            };

            // TODO
            // Send somehow
            let _msg = msgs::Payload {
                id: id.clone(),
                operation: cmd.operation.clone(),
                quroum: cmd.quroum.clone(),
            };

            // Algorithm 4.4 line 91.
            // TODO
            // This leader check is only needed for liveness during recovery as otherwise if
            // too many processes try performing recovery at the same time, they wont be able to
            // make progress as they will just increase their ballot numbers and "interrupting" the
            // recovery of the other ones
            if self.detector.is_leader(&self.node) && (cmd.bal.0 == 0 || &cmd.bal.1 != &self.node) {
                self.recover(id);
            }
        }
    }

    fn recover(&mut self, _id: OpId<NodeId>) {
        todo!("Recovery for Command")
    }
}

#[derive(Debug)]
pub enum ProcessError {
    DequeuingMessage,
    RecvIPC(ReceiveIPCError),
    RecvMsg(ReceiveMessageError),
}

impl<O, NodeId, T> Replica<O, NodeId, O::Result, T>
where
    NodeId: Eq + Ord + Clone + Hash,
    O: Operation<T> + Clone,
    O::Result: Clone,
{
    pub async fn process(
        &mut self,
        broadcaster: &mut dyn Broadcaster<O, NodeId>,
    ) -> Result<(), ProcessError> {
        let msg = match self.msg_rx.recv().await {
            Some(m) => m,
            None => return Err(ProcessError::DequeuingMessage),
        };

        match msg {
            InternalMessage::IPC(ipc_msg) => self
                .recv_ipc(ipc_msg, broadcaster)
                .map_err(ProcessError::RecvIPC),
            InternalMessage::Message(message) => self
                .recv_msg(message, broadcaster)
                .map_err(ProcessError::RecvMsg),
        }
    }
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::Payload<O, NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + PartialOrd + Ord,
{
    type Output = ();
    type Error = ();

    fn recv(&mut self, src: NodeId, payload: msgs::Payload<O, NodeId>) -> Result<Self::Output, ()> {
        match self.commands.get_mut(&payload.id) {
            Some(cmd) if matches!(cmd.phase, CommandPhase::Start) => {
                cmd.operation = payload.operation;
                cmd.quroum = payload.quroum;
                cmd.phase = CommandPhase::Payload;
            }
            Some(_) => {}
            None => {
                self.commands.insert(
                    payload.id,
                    Command {
                        operation: payload.operation,
                        timestamp: 0,
                        phase: CommandPhase::Payload,
                        quroum: payload.quroum,
                        initial: src.clone(), // TODO Is this correct?
                        bal: Ballot(0, src.clone()),
                        abal: None,

                        execute_channels: Vec::new(),
                        consensus_acks: BTreeMap::new(),
                        rec_acks: BTreeMap::new(),
                    },
                );
            }
        };

        Ok(())
    }
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::Propose<O, NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
{
    type Output = Option<(
        ResponseMsg<NodeId, msgs::ProposeAck<NodeId>>,
        AllNodeBroadcast<msgs::Bump<NodeId>>,
    )>;
    type Error = ();

    fn recv(&mut self, src: NodeId, payload: msgs::Propose<O, NodeId>) -> Result<Self::Output, ()> {
        let timestamp = match self.commands.get_mut(&payload.id) {
            Some(cmd) if matches!(cmd.phase, CommandPhase::Start) => {
                cmd.operation = payload.operation;
                cmd.quroum = payload.quroum;
                cmd.phase = CommandPhase::Propose {
                    responses: BTreeMap::new(),
                };
                let timestamp = self.proposal(payload.id.clone(), payload.timestamp);

                // TODO
                // This is kind of an ugly hack that we have to do as we otherwise still
                // hold `cmd` and try to call proposal which needs a mutable reference to the
                // entire self
                self.commands.get_mut(&payload.id).unwrap().timestamp = timestamp;

                timestamp
            }
            Some(_) => {
                return Ok(None);
            }
            None => {
                let timestamp = self.proposal(payload.id.clone(), payload.timestamp);
                self.commands.insert(
                    payload.id.clone(),
                    Command {
                        operation: payload.operation,
                        timestamp,
                        phase: CommandPhase::Propose {
                            responses: BTreeMap::new(),
                        },
                        quroum: payload.quroum,
                        initial: src.clone(), // TODO Is this correct?
                        bal: Ballot(0, src.clone()),
                        abal: None,

                        execute_channels: Vec::new(),
                        consensus_acks: BTreeMap::new(),
                        rec_acks: BTreeMap::new(),
                    },
                );

                timestamp
            }
        };

        Ok(Some((
            ResponseMsg {
                target: src,
                msg: msgs::ProposeAck {
                    id: payload.id.clone(),
                    timestamp,
                },
            },
            AllNodeBroadcast {
                msg: msgs::Bump {
                    id: payload.id,
                    timestamp,
                },
            },
        )))
    }
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::Bump<NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
{
    type Output = ();
    type Error = ();

    fn recv(&mut self, _: NodeId, payload: msgs::Bump<NodeId>) -> Result<Self::Output, ()> {
        match self.commands.get_mut(&payload.id) {
            Some(cmd) if matches!(cmd.phase, CommandPhase::Propose { .. }) => {}
            _ => return Ok(()),
        };

        self.bump(payload.timestamp);

        Ok(())
    }
}

#[derive(Debug)]
pub enum ProposeResult<NodeId> {
    Wait,
    Commit(AllNodeBroadcast<msgs::Commit<NodeId>>),
    Consensus(AllNodeBroadcast<msgs::Consensus<NodeId>>),
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::ProposeAck<NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
{
    type Output = Option<ProposeResult<NodeId>>;
    type Error = ();

    fn recv(&mut self, src: NodeId, payload: msgs::ProposeAck<NodeId>) -> Result<Self::Output, ()> {
        let (quorum, responses) = match self.commands.get_mut(&payload.id) {
            Some(cmd) => match &mut cmd.phase {
                CommandPhase::Propose { responses } => (&cmd.quroum, responses),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        responses.insert(src, payload.timestamp);

        if responses.len() < quorum.len() {
            return Ok(Some(ProposeResult::Wait));
        }

        let t = responses.values().copied().max().unwrap_or(0);

        if responses.values().copied().filter(|v| *v == t).count() >= self.tolerated_failures {
            Ok(Some(ProposeResult::Commit(AllNodeBroadcast {
                msg: msgs::Commit {
                    id: payload.id,
                    timestamp: t,
                },
            })))
        } else {
            Ok(Some(ProposeResult::Consensus(AllNodeBroadcast {
                msg: msgs::Consensus {
                    id: payload.id,
                    timestamp: t,
                    ballot: Ballot(0, self.node.clone()),
                },
            })))
        }
    }
}

#[derive(Debug)]
pub enum ConsensusResponse<NodeId> {
    Empty,
    RecNack(ResponseMsg<NodeId, msgs::RecNAck<NodeId>>),
    Ack(ResponseMsg<NodeId, msgs::ConsensusAck<NodeId>>),
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::Consensus<NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
{
    type Output = ConsensusResponse<NodeId>;
    type Error = ();

    fn recv(&mut self, src: NodeId, payload: msgs::Consensus<NodeId>) -> Result<Self::Output, ()> {
        let cmd = match self.commands.get_mut(&payload.id) {
            Some(cmd) if &cmd.bal <= &payload.ballot => cmd,
            Some(cmd) => {
                return Ok(ConsensusResponse::RecNack(ResponseMsg {
                    target: src,
                    msg: msgs::RecNAck {
                        id: payload.id,
                        ballot: cmd.bal.clone(),
                    },
                }))
            }
            None => return Ok(ConsensusResponse::Empty),
        };

        cmd.timestamp = payload.timestamp;
        cmd.bal = payload.ballot.clone();
        cmd.abal = Some(payload.ballot.clone());

        self.bump(payload.timestamp);

        Ok(ConsensusResponse::Ack(ResponseMsg {
            target: src,
            msg: msgs::ConsensusAck {
                id: payload.id,
                ballot: payload.ballot,
            },
        }))
    }
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::ConsensusAck<NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
{
    type Output = Option<ResponseMsg<NodeId, msgs::Commit<NodeId>>>;
    type Error = ();

    fn recv(
        &mut self,
        src: NodeId,
        payload: msgs::ConsensusAck<NodeId>,
    ) -> Result<Self::Output, ()> {
        let cmd = match self.commands.get_mut(&payload.id) {
            Some(cmd) if cmd.bal == payload.ballot => cmd,
            _ => return Ok(None),
        };

        // Add the response to the number of ConsensusAcks we received for this operation

        let prev_count = cmd.consensus_acks.entry(payload.ballot).or_insert(0);
        *prev_count += 1;

        let count = *prev_count;

        if count >= self.tolerated_failures + 1 {
            Ok(Some(ResponseMsg {
                target: src,
                msg: msgs::Commit {
                    id: payload.id,
                    timestamp: cmd.timestamp,
                },
            }))
        } else {
            Ok(None)
        }
    }
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::Commit<NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
{
    type Output = ();
    type Error = ();

    fn recv(&mut self, _: NodeId, payload: msgs::Commit<NodeId>) -> Result<Self::Output, ()> {
        let cmd = match self.commands.get_mut(&payload.id) {
            Some(cmd) if cmd.phase.pending() => cmd,
            _ => {
                tracing::warn!("Command is not pending");
                return Ok(());
            }
        };

        // TODO
        // This only works for state machines with 1 partition

        cmd.timestamp = payload.timestamp;
        cmd.phase = CommandPhase::Commit;

        let timestamp_ops = self.commands_pending_exec.entry(cmd.timestamp).or_default();
        timestamp_ops.push(payload.id.clone());
        timestamp_ops.sort_unstable();

        self.bump(payload.timestamp);

        Ok(())
    }
}

#[derive(Debug)]
pub enum RecResponse<NodeId>
where
    NodeId: Ord,
{
    Ack(ResponseMsg<NodeId, msgs::RecAck<NodeId>>),
    Nack(ResponseMsg<NodeId, msgs::RecNAck<NodeId>>),
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::Rec<NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
{
    type Output = RecResponse<NodeId>;
    type Error = ();

    fn recv(&mut self, src: NodeId, payload: msgs::Rec<NodeId>) -> Result<Self::Output, ()> {
        let mut cmd = match self.commands.get_mut(&payload.id) {
            Some(c) if c.bal > payload.ballot => {
                return Ok(RecResponse::Nack(ResponseMsg {
                    target: src,
                    msg: msgs::RecNAck {
                        id: payload.id,
                        ballot: c.bal.clone(),
                    },
                }))
            }
            Some(c) => c,
            None => return Err(()),
        };

        if !cmd.phase.pending() {
            return Err(());
        }
        if cmd.bal == payload.ballot {
            return Err(());
        }

        if cmd.bal.0 == 0 {
            if matches!(cmd.phase, CommandPhase::Payload) {
                let timestamp = self.proposal(payload.id.clone(), 0);
                cmd = self.commands.get_mut(&payload.id).unwrap();

                cmd.timestamp = timestamp;
                cmd.phase = CommandPhase::RecoverR;
            } else if matches!(cmd.phase, CommandPhase::Propose { .. }) {
                cmd.phase = CommandPhase::RecoverP;
            }
        }

        cmd.bal = payload.ballot.clone();

        Ok(RecResponse::Ack(ResponseMsg {
            target: src,
            msg: msgs::RecAck {
                id: payload.id,
                timestamp: cmd.timestamp,
                phase: cmd.phase.clone(),
                abal: cmd.abal.clone(),
                ballot: payload.ballot,
            },
        }))
    }
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::RecAck<NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
{
    type Output = AllNodeBroadcast<msgs::Consensus<NodeId>>;
    type Error = ();

    fn recv(&mut self, src: NodeId, payload: msgs::RecAck<NodeId>) -> Result<Self::Output, ()> {
        let cmd = match self.commands.get_mut(&payload.id) {
            Some(c) => c,
            None => return Err(()),
        };

        if cmd.bal != payload.ballot {
            return Err(());
        }

        let id = payload.id.clone();
        let ballot = payload.ballot.clone();

        cmd.rec_acks.insert(src, payload);
        let r = self.cluster.len();
        // Check for number of responses
        if cmd.rec_acks.len() < r.saturating_sub(self.tolerated_failures) {
            return Err(());
        }

        let msg = if let Some(resp) = cmd
            .rec_acks
            .values()
            .filter(|r| r.abal.is_some())
            .max_by_key(|r| r.abal.as_ref().unwrap())
        {
            msgs::Consensus {
                id,
                timestamp: resp.timestamp,
                ballot,
            }
        } else {
            let i: BTreeMap<_, _> = cmd
                .rec_acks
                .iter()
                .filter(|(key, _)| cmd.quroum.contains(key))
                .collect();

            let s = i.contains_key(&cmd.initial)
                || cmd
                    .rec_acks
                    .values()
                    .any(|resp| matches!(resp.phase, CommandPhase::RecoverR));

            let t = if s {
                cmd.rec_acks
                    .iter()
                    .map(|resp| resp.1.timestamp)
                    .max()
                    .unwrap()
            } else {
                i.values().map(|resp| resp.timestamp).max().unwrap()
            };

            msgs::Consensus {
                id,
                timestamp: t,
                ballot,
            }
        };

        // Send the message to all nodes in partition
        Ok(AllNodeBroadcast { msg })
    }
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::RecNAck<NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Ord,
{
    type Output = ();
    type Error = ();

    fn recv(&mut self, _: NodeId, payload: msgs::RecNAck<NodeId>) -> Result<Self::Output, ()> {
        let cmd = match self.commands.get_mut(&payload.id) {
            Some(c) => c,
            None => return Err(()),
        };

        if cmd.bal < payload.ballot {
            return Err(());
        }
        if self.detector.is_leader(&self.node) {
            return Err(());
        }

        cmd.bal = payload.ballot;

        todo!("Recover")
    }
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::Promises<NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
{
    type Output = (
        AllNodeBroadcast<Vec<msgs::CommitRequest<NodeId>>>,
        Option<ResponseMsg<NodeId, msgs::PromisesOk>>,
    );
    type Error = ();

    fn recv(&mut self, src: NodeId, payload: msgs::Promises<NodeId>) -> Result<Self::Output, ()> {
        let mut requests = Vec::new();

        let c =
            payload
                .attached
                .iter()
                .filter_map(|(timestamp, opid)| match self.commands.get(opid) {
                    Some(cmd)
                        if matches!(cmd.phase, CommandPhase::Commit | CommandPhase::Execute) =>
                    {
                        Some((payload.attached.node().clone(), timestamp))
                    }
                    Some(_) => {
                        requests.push(msgs::CommitRequest { id: opid.clone() });
                        None
                    }
                    _ => None,
                });

        self.promises.union_detached(payload.detached);
        self.promises.extend(c);

        // Update the highest continuous elements
        self.highest_continuous = self.promises.highest_contiguous();

        let reply_msg = self
            .highest_continuous
            .min()
            .filter(|_| self.highest_continuous.sorted().len() == self.cluster.len())
            .map(|highest| msgs::PromisesOk { highest });

        Ok((
            AllNodeBroadcast { msg: requests },
            reply_msg.map(|msg| ResponseMsg { target: src, msg }),
        ))
    }
}

impl<O, NodeId, V, T> Receive<NodeId, msgs::CommitRequest<NodeId>> for Replica<O, NodeId, V, T>
where
    NodeId: Eq + Hash + Clone + Ord,
    O: Clone,
    V: Clone,
{
    type Output = (
        ResponseMsg<NodeId, msgs::Payload<O, NodeId>>,
        ResponseMsg<NodeId, msgs::Commit<NodeId>>,
    );
    type Error = ();

    fn recv(
        &mut self,
        src: NodeId,
        payload: msgs::CommitRequest<NodeId>,
    ) -> Result<Self::Output, ()> {
        let cmd = match self.commands.get(&payload.id) {
            Some(c) => c,
            None => return Err(()),
        };

        if !matches!(cmd.phase, CommandPhase::Commit | CommandPhase::Execute) {
            return Err(());
        }

        Ok((
            ResponseMsg {
                target: src.clone(),
                msg: msgs::Payload {
                    id: payload.id.clone(),
                    operation: cmd.operation.clone(),
                    quroum: cmd.quroum.clone(),
                },
            },
            ResponseMsg {
                target: src,
                msg: msgs::Commit {
                    id: payload.id.clone(),
                    timestamp: cmd.timestamp,
                },
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::tempo::promises::PromiseValue;

    use super::*;

    #[test]
    fn replica_bump() {
        let mut replica = Replica::<(), _, (), _>::new(0, vec![0, 1, 2], (), 1);

        assert_eq!(0, replica.clock);

        replica.bump(0);
        assert_eq!(0, replica.clock);

        let mut detached_iter = replica.detached.iter();
        assert_eq!(None, detached_iter.next(),);
        drop(detached_iter);

        replica.bump(5);

        assert_eq!(5, replica.clock);

        let mut detached_iter = replica.detached.iter();
        assert_eq!(
            &PromiseValue::Ranged { start: 1, end: 5 },
            detached_iter.next().unwrap()
        );
    }

    #[test]
    fn receive_bump() {
        let mut replica = Replica::<(), _, (), _>::new(0, vec![0, 1, 2], (), 1);
        assert_eq!(0, replica.clock);
        // assert!(replica.attached.is_empty());
        // assert!(replica.detached.is_empty());

        let msg = msgs::Bump {
            id: OpId {
                node: 1,
                counter: 0,
            },
            timestamp: 2,
        };

        replica.recv(1, msg).unwrap();

        // Message was not proposed before so nothing should change
        assert_eq!(0, replica.clock);
        // assert!(replica.attached.is_empty());
        // assert!(replica.detached.is_empty());
    }

    #[test]
    fn replica_proposal() {
        let mut replica = Replica::<(), _, (), _>::new(0, vec![0, 1, 2], (), 1);
        assert_eq!(0, replica.clock);

        let ret_t = replica.proposal(
            OpId {
                node: 0,
                counter: 0,
            },
            2,
        );
        assert_eq!(2, ret_t);
        assert_eq!(2, replica.clock);

        let mut detached_iter = replica.detached.iter();
        assert_eq!(
            &PromiseValue::Single { timestamp: 1 },
            detached_iter.next().unwrap()
        );
        /*
        assert_eq!(
            [Promise {
                node: 0,
                timestamp: 1
            }]
            .into_iter()
            .collect::<BTreeSet<_>>(),
            replica.detached
        );
        */
        /*
        assert_eq!(
            [(
                OpId {
                    node: 0,
                    counter: 0
                },
                Promise {
                    node: 0,
                    timestamp: 2,
                }
            )]
            .into_iter()
            .collect::<BTreeSet<_>>(),
            replica.attached
        );
        */
    }

    #[test]
    fn receive_proposal() {
        let mut replica = Replica::<(), _, (), _>::new(0, vec![0, 1, 2], (), 1);

        let msg = msgs::Propose {
            id: OpId {
                node: 1,
                counter: 0,
            },
            operation: (),
            quroum: BTreeSet::new(),
            timestamp: 2,
        };

        let (propose_msg, bump_msg) = replica.recv(1, msg).unwrap().unwrap();

        assert_eq!(2, replica.clock);

        let stored_cmd = replica
            .commands
            .get(&OpId {
                node: 1,
                counter: 0,
            })
            .unwrap();
        assert_eq!(stored_cmd.timestamp, 2);
        assert_eq!(stored_cmd.operation, ());
        assert_eq!(
            stored_cmd.phase,
            CommandPhase::Propose {
                responses: BTreeMap::new()
            }
        );

        // Test that we should also send the corresponding messages out to the rest of the cluster
        assert_eq!(
            msgs::ProposeAck {
                id: OpId {
                    node: 1,
                    counter: 0
                },
                timestamp: 2,
            },
            propose_msg.msg
        );
        assert_eq!(
            msgs::Bump {
                id: OpId {
                    node: 1,
                    counter: 0
                },
                timestamp: 2,
            },
            bump_msg.msg
        );
    }

    #[test]
    fn receive_payload() {
        let mut replica = Replica::<(), _, (), _>::new(0, vec![0, 1, 2], (), 1);

        assert_eq!(0, replica.clock);
        // assert!(replica.attached.is_empty());
        // assert!(replica.detached.is_empty());
        assert!(replica.commands.is_empty());

        let msg = msgs::Payload {
            id: OpId {
                node: 1,
                counter: 0,
            },
            operation: (),
            quroum: BTreeSet::new(),
        };

        replica.recv(1, msg).unwrap();

        let stored_cmd = replica
            .commands
            .get(&OpId {
                node: 1,
                counter: 0,
            })
            .unwrap();

        assert_eq!(CommandPhase::Payload, stored_cmd.phase);
        assert_eq!((), stored_cmd.operation);
        assert_eq!(BTreeSet::new(), stored_cmd.quroum);
    }

    #[test]
    fn recv_propose_proposeack_fastpath() {
        let mut replica = Replica::<(), _, (), _>::new(0, vec![0, 1, 2], (), 1);

        let (ack, _bump) = replica
            .recv(
                0,
                msgs::Propose {
                    id: OpId {
                        node: 0,
                        counter: 0,
                    },
                    operation: (),
                    quroum: [0].into_iter().collect(),
                    timestamp: 1,
                },
            )
            .unwrap()
            .unwrap();

        let result = replica.recv(0, ack.msg).unwrap().unwrap();

        let commit_msg = match result {
            ProposeResult::Commit(msg) => msg,
            other => {
                panic!("Expected a Commit message but got {:?}", other);
            }
        };

        let _ = dbg!(commit_msg);
    }

    #[test]
    #[ignore = "Not sure what the correct implementation for this would be"]
    fn receive_consensus_for_unknown_command() {
        let mut replica = Replica::<(), _, (), _>::new(0, vec![0, 1, 2], (), 1);

        let msg = msgs::Consensus {
            id: OpId {
                node: 1,
                counter: 0,
            },
            timestamp: 3,
            ballot: Ballot(1, 1),
        };

        let _ack_msg = match replica.recv(1, msg).unwrap() {
            ConsensusResponse::Ack(a) => a,
            other => panic!("Expected Ack, got {:?}", other),
        };

        todo!()
    }

    #[test]
    fn receive_consensus() {
        let mut replica = Replica::<(), _, (), _>::new(0, vec![0, 1, 2], (), 1);
        replica.tolerated_failures = 0;

        let _ = replica
            .recv(
                1,
                msgs::Propose {
                    id: OpId {
                        node: 1,
                        counter: 0,
                    },
                    operation: (),
                    quroum: BTreeSet::new(),
                    timestamp: 4,
                },
            )
            .unwrap()
            .unwrap();

        let msg = msgs::Consensus {
            id: OpId {
                node: 1,
                counter: 0,
            },
            timestamp: 3,
            ballot: replica
                .commands
                .get(&OpId {
                    node: 1,
                    counter: 0,
                })
                .unwrap()
                .bal
                .clone(),
        };

        let ack_msg = match replica.recv(0, msg).unwrap() {
            ConsensusResponse::Ack(a) => a.msg,
            other => panic!("Expected ack, got {:?}", other),
        };

        // TODO
        // How do we verify this stuff?

        let _tmp = replica.recv(0, ack_msg).unwrap().unwrap();
    }

    #[test]
    #[ignore = "Testing"]
    fn receive_recover() {
        let mut replica = Replica::<(), _, (), _>::new(0, vec![0, 1, 2], (), 1);

        let _ = replica
            .recv(
                0,
                msgs::Propose {
                    id: OpId {
                        node: 0,
                        counter: 0,
                    },
                    operation: (),
                    quroum: BTreeSet::new(),
                    timestamp: 1,
                },
            )
            .unwrap()
            .unwrap();

        let resp = match replica
            .recv(
                0,
                msgs::Rec {
                    id: OpId {
                        node: 0,
                        counter: 0,
                    },
                    ballot: Ballot(1, 0),
                },
            )
            .unwrap()
        {
            RecResponse::Ack(a) => a,
            other => panic!("Expected Ack, got {:?}", other),
        };

        // TODO
        // Verify more of this stuff
        assert_eq!(
            resp.msg.id,
            OpId {
                node: 0,
                counter: 0
            }
        );

        let _ = replica.recv(0, resp.msg).unwrap();
    }

    #[test]
    fn receive_promises() {
        let mut replica = Replica::<(), _, (), _>::new(0, vec![0, 1, 2], (), 1);

        let _responses = replica
            .recv(
                0,
                msgs::Promises {
                    detached: DetachedPromises::new(0),
                    attached: AttachedPromises::new(0),
                },
            )
            .unwrap();
    }

    #[test]
    fn recv_msg_fast_path() {
        let cluster: BTreeSet<_> = [0].into_iter().collect();
        let mut broadcaster: HashMap<_, _> = [(0, VecDeque::new())].into_iter().collect();

        let mut replica = Replica::<_, _, (), _>::new(0, vec![0], (), 1);

        replica
            .recv_ipc(
                ipc::IPCRequest::Submit(ipc::Submit {
                    id: OpId {
                        node: 0,
                        counter: 0,
                    },
                    operation: (),
                    quorum: [0].into_iter().collect(),
                    listeners: Vec::new(),
                }),
                &mut broadcaster,
            )
            .unwrap();

        for _ in 0..100 {
            let msg = broadcaster.get_mut(&0).unwrap().pop_front().unwrap();
            dbg!(&msg);
            let done = matches!(msg.msg, msgs::MessageContent::Commit(_));

            replica.recv_msg(msg, &mut broadcaster).unwrap();

            if done {
                return;
            }
        }

        panic!();
    }
}
