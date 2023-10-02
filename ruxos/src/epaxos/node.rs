use std::{
    collections::BTreeMap,
    fmt::Debug,
    hash::Hash,
    sync::{
        atomic::{self, AtomicU64},
        Arc,
    },
};

use crate::epaxos::listener::OpState;

use super::{
    dependencies::Dependencies, ipc, listener::Ballot, msgs, Cluster, CommitHandle, Operation,
    OperationInstance, ResponseReceiver,
};

enum Phase1Outcome<Id> {
    FastPath { seq: u64, deps: Dependencies<Id> },
    SlowPath { seq: u64, deps: Dependencies<Id> },
}

#[derive(Debug)]
pub enum Phase1Error {
    SendingPreaccept,
    ReceivedUnexpectedResponse,
    ReceivingPreacceptResponse,
    Other(&'static str),
}

/// The main way to interact with a Node
pub struct NodeHandle<Id, O, T>
where
    O: Operation<T>,
{
    id: Id,
    instance: AtomicU64,
    epoch: Arc<AtomicU64>,
    ipc: ipc::IPCSender<Id, O, T>,
}

#[derive(Debug)]
pub enum RequestError {
    SendCluster,
    UnexpectedResponseType,
    ReceivingCluster,
    Commit(()),
    Phase1(Phase1Error),
    Other(&'static str),
}

impl<Id, O, T> NodeHandle<Id, O, T>
where
    Id: Hash + Eq + Clone,
    O: Operation<T> + Clone,
{
    pub(super) fn new(id: Id, ipc: ipc::IPCSender<Id, O, T>, epoch: Arc<AtomicU64>) -> Self {
        Self {
            id,
            instance: AtomicU64::new(0),
            epoch,
            ipc,
        }
    }
}

impl<Id, O, T> NodeHandle<Id, O, T>
where
    Id: Hash + Eq + Ord + Clone + Debug + 'static,
    O: Operation<T> + Clone + Debug + PartialEq + 'static,
    T: 'static,
{
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, operation, cluster))
    )]
    pub async fn request<C>(
        &self,
        operation: O,
        cluster: &mut C,
    ) -> Result<CommitHandle<Id, O, T>, (RequestError, OperationInstance<Id>)>
    where
        C: Cluster<Id, O>,
    {
        // 1.
        let req_instance = self.instance.fetch_add(1, atomic::Ordering::AcqRel);

        #[cfg(feature = "tracing")]
        tracing::info!(req_instance, "Requesting Operation");

        let ballot = Ballot::initial(self.epoch.load(atomic::Ordering::Acquire), self.id.clone());

        let phase1_res = self
            .phase1(
                cluster,
                self.id.clone(),
                req_instance,
                operation.clone(),
                ballot.clone(),
            )
            .await
            .map_err(|e| {
                (
                    RequestError::Phase1(e),
                    OperationInstance {
                        node: self.id.clone(),
                        instance: req_instance,
                    },
                )
            })?;

        let handle = match phase1_res {
            Phase1Outcome::FastPath { .. } => {
                #[cfg(feature = "tracing")]
                tracing::info!(req_instance, "Taking fast path");

                // 11.
                // Run commit phase
                self.commit::<C>(self.id.clone(), req_instance, ballot.clone(), cluster)
                    .await
                    .map_err(|e| {
                        (
                            RequestError::Commit(e),
                            OperationInstance {
                                node: self.id.clone(),
                                instance: req_instance,
                            },
                        )
                    })?
            }
            Phase1Outcome::SlowPath { seq, deps } => {
                #[cfg(feature = "tracing")]
                tracing::info!(req_instance, "Taking slow path");

                // 15.
                // Run the paxos accept phase at 16.
                if let Err(e) = self
                    .paxos_accept::<C>(
                        cluster,
                        self.id.clone(),
                        req_instance,
                        operation,
                        deps,
                        seq,
                        ballot.clone(),
                    )
                    .await
                {
                    tracing::error!(req_instance, "Paxos-Accept: {:?}", e);
                    return Err((
                        RequestError::Other("Paxos Accept"),
                        OperationInstance {
                            node: self.id.clone(),
                            instance: req_instance,
                        },
                    ));
                }

                // 20.
                self.commit::<C>(self.id.clone(), req_instance, ballot, cluster)
                    .await
                    .map_err(|e| {
                        (
                            RequestError::Commit(e),
                            OperationInstance {
                                node: self.id.clone(),
                                instance: req_instance,
                            },
                        )
                    })?
            }
        };

        #[cfg(feature = "tracing")]
        tracing::info!(req_instance, "Committed");

        Ok(handle)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, cluster)))]
    pub async fn explicit_prepare<C>(
        &self,
        cluster: &mut C,
        op_instance: OperationInstance<Id>,
    ) -> Result<CommitHandle<Id, O, T>, ()>
    where
        C: Cluster<Id, O>,
    {
        #[cfg(feature = "tracing")]
        tracing::info!("Starting Explicit prepare");

        // 25.
        // Increment ballot number to `epoch.(ballot + 1).Q` where b is the highest ballot number Q
        // is aware of in instance L.i
        // 26.
        // Send Prepare(epoch.(ballot+1).Q, L.i) to all replicas (including self) and wait for at
        // least floor(N/2) + 1 replies
        let (msg, self_resp) = self
            .ipc
            .send_recv(
                ipc::Request::ExplicitPrepare {
                    instance: op_instance.instance,
                    node: op_instance.node.clone(),
                },
                |resp| match resp {
                    ipc::Response::ExplicitPrepare(msg, resp) => (msg, resp),
                    _ => todo!(),
                },
            )
            .await
            .unwrap();

        let n_ballot = msg.ballot.clone();
        #[cfg(feature = "tracing")]
        tracing::trace!("Prepare Message {:?}", msg);

        #[cfg(feature = "tracing")]
        tracing::trace!("Sending Prepare messages to cluster...");

        let cluster_size = cluster.size();
        let target = cluster_size / 2;
        let mut recv_handle = match cluster
            .send(msgs::Request::Prepare(msg), cluster_size - 1, &self.id)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::error!("Sending Prepare Message");

                return Err(());
            }
        };

        let resps = {
            let mut tmp = Vec::with_capacity(target + 1);
            if !matches!(self_resp, msgs::PrepareResp::Nack) {
                tmp.push(self_resp);
            }
            let mut count = 1;

            while tmp.len() < target + 1 && count < cluster_size {
                match recv_handle.recv().await {
                    Ok(msgs::Response::PrepareResp(pok)) => {
                        if !matches!(pok, msgs::PrepareResp::Nack) {
                            tmp.push(pok);
                        }

                        count += 1;
                    }
                    Ok(other) => {
                        #[cfg(feature = "tracing")]
                        tracing::error!("Unrelated response: {:?}", other);

                        return Err(());
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::error!("Error while receiving: {:?}", e);

                        return Err(());
                    }
                };
            }
            drop(recv_handle);

            tmp
        };

        // tracing::trace!("Prepare Responses: {:?}", resps);

        // 27.
        // let R be the set of replies with the highest ballot number
        let r: Vec<_> = resps
            .into_iter()
            .filter_map(|cmd| match cmd {
                msgs::PrepareResp::Ok(c) => Some(c),
                _ => None,
            })
            .collect();
        /*
            .fold(
            Vec::new(),
            |mut state: Vec<msgs::PrepareOk<Id, O>>, cmd| match cmd {
                msgs::PrepareResp::Ok(cmd) => match state.first() {
                    Some(f) => {
                        if &cmd.ballot > &f.ballot {
                            state.clear();
                            state.push(cmd);
                            state
                        } else if cmd.ballot == f.ballot {
                            state.push(cmd);
                            state
                        } else {
                            state
                        }
                    }
                    None => {
                        state.push(cmd);
                        state
                    }
                },
                msgs::PrepareResp::Nack => state,
            },
        );
        */

        //#[cfg(feature = "tracing")]
        //tracing::debug!("R: {:?}", r);

        // 28.
        // If R contains a committed instance
        let handle = if let Some(cmd) = r
            .iter()
            .find(|cmd| matches!(cmd.state, OpState::Committed | OpState::Executed))
        {
            #[cfg(feature = "tracing")]
            tracing::info!("Running immediate Commit");

            // 29.
            // Run the commit phase for the command

            let commit_msg = self
                .ipc
                .send_recv(
                    ipc::Request::ExplicitCommit {
                        instance: op_instance.instance,
                        node: op_instance.node.clone(),
                        op: cmd.op.clone(),
                        ballot: n_ballot,
                        seq: cmd.seq,
                        deps: cmd.deps.clone(),
                    },
                    |resp| match resp {
                        ipc::Response::ExplicitCommitted(c) => c,
                        other => todo!(),
                    },
                )
                .await
                .unwrap();

            let op = commit_msg.op.clone();

            let target = cluster.size() - 1;
            if let Err(e) = cluster
                .send(msgs::Request::Commit(commit_msg), target, &self.id)
                .await
            {
                tracing::error!("Sending Commit Message to Cluster");
                return Err(());
            }

            CommitHandle::committed(self.ipc.clone(), op_instance.node, op_instance.instance, op)
        } else if let Some(cmd) = r.iter().find(|cmd| matches!(cmd.state, OpState::Accepted))
        /* 30.
         * If R contains an accepted instance
         * */
        {
            #[cfg(feature = "tracing")]
            tracing::info!("Running Paxos-Accept Phase");

            // 31.
            // Run Paxos-Accept Phase
            if let Err(e) = self
                .paxos_accept(
                    cluster,
                    op_instance.node.clone(),
                    op_instance.instance,
                    cmd.op.clone(),
                    cmd.deps.clone(),
                    cmd.seq,
                    n_ballot.clone(),
                )
                .await
            {
                tracing::error!("Running Paxos-Accept: {:?}", e);
                return Err(());
            }

            self.commit(op_instance.node, cmd.instance, n_ballot, cluster)
                .await
                .map_err(|e| ())?
        } else {
            tracing::debug!("Prepare Responses {:#?}", r);
            tracing::debug!(
                "All Responses the same {}",
                r.windows(2).all(|a| a[0] == a[1])
            );

            let response_kinds = r.iter().fold(Vec::new(), |mut kinds, cmd| {
                match kinds.iter_mut().find(|(c, _)| c == &cmd) {
                    Some((_, counter)) => {
                        *counter += 1;
                    }
                    None => {
                        kinds.push((cmd, 1));
                    }
                };

                kinds
            });
            tracing::debug!("Responses Grouped: {:#?}", response_kinds);

            if let Some(cmd) = response_kinds
                .into_iter()
                .find(|(_, count)| *count >= cluster_size / 2)
                .map(|(cmd, _)| cmd)
            /* 32.
             * If R contains at least floor(N/2) identical preaccept replies at ballot epoch.0.L and
             * none of those replies is from L
             * */
            {
                #[cfg(feature = "tracing")]
                tracing::info!("Running Paxos-Accept Phase");

                // 33.
                // Run Paxos-Accept Phase
                if let Err(e) = self
                    .paxos_accept(
                        cluster,
                        op_instance.node.clone(),
                        op_instance.instance,
                        cmd.op.clone(),
                        cmd.deps.clone(),
                        cmd.seq,
                        n_ballot.clone(),
                    )
                    .await
                {
                    tracing::error!("Running Paxos-Accept: {:?}", e);
                    return Err(());
                }

                self.commit(op_instance.node, cmd.instance, n_ballot, cluster)
                    .await
                    .map_err(|e| ())?
            } else if let Some(cmd) = r
                .iter()
                .find(|cmd| matches!(cmd.state, OpState::PreAccepted))
            /* 34.
             * If R contains at least one Pre-Accepted
             * */
            {
                #[cfg(feature = "tracing")]
                tracing::info!("Running Phase 1");

                // FIXME
                let cluster_size = cluster.size();

                let (msg, self_preaccept) = self
                    .ipc
                    .preaccept(
                        cmd.op.clone(),
                        op_instance.node.clone(),
                        op_instance.instance,
                        n_ballot.clone(),
                    )
                    .await
                    .map_err(|e| ())?;

                // Send PreAccept to other nodes
                let mut response_handle = cluster
                    .send(
                        msgs::Request::PreAccept(msg.clone()),
                        cluster_size - 2,
                        &self.id,
                    )
                    .await
                    .map_err(|e| ())?;

                // 10.
                // Wait for floor(N/2) PreAcceptOk responses
                let responses = {
                    let target = cluster_size / 2 + 1;
                    #[cfg(feature = "tracing")]
                    tracing::trace!("Waiting for {} PreAcceptOk Responses", target);

                    let mut tmp = Vec::with_capacity(target);
                    tmp.push(self_preaccept);
                    while tmp.len() < target {
                        match response_handle.recv().await {
                            Ok(msgs::Response::PreAcceptOk(v)) => {
                                tmp.push(v);
                            }
                            Ok(other) => return Err(()),
                            Err(e) => return Err(()),
                        }
                    }
                    drop(response_handle);
                    tmp
                };

                let deps = Dependencies::from_raw(
                    cmd.deps
                        .iter()
                        .cloned()
                        .chain(responses.iter().flat_map(|r| r.deps.iter()).cloned())
                        .collect(),
                );
                let seq = core::iter::once(cmd.seq)
                    .chain(responses.iter().map(|r| r.seq))
                    .max()
                    .unwrap();

                // 35.
                // Start Phase 1 for the preaccepted operation and avoid the fast path
                // 15.
                // Run the paxos accept phase at 16.
                if let Err(e) = self
                    .paxos_accept::<C>(
                        cluster,
                        op_instance.node.clone(),
                        op_instance.instance,
                        cmd.op.clone(),
                        deps,
                        seq,
                        n_ballot.clone(),
                    )
                    .await
                {
                    tracing::error!("Running Paxos-Accept {:?}", e);
                    return Err(());
                }

                // 20.
                self.commit::<C>(op_instance.node, op_instance.instance, n_ballot, cluster)
                    .await
                    .map_err(|e| ())?
            } else {
                #[cfg(feature = "tracing")]
                tracing::info!("Running Phase 1 for NoOp");

                // 37.
                // Start Phase 1 for a NoOp and avoid fast path
                let phase1_res = match self
                    .phase1(
                        cluster,
                        op_instance.node.clone(),
                        op_instance.instance,
                        O::noop(),
                        n_ballot.clone(),
                    )
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        return Err(());
                    }
                };

                match phase1_res {
                    Phase1Outcome::FastPath { seq, deps }
                    | Phase1Outcome::SlowPath { seq, deps } => {
                        // 15.
                        // Run the paxos accept phase at 16.
                        if let Err(e) = self
                            .paxos_accept::<C>(
                                cluster,
                                op_instance.node.clone(),
                                op_instance.instance,
                                O::noop(),
                                deps,
                                seq,
                                n_ballot.clone(),
                            )
                            .await
                        {
                            tracing::error!("Running Paxos-Accept {:?}", e);
                            return Err(());
                        }

                        // 20.
                        self.commit::<C>(op_instance.node, op_instance.instance, n_ballot, cluster)
                            .await
                            .map_err(|e| ())?
                    }
                }
            }
        };

        Ok(handle)
    }

    async fn paxos_accept<C>(
        &self,
        cluster: &mut C,
        node: Id,
        instance: u64,
        op: O,
        n_deps: Dependencies<Id>,
        n_seq: u64,
        ballot: Ballot<Id>,
    ) -> Result<(), &'static str>
    where
        C: Cluster<Id, O>,
    {
        // 16.
        // Update Dependencies, Sequence and set state to Accepted
        let msg = self
            .ipc
            .accept(node, instance, op, n_deps, n_seq, ballot)
            .await
            .map_err(|e| "Local Node Accept")?;

        let node_threshold = cluster.size() / 2;

        // 17.
        // Send Accept message to at least floor(N/2) other nodes
        let mut rx = match cluster
            .send(msgs::Request::Accept(msg), node_threshold, &self.id)
            .await
        {
            Ok(r) => r,
            Err(e) => return Err("Sending Accept to Cluster"),
        };

        let mut count: usize = 0;
        while count < node_threshold {
            let msg = match rx.recv().await {
                Ok(m) => m,
                Err(e) => return Err("Receiving Accept Response"),
            };

            match msg {
                msgs::Response::AcceptOk(m) if m.node.1 == instance => {
                    count += 1;
                }
                msgs::Response::Nack => return Err("Received a Nack"),
                other => todo!(),
            };
        }

        Ok(())
    }

    async fn commit<C>(
        &self,
        node: Id,
        instance: u64,
        ballot: Ballot<Id>,
        cluster: &mut C,
    ) -> Result<CommitHandle<Id, O, T>, ()>
    where
        C: Cluster<Id, O>,
    {
        // 21. + 22.
        let msg = self
            .ipc
            .commit(node.clone(), instance, ballot)
            .await
            .map_err(|e| ())?;

        let op = msg.op.clone();

        // 23.
        // Send commit to all other replicas
        match cluster
            .send(msgs::Request::Commit(msg), cluster.size() - 1, &self.id)
            .await
        {
            Ok(c) => {}
            Err(e) => return Err(()),
        };

        Ok(CommitHandle::committed(
            self.ipc.clone(),
            node,
            instance,
            op,
        ))
    }

    #[tracing::instrument(skip(self, cluster, op, ballot))]
    async fn phase1<C>(
        &self,
        cluster: &mut C,
        node: Id,
        instance: u64,
        op: O,
        ballot: Ballot<Id>,
    ) -> Result<Phase1Outcome<Id>, Phase1Error>
    where
        C: Cluster<Id, O>,
    {
        let cluster_size = cluster.size();

        let (msg, self_preaccept) = self
            .ipc
            .preaccept(op, node.clone(), instance, ballot.clone())
            .await
            .map_err(|e| Phase1Error::Other("Could not handle Preaccept"))?;

        // Send PreAccept to other nodes
        let mut response_handle = cluster
            .send(
                msgs::Request::PreAccept(msg.clone()),
                cluster_size.saturating_sub(2),
                &self.id,
            )
            .await
            .map_err(|e| Phase1Error::SendingPreaccept)?;

        // 10.
        // Wait for floor(N/2) PreAcceptOk responses
        let responses = {
            let target = cluster_size / 2 + 1;
            #[cfg(feature = "tracing")]
            tracing::trace!("Waiting for {} PreAcceptOk Responses", target);

            let mut tmp = Vec::with_capacity(target);
            while tmp.len() < cluster_size.saturating_sub(2) {
                match response_handle.recv().await {
                    Ok(msgs::Response::PreAcceptOk(v)) => {
                        tmp.push(v);
                    }
                    Ok(other) => return Err(Phase1Error::ReceivedUnexpectedResponse),
                    Err(e) => break,
                }
            }
            drop(response_handle);
            tmp
        };

        if responses.len() < cluster_size / 2 {
            return Err(Phase1Error::ReceivingPreacceptResponse);
        }

        #[cfg(feature = "tracing")]
        tracing::trace!("Got enough PreAcceptOk Responses");

        // Check if all the responses have the same sequence number and dependencies
        // if true => goto 11
        // else => goto 13
        let can_fast_path = {
            let first_state = self_preaccept;

            responses
                .iter()
                .all(|resp| first_state.seq == resp.seq && first_state.deps == resp.deps)
                && responses.len() >= cluster_size.saturating_sub(2)
        };

        if can_fast_path {
            let mut responses = responses;
            let first = responses.pop().unwrap();

            Ok(Phase1Outcome::FastPath {
                seq: first.seq,
                deps: first.deps,
            })
        } else {
            // 13.
            // Update dependencies as the union of all response dependencies
            let n_deps = {
                let mut tmp = msg.deps;
                for resp in responses.iter() {
                    tmp.union_mut(resp.deps.iter().cloned());
                }
                tmp
            };

            // 14.
            // Update seq using the max of all responses
            let n_seq = responses.iter().map(|c| c.seq).max().expect("We know that there is at least 1 element in the responses (from our own node) so there is always a max element");

            Ok(Phase1Outcome::SlowPath {
                seq: n_seq,
                deps: n_deps,
            })
        }
    }

    pub async fn generate_storage_debug<W>(&self, writer: &mut W)
    where
        W: std::io::Write,
    {
        let tmp = self
            .ipc
            .send(ipc::Request::GetStorage)
            .unwrap()
            .await
            .unwrap();

        let storage: BTreeMap<_, _> = match tmp {
            ipc::Response::Storage(s) => s.into_iter().collect(),
            _ => {
                tracing::error!("");
                return;
            }
        };

        for (node_id, instances) in storage {
            writeln!(writer, "{:?}:", node_id);

            for (instance, op) in instances {
                writeln!(writer, "  {}", instance);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

    use super::*;

    use crate::epaxos::{
        listener::NodeMessage,
        testing::{SingleCluster, TestOp},
    };

    #[tokio::test]
    async fn request_fastpath() {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_test_writer()),
        );

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let ipc_sender = ipc::IPCSender::<_, TestOp, _>::new(tx);

        let node = NodeHandle::new(0, ipc_sender, Arc::new(AtomicU64::new(0)));

        let local = tokio::task::LocalSet::new();

        local.spawn_local(async move {
            let mut empty_cluster = SingleCluster::new();

            node.request(TestOp::Read, &mut empty_cluster)
                .await
                .unwrap()
        });

        local
            .run_until(async move {
                let received = match rx.recv().await.unwrap() {
                    NodeMessage::Ipc { req: i, .. } => i,
                    NodeMessage::External { .. } => {
                        unreachable!("Got external Request");
                    }
                };
                assert!(matches!(received.req, ipc::Request::PreAccept { .. }));

                received
                    .resp
                    .send(ipc::Response::PreAcceptOk(
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
                        },
                    ))
                    .unwrap();

                let received = match rx.recv().await.unwrap() {
                    NodeMessage::Ipc { req: i, .. } => i,
                    NodeMessage::External { .. } => {
                        unreachable!("Got external Request");
                    }
                };
                assert!(matches!(received.req, ipc::Request::Commit { .. }));

                received
                    .resp
                    .send(ipc::Response::Committed(msgs::Commit {
                        op: TestOp::Read,
                        seq: 1,
                        deps: Dependencies::new(),
                        node: (0, 0),
                        ballot: Ballot::initial(0, 0),
                    }))
                    .unwrap();

                match rx.recv().await {
                    Some(_) => panic!(),
                    None => return,
                };
            })
            .await;
    }

    #[tokio::test]
    async fn explicit_prepare_committed() {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_test_writer()),
        );

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let ipc_sender = ipc::IPCSender::<_, TestOp, _>::new(tx);

        let node = NodeHandle::new(0, ipc_sender, Arc::new(AtomicU64::new(0)));

        let local = tokio::task::LocalSet::new();

        local.spawn_local(async move {
            let mut empty_cluster = SingleCluster::new();

            node.explicit_prepare(
                &mut empty_cluster,
                OperationInstance {
                    node: 0,
                    instance: 0,
                },
            )
            .await;
        });

        local
            .run_until(async move {
                let received = match rx.recv().await.unwrap() {
                    NodeMessage::Ipc { req: i, .. } => i,
                    NodeMessage::External { .. } => {
                        unreachable!("Got external message");
                    }
                };
                assert!(matches!(received.req, ipc::Request::ExplicitPrepare { .. }));

                received
                    .resp
                    .send(ipc::Response::ExplicitPrepare(
                        msgs::Prepare {
                            node: 0,
                            instance: 0,
                            ballot: Ballot::initial(0, 0),
                        },
                        msgs::PrepareResp::Ok(msgs::PrepareOk {
                            op: TestOp::Read,
                            state: OpState::Committed,
                            seq: 0,
                            instance: 0,
                            deps: Dependencies::new(),
                            node: 0,
                            ballot: Ballot::initial(0, 0),
                        }),
                    ))
                    .unwrap();

                let received = match rx.recv().await.unwrap() {
                    NodeMessage::Ipc { req: i, .. } => i,
                    NodeMessage::External { .. } => {
                        unreachable!("Got external message");
                    }
                };
                assert!(matches!(received.req, ipc::Request::ExplicitCommit { .. }));

                received
                    .resp
                    .send(ipc::Response::ExplicitCommitted(msgs::Commit {
                        op: TestOp::Read,
                        seq: 0,
                        deps: Dependencies::new(),
                        node: (0, 0),
                        ballot: Ballot::initial(0, 0),
                    }))
                    .unwrap();

                match rx.recv().await {
                    Some(_) => panic!(),
                    None => return,
                };
            })
            .await;
    }

    #[tokio::test]
    async fn explicit_prepare_accepted() {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_test_writer()),
        );

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let ipc_sender = ipc::IPCSender::<_, TestOp, _>::new(tx);

        let node = NodeHandle::new(0, ipc_sender, Arc::new(AtomicU64::new(0)));

        let local = tokio::task::LocalSet::new();

        local.spawn_local(async move {
            let mut empty_cluster = SingleCluster::new();

            node.explicit_prepare(
                &mut empty_cluster,
                OperationInstance {
                    node: 0,
                    instance: 0,
                },
            )
            .await;
        });

        local
            .run_until(async move {
                let received = match rx.recv().await.unwrap() {
                    NodeMessage::Ipc { req: i, .. } => i,
                    NodeMessage::External { .. } => {
                        unreachable!("Got external message");
                    }
                };
                assert!(matches!(received.req, ipc::Request::ExplicitPrepare { .. }));

                received
                    .resp
                    .send(ipc::Response::ExplicitPrepare(
                        msgs::Prepare {
                            node: 0,
                            instance: 0,
                            ballot: Ballot::initial(0, 0),
                        },
                        msgs::PrepareResp::Ok(msgs::PrepareOk {
                            op: TestOp::Read,
                            state: OpState::Accepted,
                            seq: 0,
                            instance: 0,
                            deps: Dependencies::new(),
                            node: 0,
                            ballot: Ballot::initial(0, 0),
                        }),
                    ))
                    .unwrap();

                let received = match rx.recv().await.unwrap() {
                    NodeMessage::Ipc { req: i, .. } => i,
                    NodeMessage::External { .. } => {
                        unreachable!("Got external message");
                    }
                };
                assert!(
                    matches!(received.req, ipc::Request::Accept { .. }),
                    "{:?}",
                    received.req
                );

                received
                    .resp
                    .send(ipc::Response::Accepted(msgs::Accept {
                        op: TestOp::Read,
                        seq: 0,
                        deps: Dependencies::new(),
                        node: (0, 0),
                        ballot: Ballot::initial(0, 0),
                    }))
                    .unwrap();

                let received = match rx.recv().await.unwrap() {
                    NodeMessage::Ipc { req: i, .. } => i,
                    NodeMessage::External { .. } => {
                        unreachable!("Got external message");
                    }
                };
                assert!(
                    matches!(received.req, ipc::Request::Commit { .. }),
                    "{:?}",
                    received.req
                );

                received
                    .resp
                    .send(ipc::Response::Committed(msgs::Commit {
                        op: TestOp::Read,
                        seq: 0,
                        deps: Dependencies::new(),
                        node: (0, 0),
                        ballot: Ballot::initial(0, 0),
                    }))
                    .unwrap();

                match rx.recv().await {
                    Some(_) => panic!(),
                    None => return,
                };
            })
            .await;
    }

    #[tokio::test]
    #[ignore = "Not yet implemented"]
    async fn explicit_prepare_preaccepted() {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_test_writer()),
        );

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let ipc_sender = ipc::IPCSender::<_, TestOp, _>::new(tx);

        let node = NodeHandle::new(0, ipc_sender, Arc::new(AtomicU64::new(0)));

        let local = tokio::task::LocalSet::new();

        local.spawn_local(async move {
            let mut empty_cluster = SingleCluster::new();

            node.explicit_prepare(
                &mut empty_cluster,
                OperationInstance {
                    node: 0,
                    instance: 0,
                },
            )
            .await;
        });

        local
            .run_until(async move {
                let received = match rx.recv().await.unwrap() {
                    NodeMessage::Ipc { req: i, .. } => i,
                    NodeMessage::External { .. } => {
                        unreachable!("Got external message");
                    }
                };
                assert!(matches!(received.req, ipc::Request::ExplicitPrepare { .. }));

                received
                    .resp
                    .send(ipc::Response::ExplicitPrepare(
                        msgs::Prepare {
                            node: 0,
                            instance: 0,
                            ballot: Ballot::initial(0, 0),
                        },
                        msgs::PrepareResp::Ok(msgs::PrepareOk {
                            op: TestOp::Read,
                            state: OpState::PreAccepted,
                            seq: 0,
                            instance: 0,
                            deps: Dependencies::new(),
                            node: 0,
                            ballot: Ballot::initial(0, 0),
                        }),
                    ))
                    .unwrap();

                let received = match rx.recv().await.unwrap() {
                    NodeMessage::Ipc { req: i, .. } => i,
                    NodeMessage::External { .. } => {
                        unreachable!("Got external message");
                    }
                };
                assert!(matches!(received.req, ipc::Request::ExplicitCommit { .. }));

                received
                    .resp
                    .send(ipc::Response::ExplicitCommitted(msgs::Commit {
                        op: TestOp::Read,
                        seq: 0,
                        deps: Dependencies::new(),
                        node: (0, 0),
                        ballot: Ballot::initial(0, 0),
                    }))
                    .unwrap();

                match rx.recv().await {
                    Some(_) => panic!(),
                    None => return,
                };
            })
            .await;
    }
}
