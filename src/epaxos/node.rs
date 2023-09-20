use std::{
    fmt::Debug,
    hash::Hash,
    sync::{
        atomic::{self, AtomicU64},
        Arc,
    },
};

use crate::epaxos::listener::OpState;

use super::{
    ipc,
    listener::{Ballot, Interference},
    msgs, Cluster, CommitHandle, Operation, ResponseReceiver,
};

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
    Commit,
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
    O: Operation<T> + Clone + Debug + 'static,
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
    ) -> Result<CommitHandle<Id, O, T>, RequestError>
    where
        C: Cluster<Id, O>,
    {
        let cluster_size = cluster.size();

        // 1.
        let req_instance = self.instance.fetch_add(1, atomic::Ordering::AcqRel);

        let ballot = Ballot::initial(self.epoch.load(atomic::Ordering::Acquire), self.id.clone());

        let (msg, self_preaccept) = self
            .ipc
            .preaccept(operation, req_instance, ballot.clone())
            .await
            .unwrap();

        // Send PreAccept to other nodes
        let mut response_handle = cluster
            .send(
                msgs::Request::PreAccept(msg.clone()),
                cluster_size / 2,
                &self.id,
            )
            .await
            .map_err(|e| RequestError::SendCluster)?;

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
                    Ok(other) => return Err(RequestError::UnexpectedResponseType),
                    Err(e) => return Err(RequestError::ReceivingCluster),
                }
            }
            drop(response_handle);
            tmp
        };

        #[cfg(feature = "tracing")]
        tracing::debug!("Respones: {:?}", responses);

        // Check if all the responses have the same sequence number and dependencies
        // if true => goto 11
        // else => goto 13
        let can_fast_path = {
            let first_state = responses.first().unwrap();

            responses
                .iter()
                .skip(1)
                .all(|resp| first_state.seq == resp.seq && first_state.deps == resp.deps)
        };

        let handle = if can_fast_path {
            #[cfg(feature = "tracing")]
            tracing::trace!("Taking fast path");

            // 11.
            // Run commit phase
            self.commit::<C>(req_instance, cluster)
                .await
                .map_err(|e| RequestError::Commit)?
        } else {
            #[cfg(feature = "tracing")]
            tracing::trace!("Taking slow path");

            // 13.
            // Update dependencies as the union of all response dependencies
            let n_deps = {
                let mut tmp = msg.deps;
                for resp in responses.iter() {
                    let resp_deps = resp.deps.as_slice();

                    for dep in resp_deps {
                        if tmp.contains(dep) {
                            continue;
                        }
                        tmp.push(dep.clone());
                    }
                }
                tmp
            };

            // 14.
            // Update seq using the max of all responses
            let n_seq = responses.iter().map(|c| c.seq).max().expect("We know that there is at least 1 element in the responses (from our own node) so there is always a max element");

            // 15.
            // Run the paxos accept phase at 16.
            self.paxos_accept::<C>(cluster, req_instance, n_deps, n_seq)
                .await;

            // 20.
            self.commit::<C>(req_instance, cluster)
                .await
                .map_err(|e| RequestError::Commit)?
        };

        #[cfg(feature = "tracing")]
        tracing::info!("Committed");

        Ok(handle)
    }

    pub async fn explicit_prepare<C>(&self, cluster: &mut C, node: Id, instance: u64)
    where
        C: Cluster<Id, O>,
    {
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
                    instance,
                    node: node.clone(),
                },
                |resp| match resp {
                    ipc::Response::ExplicitPrepare(msg, resp) => (msg, resp),
                    _ => todo!(),
                },
            )
            .await
            .unwrap();

        let target = cluster.size() / 2;
        let mut recv_handle = match cluster
            .send(msgs::Request::Prepare(msg), target, &self.id)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                todo!();
            }
        };

        let resps = {
            let mut tmp = Vec::with_capacity(target + 1);
            tmp.push(self_resp);

            while tmp.len() < target + 1 {
                match recv_handle.recv().await {
                    Ok(msgs::Response::PrepareResp(pok)) => {
                        tmp.push(pok);
                    }
                    Ok(other) => {
                        todo!()
                    }
                    Err(e) => {
                        todo!();
                    }
                };
            }
            drop(recv_handle);

            tmp
        };

        #[cfg(feature = "tracing")]
        tracing::debug!("Responses: {:?}", resps);

        // 27.
        // let R be the set of replies with the highest ballot number
        let r =
            resps
                .into_iter()
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

        #[cfg(feature = "tracing")]
        tracing::debug!("R: {:?}", r);

        // 28.
        // If R contains a committed instance
        if let Some(cmd) = r.iter().find(|cmd| matches!(cmd.state, OpState::Committed)) {
            // 29.
            // Run the commit phase for the command

            let commit_msg = self
                .ipc
                .send_recv(
                    ipc::Request::ExplicitCommit {
                        instance,
                        node,
                        op: cmd.op.clone(),
                        ballot: cmd.ballot.clone(),
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

            let target = cluster.size() - 1;
            cluster
                .send(msgs::Request::Commit(commit_msg), target, &self.id)
                .await;
        } else if let Some(cmd) = r.iter().find(|cmd| matches!(cmd.state, OpState::Accepted))
        /* 30.
         * If R contains an accepted instance
         * */
        {
            // 31.
            // Run Paxos-Accept Phase
            todo!("Run Paxos-Accept Phase");
        } else if false
        /* 32.
         * If R contains at least floor(N/2) identical preaccept replies at ballot epoch.0.L and
         * none of those replies is from L
         * */
        {
            // 33.
            // Run Paxos-Accept Phase
            todo!();
        } else if false
        /* 34.
         * If R contains at least one Pre-Accepted
         * */
        {
            // 35.
            // Start Phase 1 for the preaccepted operation and avoid the fast path
            todo!();
        } else {
            // 37.
            // Start Phase 1 for a NoOp and avoid fast path
            todo!();
        }
    }

    async fn paxos_accept<C>(
        &self,
        cluster: &mut C,
        instance: u64,
        n_deps: Vec<Interference<Id>>,
        n_seq: u64,
    ) where
        C: Cluster<Id, O>,
    {
        // 16.
        // Update Dependencies, Sequence and set state to Accepted
        let msg = self.ipc.accept(instance, n_deps, n_seq).await.unwrap();

        let node_threshold = cluster.size() / 2;

        // 17.
        // Send Accept message to at least floor(N/2) other nodes
        let mut rx = match cluster
            .send(msgs::Request::Accept(msg), node_threshold, &self.id)
            .await
        {
            Ok(r) => r,
            Err(e) => todo!(),
        };

        let mut count: usize = 0;
        while count < node_threshold {
            let msg = match rx.recv().await {
                Ok(m) => m,
                Err(e) => todo!(),
            };

            match msg {
                msgs::Response::AcceptOk(m) if m.node.1 == instance => {
                    count += 1;
                }
                other => todo!(),
            };
        }
    }

    async fn commit<C>(&self, instance: u64, cluster: &mut C) -> Result<CommitHandle<Id, O, T>, ()>
    where
        C: Cluster<Id, O>,
    {
        // 21. + 22.
        let msg = self.ipc.commit(instance).await.unwrap();

        // 23.
        // Send commit to all other replicas
        match cluster
            .send(msgs::Request::Commit(msg), cluster.size() - 1, &self.id)
            .await
        {
            Ok(c) => {}
            Err(e) => return Err(()),
        };

        Ok(CommitHandle::committed(self.ipc.clone(), instance))
    }
}
