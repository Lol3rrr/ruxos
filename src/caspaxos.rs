//! A CASPaxos implementation
//!
//! # Introduction
//! CASPaxos is a leader-less distributed consensus algorithm, that allows updates based on
//! side-effect free functions for single Values. Therefor this kind of algorithm is well suited
//! for a distributed key-value store
//!
//! # Use Cases
//! CASPaxos allows you implement arbitrary functions on a distributed system, while still reaching
//! a consistend state after a while.
//!
//! # TODO
//! * Actually respect [`config::QuorumSize`] configuration
//!
//! Reference:
//! * [Arxiv](https://arxiv.org/pdf/1802.07000.pdf)

use std::fmt::Debug;

use crate::retry::RetryStrategy;

pub mod config;
pub mod internals;
pub mod msgs;

/// A Cluster describing the members
///
/// This gives flexiblity in how the Members are managed (static, dynamic, etc.) and also in how
/// the Communication is realized in the particular situation, as this is communication agnostic as
/// long as you can send and receive the messages to/from the acceptors in the cluster.
pub trait Cluster<ID, V, MD> {
    type Quorum<'q>: ClusterQuorum<ID, V, MD>
    where
        Self: 'q;

    /// Calculate a Hash for the current Cluster configuration
    ///
    /// # Note
    /// This has to be deterministic and only based on the Set of Nodes in the Cluster and not the
    /// Order itself.
    fn hash(&self) -> internals::ClusterHash;

    /// Get the Number of Acceptors in the Cluster
    fn size(&self) -> usize;

    /// Get Quorum of Nodes, of a at least the given size, from the Cluster
    fn quorum<'q, 's>(&'s mut self, size: usize) -> Option<Self::Quorum<'q>>
    where
        's: 'q;
}

/// A Quorum in the Cluster, meaning a Set of Acceptors, and a way to communicate with these nodes
pub trait ClusterQuorum<ID, V, MD> {
    type Error;

    /// Send the provided msg to all the Acceptors in this Quorum
    async fn send<'md>(
        &mut self,
        msg: msgs::Message<msgs::ProposerMessage<ID, V>, &'md MD>,
    ) -> Result<(), Self::Error>;

    /// Try receiving a message from any of the Acceptors in this quorum
    async fn try_recv(
        &mut self,
    ) -> Result<msgs::Message<msgs::AcceptorMessage<ID, V>, MD>, Self::Error>;
}

/// Extends the Cluster to allow for dynamic membership changes
pub trait DynamicCluster<ID, V, MD, N, DMD>: Cluster<ID, V, MD> {
    type AddHandle;

    fn add(&mut self, node: N) -> Self::AddHandle;

    async fn notify<'md>(&mut self, metadata: &'md DMD);
}

/// This provides a simpler API to use compared to [`Proposer`](internals::Proposer) and aims to
/// make it easier to use and integrate into a system.
///
/// # Example
/// ```rust
/// # use ruxos::caspaxos;
/// // Create a new Proposer with the ID 1
/// let mut proposer = caspaxos::ProposeClient::<u8, u64>::new(1u8);
/// ```
pub struct ProposeClient<ID, V> {
    proposer: internals::Proposer<ID>,
    config: config::ProposerConfig,
    extra: ClientExtras<V>,
}

#[derive(Debug)]
struct ClientExtras<V> {
    round_trip: Option<(V, u64)>,
}

/// The Error returned when trying to propose an Operation to the Cluster using the
/// [`ProposeClient::propose`] method
pub enum ProposeError<F> {
    /// Could not get a Quorum for the Cluster
    ObtainingQuorum,
    /// Failed to send a Message to the Acceptors in the choosen Quorum
    SendingMessage,
    /// Received an unexpected message in the protocol
    ProtocolError {
        received: String,
        expected: String,
    },
    /// Got not enough responses from Acceptors to safely advance to the next Phase of a proposal
    FailedToFinishPromisePhase,
    /// Got not enough responses from Acceptors to safely determine if the Operation was
    /// successfully commited to the cluster
    FailedAcceptByQuorum,
    /// The Proposal had a conflict with a newer Proposal during the initial Prepare stage of a
    /// Proposal and contains the provided function that was intended for updating the Value stored
    PrepareConflict(F),
    /// The Proposal had a conflict with a newer Proposal and was therefore aborted, but the
    /// propose can be retried
    AcceptConflict,
    Other(&'static str),
}

impl<F> Debug for ProposeError<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObtainingQuorum => f.debug_struct("ObtainingQuorum").finish(),
            Self::SendingMessage => f.debug_struct("SendingMessage").finish(),
            Self::ProtocolError { received, expected } => f
                .debug_struct("ProtocolError")
                .field("received", &received)
                .field("expected", &expected)
                .finish(),
            Self::FailedToFinishPromisePhase => {
                f.debug_struct("FailedToFinishPromisePhase").finish()
            }
            Self::FailedAcceptByQuorum => f.debug_struct("FailedAcceptByQuorum").finish(),
            Self::PrepareConflict(_) => f.debug_struct("PrepareConflict").finish(),
            Self::AcceptConflict => f.debug_struct("AcceptConflict").finish(),
            Self::Other(c) => f.debug_struct("Other").field("content", c).finish(),
        }
    }
}

impl<ID, V> Debug for ProposeClient<ID, V>
where
    ID: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProposeClient")
            .field("proposer", &self.proposer)
            .field("config", &self.config)
            .field("extras", &())
            .finish()
    }
}

impl<ID, V> ProposeClient<ID, V>
where
    ID: Ord + Clone + Debug,
{
    /// Create a new Client instance with the given id, which has to be unique, and the
    /// [`ProposerConfig::basic`](`config::ProposerConfig::basic`).
    ///
    /// # Note
    /// The ID needs to be unique for the Cluster, as this otherwise can lead to incorrect
    /// behaviour and potential data loss. However this garantuee cant be checked reliably by the
    /// library itself.
    ///
    /// # Example
    /// ```rust
    /// # use ruxos::caspaxos::ProposeClient;
    /// // Create a Propose Client with the ID 13
    /// let proposer = ProposeClient::<u8, u64>::new(13u8);
    /// ```
    pub fn new(id: ID) -> Self {
        let config = config::ProposerConfig::basic();

        let extra = ClientExtras { round_trip: None };

        Self {
            proposer: internals::Proposer::new(id),
            config,
            extra,
        }
    }

    /// Creates a new [`ProposeClient`], just like [`ProposeClient::new`]
    pub fn with_config(id: ID, conf: config::ProposerConfig) -> Self {
        let extra = ClientExtras { round_trip: None };

        Self {
            proposer: internals::Proposer::new(id),
            config: conf,
            extra,
        }
    }

    /// Propose an Update to the Cluster
    ///
    /// The provided update function may be executed even when the entire operation is not
    /// successful.
    ///
    /// # Note
    /// If the Operation fails with [`ProposeError::PrepareConflict`] or [`ProposeError::AcceptConflict`]
    /// you can choose to retry as this simply means that the Proposer was not on the latest State
    /// but should now be refreshed, as long as there was not another update in between.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(cluster, func, metadata))
    )]
    pub async fn propose<'c, 'md, F, C, MD>(
        &mut self,
        cluster: &'c mut C,
        func: F,
        metadata: &'md MD,
    ) -> Result<V, ProposeError<F>>
    where
        V: Clone,
        F: FnOnce(Option<V>) -> V,
        C: Cluster<ID, V, MD>,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!("Starting Propose");

        // TODO
        // Consider: Can I only send the phase 2 messages to nodes that previously responded?

        let cluster_hash = cluster.hash();
        let cluster_size = cluster.size();
        let mut quorum = (&mut *cluster)
            .quorum(match self.config.thrifty() {
                config::Thrifty::Minimum => cluster_size / 2 + 1,
                config::Thrifty::AllNodes => cluster_size,
            })
            .ok_or(ProposeError::ObtainingQuorum)?;

        let mut proposal = match self.extra.round_trip.take() {
            Some((pvalue, pballot)) => {
                #[cfg(feature = "tracing")]
                tracing::trace!("Attemping one-round trip optimization");

                let n_value = func(Some(pvalue));

                self.proposer
                    .accept_propose(cluster_size / 2 + 1, n_value, pballot, cluster_hash)
            }
            None => {
                let mut proposal = self
                    .proposer
                    .propose::<V>(cluster_size / 2 + 1, cluster_hash);

                let propose = proposal.message();
                quorum
                    .send(msgs::Message {
                        ballot: proposal.ballot(),
                        content: msgs::ProposerMessage::Prepare(propose.owned()),
                        metadata,
                    })
                    .await
                    .map_err(|_e| ProposeError::SendingMessage)?;

                #[cfg(feature = "tracing")]
                tracing::trace!("Waiting for Promise Responses");

                while let Ok(resp) = quorum.try_recv().await {
                    if resp.ballot < proposal.ballot() {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            resp_ballot = resp.ballot,
                            proposal_ballot = proposal.ballot(),
                            resp_type = match resp.content {
                                msgs::AcceptorMessage::Promise(_) => "promise",
                                msgs::AcceptorMessage::Accepted(_) => "accepted",
                            },
                            "Received old message for different ballot for Prepare"
                        );

                        continue;
                    }

                    match resp.content {
                        msgs::AcceptorMessage::Promise(pmsg) => {
                            match proposal.process(pmsg) {
                                internals::ProcessResult::Ready => break,
                                internals::ProcessResult::Pending => {}
                                internals::ProcessResult::Conflict { existing } => {
                                    self.proposer.update_count(existing + 1);

                                    return Err(ProposeError::PrepareConflict(func));
                                }
                            };
                        }
                        msgs::AcceptorMessage::Accepted(_) => {
                            return Err(ProposeError::ProtocolError {
                                received: "Accepted".to_string(),
                                expected: "Promise".to_string(),
                            })
                        }
                    };
                }

                proposal
                    .finish(func)
                    .ok_or(ProposeError::FailedToFinishPromisePhase)?
            }
        };

        if self.config.one_roundtrip() {
            proposal.with_one_trip(proposal.ballot() + 1);
        }

        let accept = proposal.message();
        quorum
            .send(msgs::Message {
                ballot: proposal.ballot(),
                content: msgs::ProposerMessage::Accept(accept),
                metadata,
            })
            .await
            .map_err(|_e| ProposeError::SendingMessage)?;

        #[cfg(feature = "tracing")]
        tracing::trace!("Waiting for Accept Responses");

        while let Ok(resp) = quorum.try_recv().await {
            if resp.ballot < proposal.ballot() {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    resp_ballot = resp.ballot,
                    proposal_ballot = proposal.ballot(),
                    resp_type = match resp.content {
                        msgs::AcceptorMessage::Promise(_) => "promise",
                        msgs::AcceptorMessage::Accepted(_) => "accepted",
                    },
                    "Received old message for different ballot for Accept",
                );

                continue;
            }

            if resp.ballot == proposal.ballot()
                && matches!(&resp.content, msgs::AcceptorMessage::Promise(_))
            {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    resp_ballot = resp.ballot,
                    proposal_ballot = proposal.ballot(),
                    resp_type = match resp.content {
                        msgs::AcceptorMessage::Promise(_) => "promise",
                        msgs::AcceptorMessage::Accepted(_) => "accepted",
                    },
                    "Received old promise message for same ballot",
                );

                continue;
            }

            match resp.content {
                msgs::AcceptorMessage::Promise(_) => {
                    return Err(ProposeError::ProtocolError {
                        received: "Promise".to_string(),
                        expected: "Accept".to_string(),
                    });
                }
                msgs::AcceptorMessage::Accepted(amsg) => {
                    match proposal.process(amsg) {
                        internals::ProcessResult::Ready => break,
                        internals::ProcessResult::Pending => {}
                        internals::ProcessResult::Conflict { existing } => {
                            self.proposer.update_count(existing + 1);

                            return Err(ProposeError::AcceptConflict);
                        }
                    };
                }
            };
        }

        #[cfg(feature = "tracing")]
        tracing::trace!("Finished Proposal");

        let ballot = proposal.ballot() + 1;

        let result = proposal
            .finish()
            .ok_or(ProposeError::FailedAcceptByQuorum)?;

        if self.config.one_roundtrip() {
            self.extra.round_trip = Some((result.clone(), ballot));
        }

        Ok(result)
    }

    /// This function builds on [`ProposeClient::propose`] with automatic retrying on Conflicts
    ///
    /// # Note
    /// The provided function may be called multiple times while executing the operation
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(cluster, func, metadata, retry))
    )]
    pub async fn propose_with_retry<'c, 'md, F, C, MD>(
        &mut self,
        cluster: &'c mut C,
        func: F,
        metadata: &'md MD,
        retry: impl Into<RetryStrategy<'_>>,
    ) -> Result<V, ProposeError<F>>
    where
        V: Clone,
        F: Fn(Option<V>) -> V,
        C: Cluster<ID, V, MD>,
    {
        let mut retry = retry.into();

        loop {
            match self.propose(&mut *cluster, |val| func(val), metadata).await {
                Ok(v) => return Ok(v),
                Err(e) => match e {
                    ProposeError::PrepareConflict(_) => {}
                    ProposeError::AcceptConflict => {}
                    ProposeError::ObtainingQuorum => return Err(ProposeError::ObtainingQuorum),
                    ProposeError::SendingMessage => return Err(ProposeError::SendingMessage),
                    ProposeError::ProtocolError { received, expected } => {
                        return Err(ProposeError::ProtocolError { received, expected })
                    }
                    ProposeError::FailedToFinishPromisePhase => {
                        return Err(ProposeError::FailedToFinishPromisePhase)
                    }
                    ProposeError::FailedAcceptByQuorum => {
                        return Err(ProposeError::FailedAcceptByQuorum)
                    }
                    ProposeError::Other(c) => return Err(ProposeError::Other(c)),
                },
            };

            if !retry.should_retry() {
                return Err(ProposeError::Other(
                    "Stopping retries according to retry strategy",
                ));
            }

            #[cfg(feature = "tracing")]
            tracing::trace!("Retrying after conflict");
            retry.wait().await;
        }
    }
}

impl<ID, V> ProposeClient<ID, V>
where
    ID: Ord + Clone + Debug,
{
    pub async fn add_node<'md, 'dmd, C, MD, N, DMD>(
        &mut self,
        cluster: &mut C,
        metadata: &'md MD,
        dynamic_md: &'dmd DMD,
        node: N,
    ) -> Result<(), C::AddHandle>
    where
        V: Clone + Default,
        C: DynamicCluster<ID, V, MD, N, DMD>,
    {
        let old_hash = cluster.hash();

        let add_handle = cluster.add(node);
        let new_hash = cluster.hash();

        let mut proposal = self.proposer.propose::<V>(cluster.size() / 2 + 2, old_hash);

        let mut quorum = match cluster.quorum(cluster.size() / 2 + 2) {
            Some(q) => q,
            None => return Err(add_handle),
        };

        let propose = proposal.message();
        if let Err(_e) = quorum
            .send(msgs::Message {
                ballot: proposal.ballot(),
                content: msgs::ProposerMessage::Prepare(propose.owned()),
                metadata,
            })
            .await
        {
            return Err(add_handle);
        };

        #[cfg(feature = "tracing")]
        tracing::trace!("Waiting for Promise Responses");

        while let Ok(resp) = quorum.try_recv().await {
            if resp.ballot < proposal.ballot() {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    resp_ballot = resp.ballot,
                    proposal_ballot = proposal.ballot(),
                    resp_type = match resp.content {
                        msgs::AcceptorMessage::Promise(_) => "promise",
                        msgs::AcceptorMessage::Accepted(_) => "accepted",
                    },
                    "Received old message for different ballot for Prepare"
                );

                continue;
            }

            match resp.content {
                msgs::AcceptorMessage::Promise(pmsg) => {
                    match proposal.process(pmsg) {
                        internals::ProcessResult::Ready => break,
                        internals::ProcessResult::Pending => {}
                        internals::ProcessResult::Conflict { existing } => {
                            self.proposer.update_count(existing + 1);

                            return Err(add_handle);
                        }
                    };
                }
                msgs::AcceptorMessage::Accepted(_) => {
                    return Err(add_handle);
                }
            };
        }

        let mut proposal = match proposal.finish_with_cluster(|d| d.unwrap_or_default(), new_hash) {
            Some(p) => p,
            None => return Err(add_handle),
        };

        let accept = proposal.message();
        if let Err(_e) = quorum
            .send(msgs::Message {
                ballot: proposal.ballot(),
                content: msgs::ProposerMessage::Accept(accept),
                metadata,
            })
            .await
        {
            return Err(add_handle);
        }

        #[cfg(feature = "tracing")]
        tracing::trace!("Waiting for Accept Responses");

        while let Ok(resp) = quorum.try_recv().await {
            if resp.ballot < proposal.ballot() {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    resp_ballot = resp.ballot,
                    proposal_ballot = proposal.ballot(),
                    resp_type = match resp.content {
                        msgs::AcceptorMessage::Promise(_) => "promise",
                        msgs::AcceptorMessage::Accepted(_) => "accepted",
                    },
                    "Received old message for different ballot for Accept",
                );

                continue;
            }

            if resp.ballot == proposal.ballot()
                && matches!(&resp.content, msgs::AcceptorMessage::Promise(_))
            {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    resp_ballot = resp.ballot,
                    proposal_ballot = proposal.ballot(),
                    resp_type = match resp.content {
                        msgs::AcceptorMessage::Promise(_) => "promise",
                        msgs::AcceptorMessage::Accepted(_) => "accepted",
                    },
                    "Received old promise message for same ballot",
                );

                continue;
            }

            match resp.content {
                msgs::AcceptorMessage::Promise(_) => {
                    return Err(add_handle);
                }
                msgs::AcceptorMessage::Accepted(amsg) => {
                    match proposal.process(amsg) {
                        internals::ProcessResult::Ready => break,
                        internals::ProcessResult::Pending => {}
                        internals::ProcessResult::Conflict { existing } => {
                            self.proposer.update_count(existing + 1);

                            return Err(add_handle);
                        }
                    };
                }
            };
        }

        #[cfg(feature = "tracing")]
        tracing::trace!("Finished Proposal");

        if let None = proposal.finish() {
            return Err(add_handle);
        }

        drop(quorum);

        cluster.notify(dynamic_md).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use self::cluster::create;

    use super::*;

    mod cluster {
        use std::{
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc, Mutex,
            },
            time::Duration,
        };

        use rand::{Rng, SeedableRng};

        use crate::caspaxos;

        pub struct TestCluster<ID, V> {
            acceptors: Vec<
                crate::tests::mpsc::FallibleSender<(
                    caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<ID, V>, ()>,
                    crate::tests::mpsc::FallibleSender<
                        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<ID, V>, ()>,
                    >,
                )>,
            >,
            rng: Mutex<rand::rngs::SmallRng>,
            pub send_calls: Arc<AtomicUsize>,
            pub send_msgs: Arc<AtomicUsize>,
        }

        pub struct TestQuorum<ID, V> {
            entires: Vec<
                crate::tests::mpsc::FallibleSender<(
                    caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<ID, V>, ()>,
                    crate::tests::mpsc::FallibleSender<
                        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<ID, V>, ()>,
                    >,
                )>,
            >,
            sender: crate::tests::mpsc::FallibleSender<
                caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<ID, V>, ()>,
            >,
            receiver: crate::tests::mpsc::FallibleReceiver<
                caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<ID, V>, ()>,
            >,
            send_calls: Arc<AtomicUsize>,
            send_msgs: Arc<AtomicUsize>,
        }

        impl<ID, V> caspaxos::ClusterQuorum<ID, V, ()> for TestQuorum<ID, V>
        where
            ID: Clone,
            V: Clone,
        {
            type Error = ();

            async fn send<'md>(
                &mut self,
                msg: caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<ID, V>, &'md ()>,
            ) -> Result<(), Self::Error> {
                self.send_calls.fetch_add(1, Ordering::Relaxed);

                for entry in self.entires.iter_mut() {
                    // TODO
                    // Reconsider
                    let _ = entry.send((msg.clone().map_meta(|_| ()), self.sender.clone()));

                    self.send_msgs.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }

            async fn try_recv(
                &mut self,
            ) -> Result<
                caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<ID, V>, ()>,
                Self::Error,
            > {
                for _ in 0..5 {
                    match self.receiver.try_recv() {
                        Ok(c) => return Ok(c),
                        Err(std::sync::mpsc::TryRecvError::Empty) => {}
                        Err(e) => {
                            println!("Receiving {:?}", e);
                            return Err(());
                        }
                    };

                    std::thread::sleep(Duration::from_millis(50));
                }

                Err(())
            }
        }

        impl<ID, V> caspaxos::Cluster<ID, V, ()> for TestCluster<ID, V>
        where
            ID: Clone + 'static,
            V: Clone + 'static,
        {
            type Quorum<'q> = TestQuorum<ID, V>;

            fn hash(&self) -> caspaxos::internals::ClusterHash {
                // TODO
                caspaxos::internals::ClusterHash(0)
            }

            fn size(&self) -> usize {
                self.acceptors.len()
            }

            fn quorum<'q, 's>(&'s mut self, size: usize) -> Option<Self::Quorum<'q>>
            where
                's: 'q,
            {
                let (tx, rx) = crate::tests::mpsc::queue(1.0, 0);

                let mut rng = self.rng.lock().unwrap();
                let offset = if size >= self.acceptors.len() {
                    0
                } else {
                    rng.gen_range(0..(self.acceptors.len() - size))
                };

                Some(TestQuorum {
                    entires: self.acceptors[offset..size + offset].to_vec(),
                    sender: tx,
                    receiver: rx,
                    send_calls: self.send_calls.clone(),
                    send_msgs: self.send_msgs.clone(),
                })
            }
        }

        pub fn create<ID, V>(
            acceptors: usize,
            ratio: f64,
        ) -> (TestCluster<ID, V>, Vec<Box<dyn FnOnce() + Send>>)
        where
            V: 'static + Clone + Send,
            ID: 'static + Clone + PartialOrd + Send,
        {
            let (acceptor_txs, acceptor_fns): (Vec<_>, Vec<Box<dyn FnOnce() + Send>>) = (0
                ..acceptors)
                .map(|_i| {
                    let (tx, mut rx) = crate::tests::mpsc::queue(ratio, 0);

                    let afn = move || {
                        let mut acceptor = caspaxos::internals::Acceptor::<ID, V>::new();

                        loop {
                            let (msg, tx): (
                                caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<ID, V>, ()>,
                                crate::tests::mpsc::FallibleSender<
                                    caspaxos::msgs::Message<
                                        caspaxos::msgs::AcceptorMessage<ID, V>,
                                        (),
                                    >,
                                >,
                            ) = match rx.recv() {
                                Ok(m) => m,
                                Err(e) => {
                                    println!("Error Receiving {:?}", e);
                                    return;
                                }
                            };

                            let resp = match msg.content {
                                caspaxos::msgs::ProposerMessage::Accept(a) => {
                                    let r = acceptor.recv_accept(a);
                                    caspaxos::msgs::AcceptorMessage::Accepted(r)
                                }
                                caspaxos::msgs::ProposerMessage::Prepare(p) => {
                                    let r = acceptor.recv_prepare(p);
                                    caspaxos::msgs::AcceptorMessage::Promise(r)
                                }
                            };

                            // TODO
                            // Reconsider
                            let _ = tx.send(caspaxos::msgs::Message {
                                ballot: msg.ballot,
                                content: resp,
                                metadata: (),
                            });
                        }
                    };

                    (tx, Box::new(afn) as Box<dyn FnOnce() + Send>)
                })
                .unzip();

            (
                TestCluster {
                    acceptors: acceptor_txs,
                    rng: Mutex::new(rand::rngs::SmallRng::seed_from_u64(0)),
                    send_calls: Arc::new(AtomicUsize::new(0)),
                    send_msgs: Arc::new(AtomicUsize::new(0)),
                },
                acceptor_fns,
            )
        }
    }

    #[test]
    fn basic() {
        let (mut cluster, fns) = create::<u8, usize>(3, 1.0);

        let acceptor_threads: Vec<_> = fns.into_iter().map(|afn| std::thread::spawn(afn)).collect();

        let mut proposer = ProposeClient::new(0);

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        rt.block_on(async {
            proposer.propose(&mut cluster, |_| 0, &()).await.unwrap();
        });
        drop(cluster);

        for handle in acceptor_threads {
            handle.join().unwrap();
        }
    }

    #[test]
    fn basic_with_oneroundtrip() {
        let (mut cluster, fns) = create::<u8, usize>(3, 1.0);

        let acceptor_threads: Vec<_> = fns.into_iter().map(|afn| std::thread::spawn(afn)).collect();

        let mut proposer = ProposeClient::with_config(
            0,
            config::ProposerConfig::basic().with_roundtrip(config::OneRoundTrip::Enabled),
        );

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let result =
            rt.block_on(async { proposer.propose(&mut cluster, |_| 0, &()).await.unwrap() });
        assert_eq!(0, result);

        assert_eq!(2, cluster.send_calls.load(Ordering::Relaxed));

        let second_res = rt.block_on(async {
            proposer
                .propose(
                    &mut cluster,
                    |x| {
                        assert_eq!(Some(0), x);
                        1
                    },
                    &(),
                )
                .await
                .unwrap()
        });
        assert_eq!(1, second_res);

        assert_eq!(3, cluster.send_calls.load(Ordering::Relaxed));

        let third_res = rt.block_on(async {
            proposer
                .propose(
                    &mut cluster,
                    |x| {
                        assert_eq!(Some(1), x);
                        2
                    },
                    &(),
                )
                .await
                .unwrap()
        });
        assert_eq!(2, third_res);

        assert_eq!(4, cluster.send_calls.load(Ordering::Relaxed));

        drop(cluster);

        for handle in acceptor_threads {
            handle.join().unwrap();
        }
    }

    #[test]
    fn basic_with_thrifty_minimal() {
        let (mut cluster, fns) = create::<u8, usize>(3, 1.0);

        let acceptor_threads: Vec<_> = fns.into_iter().map(|afn| std::thread::spawn(afn)).collect();

        let mut proposer = ProposeClient::with_config(
            0,
            config::ProposerConfig::basic().with_thrifty(config::Thrifty::Minimum),
        );

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let result =
            rt.block_on(async { proposer.propose(&mut cluster, |_| 0, &()).await.unwrap() });
        assert_eq!(0, result);

        assert_eq!(4, cluster.send_msgs.load(Ordering::Relaxed));

        let second_res = rt.block_on(async {
            proposer
                .propose(
                    &mut cluster,
                    |x| {
                        assert_eq!(Some(0), x);
                        1
                    },
                    &(),
                )
                .await
                .unwrap()
        });
        assert_eq!(1, second_res);

        assert_eq!(8, cluster.send_msgs.load(Ordering::Relaxed));

        drop(cluster);

        for handle in acceptor_threads {
            handle.join().unwrap();
        }
    }

    #[test]
    fn basic_with_thrifty_all() {
        let (mut cluster, fns) = create::<u8, usize>(3, 1.0);

        let acceptor_threads: Vec<_> = fns.into_iter().map(|afn| std::thread::spawn(afn)).collect();

        let mut proposer = ProposeClient::with_config(
            0,
            config::ProposerConfig::basic().with_thrifty(config::Thrifty::AllNodes),
        );

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let result =
            rt.block_on(async { proposer.propose(&mut cluster, |_| 0, &()).await.unwrap() });
        assert_eq!(0, result);

        assert_eq!(6, cluster.send_msgs.load(Ordering::Relaxed));

        let second_res = rt.block_on(async {
            proposer
                .propose(
                    &mut cluster,
                    |x| {
                        assert_eq!(Some(0), x);
                        1
                    },
                    &(),
                )
                .await
                .unwrap()
        });
        assert_eq!(1, second_res);

        assert_eq!(12, cluster.send_msgs.load(Ordering::Relaxed));

        drop(cluster);

        for handle in acceptor_threads {
            handle.join().unwrap();
        }
    }

    #[test]
    #[ignore = "Unreliable"]
    fn failability() {
        let (mut cluster, fns) = create::<u8, usize>(99, 0.75);

        let acceptor_threads: Vec<_> = fns.into_iter().map(|afn| std::thread::spawn(afn)).collect();

        let mut proposer = ProposeClient::new(0);

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        rt.block_on(async {
            const ATTEMPTS: usize = 100;

            let mut failures = 0;
            for i in 0..ATTEMPTS {
                match proposer.propose(&mut cluster, |_| i, &()).await {
                    Ok(v) => {
                        assert_eq!(v, i);
                    }
                    Err(e) => {
                        failures += 1;
                        println!("[{}] Failed with {:?}", i, e);
                    }
                };
            }

            assert!(
                failures > 0,
                "Expected at least one failure from the messages being dropped but had no failures"
            );

            assert!(failures < ATTEMPTS);
        });
        drop(cluster);

        for handle in acceptor_threads {
            handle.join().unwrap();
        }
    }
}
