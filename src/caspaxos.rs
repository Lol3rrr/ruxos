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
//! * Dynamically adjust nodes in cluster
//!
//! Reference:
//! * [Arxiv](https://arxiv.org/pdf/1802.07000.pdf)

use std::fmt::Debug;

pub mod config;
pub mod internals;
pub mod msgs;

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

/// A Cluster describing the members
///
/// This gives flexiblity in how the Members are managed (static, dynamic, etc.) and also in how
/// the Communication is realized in the particular situation, as this is communication agnostic as
/// long as you can send and receive the messages to/from the acceptors in the cluster.
pub trait Cluster<ID, V, MD> {
    type Quorum<'q>: ClusterQuorum<ID, V, MD>
    where
        Self: 'q;

    /// Get the Number of Acceptors in the Cluster
    fn size(&self) -> usize;

    /// Get Quorum of Nodes, of a at least the given size, from the Cluster
    fn quorum<'q, 's>(&'s self, size: usize) -> Option<Self::Quorum<'q>>
    where
        's: 'q;
}

/// This provides a simpler API to use compared to [`Proposer`](internals::Proposer) and aims to
/// make it easier to use and integrate into a system.
///
/// # Example
/// ```rust
/// # use ruxos::caspaxos;
/// // Create a new Proposer with the ID 1
/// let mut proposer = caspaxos::ProposeClient::new(1u8);
/// ```
#[derive(Debug)]
pub struct ProposeClient<ID> {
    proposer: internals::Proposer<ID>,
    config: config::ProposerConfig,
}

/// The Error returned when trying to propose an Operation to the Cluster using the
/// [`ProposeClient::propose`] method
pub enum ProposeError<QE, F> {
    /// Could not get a Quorum for the Cluster
    ObtainingQuorum,
    /// Failed to send a Message to the Acceptors in the choosen Quorum
    SendingMessage(QE),
    /// Received an unexpected message in the protocol
    ProtocolError { received: String, expected: String },
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
}

impl<QE, F> Debug for ProposeError<QE, F>
where
    QE: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObtainingQuorum => f.debug_struct("ObtainingQuorum").finish(),
            Self::SendingMessage(e) => f.debug_struct("SendingMessage").field("0", e).finish(),
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
        }
    }
}

impl<ID> ProposeClient<ID>
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
    /// let proposer = ProposeClient::new(13u8);
    /// ```
    pub fn new(id: ID) -> Self {
        Self {
            proposer: internals::Proposer::new(id),
            config: config::ProposerConfig::basic(),
        }
    }

    /// Creates a new [`ProposeClient`], just like [`ProposeClient::new`]
    pub fn with_config(id: ID, conf: config::ProposerConfig) -> Self {
        Self {
            proposer: internals::Proposer::new(id),
            config: conf,
        }
    }

    /// Propose an Update to the Cluster
    ///
    /// # Note
    /// If the Operation fails with [`ProposeError::PrepareConflict`] or [`ProposeError::AcceptConflict`]
    /// you can choose to retry as this simply means that the Proposer was not on the latest State
    /// but should now be refreshed, as long as there was not another update in between.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(cluster, func, metadata))
    )]
    pub async fn propose<'c, 'md, V, F, C, MD>(
        &mut self,
        cluster: &'c C,
        func: F,
        metadata: &'md MD,
    ) -> Result<V, ProposeError<<C::Quorum<'c> as ClusterQuorum<ID, V, MD>>::Error, F>>
    where
        V: Clone,
        F: FnOnce(Option<V>) -> V,
        C: Cluster<ID, V, MD>,
    {
        #[cfg(feature = "tracing")]
        tracing::trace!("Starting Propose");

        let cluster_size = cluster.size();
        let mut quorum = cluster
            .quorum(cluster_size / 2 + 1)
            .ok_or(ProposeError::ObtainingQuorum)?;

        let mut proposal = self.proposer.propose::<V>(cluster_size / 2 + 1);

        let propose = proposal.message();
        quorum
            .send(msgs::Message {
                ballot: proposal.ballot(),
                content: msgs::ProposerMessage::Prepare(propose.owned()),
                metadata,
            })
            .await
            .map_err(|e| ProposeError::SendingMessage(e))?;

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
        let mut proposal = proposal
            .finish(func)
            .ok_or(ProposeError::FailedToFinishPromisePhase)?;

        let accept = proposal.message();
        quorum
            .send(msgs::Message {
                ballot: proposal.ballot(),
                content: msgs::ProposerMessage::Accept(accept),
                metadata,
            })
            .await
            .map_err(|e| ProposeError::SendingMessage(e))?;

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

        proposal.finish().ok_or(ProposeError::FailedAcceptByQuorum)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(cluster, func, metadata))
    )]
    pub async fn propose_with_retry<'c, 'md, V, F, C, MD>(
        &mut self,
        cluster: &'c C,
        func: F,
        metadata: &'md MD,
    ) -> Result<V, ProposeError<<C::Quorum<'c> as ClusterQuorum<ID, V, MD>>::Error, F>>
    where
        V: Clone,
        F: Fn(Option<V>) -> V,
        C: Cluster<ID, V, MD>,
    {
        loop {
            match self.propose(cluster, |val| func(val), metadata).await {
                Ok(v) => return Ok(v),
                Err(e) => match e {
                    ProposeError::PrepareConflict(_) => {}
                    ProposeError::AcceptConflict => {}
                    ProposeError::ObtainingQuorum => return Err(ProposeError::ObtainingQuorum),
                    ProposeError::SendingMessage(e) => return Err(ProposeError::SendingMessage(e)),
                    ProposeError::ProtocolError { received, expected } => {
                        return Err(ProposeError::ProtocolError { received, expected })
                    }
                    ProposeError::FailedToFinishPromisePhase => {
                        return Err(ProposeError::FailedToFinishPromisePhase)
                    }
                    ProposeError::FailedAcceptByQuorum => {
                        return Err(ProposeError::FailedAcceptByQuorum)
                    }
                },
            };

            tracing::trace!("Retrying after conflict");
        }
    }
}

#[cfg(test)]
mod tests {
    use self::cluster::create;

    use super::*;

    mod cluster {
        use std::{sync::Mutex, time::Duration};

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
                for entry in self.entires.iter_mut() {
                    entry
                        .send((msg.clone().map_meta(|_| ()), self.sender.clone()))
                        .unwrap();
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

            fn size(&self) -> usize {
                self.acceptors.len()
            }

            fn quorum<'q, 's>(&'s self, size: usize) -> Option<Self::Quorum<'q>>
            where
                's: 'q,
            {
                let (tx, rx) = crate::tests::mpsc::queue(1.0, 0);

                let mut rng = self.rng.lock().unwrap();
                let offset = rng.gen_range(0..(self.acceptors.len() - size));

                Some(TestQuorum {
                    entires: self.acceptors[offset..size + offset].to_vec(),
                    sender: tx,
                    receiver: rx,
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

                            tx.send(caspaxos::msgs::Message {
                                ballot: msg.ballot,
                                content: resp,
                                metadata: (),
                            })
                            .unwrap();
                        }
                    };

                    (tx, Box::new(afn) as Box<dyn FnOnce() + Send>)
                })
                .unzip();

            (
                TestCluster {
                    acceptors: acceptor_txs,
                    rng: Mutex::new(rand::rngs::SmallRng::seed_from_u64(0)),
                },
                acceptor_fns,
            )
        }
    }

    #[test]
    fn basic() {
        let (cluster, fns) = create::<u8, usize>(3, 1.0);

        let acceptor_threads: Vec<_> = fns.into_iter().map(|afn| std::thread::spawn(afn)).collect();

        let mut proposer = ProposeClient::new(0);

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        rt.block_on(async {
            proposer.propose(&cluster, |_| 0, &()).await.unwrap();
        });
        drop(cluster);

        for handle in acceptor_threads {
            handle.join().unwrap();
        }
    }

    #[test]
    fn failability() {
        let (cluster, fns) = create::<u8, usize>(99, 0.75);

        let acceptor_threads: Vec<_> = fns.into_iter().map(|afn| std::thread::spawn(afn)).collect();

        let mut proposer = ProposeClient::new(0);

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        rt.block_on(async {
            const ATTEMPTS: usize = 100;

            let mut failures = 0;
            for i in 0..ATTEMPTS {
                match proposer.propose(&cluster, |_| i, &()).await {
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
