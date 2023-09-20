#![feature(async_fn_in_trait)]

use std::{sync::mpsc, time::Duration};

use rand::Rng;
use ruxos::caspaxos::{self, internals::ClusterHash};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

#[derive(Debug, Clone)]
struct TestCluster {
    nodes: Vec<mpsc::Sender<Message>>,
}

struct TestQuorum<ID, V> {
    entries: Vec<mpsc::Sender<Message>>,
    receiver: mpsc::Receiver<caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<ID, V>, ()>>,
    sender: mpsc::Sender<caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<ID, V>, ()>>,
}

impl caspaxos::ClusterQuorum<u8, usize, ()> for TestQuorum<u8, usize> {
    type Error = ();

    async fn send<'md>(
        &mut self,
        msg: caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<u8, usize>, &'md ()>,
    ) -> Result<(), Self::Error> {
        for entry in self.entries.iter() {
            entry
                .send(Message {
                    msg: msg.clone().map_meta(|_| ()),
                    ret: self.sender.clone(),
                })
                .unwrap();
        }

        Ok(())
    }

    async fn try_recv(
        &mut self,
    ) -> Result<caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<u8, usize>, ()>, Self::Error>
    {
        self.receiver.recv().map_err(|_e| ())
    }
}

impl caspaxos::Cluster<u8, usize, ()> for TestCluster {
    type Quorum<'q> = TestQuorum<u8, usize>;

    fn hash(&self) -> caspaxos::internals::ClusterHash {
        ClusterHash(0)
    }

    fn size(&self) -> usize {
        self.nodes.len()
    }

    fn quorum<'q, 's>(&'s mut self, size: usize) -> Option<Self::Quorum<'q>>
    where
        's: 'q,
    {
        if self.nodes.len() < size {
            return None;
        }

        let (tx, rx) = mpsc::channel();

        Some(TestQuorum {
            entries: self.nodes[0..size].to_vec(),
            receiver: rx,
            sender: tx,
        })
    }
}

#[derive(Debug, Clone)]
enum Operation {
    Set(usize),
    // Assert(usize),
    Print,
}

struct Message {
    msg: caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<u8, usize>, ()>,
    ret: mpsc::Sender<caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<u8, usize>, ()>>,
}

#[tracing::instrument(skip(recv))]
fn acceptor(idx: usize, recv: mpsc::Receiver<Message>) {
    let mut acceptor = caspaxos::internals::Acceptor::new();

    tracing::info!("Starting Acceptor {}", idx);

    while let Ok(raw_msg) = recv.recv() {
        let msg = raw_msg.msg;
        let tx = raw_msg.ret;

        match msg.content {
            caspaxos::msgs::ProposerMessage::Prepare(pmsg) => {
                let resp = acceptor.recv_prepare(pmsg);
                tx.send(caspaxos::msgs::Message {
                    ballot: msg.ballot,
                    content: caspaxos::msgs::AcceptorMessage::Promise(resp),
                    metadata: (),
                })
                .unwrap();
            }
            caspaxos::msgs::ProposerMessage::Accept(amsg) => {
                let resp = acceptor.recv_accept(amsg);
                tx.send(caspaxos::msgs::Message {
                    ballot: msg.ballot,
                    content: caspaxos::msgs::AcceptorMessage::Accepted(resp),
                    metadata: (),
                })
                .unwrap();
            }
        };
    }

    tracing::info!("Finished Acceptor {:?}", idx);
}

#[tracing::instrument(skip(recv, cluster))]
fn proposer(idx: u8, recv: mpsc::Receiver<Operation>, cluster: &mut TestCluster) {
    let mut proposer = caspaxos::ProposeClient::new(idx);

    tracing::info!("Started Proposer {}", idx);

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    while let Ok(op) = recv.recv() {
        tracing::debug!("[{}] Received Operation {:?}", idx, op);

        rt.block_on(async {
            match op {
                Operation::Set(value) => loop {
                    match proposer.propose(cluster, |_| value, &()).await {
                        Ok(_) => break,
                        Err(e) => {
                            tracing::error!("[{}] Retrying Set because {:?}", idx, e);
                        }
                    };
                },
                /*
                Operation::Assert(a_value) => loop {
                    match proposer
                        .propose(cluster, |lvalue| {
                            assert_eq!(Some(a_value), lvalue);
                            a_value
                        })
                        .await
                    {
                        Ok(_) => break,
                        Err(e) => {
                            tracing::error!("[{}] Retrying Assert because {:?}", idx, e);
                        }
                    };
                },
                */
                Operation::Print => loop {
                    match proposer
                        .propose(
                            cluster,
                            |value| {
                                tracing::info!("Printing from {}: {:?}", idx, value);

                                value.unwrap_or_default()
                            },
                            &(),
                        )
                        .await
                    {
                        Ok(_) => break,
                        Err(e) => {
                            tracing::error!("[{}] Retrying Print because {:?}", idx, e);
                        }
                    };
                },
            };
        });

        tracing::info!("[{}] Finished Operation", idx);
    }
}

fn main() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(tracing_subscriber::fmt::layer()),
    )
    .unwrap();

    let (a_handles, acceptors): (Vec<_>, Vec<_>) = (0..5)
        .map(|idx| {
            let (tx, rx) = mpsc::channel();

            let handle = std::thread::spawn(move || acceptor(idx, rx));

            (handle, tx)
        })
        .unzip();

    let cluster = TestCluster { nodes: acceptors };

    std::thread::scope(|scope| {
        let proposer: Vec<_> = (0..4)
            .map(|idx| {
                let (tx, rx) = mpsc::channel();

                let mut acceptor_ref = cluster.clone();

                scope.spawn(move || proposer(idx, rx, &mut acceptor_ref));

                tx
            })
            .collect();

        for _ in 0..100 {
            let pidx = rand::thread_rng().gen_range(0..proposer.len());

            let op = match rand::thread_rng().gen_range(0..2) {
                0 => {
                    let value = rand::thread_rng().gen();
                    Operation::Set(value)
                }
                1 => Operation::Print,
                _ => unreachable!(),
            };

            tracing::debug!("Added Operation: {:?} to proposer {}", op, pidx);
            proposer[pidx].send(op).unwrap();

            std::thread::sleep(Duration::from_millis(25));
        }
    });

    drop(cluster);

    for handle in a_handles {
        handle.join().unwrap();
    }
}
