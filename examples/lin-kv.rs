#![feature(async_fn_in_trait)]
#![feature(impl_trait_projections)]

use rand::Rng;
use ruxos::{
    caspaxos::{self, internals::ClusterHash, ProposeClient},
    retry::RetryStrategy,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{BufRead, Write},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Metadata {
    key: u64,
}

type ValueType = Option<u64>;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
struct Key(u64);

fn reservoir_sample<IT, T>(iter: IT, size: usize) -> Vec<T>
where
    IT: IntoIterator<Item = T>,
{
    let mut result = Vec::with_capacity(size);

    let mut iter = iter.into_iter();
    result.extend(iter.by_ref().take(size));

    let mut rng = rand::thread_rng();
    for (i, value) in iter.enumerate().map(|(i, v)| (i + size, v)) {
        let j = rng.gen_range(0..i);
        if j < size {
            result[j] = value;
        }
    }

    result
}

#[derive(Debug, Deserialize, Serialize)]
enum PaxosBody {
    PaxosProposer(
        caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<String, ValueType>, Metadata>,
    ),
    PaxosAcceptor(
        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<String, ValueType>, Metadata>,
    ),
}

#[derive(Debug, Serialize, Deserialize)]
struct Message<B> {
    dest: String,
    src: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    body: Combination<B>,
}

type Request = Message<MessageBody>;

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum Combination<B> {
    Body(B),
    Paxos(PaxosBody),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum MessageBody {
    #[serde(rename = "init")]
    Init {
        msg_id: u64,
        node_id: String,
        node_ids: Vec<String>,
    },
    #[serde(rename = "read")]
    Read { key: u64, msg_id: u64 },
    #[serde(rename = "write")]
    Write { key: u64, value: u64, msg_id: u64 },
    #[serde(rename = "cas")]
    Cas {
        key: u64,
        from: u64,
        to: u64,
        msg_id: u64,
    },
}

fn receiver(
    tx: std::sync::mpsc::Sender<Request>,
    paxos_tx: std::sync::mpsc::Sender<
        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<String, ValueType>, Metadata>,
    >,
    propose_tx: std::sync::mpsc::Sender<(
        String,
        caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<String, ValueType>, Metadata>,
    )>,
) {
    let mut stdin = std::io::stdin().lock();

    loop {
        let line = {
            let mut tmp = String::new();
            match stdin.read_line(&mut tmp) {
                Ok(_) => tmp,
                Err(e) => {
                    panic!("Reading Line: {:?}", e);
                }
            }
        };

        let message: Request = serde_json::from_str(&line).unwrap();

        match message.body {
            Combination::Body(b) => {
                let msg = Request {
                    dest: message.dest,
                    src: message.src,
                    id: message.id,
                    body: Combination::Body(b),
                };

                if let Err(e) = tx.send(msg) {
                    panic!("Receiving Message: {:?}", e);
                }
            }
            Combination::Paxos(p) => match p {
                PaxosBody::PaxosProposer(pmsg) => {
                    if let Err(e) = propose_tx.send((message.src, pmsg)) {
                        panic!("Receiving Message: {:?}", e);
                    }
                }
                PaxosBody::PaxosAcceptor(amsg) => {
                    if let Err(e) = paxos_tx.send(amsg) {
                        panic!("Receiving Message: {:?}", e);
                    }
                }
            },
        }
    }
}

type Response = Message<ResponseBody>;

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum ResponseBody {
    #[serde(rename = "init_ok")]
    InitOk { in_reply_to: u64 },
    #[serde(rename = "read_ok")]
    ReadOk { value: u64, in_reply_to: u64 },
    #[serde(rename = "write_ok")]
    WriteOk { in_reply_to: u64 },
    #[serde(rename = "cas_ok")]
    CasOk { in_reply_to: u64 },
    #[serde(rename = "error")]
    Error {
        in_reply_to: u64,
        code: u64,
        text: String,
    },
}

fn sender(rx: std::sync::mpsc::Receiver<Response>) {
    let mut stdout = std::io::stdout().lock();

    while let Ok(msg) = rx.recv() {
        serde_json::to_writer(&mut stdout, &msg).unwrap();
        writeln!(&mut stdout).unwrap();
    }
}

struct Cluster {
    nodes: Vec<String>,
    sender: std::sync::mpsc::Sender<Response>,
    receiver: std::sync::mpsc::Receiver<
        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<String, ValueType>, Metadata>,
    >,
    src: String,
}
struct Quorum<'c> {
    sender: &'c std::sync::mpsc::Sender<Response>,
    receiver: &'c std::sync::mpsc::Receiver<
        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<String, ValueType>, Metadata>,
    >,
    nodes: Vec<&'c String>,
    src: &'c str,
}

impl caspaxos::Cluster<String, ValueType, Metadata> for Cluster {
    type Quorum<'q> = Quorum<'q>;

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
        if size > self.nodes.len() {
            return None;
        }

        return Some(Quorum {
            sender: &self.sender,
            receiver: &self.receiver,
            nodes: reservoir_sample(self.nodes.iter(), size),
            src: &self.src,
        });
    }
}

impl<'c> caspaxos::ClusterQuorum<String, ValueType, Metadata> for Quorum<'c> {
    type Error = ();

    async fn send<'m>(
        &mut self,
        msg: caspaxos::msgs::Message<
            caspaxos::msgs::ProposerMessage<String, ValueType>,
            &'m Metadata,
        >,
    ) -> Result<(), Self::Error> {
        for node in self.nodes.iter() {
            let node_msg = Message {
                dest: node.to_string(),
                src: self.src.to_string(),
                id: None,
                body: Combination::Paxos(PaxosBody::PaxosProposer(
                    msg.clone().map_meta(|m| Metadata { key: m.key }),
                )),
            };

            self.sender.send(node_msg).unwrap();
        }

        Ok(())
    }

    async fn try_recv(
        &mut self,
    ) -> Result<
        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<String, ValueType>, Metadata>,
        Self::Error,
    > {
        self.receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| ())
    }
}

fn handler(
    mut cluster: Cluster,
    recv_rx: std::sync::mpsc::Receiver<Request>,
    send_tx: std::sync::mpsc::Sender<Response>,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut proposers: HashMap<u64, ProposeClient<String, ValueType>> = HashMap::new();

    while let Ok(msg) = recv_rx.recv() {
        match msg.body {
            Combination::Body(body) => match body {
                MessageBody::Init { .. } => {
                    // Respond to Init message
                    panic!("Unexpected Init Message");
                }
                MessageBody::Read { key, msg_id } => {
                    let proposer = proposers.entry(key).or_insert_with(|| {
                        ProposeClient::with_config(
                            cluster.src.clone(),
                            caspaxos::config::ProposerConfig::basic()
                                .with_roundtrip(caspaxos::config::OneRoundTrip::Enabled)
                                .with_thrifty(caspaxos::config::Thrifty::AllNodes),
                        )
                    });

                    let msg = match rt.block_on(proposer.propose_with_retry(
                        &mut cluster,
                        |x| x.unwrap_or(None),
                        &Metadata { key },
                        RetryStrategy::new(
                            &mut ruxos::retry::FilteredBackoff::unlimited_no_backoff(),
                        ),
                    )) {
                        Ok(Some(value)) => Response {
                            dest: msg.src,
                            src: msg.dest,
                            id: None,
                            body: Combination::Body(ResponseBody::ReadOk {
                                value,
                                in_reply_to: msg_id,
                            }),
                        },
                        Ok(None) => Response {
                            dest: msg.src,
                            src: msg.dest,
                            id: None,
                            body: Combination::Body(ResponseBody::Error {
                                in_reply_to: msg_id,
                                code: 20,
                                text: format!("Key not found: {:?}", key),
                            }),
                        },
                        Err(e) => Response {
                            dest: msg.src,
                            src: msg.dest,
                            id: None,
                            body: Combination::Body(ResponseBody::Error {
                                in_reply_to: msg_id,
                                code: 13,
                                text: format!("Reading Key: {:?} - {:?}", key, e),
                            }),
                        },
                    };

                    send_tx.send(msg).unwrap();
                }
                MessageBody::Write { key, value, msg_id } => {
                    let proposer = proposers
                        .entry(key)
                        .or_insert_with(|| ProposeClient::new(cluster.src.clone()));

                    let msg = match rt.block_on(proposer.propose_with_retry(
                        &mut cluster,
                        |_| Some(value.clone()),
                        &Metadata { key },
                        RetryStrategy::new(
                            &mut ruxos::retry::FilteredBackoff::unlimited_no_backoff(),
                        ),
                    )) {
                        Ok(val) => {
                            assert_eq!(val, Some(value));

                            Response {
                                dest: msg.src,
                                src: msg.dest,
                                id: None,
                                body: Combination::Body(ResponseBody::WriteOk {
                                    in_reply_to: msg_id,
                                }),
                            }
                        }
                        Err(e) => Response {
                            dest: msg.src,
                            src: msg.dest,
                            id: None,
                            body: Combination::Body(ResponseBody::Error {
                                in_reply_to: msg_id,
                                code: 13,
                                text: format!("Writing Key: {:?}->{:?} - {:?}", key, value, e),
                            }),
                        },
                    };

                    send_tx.send(msg).unwrap();
                }
                MessageBody::Cas {
                    key,
                    from,
                    to,
                    msg_id,
                } => {
                    let proposer = proposers
                        .entry(key)
                        .or_insert_with(|| ProposeClient::new(cluster.src.clone()));

                    let worked = AtomicBool::new(false);
                    let propose_res = rt.block_on(proposer.propose_with_retry(
                        &mut cluster,
                        |val| {
                            if val == Some(Some(from)) {
                                worked.store(true, Ordering::SeqCst);
                                Some(to.clone())
                            } else {
                                worked.store(false, Ordering::SeqCst);
                                val.unwrap_or(None)
                            }
                        },
                        &Metadata { key },
                        RetryStrategy::new(
                            &mut ruxos::retry::FilteredBackoff::unlimited_no_backoff(),
                        ),
                    ));

                    let msg = match propose_res {
                        Ok(Some(val)) if worked.load(Ordering::SeqCst) => {
                            assert_eq!(val, to);

                            Response {
                                dest: msg.src,
                                src: msg.dest,
                                id: None,
                                body: Combination::Body(ResponseBody::CasOk {
                                    in_reply_to: msg_id,
                                }),
                            }
                        }
                        Ok(Some(val)) => {
                            assert_ne!(val, from);

                            Response {
                                dest: msg.src,
                                src: msg.dest,
                                id: None,
                                body: Combination::Body(ResponseBody::Error {
                                    in_reply_to: msg_id,
                                    code: 22,
                                    text: format!(
                                        "Mismatched from value, tried CAS ({:?} -> {:?}) but saw {:?}",
                                        from, to, val
                                    ),
                                }),
                            }
                        }
                        Ok(None) => Response {
                            dest: msg.src,
                            src: msg.dest,
                            id: None,
                            body: Combination::Body(ResponseBody::Error {
                                in_reply_to: msg_id,
                                code: 20,
                                text: format!("Key not found: {:?}", key),
                            }),
                        },
                        Err(e) => Response {
                            dest: msg.src,
                            src: msg.dest,
                            id: None,
                            body: Combination::Body(ResponseBody::Error {
                                in_reply_to: msg_id,
                                code: 13,
                                text: format!("CAS Key: {:?} {:?}->{:?} - {:?}", key, from, to, e),
                            }),
                        },
                    };

                    send_tx.send(msg).unwrap();
                }
            },
            Combination::Paxos(p) => unreachable!("Received Paxos Message {:?}", p),
        };
    }
}

fn acceptor_handler(
    src: String,
    p_rx: std::sync::mpsc::Receiver<(
        String,
        caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<String, ValueType>, Metadata>,
    )>,
    a_tx: std::sync::mpsc::Sender<Response>,
) {
    let mut acceptors = HashMap::new();

    while let Ok((dest, msg)) = p_rx.recv() {
        let key = Key(msg.metadata.key);

        let acceptor = acceptors
            .entry(key.clone())
            .or_insert_with(|| caspaxos::internals::Acceptor::new());

        let resp_body = match msg.content {
            caspaxos::msgs::ProposerMessage::Prepare(pmsg) => {
                caspaxos::msgs::AcceptorMessage::Promise(acceptor.recv_prepare(pmsg))
            }
            caspaxos::msgs::ProposerMessage::Accept(amsg) => {
                caspaxos::msgs::AcceptorMessage::Accepted(acceptor.recv_accept(amsg))
            }
        };

        a_tx.send(Response {
            dest,
            src: src.clone(),
            id: None,
            body: Combination::Paxos(PaxosBody::PaxosAcceptor(caspaxos::msgs::Message {
                ballot: msg.ballot,
                content: resp_body,
                metadata: Metadata { key: key.0 },
            })),
        })
        .unwrap();
    }
}

fn main() {
    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(|| std::io::stderr());

    let sub = tracing_subscriber::registry().with(fmt_layer);

    tracing::subscriber::set_global_default(sub).unwrap();

    let (recv_tx, recv_rx) = std::sync::mpsc::channel();
    let (send_tx, send_rx) = std::sync::mpsc::channel::<Response>();
    let (paxos_tx, paxos_rx) = std::sync::mpsc::channel();
    let (propose_tx, propose_rx) = std::sync::mpsc::channel();

    let recv_handle = std::thread::spawn(move || receiver(recv_tx, paxos_tx, propose_tx));
    let send_handle = std::thread::spawn(move || sender(send_rx));

    let counter = Arc::new(AtomicU64::new(0));

    let cluster = match recv_rx.recv() {
        Ok(msg) => match msg.body {
            Combination::Body(MessageBody::Init {
                msg_id,
                node_id,
                node_ids,
            }) => {
                // Respond to Init message
                send_tx
                    .send(Message {
                        dest: msg.src.clone(),
                        src: node_id.clone(),
                        id: Some(1),
                        body: Combination::Body(ResponseBody::InitOk {
                            in_reply_to: msg_id,
                        }),
                    })
                    .unwrap();

                Cluster {
                    nodes: node_ids,
                    sender: send_tx.clone(),
                    receiver: paxos_rx,
                    src: node_id,
                }
            }
            other => {
                panic!("Expected Init Message {:?}", other);
            }
        },
        Err(e) => {
            panic!("Expected Init Message: {:?}", e);
        }
    };

    let src = cluster.src.clone();
    let a_tx = send_tx.clone();
    let acceptor_handle = std::thread::spawn(move || acceptor_handler(src, propose_rx, a_tx));

    handler(cluster, recv_rx, send_tx);

    for handle in [recv_handle, send_handle, acceptor_handle] {
        handle.join().unwrap();
    }
}
