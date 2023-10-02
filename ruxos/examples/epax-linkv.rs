#![feature(async_fn_in_trait)]

use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    sync::{Arc, Mutex},
    time::Duration,
};

use rand::{seq::SliceRandom, Rng, SeedableRng};
use ruxos::epaxos::{self};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

// { "src": "c1", "dest": "s1", "body": {"type":"init","msg_id":1,"node_id":"n3","node_ids":["n1", "n2", "n3"]}}

#[derive(Debug, Clone)]
enum SendMessage {
    ClusterRequest {
        target: String,
        message: epaxos::msgs::Request<String, KVOp<usize, usize>>,
        responses:
            tokio::sync::mpsc::UnboundedSender<epaxos::msgs::Response<String, KVOp<usize, usize>>>,
    },
    ClusterResponse {
        target: String,
        req_id: u64,
        message: epaxos::msgs::Response<String, KVOp<usize, usize>>,
    },
    Response(
        maelstrom_api::Message<
            maelstrom_api::workflow::linear_kv::Request<
                epaxos::msgs::Message<String, KVOp<usize, usize>>,
            >,
        >,
        maelstrom_api::workflow::linear_kv::Response,
    ),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum KVOp<K, V> {
    Read { key: K },
    Write { key: K, value: V },
    Cas { key: K, from: V, to: V },
    Noop,
}

impl<K> epaxos::Operation<HashMap<K, usize>> for KVOp<K, usize>
where
    K: Hash + Eq + Clone + Debug,
{
    type ApplyResult = maelstrom_api::workflow::linear_kv::Response;

    const TRANSITIVE: bool = true;

    fn noop() -> Self {
        Self::Noop
    }

    fn interfere(&self, _other: &Self) -> bool {
        /*
        let own_key = match self {
            Self::Read { key } => key,
            Self::Write { key, .. } => key,
            Self::Cas { key, .. } => key,
        };

        let other_key = match other {
            Self::Read { key } => key,
            Self::Write { key, .. } => key,
            Self::Cas { key, .. } => key,
        };

        own_key == other_key
        */
        true
    }

    fn apply(&mut self, state: &mut HashMap<K, usize>) -> Self::ApplyResult {
        match self {
            Self::Read { key } => match state.get(key) {
                Some(v) => maelstrom_api::workflow::linear_kv::Response::ReadOk { value: *v },
                None => maelstrom_api::workflow::linear_kv::Response::Error {
                    code: 20,
                    text: format!("Unknown Key {:?}", key),
                },
            },
            Self::Write { key, value } => {
                state.insert(key.clone(), value.clone());
                maelstrom_api::workflow::linear_kv::Response::WriteOk
            }
            Self::Cas { key, from, to } => match state.get_mut(key) {
                Some(v) => {
                    if v == from {
                        *v = to.clone();
                        maelstrom_api::workflow::linear_kv::Response::CasOk
                    } else {
                        maelstrom_api::workflow::linear_kv::Response::Error {
                            code: 22,
                            text: format!(
                                "Found Value {:?} instead of {:?} for key {:?}",
                                v, from, key
                            ),
                        }
                    }
                }
                None => maelstrom_api::workflow::linear_kv::Response::Error {
                    code: 20,
                    text: format!("Unknown Key {:?}", key),
                },
            },
            Self::Noop => maelstrom_api::workflow::linear_kv::Response::Error {
                code: 13,
                text: "Running Noop".to_string(),
            },
        }
    }
}

struct MCluster {
    msg_tx: tokio::sync::mpsc::UnboundedSender<SendMessage>,
    other_nodes: Vec<String>,
    rng: rand::rngs::SmallRng,
}

struct MReceiver {
    responses:
        tokio::sync::mpsc::UnboundedReceiver<epaxos::msgs::Response<String, KVOp<usize, usize>>>,
}

impl epaxos::Cluster<String, KVOp<usize, usize>> for MCluster {
    type Error = ();
    type Receiver<'r> = MReceiver;

    fn size(&self) -> usize {
        self.other_nodes.len() + 1
    }

    async fn send<'s, 'r>(
        &'s mut self,
        msg: epaxos::msgs::Request<String, KVOp<usize, usize>>,
        count: usize,
        local: &String,
    ) -> Result<Self::Receiver<'r>, Self::Error>
    where
        's: 'r,
    {
        let nodes = self.other_nodes.choose_multiple(&mut self.rng, count);

        let (tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();

        // Send the messages somehow
        for node in nodes {
            self.msg_tx.send(SendMessage::ClusterRequest {
                target: node.clone(),
                message: msg.clone(),
                responses: tx.clone(),
            });
        }

        Ok(MReceiver { responses: resp_rx })
    }
}

impl epaxos::ResponseReceiver<String, KVOp<usize, usize>> for MReceiver {
    async fn recv(&mut self) -> Result<epaxos::msgs::Response<String, KVOp<usize, usize>>, ()> {
        let raw_res = tokio::time::timeout(Duration::from_millis(100), self.responses.recv()).await;

        match raw_res {
            Ok(Some(r)) => Ok(r),
            Ok(None) => Err(()),
            Err(e) => {
                tracing::error!("Timedout {:?}", e);
                Err(())
            }
        }
    }
}

fn main() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_writer(|| std::io::stderr())
                .with_ansi(false),
        ),
    )
    .unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .thread_keep_alive(Duration::MAX)
        .build()
        .unwrap();

    let (mut m_sender, mut m_receiver) = maelstrom_api::io_recv_send();

    let (msg_tx, mut msg_rx) = tokio::sync::mpsc::unbounded_channel();

    let ((mut listener, node), mut cluster, node_id) =
        maelstrom_api::init(&mut m_receiver, &mut m_sender, |id, nodes| {
            // TODO
            let tmp = epaxos::new::<_, KVOp<usize, usize>, _>(id.to_owned(), HashMap::new());
            let cluster = MCluster {
                msg_tx: msg_tx.clone(),
                other_nodes: nodes.iter().filter(|n| n != &id).cloned().collect(),
                rng: rand::rngs::SmallRng::from_entropy(),
            };

            (tmp, cluster, id.to_owned())
        })
        .unwrap();

    let listener_handle = listener.handle();

    let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<(
        maelstrom_api::Message<maelstrom_api::workflow::linear_kv::Request<_>>,
        _,
    )>();

    runtime.spawn_blocking(move || loop {
        listener.poll();
    });

    let response_map = Arc::new(Mutex::new(HashMap::<
        _,
        tokio::sync::mpsc::UnboundedSender<_>,
    >::new()));
    let response_map2 = response_map.clone();

    let msg_tx2 = msg_tx.clone();
    runtime.spawn_blocking(move || {
        let msg_tx = msg_tx2;

        while let Ok(msg) = m_receiver.recv::<maelstrom_api::workflow::linear_kv::Request<
            epaxos::msgs::Message<String, KVOp<usize, usize>>,
        >>() {
            match msg.body().content() {
                maelstrom_api::workflow::linear_kv::Request::Custom { content } => {
                    match content {
                        epaxos::msgs::Message::Request(req) => {
                            let (tx, rx) = tokio::sync::oneshot::channel();
                            listener_handle.raw_feed(req.clone(), tx);

                            let resp = match rx.blocking_recv() {
                                Ok(r) => r,
                                Err(e) => {
                                    tracing::error!("Error processing Request");
                                    continue;
                                }
                            };

                            msg_tx.send(SendMessage::ClusterResponse {
                                target: msg.src().to_owned(),
                                req_id: msg.body().id().unwrap(),
                                message: resp,
                            });
                        }
                        epaxos::msgs::Message::Response(resp) => {
                            let msg_id = match msg.body().replied_to() {
                                Some(i) => i,
                                None => continue,
                            };

                            let guard = response_map.lock().unwrap();
                            match guard.get(&msg_id) {
                                Some(tx) => {
                                    tx.send(resp.clone());
                                }
                                None => {
                                    tracing::error!("Unknown Response {:?}", msg_id,);
                                }
                            };
                            drop(guard);
                        }
                    };
                }
                maelstrom_api::workflow::linear_kv::Request::Read { key } => {
                    let op = KVOp::Read { key: *key };
                    req_tx.send((msg, op));
                }
                maelstrom_api::workflow::linear_kv::Request::Write { key, value } => {
                    let op = KVOp::Write {
                        key: *key,
                        value: *value,
                    };
                    req_tx.send((msg, op));
                }
                maelstrom_api::workflow::linear_kv::Request::Cas { key, from, to } => {
                    let op = KVOp::Cas {
                        key: *key,
                        from: *from,
                        to: *to,
                    };
                    req_tx.send((msg, op));
                }
            };
        }
    });

    let response_map = response_map2;
    runtime.spawn(async move {
        let mut msg_id = 1;

        while let Some(msg) = msg_rx.recv().await {
            match msg {
                SendMessage::Response(src_msg, content) => {
                    msg_id += 1;
                    let resp = src_msg.reply(src_msg.body().reply(msg_id, content));
                    m_sender.send(resp);
                }
                SendMessage::ClusterRequest {
                    target,
                    message,
                    responses,
                } => {
                    msg_id += 1;

                    let resp = maelstrom_api::Message::new(
                        node_id.clone(),
                        target,
                        maelstrom_api::MessageBody::new(
                            Some(msg_id),
                            None,
                            maelstrom_api::workflow::linear_kv::Request::Custom {
                                content: epaxos::msgs::Message::Request(message),
                            },
                        ),
                    );
                    // Register for responses
                    let mut guard = response_map.lock().unwrap();
                    assert!(guard.insert(msg_id, responses).is_none());
                    drop(guard);
                    m_sender.send(resp);
                }
                SendMessage::ClusterResponse {
                    target,
                    req_id,
                    message,
                } => {
                    msg_id += 1;

                    let resp = maelstrom_api::Message::new(
                        node_id.clone(),
                        target,
                        maelstrom_api::MessageBody::new(
                            Some(msg_id),
                            Some(req_id),
                            maelstrom_api::workflow::linear_kv::Request::Custom {
                                content: epaxos::msgs::Message::Response(message),
                            },
                        ),
                    );
                    m_sender.send(resp);
                }
            };
        }
    });

    runtime.block_on(async move {
        while let Some((msg, req)) = req_rx.recv().await {
            tracing::info!("Requested: {:?}", req);

            match node.request(req.clone(), &mut cluster).await {
                Ok(c) => {
                    tracing::info!("Committed Operation");

                    let result = loop {
                        let handle = match c.try_execute() {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::error!("Executing Operation: {:?}", e);
                                continue;
                            }
                        };

                        let result = match handle.await {
                            Ok(r) => r,
                            Err(epaxos::TryExecuteError::UnknownCommand(instance)) => {
                                tracing::error!("Unknown Command {:?}", instance);
                                match node.explicit_prepare(&mut cluster, instance).await {
                                    Ok(_) => {}
                                    Err(_) => {
                                        tracing::error!("Failed Explicit prepare");
                                    }
                                };

                                tokio::time::sleep(Duration::from_micros(
                                    rand::thread_rng().gen_range(100..1000),
                                ))
                                .await;

                                continue;
                            }
                            Err(epaxos::TryExecuteError::NotCommited(instance, c)) => {
                                tracing::error!("Command not commited {:?}", instance);
                                match node.explicit_prepare(&mut cluster, instance).await {
                                    Ok(_) => {}
                                    Err(_) => {
                                        tracing::error!("Failed Explicit prepare");
                                    }
                                };

                                tokio::time::sleep(Duration::from_micros(
                                    rand::thread_rng().gen_range(100..1000),
                                ))
                                .await;

                                continue;
                            }
                            Err(e) => {
                                tracing::error!("Other Error {:?}", e);
                                break maelstrom_api::workflow::linear_kv::Response::Error {
                                    code: 13,
                                    text: "Help".to_string(),
                                };
                            }
                        };

                        break result;
                    };

                    tracing::info!("Executed Operation: {:?}", result);

                    msg_tx.send(SendMessage::Response(msg, result));
                }
                Err((e, instance)) => {
                    tracing::error!("Error Requesting: {:?}", e);

                    let resp = loop {
                        let res = match node.explicit_prepare(&mut cluster, instance.clone()).await
                        {
                            Ok(handle) => {
                                if handle.op() == &req {
                                    handle
                                } else {
                                    break maelstrom_api::workflow::linear_kv::Response::Error {
                                        code: 13,
                                        text: format!("Performing Request: {:?}", e),
                                    };
                                }
                            }
                            Err(e) => {
                                continue;
                            }
                        };

                        let res = loop {
                            let handle = match res.try_execute() {
                                Ok(r) => r,
                                Err(e) => {
                                    break maelstrom_api::workflow::linear_kv::Response::Error {
                                        code: 13,
                                        text: format!("Performing Request: {:?}", e),
                                    }
                                }
                            };

                            match handle.await {
                                Ok(r) => break r,
                                Err(epaxos::TryExecuteError::UnknownCommand(instance))
                                | Err(epaxos::TryExecuteError::NotCommited(instance, _)) => {
                                    tracing::error!("Unknown Command {:?}", instance);
                                    match node.explicit_prepare(&mut cluster, instance).await {
                                        Ok(_) => {}
                                        Err(_) => {
                                            tracing::error!("Failed Explicit prepare");
                                        }
                                    };

                                    tokio::time::sleep(Duration::from_micros(
                                        rand::thread_rng().gen_range(100..1000),
                                    ))
                                    .await;

                                    continue;
                                }
                                Err(e) => {
                                    break maelstrom_api::workflow::linear_kv::Response::Error {
                                        code: 13,
                                        text: format!("Performing Request: {:?}", e),
                                    }
                                }
                            }
                        };

                        break res;
                    };

                    msg_tx.send(SendMessage::Response(msg, resp));
                }
            };
        }
    });

    eprintln!("Stopping...");
    runtime.shutdown_timeout(Duration::from_secs(2));
    eprintln!("Stopped");
}
