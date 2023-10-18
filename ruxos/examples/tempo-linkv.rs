use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use ruxos::tempo::{self, replica::Broadcaster};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum KVOp {
    Read { key: usize },
    Write { key: usize, value: usize },
    Cas { key: usize, from: usize, to: usize },
}

impl tempo::Operation<HashMap<usize, usize>> for KVOp {
    type Result = maelstrom_api::workflow::linear_kv::Response;

    fn apply(&self, state: &mut HashMap<usize, usize>) -> Self::Result {
        match self {
            Self::Read { key } => match state.get(key) {
                Some(v) => maelstrom_api::workflow::linear_kv::Response::ReadOk { value: *v },
                None => maelstrom_api::workflow::linear_kv::Response::Error {
                    code: 20,
                    text: format!("Unknown Key {:?}", key),
                },
            },
            Self::Write { key, value } => {
                state.insert(*key, *value);
                maelstrom_api::workflow::linear_kv::Response::WriteOk
            }
            Self::Cas { key, from, to } => match state.get_mut(key) {
                Some(v) if v == from => {
                    *v = *to;
                    maelstrom_api::workflow::linear_kv::Response::CasOk
                }
                Some(v) => maelstrom_api::workflow::linear_kv::Response::Error {
                    code: 22,
                    text: format!("For Key {:?} expected {:?} but found {:?}", key, from, v),
                },
                None => maelstrom_api::workflow::linear_kv::Response::Error {
                    code: 20,
                    text: format!("Unknown Key {:?}", key),
                },
            },
        }
    }
}

pub struct MaelstromBroadcaster {
    node: String,
    inner_handle: tempo::Handle<
        KVOp,
        String,
        HashMap<usize, usize>,
        maelstrom_api::workflow::linear_kv::Response,
    >,
    other: tokio::sync::mpsc::UnboundedSender<OutputMessage>,
}

impl Broadcaster<KVOp, String> for MaelstromBroadcaster {
    fn send(&mut self, target: &String, content: tempo::msgs::Message<KVOp, String>) {
        if target == &self.node {
            if let Err(e) = self.inner_handle.message(content) {
                tracing::error!("Sending Message to local Node");
            }
        } else {
            let _ = self.other.send(OutputMessage::Internal {
                target: target.clone(),
                msg: content,
            });
        }
    }
}

enum OutputMessage {
    Res {
        src_msg: u64,
        target: String,
        res: maelstrom_api::workflow::linear_kv::Response,
    },
    Internal {
        target: String,
        msg: tempo::msgs::Message<KVOp, String>,
    },
}

fn main() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .finish(),
    );

    let (mut tx, mut rx) = maelstrom_api::io_recv_send();

    let (replica, nodes, broadcaster, (node_msg_tx, node_msg_rx)) =
        maelstrom_api::init(&mut rx, &mut tx, |node, cluster| {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

            let replica =
                tempo::replica::Replica::new(node.to_string(), cluster.len(), HashMap::new());
            let handle = replica.handle();

            (
                replica,
                cluster.into_iter().cloned().collect::<BTreeSet<_>>(),
                MaelstromBroadcaster {
                    node: node.to_string(),
                    inner_handle: handle,
                    other: tx.clone(),
                },
                (tx, rx),
            )
        })
        .unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let handle = replica.handle();
    runtime.spawn(async move {
        loop {
            handle.try_execute();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let handle = replica.handle();
    runtime.spawn(async move {
        loop {
            handle.promises();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let handle = replica.handle();
    runtime.spawn(async move {
        let mut node_msg_rx = node_msg_rx;

        while let Some(msg) = node_msg_rx.recv().await {
            match msg {
                OutputMessage::Res {
                    src_msg,
                    res,
                    target,
                } => {
                    let msg = maelstrom_api::Message::new(
                        handle.id().to_string(),
                        target.clone(),
                        maelstrom_api::MessageBody::new(None, Some(src_msg), res),
                    );

                    tx.send(msg);
                }
                OutputMessage::Internal { target, msg } => {
                    let msg = maelstrom_api::Message::new(
                        handle.id().to_string(),
                        target,
                        maelstrom_api::MessageBody::new(
                            None,
                            None,
                            maelstrom_api::workflow::linear_kv::Request::Custom { content: msg },
                        ),
                    );

                    tx.send(msg);
                }
            };
        }
    });

    let handle = replica.handle();
    let node_clone = nodes.clone();
    let runtime_handle = runtime.handle().clone();
    runtime.spawn_blocking(move || {
        while let Ok(tmp) = rx.recv::<
            maelstrom_api::workflow::linear_kv::Request<tempo::msgs::Message<KVOp, String>>
        >() {
            match tmp.body().content() {
                maelstrom_api::workflow::linear_kv::Request::Custom { content } => {
                    handle.message(content.clone());
                }
                other => {
                    let op = match other {
                        maelstrom_api::workflow::linear_kv::Request::Read { key } => KVOp::Read { key: *key },
                        maelstrom_api::workflow::linear_kv::Request::Write { key, value } => KVOp::Write { key: *key, value: *value},
                        maelstrom_api::workflow::linear_kv::Request::Cas { key, from, to } => KVOp::Cas { key: *key, from: *from, to: *to },
                        maelstrom_api::workflow::linear_kv::Request::Custom { content } => unreachable!(),
                    };

                    let nhandle = handle.clone();
                    let node_msg_tx = node_msg_tx.clone();
                    let nodes = node_clone.clone();
                    let src_node = tmp.src().to_string();
                    let src_msg_id = tmp.body().id().unwrap();
                    
                    runtime_handle.spawn(async move {
                        let quorum = nodes.clone();

                        let res = match nhandle.submit(op, quorum).await {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::error!("Submitting {:?}", e);

                                maelstrom_api::workflow::linear_kv::Response::Error { code: 13, text: "IDK".to_string() }
                            }
                        };

                        tracing::debug!("Response {:?}", res);

                        let _ = node_msg_tx.send(OutputMessage::Res { src_msg: src_msg_id, target: src_node, res });
                    });
                }
            };
        }
    });

    runtime.block_on(async move {
        let mut replica = replica;
        let mut broadcaster = broadcaster;

        loop {
            match replica.process(&nodes, &mut broadcaster).await {
                Ok(r) => {}
                Err(e) => {
                    tracing::error!("Error Processing: {:?}", e);
                }
            };
        }
    });
}
