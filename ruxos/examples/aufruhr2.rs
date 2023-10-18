#![feature(async_fn_in_trait)]

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    hash::Hash,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::{Duration, Instant},
};

use aufruhr2::checker::Checker;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use ruxos::epaxos;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum KVOp<K, V> {
    Read { key: K },
    Write { key: K, value: V },
    Cas { key: K, from: V, to: V },
    Noop,
}

impl<K> aufruhr2::checker::Operation<HashMap<K, usize>> for KVOp<K, usize>
where
    K: Eq + Hash + Clone + Display,
{
    type Result = maelstrom_api::workflow::linear_kv::Response;

    fn apply(&self, state: &mut HashMap<K, usize>) -> Self::Result {
        match self {
            Self::Read { key } => match state.get(key) {
                Some(v) => maelstrom_api::workflow::linear_kv::Response::ReadOk { value: *v },
                None => maelstrom_api::workflow::linear_kv::Response::Error {
                    code: 20,
                    text: format!("Unknown Key {}", key),
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
                                "Found Value {:?} instead of {:?} for key {}",
                                v, from, key
                            ),
                        }
                    }
                }
                None => maelstrom_api::workflow::linear_kv::Response::Error {
                    code: 20,
                    text: format!("Unknown Key {}", key),
                },
            },
            Self::Noop => maelstrom_api::workflow::linear_kv::Response::Error {
                code: 13,
                text: "Running Noop".to_string(),
            },
        }
    }
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

    fn interfere(&self, other: &Self) -> bool {
        let own_key = match self {
            Self::Read { key } => key,
            Self::Write { key, .. } => key,
            Self::Cas { key, .. } => key,
            Self::Noop => return false,
        };

        let other_key = match other {
            Self::Read { key } => key,
            Self::Write { key, .. } => key,
            Self::Cas { key, .. } => key,
            Self::Noop => return false,
        };

        own_key == other_key
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

struct AufruhrCluster {
    other_nodes: Vec<SocketAddr>,
}
struct AufruhrReceiver {
    connections: Vec<aufruhr2::net::TcpStream>,
}
impl epaxos::Cluster<usize, KVOp<usize, usize>> for AufruhrCluster {
    type Error = ();
    type Receiver<'r> = AufruhrReceiver;
    fn size(&self) -> usize {
        self.other_nodes.len() + 1
    }
    async fn send<'s, 'r>(
        &'s mut self,
        msg: epaxos::msgs::Request<usize, KVOp<usize, usize>>,
        count: usize,
        local: &usize,
    ) -> Result<Self::Receiver<'r>, Self::Error>
    where
        's: 'r,
    {
        let mut connections = Vec::new();

        let serde_msg = serde_json::to_vec(&msg).unwrap();
        let msg_len = (serde_msg.len() as u32).to_be_bytes();

        for addr in self.other_nodes.iter().take(count) {
            let mut con = aufruhr2::net::TcpStream::connect(addr).await.unwrap();

            con.write(&msg_len).await.unwrap();
            con.write(&serde_msg).await.unwrap();

            connections.push(con);
        }

        Ok(AufruhrReceiver { connections })
    }
}
impl epaxos::ResponseReceiver<usize, KVOp<usize, usize>> for AufruhrReceiver {
    async fn recv(&mut self) -> Result<epaxos::msgs::Response<usize, KVOp<usize, usize>>, ()> {
        let mut con = self.connections.pop().unwrap();

        let mut size_buf = [0; 4];
        let read = con.read(&mut size_buf).await.map_err(|e| ())?;
        assert_eq!(4, read);

        let size = u32::from_be_bytes(size_buf);

        let mut buf = vec![0; size as usize];
        let read = con.read(&mut buf).await.unwrap();
        assert_eq!(size as usize, read);

        serde_json::from_slice(&buf).map_err(|e| ())
    }
}

const REQ_PORT: u16 = 80;
const BACKGROUND_PORT: u16 = 88;

fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::fmt().finish()).unwrap();

    let mut simulation = aufruhr2::Simulation::<
        KVOp<usize, usize>,
        maelstrom_api::workflow::linear_kv::Response,
    >::new();

    let server: Vec<_> = (0..3).map(|i| Ipv4Addr::new(0, 0, 0, i)).collect();

    for i in 0..3 {
        let ip = server[i].clone();

        let cluster = AufruhrCluster {
            other_nodes: server
                .iter()
                .take(i)
                .chain(server.iter().skip(i + 1))
                .map(|ip| SocketAddr::V4(SocketAddrV4::new(ip.clone(), BACKGROUND_PORT)))
                .collect(),
        };

        simulation.server(async move {
            let (mut listener, handle) = epaxos::new(i, HashMap::new());

            let handle = Arc::new(handle);

            aufruhr2::runtime::spawn(async move {
                let mut listener = aufruhr2::net::TcpListener::bind(SocketAddr::V4(
                    SocketAddrV4::new(ip, REQ_PORT),
                ))
                .unwrap();

                loop {
                    let (mut con, _) = listener.accept().await.unwrap();

                    let handle = handle.clone();

                    let mut cluster = AufruhrCluster {
                        other_nodes: cluster.other_nodes.clone(),
                    };

                    aufruhr2::runtime::spawn(async move {
                        // Receive the request from the client and handle it
                        let mut size_buf = [0; 4];
                        let read = con.read(&mut size_buf).await.unwrap();
                        assert_eq!(4, read);

                        let size = u32::from_be_bytes(size_buf);

                        let mut buf = vec![0; size as usize];
                        let read = con.read(&mut buf).await.unwrap();
                        assert_eq!(size, read as u32);

                        let msg: KVOp<usize, usize> = serde_json::from_slice(&buf).unwrap();

                        let chandle = match handle.request(msg, &mut cluster).await {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::error!("Requesting Operation");
                                return;
                            }
                        };

                        let mut attempts = 0;

                        let res = loop {
                            let execute_handle = match chandle.try_execute() {
                                Ok(handle) => handle,
                                Err(e) => {
                                    tracing::error!("Trying Execute");
                                    return;
                                }
                            };

                            match execute_handle.await {
                                Ok(r) => break r,
                                Err(e) => {
                                    match e {
                                        epaxos::TryExecuteError::UnknownCommand(instance)
                                        | epaxos::TryExecuteError::NotCommited(instance, _) => {
                                            match handle
                                                .explicit_prepare(&mut cluster, instance)
                                                .await
                                            {
                                                Ok(_) => {}
                                                Err(_) => {}
                                            };
                                        }
                                        epaxos::TryExecuteError::Other(msg) => {
                                            tracing::error!("Other {:?}", msg);
                                            return;
                                        }
                                    };
                                }
                            }

                            tokio::time::sleep(Duration::from_micros(
                                rand::thread_rng().gen_range(5..25),
                            ))
                            .await;

                            attempts += 1;

                            if attempts > 10 {
                                tracing::error!("Exceeded 100 attempts");
                                return;
                            }
                        };

                        let res_msg = serde_json::to_vec(&res).unwrap();
                        let res_size = (res_msg.len() as u32).to_be_bytes();

                        con.write(&res_size).await.unwrap();
                        con.write(&res_msg).await.unwrap();
                    });
                }
            });

            let listener_handle = listener.handle();
            aufruhr2::runtime::spawn(async move {
                let mut listener = aufruhr2::net::TcpListener::bind(SocketAddr::V4(
                    SocketAddrV4::new(ip, BACKGROUND_PORT),
                ))
                .unwrap();

                loop {
                    let (mut con, _) = listener.accept().await.unwrap();

                    let listener_handle = listener_handle.clone();

                    aufruhr2::runtime::spawn(async move {
                        // Receive the message from the other server and handle it
                        let mut size_buf = [0; 4];
                        let read = con.read(&mut size_buf).await.unwrap();
                        assert_eq!(4, read);

                        let size = u32::from_be_bytes(size_buf);

                        let mut buf = vec![0; size as usize];
                        let read = con.read(&mut buf).await.unwrap();
                        assert_eq!(size, read as u32);

                        let req = serde_json::from_slice(&buf).unwrap();

                        let resp = match listener_handle.feed(req).await {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::error!("Could not feed request into system");
                                return;
                            }
                        };

                        let serd_resp = serde_json::to_vec(&resp).unwrap();

                        let send_size = (serd_resp.len() as u32).to_be_bytes();

                        con.write(&send_size).await;
                        con.write(&serd_resp).await;
                    });
                }
            });

            aufruhr2::runtime::spawn(async move {
                loop {
                    listener.poll().await;
                }
            });
        });
    }

    const CLIENTS: i32 = 10;
    const OPSPERCLIENT: i32 = 1000;
    const KEYS: [usize; 5] = [0, 1, 2, 3, 4];

    let mut seed_rng = rand::rngs::SmallRng::seed_from_u64(2983471894123123);
    for i in 0..CLIENTS {
        let server = server.clone();

        let mut rng = rand::rngs::SmallRng::from_rng(&mut seed_rng).unwrap();
        simulation.client(|tracker| async move {
            tokio::task::yield_now().await;

            let start = Instant::now();
            for req in 0..OPSPERCLIENT {
                let target = &server[rng.gen_range(0..3)];

                let key = KEYS.choose(&mut rng).copied().unwrap();
                let op = match rng.gen_range(0..3) {
                    0 => KVOp::Read { key },
                    1 => KVOp::Write {
                        key,
                        value: rng.gen_range(0..20),
                    },
                    2 => KVOp::Cas {
                        key,
                        from: rng.gen_range(0..20),
                        to: rng.gen_range(0..20),
                    },
                    _ => unreachable!(),
                };

                let op_tracking = tracker.start_op(op.clone());

                let mut con = aufruhr2::net::TcpStream::connect(SocketAddr::V4(SocketAddrV4::new(
                    target.clone(),
                    REQ_PORT,
                )))
                .await
                .unwrap();

                let req_msg = serde_json::to_vec(&op).unwrap();
                let req_len = (req_msg.len() as u32).to_be_bytes();

                con.write(&req_len).await.unwrap();
                con.write(&req_msg).await.unwrap();

                let mut size_buf = [0; 4];
                match con.read(&mut size_buf).await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("Receiving Response");
                        continue;
                    }
                };
                let size = u32::from_be_bytes(size_buf);

                let mut buf = vec![0; size as usize];
                con.read(&mut buf).await.unwrap();

                let resp: maelstrom_api::workflow::linear_kv::Response =
                    serde_json::from_slice(&buf).unwrap();

                tracing::info!("Response {:?}", resp);

                op_tracking.stop(resp);
            }
            let elapsed = start.elapsed();

            tokio::time::sleep(Duration::from_secs(5)).await;

            tracing::info!(
                "Executed {} Operations in {:?} => {}ops/s",
                OPSPERCLIENT,
                elapsed,
                (OPSPERCLIENT as f64 / elapsed.as_secs_f64())
            );
        });
    }

    let result = simulation.run();
    let operations = &result.history;

    // tracing::info!("History: {:#?}", operations);
    tracing::info!("History length: {:?}", operations.len());
    tracing::info!(
        "OKs {:?}",
        operations
            .events
            .iter()
            .filter(|event| match event {
                aufruhr2::checker::OperationEvent::Return { result, .. } => match result {
                    maelstrom_api::workflow::linear_kv::Response::WriteOk => true,
                    maelstrom_api::workflow::linear_kv::Response::ReadOk { .. } => true,
                    maelstrom_api::workflow::linear_kv::Response::CasOk { .. } => true,
                    _ => false,
                },
                _ => false,
            })
            .count()
    );
    tracing::info!(
        "Errors {:?}",
        operations
            .events
            .iter()
            .filter(|event| match event {
                aufruhr2::checker::OperationEvent::Return { result, .. } => match result {
                    maelstrom_api::workflow::linear_kv::Response::WriteOk => false,
                    maelstrom_api::workflow::linear_kv::Response::ReadOk { .. } => false,
                    maelstrom_api::workflow::linear_kv::Response::CasOk { .. } => false,
                    _ => true,
                },
                _ => false,
            })
            .count()
    );

    let network_percentiles = result.network_latency_percentiles([1.0, 50.0, 95.0, 99.0]);
    tracing::info!("[Network] 1th Percentile: {:?}", network_percentiles[0]);
    tracing::info!("[Network] 50th Percentile: {:?}", network_percentiles[1]);
    tracing::info!("[Network] 95th Percentile: {:?}", network_percentiles[2]);
    tracing::info!("[Network] 99th Percentile: {:?}", network_percentiles[3]);

    let latencies = operations.latencies();
    tracing::info!("Min: {:?}", latencies.values().min().unwrap());
    tracing::info!("Max: {:?}", latencies.values().max().unwrap());
    let mut latency_list: Vec<_> = latencies.values().cloned().collect();
    latency_list.sort();
    tracing::info!(
        "50th Percentile: {:?}",
        latency_list.get(latency_list.len() / 2)
    );
    tracing::info!(
        "90th Percentile {:?}",
        latency_list.get((latency_list.len() as f64 / 100.0 * 90.0) as usize)
    );

    /*
    let serializable = aufruhr2::checker::serialize::SerializeChecker::new().check(&operations);
    if serializable {
        println!("Serializable");
    } else {
        panic!("Not Serializable");
    }
    */

    let linearizable =
        aufruhr2::checker::linearizable::LinearizableChecker::new().check(&operations);
    if linearizable {
        println!("Linearizable");
    } else {
        println!("History: {:#?}", operations);
        panic!("Not Linearizable");
    }
}
