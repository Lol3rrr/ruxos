#![feature(async_fn_in_trait)]

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use aufruhr::{NodeId, OperationResult, ServerHandle};
use rand::seq::SliceRandom;
use ruxos::caspaxos;

const ACCEPTOR_PORT: u16 = 80;
const PROPOSER_PORT: u16 = 81;

struct TestCluster {
    listener: aufruhr::messages::MListener<
        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<NodeId, u64>, ()>,
    >,
    handler: Arc<ServerHandle<Operation>>,
    acceptor_nodes: HashMap<
        NodeId,
        aufruhr::messages::MSender<
            caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<NodeId, u64>, ()>,
        >,
    >,
}

struct TestQuorum<'h> {
    listener: &'h mut aufruhr::messages::MListener<
        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<NodeId, u64>, ()>,
    >,
    nodes: Vec<NodeId>,
    acceptor_nodes: &'h mut HashMap<
        NodeId,
        aufruhr::messages::MSender<
            caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<NodeId, u64>, ()>,
        >,
    >,
}

impl caspaxos::Cluster<NodeId, u64, ()> for TestCluster {
    type Quorum<'q> = TestQuorum<'q> where Self: 'q;

    fn size(&self) -> usize {
        self.handler.all_nodes.len()
    }

    fn quorum<'q, 's>(&'s mut self, size: usize) -> Option<Self::Quorum<'q>>
    where
        's: 'q,
    {
        let nodes: Vec<_> = self
            .handler
            .all_nodes
            .choose_multiple(&mut rand::thread_rng(), size)
            .cloned()
            .collect();

        Some(TestQuorum {
            listener: &mut self.listener,
            nodes,
            acceptor_nodes: &mut self.acceptor_nodes,
        })
    }
}

impl<'h> caspaxos::ClusterQuorum<NodeId, u64, ()> for TestQuorum<'h> {
    type Error = ();

    async fn send<'md>(
        &mut self,
        msg: caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<NodeId, u64>, &'md ()>,
    ) -> Result<(), Self::Error> {
        for node in self.nodes.iter() {
            let tx = self.acceptor_nodes.get_mut(node).unwrap();

            tx.send(msg.clone().map_meta(|_| ())).map_err(|e| ())?;
        }

        Ok(())
    }

    async fn try_recv(
        &mut self,
    ) -> Result<
        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<NodeId, u64>, ()>,
        Self::Error,
    > {
        let (_, msg) = self
            .listener
            .recv_timeout(Duration::from_millis(100))
            .await
            .map_err(|e| ())?;

        Ok(msg)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Generate,
}

impl aufruhr::Operation for Operation {
    type Response = OpResponse;

    fn generate<R>(_rng: &mut R) -> Self
    where
        R: rand::Rng,
    {
        Operation::Generate
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OpResponse {
    GenerateOk { id: u64 },
}

async fn proposer(
    mut ops: aufruhr::OperationHandle<Operation>,
    listener: aufruhr::messages::MListener<
        caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<NodeId, u64>, ()>,
    >,
    handle: Arc<ServerHandle<Operation>>,
) {
    let id = handle.id;

    let mut proposer = caspaxos::ProposeClient::<NodeId, u64>::with_config(
        id,
        caspaxos::config::ProposerConfig::basic()
            .with_roundtrip(caspaxos::config::OneRoundTrip::Enabled),
    );

    let acceptor_connections: HashMap<_, _> = handle
        .all_nodes
        .iter()
        .map(|id| {
            (
                *id,
                aufruhr::messages::MSender::open(&handle, *id, ACCEPTOR_PORT).unwrap(),
            )
        })
        .collect();

    let mut cluster = TestCluster {
        listener,
        handler: handle,
        acceptor_nodes: acceptor_connections,
    };

    while let Some((op, resp_tx)) = ops.recv().await {
        let metadata = ();

        let resp = match op {
            Operation::Generate => {
                match proposer
                    .propose_with_retry(
                        &mut cluster,
                        |val| val.map(|x| x + 1).unwrap_or(0),
                        &metadata,
                    )
                    .await
                {
                    Ok(v) => OpResponse::GenerateOk { id: v },
                    Err(e) => {
                        // println!("{:?}", e);
                        continue;
                    }
                }
            }
        };

        resp_tx.send(resp);
    }
}

async fn acceptor(
    mut listener: aufruhr::messages::MListener<
        caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<NodeId, u64>, ()>,
    >,
    handle: Arc<ServerHandle<Operation>>,
) {
    let mut acceptor = caspaxos::internals::Acceptor::<NodeId, u64>::new();

    while let Ok((src, msg)) = listener.recv().await {
        let resp_msg = match msg.content {
            caspaxos::msgs::ProposerMessage::Prepare(p) => {
                caspaxos::msgs::AcceptorMessage::Promise(acceptor.recv_prepare(p))
            }
            caspaxos::msgs::ProposerMessage::Accept(a) => {
                caspaxos::msgs::AcceptorMessage::Accepted(acceptor.recv_accept(a))
            }
        };

        let mut tx = match aufruhr::messages::MSender::open(&handle, src, PROPOSER_PORT) {
            Ok(tx) => tx,
            Err(e) => continue,
        };

        if let Err(e) = tx.send(caspaxos::msgs::Message {
            ballot: msg.ballot,
            content: resp_msg,
            metadata: (),
        }) {
            // TODO
        }
    }
}

pub struct Progress(indicatif::ProgressBar);

impl aufruhr::ProgressDisplay for Progress {
    fn executed_op(&self) {
        self.0.inc(1)
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.0.finish();
    }
}

fn main() {
    let sim_config = aufruhr::SimulationConfig::perfect(1, 3)
        .with_latency(aufruhr::messages::MessageLatency::Range(
            aufruhr::messages::LatencyTimeRange::Micros(250..2000),
        ))
        //.with_possible_failure(aufruhr::PossibleFailureEvents::NodePause)
        ;
    let simulation = aufruhr::Simulation::new(132139387719, sim_config);

    let simulation = Arc::new(simulation);

    const OP_COUNT: usize = 300;

    let progress_bar = indicatif::ProgressBar::new(OP_COUNT as u64).with_style(
        indicatif::ProgressStyle::default_bar()
            .template("Executed Operations: {bar:50} [{pos:06}/{len:06}] [{elapsed}/{duration}]")
            .unwrap(),
    );
    progress_bar.tick();
    let progress = Progress(progress_bar);

    let start = Instant::now();
    let sim_res = simulation.run::<Operation, _, _>(
        aufruhr::RunCondition::Operations(OP_COUNT),
        progress,
        |ops, mut handle, mut runtime| {
            let acceptor_listener =
                aufruhr::messages::MListener::create(&mut handle, ACCEPTOR_PORT).unwrap();
            let proposer_listener =
                aufruhr::messages::MListener::create(&mut handle, PROPOSER_PORT).unwrap();

            let handle = Arc::new(handle);
            runtime.spawn(acceptor(acceptor_listener, handle.clone()));
            runtime.spawn(proposer(ops, proposer_listener, handle));
        },
    );
    let run_duration = start.elapsed();

    sim_res.analyze_latencies();

    {
        println!();
        println!("Analyzing Operations...");

        let mut success = 0;
        let mut failures = 0;
        let mut duplicates = Vec::new();

        let mut ids = HashSet::new();
        for executed_op in sim_res.ops() {
            match executed_op.op {
                Operation::Generate => {
                    match executed_op.res {
                        OperationResult::Ok(OpResponse::GenerateOk { id }) => {
                            if ids.contains(&id) {
                                duplicates.push(id);
                            } else {
                                success += 1;
                                ids.insert(id);
                            }
                        }
                        OperationResult::Timeout | OperationResult::Error => {
                            failures += 1;
                        }
                    };
                }
            };
        }

        println!("---- Operations ----");
        println!("Success: {:?}", success);
        println!("Failures: {:?}", failures);
        println!("Duplicates: {:?}", duplicates);
        println!(
            "Ops/s: {:0.2?}",
            sim_res.ops().len() as f64 / run_duration.as_secs_f64()
        );
    }

    {
        println!();
        println!("---- Messages ----");
        println!("Total Messages from Servers: {:?}", sim_res.message_count());
        println!("Messages per Op: {:?}", sim_res.messages_per_op());
    }

    {
        println!();
        println!("---- Events ----");
        println!("Total: {:?}", sim_res.events().len());
        println!("Events: ");
        for event in sim_res.events() {
            println!("  - Event:");
            println!("    start: {:?}", event.start.to_rfc3339());
            println!("    end: {:?}", event.end.to_rfc3339());
            println!("    type: {:?}", event.event);
        }
    }

    svg::save("operations.svg", &sim_res.operations_svg()).unwrap();
    svg::save("messages.svg", &sim_res.messages_svg()).unwrap();
}
