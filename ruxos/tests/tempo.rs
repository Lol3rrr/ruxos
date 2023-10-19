use std::collections::BTreeSet;

use ruxos::tempo::{self, replica::Receive, Replica};

#[test]
fn tempo_submit() {
    let mut replicas: [_; 3] = core::array::from_fn(|idx| {
        tempo::Builder::new()
            .id(idx)
            .nodes([idx])
            .finish::<(), (), ()>(())
    });

    let fast_quorum: BTreeSet<_> = [0, 1].into_iter().collect();

    let op_id = tempo::replica::OpId {
        node: 0,
        counter: 1,
    };
    let op_timestamp = 1;

    let propose_msg = tempo::msgs::Propose {
        id: op_id.clone(),
        operation: (),
        quroum: fast_quorum.clone(),
        timestamp: op_timestamp,
    };

    let payload_msg = tempo::msgs::Payload {
        id: op_id.clone(),
        operation: (),
        quroum: fast_quorum.clone(),
    };

    let propose_resps = [
        replicas[0].recv(0, propose_msg.clone()).unwrap().unwrap(),
        replicas[1].recv(0, propose_msg.clone()).unwrap().unwrap(),
    ];

    let tmp = replicas[2].recv(0, payload_msg).unwrap();

    let mut action = None;
    for (src, (ack, bump)) in propose_resps.into_iter().enumerate() {
        match replicas[0].recv(src, ack.msg).unwrap().unwrap() {
            tempo::replica::ProposeResult::Wait => {
                assert!(action.is_none());
            }
            tempo::replica::ProposeResult::Commit(commit) => {
                assert!(action.is_none());
                action = Some(tempo::replica::ProposeResult::Commit(commit));
            }
            tempo::replica::ProposeResult::Consensus(consens) => {
                assert!(action.is_none());
                action = Some(tempo::replica::ProposeResult::Consensus(consens));
            }
        };

        let tmp = replicas[0].recv(src, bump.msg.clone()).unwrap();
        let tmp = replicas[1].recv(src, bump.msg).unwrap();
    }

    assert!(action.is_some());

    match action.unwrap() {
        tempo::replica::ProposeResult::Wait => unreachable!(),
        tempo::replica::ProposeResult::Commit(commit) => {
            let commit = commit.msg;
            for replica in replicas.iter_mut() {
                let tmp = replica.recv(0, commit.clone()).unwrap();
            }
        }
        tempo::replica::ProposeResult::Consensus(consensus) => {
            panic!("We should immediately commit as all the nodes are fresh and should therefore all agree on the timestamp")
        }
    };
}

#[test]
fn tempo_submit_hangs() {
    let mut replica = tempo::Builder::new()
        .id(0)
        .nodes(vec![0])
        .accepted_failures(0)
        .finish::<(), (), ()>(());

    replica
        .recv(
            0,
            tempo::msgs::Propose {
                id: tempo::replica::OpId {
                    node: 0,
                    counter: 1,
                },
                operation: (),
                quroum: [0].into_iter().collect(),
                timestamp: 1,
            },
        )
        .unwrap();
}
