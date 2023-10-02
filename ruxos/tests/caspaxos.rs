#![feature(async_fn_in_trait)]
#![feature(impl_trait_projections)]

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    hash::SipHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
    time::Duration,
};

use rand::seq::IteratorRandom;
use ruxos::caspaxos::{
    self,
    internals::Acceptor,
    msgs::{AcceptorMessage, Message, ProposerMessage},
    ProposeClient,
};

struct ExpandableCluster<V> {
    acceptor: BTreeMap<
        usize,
        tokio::sync::mpsc::UnboundedSender<(
            caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<u64, V>, ()>,
            tokio::sync::oneshot::Sender<
                caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<u64, V>, ()>,
            >,
        )>,
    >,
    proposer: BTreeSet<u64>,
    _marker: PhantomData<V>,
}

impl<V> caspaxos::Cluster<u64, V, ()> for ExpandableCluster<V>
where
    V: Clone,
{
    type Quorum<'q> = ExpandableQuorum<V> where V: 'q;

    fn hash(&self) -> caspaxos::internals::ClusterHash {
        let mut hash = SipHasher::new();
        for proposer in self.proposer.iter() {
            proposer.hash(&mut hash);
        }

        caspaxos::internals::ClusterHash(hash.finish())
    }

    fn size(&self) -> usize {
        self.acceptor.len()
    }

    fn quorum<'q, 's>(&'s mut self, size: usize) -> Option<Self::Quorum<'q>>
    where
        's: 'q,
    {
        let acceptors = self
            .acceptor
            .values()
            .cloned()
            .choose_multiple(&mut rand::thread_rng(), size);

        Some(ExpandableQuorum {
            acceptor: acceptors,
            recvs: Vec::new(),
            _marker: PhantomData {},
        })
    }
}

struct ExpandableQuorum<V> {
    acceptor: Vec<
        tokio::sync::mpsc::UnboundedSender<(
            caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<u64, V>, ()>,
            tokio::sync::oneshot::Sender<
                caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<u64, V>, ()>,
            >,
        )>,
    >,
    recvs: Vec<
        tokio::sync::oneshot::Receiver<
            caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<u64, V>, ()>,
        >,
    >,
    _marker: PhantomData<V>,
}

impl<V> caspaxos::ClusterQuorum<u64, V, ()> for ExpandableQuorum<V>
where
    V: Clone,
{
    type Error = ();

    async fn send<'md>(
        &mut self,
        msg: caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<u64, V>, &'md ()>,
    ) -> Result<(), Self::Error> {
        self.recvs.clear();

        let msg = msg.map_meta(|_| ());
        for accept in self.acceptor.iter_mut() {
            let (tx, rx) = tokio::sync::oneshot::channel();

            accept.send((msg.clone(), tx));
            self.recvs.push(rx);
        }

        Ok(())
    }

    async fn try_recv(
        &mut self,
    ) -> Result<caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<u64, V>, ()>, Self::Error>
    {
        tokio::time::timeout(Duration::from_micros(500), self.recvs.pop().ok_or(())?)
            .await
            .map_err(|_| ())?
            .map_err(|_| ())
    }
}

impl<V>
    caspaxos::DynamicCluster<
        u64,
        V,
        (),
        (
            usize,
            tokio::sync::mpsc::UnboundedSender<(
                caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<u64, V>, ()>,
                tokio::sync::oneshot::Sender<
                    caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<u64, V>, ()>,
                >,
            )>,
        ),
        (),
    > for ExpandableCluster<V>
where
    V: Clone,
{
    type AddHandle = ();

    fn add(
        &mut self,
        node: (
            usize,
            tokio::sync::mpsc::UnboundedSender<(
                caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<u64, V>, ()>,
                tokio::sync::oneshot::Sender<
                    caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<u64, V>, ()>,
                >,
            )>,
        ),
    ) -> Self::AddHandle {
        self.acceptor.insert(node.0, node.1);
        ()
    }

    async fn notify<'md>(&mut self, metadata: &'md ()) {
        // Do nothing
    }
}

async fn acceptor<V>(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(
        caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<u64, V>, ()>,
        tokio::sync::oneshot::Sender<
            caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<u64, V>, ()>,
        >,
    )>,
) where
    V: Clone,
{
    let mut acceptor = Acceptor::<u64, V>::new();

    while let Some((msg, tx)) = rx.recv().await {
        let resp_msg = match msg.content {
            ProposerMessage::Prepare(p) => {
                let r = acceptor.recv_prepare(p);
                Message {
                    ballot: msg.ballot,
                    content: AcceptorMessage::Promise(r),
                    metadata: (),
                }
            }
            ProposerMessage::Accept(a) => {
                let r = acceptor.recv_accept(a);
                Message {
                    ballot: msg.ballot,
                    content: AcceptorMessage::Accepted(r),
                    metadata: (),
                }
            }
        };

        tx.send(resp_msg);
    }
}
fn new_cluster<V>(
    acceptor_count: usize,
    proposer: impl Iterator<Item = u64>,
) -> (ExpandableCluster<V>, Vec<impl Future<Output = ()>>)
where
    V: Clone,
{
    let (queues, acceptors): (BTreeMap<_, _>, Vec<_>) = (0..acceptor_count)
        .map(|i| {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(
                caspaxos::msgs::Message<caspaxos::msgs::ProposerMessage<u64, V>, ()>,
                tokio::sync::oneshot::Sender<
                    caspaxos::msgs::Message<caspaxos::msgs::AcceptorMessage<u64, V>, ()>,
                >,
            )>();

            ((i, tx), acceptor(rx))
        })
        .unzip();

    (
        ExpandableCluster {
            acceptor: queues,
            proposer: proposer.collect(),
            _marker: PhantomData {},
        },
        acceptors,
    )
}

#[test]
fn add_node_no_contention() {
    type Value = usize;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();

    let (mut cluster, acceptors) = new_cluster::<Value>(3, 0..1);
    for acceptor in acceptors {
        rt.spawn(acceptor);
    }

    let mut proposer: ProposeClient<_, Value> = ProposeClient::new(0);

    let value = rt
        .block_on(proposer.propose(&mut cluster, |_| 0, &()))
        .unwrap();

    assert_eq!(0, value);

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    rt.spawn(acceptor(rx));
    rt.block_on(proposer.add_node(&mut cluster, &(), &(), (4, tx)))
        .unwrap();

    let value = rt
        .block_on(proposer.propose(&mut cluster, |_| 1, &()))
        .unwrap();

    assert_eq!(1, value);
}
