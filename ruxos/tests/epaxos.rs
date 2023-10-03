#![feature(async_fn_in_trait)]

use std::{collections::HashMap, mem::ManuallyDrop};

use ruxos::epaxos::{self, TryExecuteError};
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

struct TestCluster {}

struct TestReceiver {}

impl<Id, O> epaxos::Cluster<Id, O> for TestCluster {
    type Error = ();
    type Receiver<'r> = TestReceiver;

    fn size(&self) -> usize {
        // TODO
        1
    }

    async fn send<'s, 'r>(
        &'s mut self,
        _msg: epaxos::msgs::Request<Id, O>,
        count: usize,
        _local: &Id,
    ) -> Result<Self::Receiver<'r>, Self::Error>
    where
        's: 'r,
    {
        for _ in 0..count {
            todo!()
        }

        Ok(TestReceiver {})
    }
}

impl<Id, O> epaxos::ResponseReceiver<Id, O> for TestReceiver {
    async fn recv(&mut self) -> Result<epaxos::msgs::Response<Id, O>, ()> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq)]
enum TestOperation {
    Write { key: u64, value: u64 },
    Read { key: u64 },
    Noop,
}

impl epaxos::Operation<HashMap<u64, u64>> for TestOperation {
    type ApplyResult = Option<u64>;

    const TRANSITIVE: bool = false;

    fn noop() -> Self {
        Self::Noop
    }

    fn interfere(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Write { key: fkey, .. }, Self::Write { key: skey, .. }) => fkey == skey,
            (Self::Write { key: fkey, .. }, Self::Read { key: skey }) => fkey == skey,
            (Self::Read { key: fkey }, Self::Write { key: skey, .. }) => fkey == skey,
            (Self::Read { key: fkey }, Self::Read { key: skey }) => fkey == skey,
            _ => true,
        }
    }

    fn apply(&mut self, state: &mut HashMap<u64, u64>) -> Self::ApplyResult {
        match self {
            Self::Write { key, value } => {
                let entry = state.entry(*key).or_default();
                *entry = *value;
                Some(*value)
            }
            Self::Read { key } => state.get(key).copied(),
            Self::Noop => None,
        }
    }
}

#[test]
fn basic() {
    // To prevent blocking on drop
    let runtime = ManuallyDrop::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    );

    let (mut listener, node) = epaxos::new(0, HashMap::new());
    runtime.spawn_blocking(move || loop {
        listener.try_poll();
    });

    let mut cluster = TestCluster {};

    runtime.block_on(async {
        let handle = node
            .request(TestOperation::Write { key: 0, value: 1 }, &mut cluster)
            .await
            .unwrap();

        println!("Commited write");

        let _ = handle;
    });

    runtime.block_on(async {
        let handle = node
            .request(TestOperation::Read { key: 0 }, &mut cluster)
            .await
            .unwrap();

        println!("Commited read");

        let execute_handle = handle.try_execute().unwrap();

        let result = execute_handle.await.unwrap();

        assert_eq!(Some(1), result);
    });
}

#[test]
fn local_cluster() {
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(tracing_subscriber::fmt::layer().with_test_writer()),
    );

    // Dont block when "dropping" runtime
    let runtime = ManuallyDrop::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    );

    let (mut listener1, node1) =
        epaxos::new::<usize, TestOperation, HashMap<_, _>>(0, HashMap::new());
    let lhandle1 = listener1.handle();
    runtime.spawn_blocking(move || {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_test_writer()),
        );
        loop {
            listener1.try_poll();
        }
    });

    let (mut listener2, node2) =
        epaxos::new::<usize, TestOperation, HashMap<_, _>>(1, HashMap::new());
    let lhandle2 = listener2.handle();
    runtime.spawn_blocking(move || {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_test_writer()),
        );
        loop {
            listener2.try_poll();
        }
    });

    let (mut listener3, node3) =
        epaxos::new::<usize, TestOperation, HashMap<_, _>>(2, HashMap::new());
    let lhandle3 = listener3.handle();
    runtime.spawn_blocking(move || {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_test_writer()),
        );
        loop {
            listener3.try_poll();
        }
    });

    let mut cluster =
        epaxos::testing::LocalCluster::new([(0, lhandle1), (1, lhandle2), (2, lhandle3)]);

    runtime.block_on(async {
        tracing::info!("Starting Operation");

        let handle = node1
            .request(TestOperation::Write { key: 0, value: 1 }, &mut cluster)
            .await
            .unwrap();

        tracing::info!("Commited write");

        handle.try_execute().unwrap().await.unwrap();
    });

    for (id, node) in [node1, node2, node3].into_iter().enumerate() {
        runtime.block_on(async {
            tracing::info!("Starting Operation");

            let handle = node
                .request(TestOperation::Read { key: 0 }, &mut cluster)
                .await
                .unwrap();

            tracing::info!("Commited write");

            let res = handle.try_execute().unwrap().await.unwrap();

            assert_eq!(Some(1), res, "Reading on Node {}", id);
        });
    }

    tracing::info!("Done");
}

#[test]
fn local_cluster_with_partition() {
    // In this test, we intentionally dont include node3 in the cluster configuration to mimic all
    // relevant messages being dropped/going missing to it.
    // Then we can test the slow path

    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(tracing_subscriber::fmt::layer().with_test_writer()),
    );

    // Use manually drop to not block when "dropping the runtime"
    let runtime = ManuallyDrop::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    );

    let (mut listener1, node1) =
        epaxos::new::<usize, TestOperation, HashMap<_, _>>(0, HashMap::new());
    let lhandle1 = listener1.handle();
    runtime.spawn_blocking(move || {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_test_writer()),
        );
        loop {
            listener1.try_poll();
        }
    });

    let (mut listener2, node2) =
        epaxos::new::<usize, TestOperation, HashMap<_, _>>(1, HashMap::new());
    let lhandle2 = listener2.handle();
    runtime.spawn_blocking(move || {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_test_writer()),
        );
        loop {
            listener2.try_poll();
        }
    });

    let (mut listener3, node3) =
        epaxos::new::<usize, TestOperation, HashMap<_, _>>(2, HashMap::new());
    let lhandle3 = listener3.handle();
    runtime.spawn_blocking(move || {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_test_writer()),
        );
        loop {
            listener3.try_poll();
        }
    });

    let mut cluster =
        epaxos::testing::LocalCluster::new([(0, lhandle1), (1, lhandle2) /*(2, lhandle3)*/]);

    runtime.block_on(async {
        tracing::info!("Starting Operation");

        let handle = node1
            .request(TestOperation::Write { key: 0, value: 1 }, &mut cluster)
            .await
            .unwrap();

        tracing::info!("Commited write");

        handle.try_execute().unwrap().await.unwrap();

        tracing::info!("Write was executed");
    });

    runtime.block_on(async {
        tracing::info!("Starting Operation");

        let handle = node3
            .request(TestOperation::Read { key: 0 }, &mut cluster)
            .await
            .unwrap();

        tracing::info!("Commited Read");

        let res = handle.try_execute().unwrap().await;

        assert!(res.is_err());

        let instance = match res {
            Err(TryExecuteError::UnknownCommand(instance)) => instance,
            _ => unreachable!(),
        };

        node3.explicit_prepare(&mut cluster, instance).await;
    });

    tracing::info!("Done");
}
