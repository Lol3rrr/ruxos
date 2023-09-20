use std::sync::Arc;

use atomic_waker::AtomicWaker;

use super::{
    listener::{Ballot, Interference, NodeMessage, TryExecuteError},
    msgs, Operation,
};

pub(super) struct Message<Id, O, T>
where
    O: Operation<T>,
{
    pub req: Request<Id, O>,
    pub resp: tokio::sync::oneshot::Sender<Response<Id, O, T>>,
}

pub(super) enum Request<Id, O> {
    PreAccept {
        op: O,
        instance: u64,
        ballot: Ballot<Id>,
    },
    Accept {
        instance: u64,
        n_deps: Vec<Interference<Id>>,
        n_seq: u64,
    },
    Commit {
        instance: u64,
    },
    Execute {
        instance: u64,
        waker: Option<Arc<AtomicWaker>>,
    },
    ExplicitPrepare {
        instance: u64,
        node: Id,
    },
    ExplicitCommit {
        instance: u64,
        node: Id,
        op: O,
        ballot: Ballot<Id>,
        seq: u64,
        deps: Vec<Interference<Id>>,
    },
}

pub(super) enum Response<Id, O, T>
where
    O: Operation<T>,
{
    PreAcceptOk(msgs::PreAccept<Id, O>, msgs::PreAcceptOk<Id, O>),
    Accepted(msgs::Accept<Id, O>),
    Committed(msgs::Commit<Id, O>),
    Executed(Result<O::ApplyResult, TryExecuteError<Id, O::ApplyResult>>),
    ExplicitPrepare(msgs::Prepare<Id>, msgs::PrepareResp<Id, O>),
    ExplicitCommitted(msgs::Commit<Id, O>),
}

#[derive(Debug)]
pub(super) struct IPCSender<Id, O, T>(tokio::sync::mpsc::UnboundedSender<NodeMessage<Id, O, T>>)
where
    O: Operation<T>;

impl<Id, O, T> Clone for IPCSender<Id, O, T>
where
    O: Operation<T>,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<Id, O, T> IPCSender<Id, O, T>
where
    O: Operation<T>,
{
    pub fn new(tx: tokio::sync::mpsc::UnboundedSender<NodeMessage<Id, O, T>>) -> Self {
        Self(tx)
    }

    pub fn send(
        &self,
        op: Request<Id, O>,
    ) -> Result<
        tokio::sync::oneshot::Receiver<Response<Id, O, T>>,
        tokio::sync::mpsc::error::SendError<NodeMessage<Id, O, T>>,
    > {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.0
            .send(NodeMessage::Ipc(Message { req: op, resp: tx }))?;

        Ok(rx)
    }

    pub async fn send_recv<M, T2>(&self, op: Request<Id, O>, mapping: M) -> Result<T2, ()>
    where
        M: FnOnce(Response<Id, O, T>) -> T2,
    {
        let rx = self.send(op).map_err(|e| ())?;

        let raw = rx.await.map_err(|e| ())?;

        Ok(mapping(raw))
    }

    pub async fn preaccept(
        &self,
        op: O,
        instance: u64,
        ballot: Ballot<Id>,
    ) -> Result<(msgs::PreAccept<Id, O>, msgs::PreAcceptOk<Id, O>), ()> {
        let rx = self
            .send(Request::PreAccept {
                op,
                instance,
                ballot,
            })
            .map_err(|e| ())?;

        let resp = rx.await.map_err(|e| ())?;

        match resp {
            Response::PreAcceptOk(msg, confirm) => Ok((msg, confirm)),
            _ => Err(()),
        }
    }

    pub async fn accept(
        &self,
        instance: u64,
        n_deps: Vec<Interference<Id>>,
        n_seq: u64,
    ) -> Result<msgs::Accept<Id, O>, ()> {
        let rx = self
            .send(Request::Accept {
                instance,
                n_deps,
                n_seq,
            })
            .map_err(|e| ())?;

        let resp = rx.await.map_err(|e| ())?;

        match resp {
            Response::Accepted(msg) => Ok(msg),
            _ => Err(()),
        }
    }

    pub async fn commit(&self, instance: u64) -> Result<msgs::Commit<Id, O>, ()> {
        let rx = self.send(Request::Commit { instance }).map_err(|e| ())?;

        let resp = rx.await.map_err(|e| ())?;

        match resp {
            Response::Committed(c) => Ok(c),
            _ => Err(()),
        }
    }
}
