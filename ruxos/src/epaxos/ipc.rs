use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use atomic_waker::AtomicWaker;

use super::{
    dependencies::Dependencies,
    listener::{Ballot, CmdOp, NodeMessage, TryExecuteError},
    msgs, Operation,
};

pub(super) struct Message<Id, O, T>
where
    O: Operation<T>,
{
    pub req: Request<Id, O>,
    pub resp: tokio::sync::oneshot::Sender<Response<Id, O, T>>,
}

#[derive(Debug)]
pub(super) enum Request<Id, O> {
    PreAccept {
        op: O,
        node: Id,
        instance: u64,
        ballot: Ballot<Id>,
    },
    Accept {
        op: O,
        node: Id,
        instance: u64,
        n_deps: Dependencies<Id>,
        n_seq: u64,
        ballot: Ballot<Id>,
    },
    Commit {
        node: Id,
        instance: u64,
        ballot: Ballot<Id>,
    },
    Execute {
        node: Id,
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
        deps: Dependencies<Id>,
    },
    GetStorage,
}

#[derive(Debug)]
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
    Nack,
    Storage(HashMap<Id, BTreeMap<u64, CmdOp<Id, O, T>>>),
}

impl<Id, O, T> PartialEq for Response<Id, O, T>
where
    Id: PartialEq,
    O: Operation<T> + PartialEq,
    T: PartialEq,
    O::ApplyResult: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::PreAcceptOk(pa1, pok1), Self::PreAcceptOk(pa2, pok2)) => {
                pa1 == pa2 && pok1 == pok2
            }
            (Self::Accepted(a1), Self::Accepted(a2)) => a1 == a2,
            (Self::Committed(c1), Self::Committed(c2)) => c1 == c2,
            (Self::Executed(e1), Self::Executed(e2)) => e1 == e2,
            (Self::ExplicitPrepare(p1, pr1), Self::ExplicitPrepare(p2, pr2)) => {
                p1 == p2 && pr1 == pr2
            }
            (Self::ExplicitCommitted(ec1), Self::ExplicitCommitted(ec2)) => ec1 == ec2,
            (Self::Nack, Self::Nack) => true,
            (_, _) => false,
        }
    }
}

impl<Id, O, T> Message<Id, O, T>
where
    O: Operation<T>,
{
    pub fn new(
        req: Request<Id, O>,
        resp: tokio::sync::oneshot::Sender<Response<Id, O, T>>,
    ) -> Self {
        Message { req, resp }
    }
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

        self.0.send(NodeMessage::Ipc {
            req: Message { req: op, resp: tx },
            #[cfg(feature = "tracing")]
            span: {
                let span = tracing::span!(tracing::Level::INFO, "ipc_send");
                span.follows_from(tracing::Span::current());
                span
            },
        })?;

        Ok(rx)
    }

    pub async fn send_recv<M, T2>(&self, op: Request<Id, O>, mapping: M) -> Result<T2, ()>
    where
        M: FnOnce(Response<Id, O, T>) -> T2,
    {
        let rx = self.send(op).map_err(|_e| ())?;

        let raw = rx.await.map_err(|_e| ())?;

        Ok(mapping(raw))
    }

    pub async fn preaccept(
        &self,
        op: O,
        node: Id,
        instance: u64,
        ballot: Ballot<Id>,
    ) -> Result<(msgs::PreAccept<Id, O>, msgs::PreAcceptOk<Id, O>), ()> {
        let rx = self
            .send(Request::PreAccept {
                op,
                node,
                instance,
                ballot,
            })
            .map_err(|_e| ())?;

        let resp = rx.await.map_err(|_e| ())?;

        match resp {
            Response::PreAcceptOk(msg, confirm) => Ok((msg, confirm)),
            _ => Err(()),
        }
    }

    pub async fn accept(
        &self,
        node: Id,
        instance: u64,
        op: O,
        n_deps: Dependencies<Id>,
        n_seq: u64,
        ballot: Ballot<Id>,
    ) -> Result<msgs::Accept<Id, O>, ()> {
        let rx = self
            .send(Request::Accept {
                op,
                node,
                instance,
                n_deps,
                n_seq,
                ballot,
            })
            .map_err(|_e| ())?;

        let resp = rx.await.map_err(|_e| ())?;

        match resp {
            Response::Accepted(msg) => Ok(msg),
            _ => Err(()),
        }
    }

    pub async fn commit(
        &self,
        node: Id,
        instance: u64,
        ballot: Ballot<Id>,
    ) -> Result<msgs::Commit<Id, O>, ()> {
        let rx = self
            .send(Request::Commit {
                node,
                instance,
                ballot,
            })
            .map_err(|_e| ())?;

        let resp = rx.await.map_err(|_e| ())?;

        match resp {
            Response::Committed(c) => Ok(c),
            _ => Err(()),
        }
    }
}
