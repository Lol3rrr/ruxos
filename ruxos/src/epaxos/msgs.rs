//! The Messages exchanged between differnet Nodes in the EPaxos-Cluster

use super::{
    dependencies::{Dependencies, Interference},
    listener::{Ballot, OpState},
};

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub enum Message<Id, O> {
    Request(Request<Id, O>),
    Response(Response<Id, O>),
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub enum Request<Id, O> {
    PreAccept(PreAccept<Id, O>),
    Accept(Accept<Id, O>),
    Commit(Commit<Id, O>),
    Prepare(Prepare<Id>),
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub enum Response<Id, O> {
    PreAcceptOk(PreAcceptOk<Id, O>),
    AcceptOk(AcceptOk<Id>),
    Commit,
    PrepareResp(PrepareResp<Id, O>),
    Nack,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub struct PreAccept<Id, O> {
    pub(super) op: O,
    pub(super) seq: u64,
    pub(super) deps: Dependencies<Id>,
    pub(super) node: (Id, u64),
    pub(super) ballot: Ballot<Id>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub struct PreAcceptOk<Id, O> {
    pub(super) op: O,
    pub(super) seq: u64,
    pub(super) deps: Dependencies<Id>,
    pub(super) node: Id,
    pub(super) instance: u64,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub struct Accept<Id, O> {
    pub(super) op: O,
    pub(super) seq: u64,
    pub(super) deps: Dependencies<Id>,
    pub(super) node: (Id, u64),
    pub(super) ballot: Ballot<Id>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub struct AcceptOk<Id> {
    pub(super) node: (Id, u64),
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub struct Commit<Id, O> {
    pub(super) op: O,
    pub(super) seq: u64,
    pub(super) deps: Dependencies<Id>,
    pub(super) node: (Id, u64),
    pub(super) ballot: Ballot<Id>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub struct Prepare<Id> {
    pub(super) node: Id,
    pub(super) instance: u64,
    pub(super) ballot: Ballot<Id>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub enum PrepareResp<Id, O> {
    Ok(PrepareOk<Id, O>),
    Nack,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(::serde::Deserialize, ::serde::Serialize))]
pub struct PrepareOk<Id, O> {
    pub(super) op: O,
    pub(super) state: OpState,
    pub(super) seq: u64,
    pub(super) deps: Dependencies<Id>,
    pub(super) node: Id,
    pub(super) instance: u64,
    pub(super) ballot: Ballot<Id>,
}
