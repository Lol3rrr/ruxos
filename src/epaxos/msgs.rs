use super::listener::{Ballot, Interference, OpState};

#[derive(Debug, Clone)]
pub enum Request<Id, O> {
    PreAccept(PreAccept<Id, O>),
    Accept(Accept<Id, O>),
    Commit(Commit<Id, O>),
    Prepare(Prepare<Id>),
}

pub enum Response<Id, O> {
    PreAcceptOk(PreAcceptOk<Id, O>),
    AcceptOk(AcceptOk<Id>),
    Commit,
    PrepareResp(PrepareResp<Id, O>),
}

#[derive(Debug, Clone)]
pub struct PreAccept<Id, O> {
    pub(super) op: O,
    pub(super) seq: u64,
    pub(super) deps: Vec<Interference<Id>>,
    pub(super) node: (Id, u64),
    pub(super) ballot: Ballot<Id>,
}

#[derive(Debug, Clone)]
pub struct PreAcceptOk<Id, O> {
    pub(super) op: O,
    pub(super) seq: u64,
    pub(super) deps: Vec<Interference<Id>>,
    pub(super) node: Id,
    pub(super) instance: u64,
}

#[derive(Debug, Clone)]
pub struct Accept<Id, O> {
    pub(super) op: O,
    pub(super) seq: u64,
    pub(super) deps: Vec<Interference<Id>>,
    pub(super) node: (Id, u64),
}

#[derive(Debug, Clone)]
pub struct AcceptOk<Id> {
    pub(super) node: (Id, u64),
}

#[derive(Debug, Clone)]
pub struct Commit<Id, O> {
    pub(super) op: O,
    pub(super) seq: u64,
    pub(super) deps: Vec<Interference<Id>>,
    pub(super) node: (Id, u64),
    pub(super) ballot: Ballot<Id>,
}

#[derive(Debug, Clone)]
pub struct Prepare<Id> {
    pub(super) node: Id,
    pub(super) instance: u64,
    pub(super) ballot: Ballot<Id>,
}

#[derive(Debug, Clone)]
pub enum PrepareResp<Id, O> {
    Ok(PrepareOk<Id, O>),
    Nack,
}

#[derive(Debug, Clone)]
pub struct PrepareOk<Id, O> {
    pub(super) op: O,
    pub(super) state: OpState,
    pub(super) seq: u64,
    pub(super) deps: Vec<Interference<Id>>,
    pub(super) node: Id,
    pub(super) instance: u64,
    pub(super) ballot: Ballot<Id>,
}
