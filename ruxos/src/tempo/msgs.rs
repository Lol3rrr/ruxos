use std::collections::BTreeSet;

use super::{
    promises::{AttachedPromises, DetachedPromises},
    replica::{Ballot, CommandPhase, OpId},
};

macro_rules! impl_into {
    ($name:ident, $src:ty) => {
        impl<O, NodeId> From<$src> for MessageContent<O, NodeId>
        where
            NodeId: Ord,
        {
            fn from(value: $src) -> Self {
                Self::$name(value)
            }
        }
    };
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Message<O, NodeId>
where
    NodeId: Ord,
{
    pub src: NodeId,
    pub msg: MessageContent<O, NodeId>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MessageContent<O, NodeId>
where
    NodeId: Ord,
{
    Propose(Propose<O, NodeId>),
    Payload(Payload<O, NodeId>),
    ProposeAck(ProposeAck<NodeId>),
    Bump(Bump<NodeId>),
    Commit(Commit<NodeId>),
    Consensus(Consensus<NodeId>),
    ConsensusAck(ConsensusAck<NodeId>),
    Promises(Promises<NodeId>),
    PromisesOk(PromisesOk),
    CommitRequest(CommitRequest<NodeId>),
    Stable(Stable<NodeId>),
    Rec(Rec<NodeId>),
    RecAck(RecAck<NodeId>),
    RecNAck(RecNAck<NodeId>),
}

impl_into!(Propose, Propose<O, NodeId>);
impl_into!(Payload, Payload<O, NodeId>);
impl_into!(ProposeAck, ProposeAck<NodeId>);
impl_into!(Bump, Bump<NodeId>);
impl_into!(Commit, Commit<NodeId>);
impl_into!(Consensus, Consensus<NodeId>);
impl_into!(ConsensusAck, ConsensusAck<NodeId>);
impl_into!(Promises, Promises<NodeId>);
impl_into!(PromisesOk, PromisesOk);
impl_into!(CommitRequest, CommitRequest<NodeId>);
impl_into!(Stable, Stable<NodeId>);
impl_into!(Rec, Rec<NodeId>);
impl_into!(RecAck, RecAck<NodeId>);
impl_into!(RecNAck, RecNAck<NodeId>);

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Propose<O, NodeId>
where
    NodeId: Ord,
{
    pub id: OpId<NodeId>,
    pub operation: O,
    pub quroum: BTreeSet<NodeId>,
    pub timestamp: u64,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Payload<O, NodeId>
where
    NodeId: Ord,
{
    pub id: OpId<NodeId>,
    pub operation: O,
    pub quroum: BTreeSet<NodeId>,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProposeAck<NodeId> {
    pub id: OpId<NodeId>,
    pub timestamp: u64,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Bump<NodeId> {
    pub id: OpId<NodeId>,
    pub timestamp: u64,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Commit<NodeId> {
    pub id: OpId<NodeId>,
    pub timestamp: u64,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Consensus<NodeId> {
    pub id: OpId<NodeId>,
    pub timestamp: u64,
    pub ballot: Ballot<NodeId>,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ConsensusAck<NodeId> {
    pub id: OpId<NodeId>,
    pub ballot: Ballot<NodeId>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Promises<NodeId>
where
    NodeId: Ord,
{
    pub detached: DetachedPromises<NodeId>,
    pub attached: AttachedPromises<NodeId>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PromisesOk {
    pub highest: u64,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommitRequest<NodeId> {
    pub id: OpId<NodeId>,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Stable<NodeId> {
    pub id: OpId<NodeId>,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Rec<NodeId> {
    pub id: OpId<NodeId>,
    pub ballot: Ballot<NodeId>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RecAck<NodeId>
where
    NodeId: Ord,
{
    pub id: OpId<NodeId>,
    pub timestamp: u64,
    pub phase: CommandPhase<NodeId>,
    pub abal: Option<Ballot<NodeId>>,
    pub ballot: Ballot<NodeId>,
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RecNAck<NodeId> {
    pub id: OpId<NodeId>,
    pub ballot: Ballot<NodeId>,
}
