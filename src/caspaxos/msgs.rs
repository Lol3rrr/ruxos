//! The Messages being exchanged between the [`Acceptor`](super::internals::Acceptor)s and [`Proposer`](super::internals::Proposer)s.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct Message<C, MD> {
    pub ballot: u64,
    pub content: C,
    pub metadata: MD,
}

impl<C, MD> Message<C, MD> {
    pub fn map_meta<F, MD2>(self, func: F) -> Message<C, MD2>
    where
        F: FnOnce(MD) -> MD2,
    {
        Message {
            ballot: self.ballot,
            content: self.content,
            metadata: func(self.metadata),
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum ProposerMessage<ID, V> {
    Prepare(PrepareMessage<ID>),
    Accept(AcceptMessage<ID, V>),
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct PrepareMessage<ID> {
    pub ballot_number: (u64, ID),
}

impl<ID> PrepareMessage<&ID>
where
    ID: Clone,
{
    pub fn owned(self) -> PrepareMessage<ID> {
        PrepareMessage {
            ballot_number: (self.ballot_number.0, self.ballot_number.1.clone()),
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct AcceptMessage<ID, V> {
    pub id: (u64, ID),
    pub value: V,
}

impl<ID, V> AcceptMessage<&ID, V>
where
    ID: Clone,
{
    pub fn owned(self) -> AcceptMessage<ID, V> {
        AcceptMessage {
            id: (self.id.0, self.id.1.clone()),
            value: self.value,
        }
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum AcceptorMessage<ID, V> {
    Promise(PrepareResponse<ID, V>),
    Accepted(AcceptResponse<ID>),
}

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum PrepareResponse<ID, V> {
    Conflict { proposed: (u64, ID), existing: u64 },
    Promise(Option<((u64, ID), V)>),
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum AcceptResponse<ID> {
    Conflict { proposed: (u64, ID), existing: u64 },
    Confirm,
}
