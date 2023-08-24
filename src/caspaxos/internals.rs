//! The internal Structures used for the CASPaxos algorithm

use super::msgs::*;

/// A Proposer is basically the entry Point, through which you want to interact with the rest of
/// the distributed system. So all reads and writes get started from a [`Proposer`] which will then
/// handle all the protocol type of communication with the [`Acceptor`], which will actually store
/// values.
#[derive(Debug)]
pub struct Proposer<ID> {
    counter: u64,
    id: ID,
}

/// A running proposal for a Value
///
/// # Example
/// ```rust
/// # use ruxos::caspaxos::{internals::Proposer, msgs::{PrepareResponse, AcceptResponse}};
/// let mut proposer = Proposer::new("someID");
///
/// // We want to propose a String and require a Quorum of 1 Node
/// let mut proposal = proposer.propose::<String>(1);
///
/// let msg = proposal.message();
/// // Somehow communicate that message with the acceptors
///
/// # let resps = [PrepareResponse::Promise(None)];
/// for resp in resps {
///     proposal.process(resp);
/// }
///
/// let mut proposal = proposal.finish(|_| {
///     "Our Value".to_string()
/// }).unwrap();
///
/// let msg = proposal.message();
/// // Again somehow communicate that message with the acceptors
///
/// # let resps = [AcceptResponse::Confirm];
/// for resp in resps {
///     proposal.process(resp);
/// }
///
/// let value = proposal.finish().unwrap();
/// assert_eq!("Our Value", value.as_str());
/// ```
pub struct Proposal<'p, ID, PS> {
    proposer: &'p mut Proposer<ID>,
    ballot: u64,
    quorum_threshold: usize,
    state: PS,
}

/// A State for a [`Proposal`] in which we are still waiting for Promises from Acceptors
pub struct WaitingForPromises<ID, V> {
    quorum_count: usize,
    highest_value: Option<((u64, ID), V)>,
    had_conflict: Option<u64>,
}

/// A State for a [`Proposal`] in which we are only waiting for Accepts from Acceptors
pub struct WaitingForAccepts<V> {
    quorum_count: usize,
    value: V,
    had_conflict: Option<u64>,
}

/// The Result from processing a single message
#[derive(Debug, PartialEq)]
pub enum ProcessResult {
    /// There was a conflict with the given ballot number
    Conflict { existing: u64 },
    /// The current proposal is still running, but there are more messages needed to reach a
    /// conclusion
    Pending,
    /// The Proposal is ready/done and can be finished/moved to the next Step
    Ready,
}

impl<ID> Proposer<ID>
where
    ID: Ord,
{
    /// Create a new Proposer with the given ID
    ///
    /// # Note:
    /// The provided ID needs to be unique in the Cluster, as that is used as a tie breaker for
    /// ordering in Paxos itself. Ideally you would use a unique attribute of the system that this
    /// proposer is running on/in (like an IP/hostname).
    /// However if this invariant is broken, it does not lead to memory unsafety but then certain
    /// properties (which are usually garantueed by Paxos) will no longer hold.
    pub fn new(id: ID) -> Self {
        Self { counter: 0, id }
    }

    /// Start a new Proposal round with the given `quorum_threshold` indicating how many nodes are
    /// required to reach quorum (usually if you have 2n+1 nodes you would use n+1 as the threshold).
    pub fn propose<V>(
        &mut self,
        quorum_threshold: usize,
    ) -> Proposal<'_, ID, WaitingForPromises<ID, V>> {
        self.counter += 1;

        Proposal {
            ballot: self.counter,
            proposer: self,
            quorum_threshold,
            state: WaitingForPromises {
                quorum_count: 0,
                highest_value: None,
                had_conflict: None,
            },
        }
    }

    pub(crate) fn update_count(&mut self, new: u64) {
        self.counter = new;
    }
}

impl<'p, ID, T> Proposal<'p, ID, T> {
    pub fn ballot(&self) -> u64 {
        self.ballot
    }
}

impl<'p, ID, V> Proposal<'p, ID, WaitingForPromises<ID, V>>
where
    ID: Ord + Clone,
    V: Clone,
{
    /// The Message that should be send to the acceptors
    pub fn message(&self) -> PrepareMessage<&ID> {
        PrepareMessage {
            ballot_number: (self.ballot, &self.proposer.id),
        }
    }

    /// Processes a single promise returned by an acceptor.
    ///
    /// # Return Value
    /// If this function returns true, it means that enough successful responses have been received
    /// to reach quorum and you can proceed to the next step
    pub fn process(&mut self, msg: PrepareResponse<ID, V>) -> ProcessResult {
        if let Some(existing) = self.state.had_conflict {
            return ProcessResult::Conflict { existing };
        }

        let promised_value = match msg {
            PrepareResponse::Conflict {
                proposed: (bid, pid),
                existing,
            } if (&bid, &pid) == (&self.ballot, &self.proposer.id) => {
                self.state.had_conflict = Some(existing);
                return ProcessResult::Conflict { existing };
            }
            PrepareResponse::Conflict { .. } => {
                return ProcessResult::Pending;
            }
            PrepareResponse::Promise(promise) => promise,
        };

        if let Some((p_id, pvalue)) = promised_value {
            let n_highest = match self.state.highest_value.take() {
                Some((prev_id, prev_value)) => {
                    if prev_id > p_id {
                        (prev_id, prev_value)
                    } else {
                        (p_id, pvalue)
                    }
                }
                None => (p_id, pvalue),
            };

            self.state.highest_value = Some(n_highest);
        }

        self.state.quorum_count += 1;

        if self.state.quorum_count >= self.quorum_threshold {
            ProcessResult::Ready
        } else {
            ProcessResult::Pending
        }
    }

    /// Finishes this stage of the Proposal
    ///
    /// # Return Value
    /// Returns Some with the new Proposal state, if the quroum threshold of promises has been
    /// reached and you can continue onto the next phase
    pub fn finish<F>(mut self, func: F) -> Option<Proposal<'p, ID, WaitingForAccepts<V>>>
    where
        F: FnOnce(Option<V>) -> V,
    {
        if self.state.quorum_count < self.quorum_threshold || self.state.had_conflict.is_some() {
            return None;
        }

        let prev_value = self.state.highest_value.take().map(|(_, v)| v);

        let n_value = func(prev_value);

        Some(Proposal {
            proposer: self.proposer,
            ballot: self.ballot,
            quorum_threshold: self.quorum_threshold,
            state: WaitingForAccepts {
                quorum_count: 0,
                value: n_value.clone(),
                had_conflict: None,
            },
        })
    }
}

impl<'p, ID, V> Proposal<'p, ID, WaitingForAccepts<V>>
where
    ID: Ord + Clone,
    V: Clone,
{
    pub fn message(&self) -> AcceptMessage<ID, V> {
        AcceptMessage {
            id: (self.ballot, self.proposer.id.clone()),
            value: self.state.value.clone(),
        }
    }

    pub fn process(&mut self, msg: AcceptResponse<ID>) -> ProcessResult {
        if let Some(existing) = self.state.had_conflict {
            return ProcessResult::Conflict { existing };
        }

        match msg {
            AcceptResponse::Conflict {
                proposed: (bid, pid),
                existing,
            } if (&bid, &pid) == (&self.ballot, &self.proposer.id) => {
                self.state.had_conflict = Some(existing);
                return ProcessResult::Conflict { existing };
            }
            AcceptResponse::Conflict { .. } => {
                return ProcessResult::Pending;
            }
            AcceptResponse::Confirm => {}
        };

        self.state.quorum_count += 1;

        if self.state.quorum_count >= self.quorum_threshold {
            ProcessResult::Ready
        } else {
            ProcessResult::Pending
        }
    }

    pub fn finish(self) -> Option<V> {
        if self.state.quorum_count < self.quorum_threshold {
            return None;
        }

        Some(self.state.value)
    }
}

/// An Acceptor is basically the backing storage for the Cluster
pub struct Acceptor<ID, V> {
    promise: Option<(u64, ID)>,
    accepted: Option<((u64, ID), V)>,
}

impl<ID, V> Default for Acceptor<ID, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<ID, V> Acceptor<ID, V> {
    pub fn new() -> Self {
        Self {
            promise: None,
            accepted: None,
        }
    }
}

impl<ID, V> Acceptor<ID, V>
where
    ID: PartialOrd + Clone,
    V: Clone,
{
    pub fn recv_prepare(&mut self, msg: PrepareMessage<ID>) -> PrepareResponse<ID, V> {
        // If we already have a promise whose id is larger than or equal to the received ballot
        // number, we have a conflict and should indicate that
        match self.promise.as_ref() {
            Some(val) if val >= &msg.ballot_number => {
                return PrepareResponse::Conflict {
                    proposed: msg.ballot_number,
                    existing: val.0,
                }
            }
            _ => {}
        };
        // If we already have an accepted value, whose id is larger than or equal to the received
        // ballot number, we have a conflict and should indicate that
        match self.accepted.as_ref() {
            Some((val, _)) if val >= &msg.ballot_number => {
                return PrepareResponse::Conflict {
                    proposed: msg.ballot_number,
                    existing: val.0,
                }
            }
            _ => {}
        };

        self.promise = Some(msg.ballot_number);

        PrepareResponse::Promise(self.accepted.clone())
    }

    pub fn recv_accept(&mut self, msg: AcceptMessage<ID, V>) -> AcceptResponse<ID> {
        // If we received a new promise whose ballot number is larger than the accept messages
        // ballot number, we have a conflict/the accept message is out of date
        match self.promise.as_ref() {
            Some(val) if val > &msg.id => {
                return AcceptResponse::Conflict {
                    proposed: msg.id,
                    existing: val.0,
                }
            }
            _ => {}
        };
        // If the currently accepted ballot number is equal to or larger than the proposed value,
        // we have a conflict
        match self.accepted.as_ref() {
            Some((id, _)) if id >= &msg.id => {
                return AcceptResponse::Conflict {
                    proposed: msg.id,
                    existing: id.0,
                }
            }
            _ => {}
        };

        self.promise = None;
        self.accepted = Some((msg.id, msg.value));

        AcceptResponse::Confirm
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn propose_msg() {
        let mut proposer = Proposer::new(13u8);

        let proposal: Proposal<'_, u8, _> = proposer.propose::<u8>(1);

        assert_eq!(
            PrepareMessage {
                ballot_number: (1, 13)
            },
            proposal.message().owned()
        );
    }

    #[test]
    fn workflow_initialize_read() {
        let mut proposer = Proposer::new(13u8);

        let mut acceptor = Acceptor::new();

        let mut proposal = proposer.propose::<String>(1);

        let propose_msg = proposal.message().owned();

        let promise_msg = acceptor.recv_prepare(propose_msg);

        assert_eq!(ProcessResult::Ready, proposal.process(promise_msg.clone()));
        let mut proposal = proposal
            .finish(|data| {
                assert_eq!(None, data);
                "Test Data".to_string()
            })
            .expect("The Processing for the promise stuff should be done and return a message");

        let accept_msg = proposal.message();
        let accepted_msgs = acceptor.recv_accept(accept_msg);

        assert_eq!(ProcessResult::Ready, proposal.process(accepted_msgs));
        let n_value = proposal.finish().expect("");

        assert_eq!("Test Data", n_value.as_str());
    }

    #[test]
    fn failure_3nodes_1down() {
        let mut proposer = Proposer::new(10u8);

        let mut acceptor1 = Acceptor::new();
        let mut acceptor2 = Acceptor::new();
        // let mut acceptor3 = Acceptor::new();

        let mut proposal = proposer.propose::<String>(2);

        let a1_promise = acceptor1.recv_prepare(proposal.message().owned());
        let a2_promise = acceptor2.recv_prepare(proposal.message().owned());

        assert_eq!(ProcessResult::Pending, proposal.process(a1_promise.clone()));
        assert_eq!(ProcessResult::Ready, proposal.process(a2_promise.clone()));
        let mut proposal = proposal
            .finish(|data| {
                assert_eq!(None, data);
                "Test Data".to_string()
            })
            .expect("We supplied 2 promise messages");

        let a1_accepted = acceptor1.recv_accept(proposal.message());
        let a2_accepted = acceptor2.recv_accept(proposal.message());

        assert_eq!(ProcessResult::Pending, proposal.process(a1_accepted));
        assert_eq!(ProcessResult::Ready, proposal.process(a2_accepted));
        let value = proposal.finish().expect("");

        assert_eq!("Test Data", value.as_str());
    }
}
