use std::marker::PhantomData;

pub struct Proposer<V> {
    current_n: usize,
    _marker: PhantomData<V>,
}

/// The Prepare Message send as part of Phase 1a: Prepare
#[derive(Debug, Clone)]
pub struct PrepareMessage {
    n: usize,
}

#[derive(Debug, Clone)]
pub struct AcceptRequest<V> {
    n: usize,
    value: V,
}

impl<V> Proposer<V> {
    // Propose a Value

    pub fn propose_msg(&self) -> PrepareMessage {
        PrepareMessage { n: self.current_n }
    }

    pub fn accept_msgs<PI>(&self, responses: PI, value: V) -> AcceptRequest<V>
    where
        PI: IntoIterator<Item = PromiseResponse<V>>,
    {
        let max_accepted_value = responses
            .into_iter()
            .filter_map(|msgs| msgs.accepted)
            .max_by(|f, s| f.0.cmp(&s.0));

        match max_accepted_value {
            Some((_, z)) => AcceptRequest {
                n: self.current_n,
                value: z,
            },
            None => AcceptRequest {
                n: self.current_n,
                value,
            },
        }
    }
}

pub struct Acceptor<V> {
    highest: usize,
    accepted: Option<(usize, V)>,
}

#[derive(Debug, PartialEq)]
pub struct PromiseResponse<V> {
    accepted: Option<(usize, V)>,
}

pub struct AcceptedResponse<V> {
    n: usize,
    value: V,
}

impl<V> Acceptor<V>
where
    V: Clone,
{
    // Phase 1b: Promise
    pub fn received_prepare(&mut self, msg: PrepareMessage) -> Option<PromiseResponse<V>> {
        if msg.n > self.highest {
            let previous_values = self.accepted.clone();
            self.highest = msg.n;

            Some(PromiseResponse {
                accepted: previous_values,
            })
        } else {
            None
        }
    }

    pub fn recv_accept(&mut self, msg: AcceptRequest<V>) -> Option<AcceptedResponse<V>> {
        if self.highest > msg.n {
            return None;
        }

        self.accepted = Some((msg.n, msg.value.clone()));

        Some(AcceptedResponse {
            n: msg.n,
            value: msg.value,
        })
    }
}

pub struct Learner<V> {
    n: usize,
    value: V,
}

impl<V> Learner<V> {
    pub fn accept<AI>(&mut self, msgs: AI, acceptors: usize)
    where
        AI: IntoIterator<Item = AcceptedResponse<V>>,
    {
        let mut msgs = msgs.into_iter();

        let msg = match msgs.next() {
            Some(m) => m,
            None => return,
        };
        let counts = 1 + msgs.count();

        if counts <= acceptors / 2 {
            return;
        }

        self.n = msg.n;
        self.value = msg.value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_proposes() {
        let proposer1: Proposer<u8> = Proposer {
            current_n: 1,
            _marker: PhantomData {},
        };
        let proposer2: Proposer<u8> = Proposer {
            current_n: 1,
            _marker: PhantomData {},
        };

        let mut acceptor: Acceptor<u8> = Acceptor {
            highest: 0,
            accepted: None,
        };

        let p1_msg = proposer1.propose_msg();
        let p2_msg = proposer2.propose_msg();

        let a1_promise = acceptor.received_prepare(p1_msg).unwrap();
        assert_eq!(None, acceptor.received_prepare(p2_msg));

        let p1_accept = proposer1.accept_msgs([a1_promise], 3);

        let a1_accepted = acceptor.recv_accept(p1_accept.clone()).unwrap();
        let a2_accepted = acceptor.recv_accept(p1_accept).unwrap();

        assert_eq!(3, a1_accepted.value);
        assert_eq!(3, a2_accepted.value);
    }

    #[test]
    fn basic() {
        let proposer1: Proposer<u8> = Proposer {
            current_n: 1,
            _marker: PhantomData {},
        };

        let mut acceptor1: Acceptor<u8> = Acceptor {
            highest: 0,
            accepted: None,
        };
        let mut acceptor2: Acceptor<u8> = Acceptor {
            highest: 0,
            accepted: None,
        };

        let mut learner1: Learner<u8> = Learner { n: 0, value: 0 };

        let p1_msg = proposer1.propose_msg();

        let a1 = acceptor1.received_prepare(p1_msg.clone()).unwrap();
        let a2 = acceptor2.received_prepare(p1_msg.clone()).unwrap();

        let p1_msg2 = proposer1.accept_msgs([a1, a2], 3);

        let a1_m2 = acceptor1.recv_accept(p1_msg2.clone()).unwrap();
        let a2_m2 = acceptor2.recv_accept(p1_msg2.clone()).unwrap();

        learner1.accept([a1_m2, a2_m2], 3);

        assert_eq!(3, learner1.value);
    }
}
