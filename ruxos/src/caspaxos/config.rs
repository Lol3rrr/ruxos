//! Contains all the Configurations for the Proposers and Acceptors in a CASPaxos System
//!
//! # Note
//! Most of these configurations are highly dependant on the rest of the system and how this is
//! system is used, so even though there are general tips to what should be used, it is best to
//! test with different configurations and see what works best.

/// Configures the One-Round-Trip Optimization
///
/// The One-Round-Trip Optimization attempts to reduce the number of messages for a proposal from 2
/// to 1, by sending a "premature" prepare message with the last accept message so that if the same
/// proposer executes the next proposal as well, it can skip the prepare phase and just execute the
/// accept phase.
///
/// This does not impact correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OneRoundTrip {
    /// Disables the Optimization
    Disabled,
    /// Enables the Optimization
    Enabled,
}

/// Thrifty is an Optimization about reducing the number of messages being send.
///
/// Even though the algorithm only needs to reach a quorum of `n/2 + 1` nodes, it can help to send
/// the requests to more nodes to proactively avoid delays if some nodes are slow or maybe crashed.
/// However this does introduce extra networking (and cpu) overhead.
///
/// Importantly this does *not* influence the size of the quorum that actually needs to be reached
/// for consensus, only the number of nodes that get the messages while trying to reach consensus.
/// Therefore this only represents a *performance* optimization, which does *not* influence
/// correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Thrifty {
    /// Sends the messages to all Nodes
    AllNodes,
    /// Sends the messages to the minimum number of nodes
    Minimum,
    // TODO
    // Some kind of middleground ?
}

/// Controls for how the Quorum sizes are choosen
///
/// # References
/// * [Flexible Paxos: Quorum intersection revisited](https://arxiv.org/pdf/1608.06696.pdf)
/// * [Relaxed Paxos: Quorum intersection revisited (again)](https://dl.acm.org/doi/pdf/10.1145/3517209.3524040)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QuorumSize {
    /// Uses the normal Quorums for Paxos
    Normal,
    /// Uses a function to calculate the size of the quorum for the prepare quorum, the accept
    /// quroum size is then determined based on the prepare size
    Simple { prepare_size: fn(usize) -> usize },
}

/// The General Configuration that holds all the options
#[derive(Debug, Clone)]
pub struct ProposerConfig {
    one_round_trip: OneRoundTrip,
    thrifty: Thrifty,
    qsize: QuorumSize,
}

impl ProposerConfig {
    /// Gets the most basic Configuration possible, without any optimizations enabled
    ///
    /// # Settings
    /// * [`OneRoundTrip::Disabled`]
    /// * [`Thrifty::Minimum`]
    /// * [`QuorumSize::Normal`]
    ///
    /// ```rust
    /// # use ruxos::caspaxos::config::*;
    /// let config = ProposerConfig::basic();
    /// assert_eq!(false, config.one_roundtrip());
    /// assert_eq!(&Thrifty::Minimum, config.thrifty());
    /// assert_eq!(&QuorumSize::Normal, config.quroum_size());
    /// ```
    pub fn basic() -> Self {
        Self {
            one_round_trip: OneRoundTrip::Disabled,
            thrifty: Thrifty::Minimum,
            qsize: QuorumSize::Normal,
        }
    }

    /// Gets an Configuration with all optimizations enabled
    ///
    /// # Settings
    /// * [`OneRoundTrip::Enabled`]
    /// * [`Thrifty::AllNodes`]
    /// * [`QuorumSize::Normal`]
    ///
    /// ```rust
    /// # use ruxos::caspaxos::config::*;
    /// let config = ProposerConfig::optimized();
    /// assert_eq!(true, config.one_roundtrip());
    /// assert_eq!(&Thrifty::AllNodes, config.thrifty());
    /// assert_eq!(&QuorumSize::Normal, config.quroum_size());
    /// ```
    pub fn optimized() -> Self {
        Self {
            one_round_trip: OneRoundTrip::Enabled,
            thrifty: Thrifty::AllNodes,
            qsize: QuorumSize::Normal,
        }
    }

    /// Updates the setting for the [`OneRoundTrip`] Optimization
    pub fn with_roundtrip(mut self, value: OneRoundTrip) -> Self {
        self.one_round_trip = value;
        self
    }

    /// Check if the [`OneRoundTrip`] Optimization is enabled
    pub fn one_roundtrip(&self) -> bool {
        matches!(self.one_round_trip, OneRoundTrip::Enabled)
    }

    /// Updates the setting for the [`Thrifty`] Optimization
    pub fn with_thrifty(mut self, value: Thrifty) -> Self {
        self.thrifty = value;
        self
    }

    /// Get the current [`Thrifty`] setting
    pub fn thrifty(&self) -> &Thrifty {
        &self.thrifty
    }

    pub fn with_quorum_size(mut self, value: QuorumSize) -> Self {
        self.qsize = value;
        self
    }

    pub fn quroum_size(&self) -> &QuorumSize {
        &self.qsize
    }
}
