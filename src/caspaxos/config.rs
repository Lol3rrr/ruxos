//! Contains all the Configurations for the Proposers and Acceptors in a CASPaxos System

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

/// The General Configuration that holds all the options
#[derive(Debug, Clone)]
pub struct ProposerConfig {
    one_round_trip: OneRoundTrip,
}

impl ProposerConfig {
    /// Gets the most basic Configuration possible, without any optimizations enabled
    pub fn basic() -> Self {
        Self {
            one_round_trip: OneRoundTrip::Disabled,
        }
    }

    /// Gets an Configuration with all optimizations enabled
    pub fn optimized() -> Self {
        Self {
            one_round_trip: OneRoundTrip::Enabled,
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
}
