#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OneRoundTrip {
    Disabled,
    Enabled,
}

#[derive(Debug, Clone)]
pub struct ProposerConfig {
    one_round_trip: OneRoundTrip,
}

impl ProposerConfig {
    pub fn basic() -> Self {
        Self {
            one_round_trip: OneRoundTrip::Disabled,
        }
    }
}
