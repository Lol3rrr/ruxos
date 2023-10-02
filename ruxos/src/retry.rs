use std::time::Duration;

/// A simple Wrapper around a [`Retry`] implementation that is used by most of the code in this
/// crate, instead of using the [`Retry`] trait directly
pub struct RetryStrategy<'r> {
    retry: &'r mut dyn Retry,
}

impl<'r> RetryStrategy<'r> {
    /// Constructs a new Wrapper
    pub fn new(inner: &'r mut dyn Retry) -> Self {
        Self { retry: inner }
    }

    pub(crate) fn should_retry(&mut self) -> bool {
        self.retry.should_retry()
    }

    pub(crate) async fn wait(&mut self) {
        if let Some(dur) = self.retry.wait_time() {
            tokio::time::sleep(dur).await;
        }
    }
}

impl<'r, R> From<&'r mut R> for RetryStrategy<'r>
where
    R: Retry,
{
    fn from(value: &'r mut R) -> Self {
        Self { retry: value }
    }
}

/// How operations in this crate should be retried
pub trait Retry {
    /// Check if you the operation should continue to be retried
    fn should_retry(&mut self) -> bool;

    /// The time that should be waited between retries, where [`None`] represents no waiting time
    fn wait_time(&mut self) -> Option<Duration>;
}

impl Retry for () {
    fn should_retry(&mut self) -> bool {
        true
    }

    fn wait_time(&mut self) -> Option<Duration> {
        None
    }
}

/// Provides a flexible structure to construct a Retry-Strategy based on custom functions for
/// determining if a retry should happen and what the delay/wait-time should be
pub struct FilteredBackoff<F, B> {
    filter: F,
    backoff: B,
}

impl<F, B> Retry for FilteredBackoff<F, B>
where
    F: FnMut() -> bool,
    B: FnMut() -> Option<Duration>,
{
    fn should_retry(&mut self) -> bool {
        (self.filter)()
    }

    fn wait_time(&mut self) -> Option<Duration> {
        (self.backoff)()
    }
}

// TODO
// This seems ugly
impl FilteredBackoff<(), ()> {
    /// Allows for a custom construction based on the provided functions
    pub fn custom<F, B>(filter: F, backoff: B) -> FilteredBackoff<F, B>
    where
        F: FnMut() -> bool,
        B: FnMut() -> Option<Duration>,
    {
        FilteredBackoff { filter, backoff }
    }

    /// Retries the given number of times with no wait-time inbetween retries
    pub fn limit_no_backoff(
        limit: usize,
    ) -> FilteredBackoff<impl FnMut() -> bool, impl FnMut() -> Option<Duration>> {
        let mut current_retry: usize = 0;
        FilteredBackoff {
            filter: move || {
                current_retry = current_retry.saturating_add(1);
                current_retry <= limit
            },
            backoff: move || None,
        }
    }

    /// Continues retrying forever with no wait-time
    pub fn unlimited_no_backoff(
    ) -> FilteredBackoff<impl FnMut() -> bool, impl FnMut() -> Option<Duration>> {
        FilteredBackoff {
            filter: move || true,
            backoff: move || None,
        }
    }

    /// Retries the given number of times and always waits a constant wait-time between retries
    pub fn limit_constant(
        limit: usize,
        constant: Duration,
    ) -> FilteredBackoff<impl FnMut() -> bool, impl FnMut() -> Option<Duration>> {
        let mut current_retry: usize = 0;
        FilteredBackoff {
            filter: move || {
                current_retry = current_retry.saturating_add(1);
                current_retry <= limit
            },
            backoff: move || Some(constant.clone()),
        }
    }

    /// Retries forever and always waits a constant wait-time between retries
    pub fn unlimited_constant(
        constant: Duration,
    ) -> FilteredBackoff<impl FnMut() -> bool, impl FnMut() -> Option<Duration>> {
        FilteredBackoff {
            filter: move || true,
            backoff: move || Some(constant.clone()),
        }
    }

    /// Retries the given number of times and increases the wait-time for each retry, starting at
    /// `start` and always increasing by `increment`
    pub fn limit_linear_backoff(
        limit: usize,
        mut start: Duration,
        increment: Duration,
    ) -> FilteredBackoff<impl FnMut() -> bool, impl FnMut() -> Option<Duration>> {
        let mut current_retry: usize = 0;
        FilteredBackoff {
            filter: move || {
                current_retry = current_retry.saturating_add(1);
                current_retry <= limit
            },
            backoff: move || {
                let res = start.clone();
                start = start.saturating_add(increment);

                Some(res)
            },
        }
    }

    /// Retries forever and increases the wait-time for each retry, starting at `start` and always
    /// increasing by `increment`
    pub fn unlimited_linear_backoff(
        mut start: Duration,
        increment: Duration,
    ) -> FilteredBackoff<impl FnMut() -> bool, impl FnMut() -> Option<Duration>> {
        FilteredBackoff {
            filter: move || true,
            backoff: move || {
                let res = start.clone();
                start = start.saturating_add(increment);

                Some(res)
            },
        }
    }

    /// Retries the given number of times and increases the wait-time for each retry, starting at
    /// `start` and always multiplying by the provided factor
    pub fn limit_exponential_backoff(
        limit: usize,
        mut start: Duration,
        factor: u32,
    ) -> FilteredBackoff<impl FnMut() -> bool, impl FnMut() -> Option<Duration>> {
        let mut current_retry: usize = 0;
        FilteredBackoff {
            filter: move || {
                current_retry = current_retry.saturating_add(1);
                current_retry <= limit
            },
            backoff: move || {
                let res = start.clone();
                start = start.saturating_mul(factor);

                Some(res)
            },
        }
    }

    /// Retries forever and increases the wait-time for each retry, starting at `start` and always
    /// multiplying by the provided factor
    pub fn unlimited_exponential_backoff(
        mut start: Duration,
        factor: u32,
    ) -> FilteredBackoff<impl FnMut() -> bool, impl FnMut() -> Option<Duration>> {
        FilteredBackoff {
            filter: move || true,
            backoff: move || {
                let res = start.clone();
                start = start.saturating_mul(factor);

                Some(res)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_limits() {
        let mut limited_none = FilteredBackoff::limit_no_backoff(2);
        assert!(limited_none.should_retry());
        assert!(limited_none.should_retry());
        assert!(!limited_none.should_retry());

        let mut limited_constant = FilteredBackoff::limit_constant(2, Duration::from_secs(1));
        assert!(limited_constant.should_retry());
        assert!(limited_constant.should_retry());
        assert!(!limited_constant.should_retry());

        let mut limited_linear = FilteredBackoff::limit_linear_backoff(
            2,
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        assert!(limited_linear.should_retry());
        assert!(limited_linear.should_retry());
        assert!(!limited_linear.should_retry());

        let mut limited_exponential =
            FilteredBackoff::limit_exponential_backoff(2, Duration::from_secs(1), 1);
        assert!(limited_exponential.should_retry());
        assert!(limited_exponential.should_retry());
        assert!(!limited_exponential.should_retry());
    }

    #[test]
    fn wait_times() {
        let mut limited_none = FilteredBackoff::limit_no_backoff(2);
        assert_eq!(None, limited_none.wait_time());

        let mut limited_constant = FilteredBackoff::limit_constant(2, Duration::from_secs(1));
        assert_eq!(Some(Duration::from_secs(1)), limited_constant.wait_time());
        assert_eq!(Some(Duration::from_secs(1)), limited_constant.wait_time());

        let mut limited_linear = FilteredBackoff::limit_linear_backoff(
            2,
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        assert_eq!(Some(Duration::from_secs(1)), limited_linear.wait_time());
        assert_eq!(Some(Duration::from_secs(2)), limited_linear.wait_time());

        let mut limited_exponential =
            FilteredBackoff::limit_exponential_backoff(2, Duration::from_secs(1), 2);
        assert_eq!(
            Some(Duration::from_secs(1)),
            limited_exponential.wait_time()
        );
        assert_eq!(
            Some(Duration::from_secs(2)),
            limited_exponential.wait_time()
        );
        assert_eq!(
            Some(Duration::from_secs(4)),
            limited_exponential.wait_time()
        );
    }
}
