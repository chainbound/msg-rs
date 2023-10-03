//! Provides options to configure the behavior of stubborn-io items,
//! specifically related to reconnect behavior.
use std::time::Duration;

use super::strategies::ExpBackoffStrategy;

pub type DurationIterator = Box<dyn Iterator<Item = Duration> + Send + Sync>;

/// User specified options that control the behavior of the stubborn-io upon disconnect.
pub struct ReconnectOptions {
    /// Represents a function that generates an Iterator
    /// to schedule the wait between reconnection attempts.
    pub retries_to_attempt_fn: Box<dyn Fn() -> DurationIterator + Send + Sync>,

    /// If this is set to true, if the initial connect method of the stubborn-io item fails,
    /// then no further reconnects will be attempted
    pub exit_if_first_connect_fails: bool,

    /// Invoked when the StubbornIo establishes a connection
    pub on_connect_callback: Box<dyn Fn() + Send + Sync>,

    /// Invoked when the StubbornIo loses its active connection
    pub on_disconnect_callback: Box<dyn Fn() + Send + Sync>,

    /// Invoked when the StubbornIo fails a connection attempt
    pub on_connect_fail_callback: Box<dyn Fn() + Send + Sync>,
}

impl ReconnectOptions {
    /// By default, the stubborn-io will not try to reconnect if the first connect attempt fails.
    /// By default, the retries iterator waits longer and longer between reconnection attempts,
    /// until it eventually perpetually tries to reconnect every 30 minutes.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        ReconnectOptions {
            retries_to_attempt_fn: Box::new(|| Box::new(ExpBackoffStrategy::default().into_iter())),
            exit_if_first_connect_fails: true,
            on_connect_callback: Box::new(|| {}),
            on_disconnect_callback: Box::new(|| {}),
            on_connect_fail_callback: Box::new(|| {}),
        }
    }

    /// This convenience function allows the user to provide any function that returns a value
    /// that is convertible into an iterator, such as an actual iterator or a Vec.
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use stubborn_io::ReconnectOptions;
    ///
    /// // With the below vector, the stubborn-io item will try to reconnect three times,
    /// // waiting 2 seconds between each attempt. Once all three tries are exhausted,
    /// // it will stop attempting.
    /// let options = ReconnectOptions::new().with_retries_generator(|| {
    ///     vec![
    ///         Duration::from_secs(2),
    ///         Duration::from_secs(2),
    ///         Duration::from_secs(2),
    ///     ]
    /// });
    /// ```
    pub fn with_retries_generator<F, I, IN>(mut self, retries_generator: F) -> Self
    where
        F: 'static + Send + Sync + Fn() -> IN,
        I: 'static + Send + Sync + Iterator<Item = Duration>,
        IN: IntoIterator<IntoIter = I, Item = Duration>,
    {
        self.retries_to_attempt_fn = Box::new(move || Box::new(retries_generator().into_iter()));
        self
    }

    pub fn with_exit_if_first_connect_fails(mut self, value: bool) -> Self {
        self.exit_if_first_connect_fails = value;
        self
    }

    pub fn with_on_connect_callback(mut self, cb: impl Fn() + 'static + Send + Sync) -> Self {
        self.on_connect_callback = Box::new(cb);
        self
    }

    pub fn with_on_disconnect_callback(mut self, cb: impl Fn() + 'static + Send + Sync) -> Self {
        self.on_disconnect_callback = Box::new(cb);
        self
    }

    pub fn with_on_connect_fail_callback(mut self, cb: impl Fn() + 'static + Send + Sync) -> Self {
        self.on_connect_fail_callback = Box::new(cb);
        self
    }
}
