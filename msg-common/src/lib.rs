use std::time::SystemTime;

use futures::future::BoxFuture;

/// Returns the current UNIX timestamp in microseconds.
#[inline]
pub fn unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// Wraps the given error in a boxed future.
pub fn async_error<E: std::error::Error + Send + 'static, T>(
    e: E,
) -> BoxFuture<'static, Result<T, E>> {
    Box::pin(async move { Err(e) })
}
