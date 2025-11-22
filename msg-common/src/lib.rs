//! Common utilities and types for msg-rs.

#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};

use derive_more::{Deref, DerefMut};
use futures::future::BoxFuture;

mod channel;
pub use channel::{Channel, channel};

mod task;
pub use task::JoinMap;

use rand::Rng;

/// Returns the current UNIX timestamp in microseconds.
#[inline]
pub fn unix_micros() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as u64
}

/// Wraps the given error in a boxed future.
pub fn async_error<E: std::error::Error + Send + 'static, T>(
    e: E,
) -> BoxFuture<'static, Result<T, E>> {
    Box::pin(async move { Err(e) })
}

#[allow(non_upper_case_globals)]
pub mod constants {
    pub const KiB: u32 = 1024;
    pub const MiB: u32 = 1024 * KiB;
    pub const GiB: u32 = 1024 * MiB;
}

/// Extension trait for `SocketAddr`.
pub trait SocketAddrExt: Sized {
    /// Returns the unspecified IPv4 socket address, bound to port 0.
    fn unspecified_v4() -> Self;

    /// Returns the unspecified IPv6 socket address, bound to port 0.
    fn unspecified_v6() -> Self;

    /// Returns the unspecified socket address of the same family as `other`, bound to port 0.
    fn as_unspecified(&self) -> Self;
}

impl SocketAddrExt for SocketAddr {
    #[inline]
    fn unspecified_v4() -> Self {
        Self::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
    }

    #[inline]
    fn unspecified_v6() -> Self {
        Self::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0))
    }

    #[inline]
    fn as_unspecified(&self) -> Self {
        match self {
            Self::V4(_) => Self::unspecified_v4(),
            Self::V6(_) => Self::unspecified_v6(),
        }
    }
}

/// Extension trait for IP addresses.
pub trait IpAddrExt: Sized {
    /// Returns the localhost address of the same family as `other`.
    fn as_localhost(&self) -> Self;
}

impl IpAddrExt for IpAddr {
    #[inline]
    fn as_localhost(&self) -> Self {
        match self {
            Self::V4(_) => Self::V4(Ipv4Addr::LOCALHOST),
            Self::V6(_) => Self::V6(Ipv6Addr::LOCALHOST),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdBase58<const N: usize = 6>([u8; N]);

impl IdBase58 {
    /// The Base58 alphabet.
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    /// Generates a new random Base58 short ID.
    #[inline]
    pub fn new() -> Self {
        let raw: u64 = rand::random();
        Self::to_base58(raw)
    }

    /// Generates a new random Base58 short ID using the given RNG.
    #[inline]
    pub fn new_with_rng<T: Rng>(rng: &mut T) -> Self {
        let raw: u64 = rng.r#gen();
        Self::to_base58(raw)
    }

    /// Converts the given u64 to a Base58 short ID.
    #[inline]
    pub fn to_base58(mut x: u64) -> Self {
        let mut out = [b'1'; 6]; // '1' is the first in base58

        for i in (0..6).rev() {
            out[i] = Self::ALPHABET[(x % 58) as usize];
            x /= 58;
        }

        Self(out)
    }
}

impl Default for IdBase58 {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for IdBase58 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // SAFETY: it is always valid UTF-8 since it only contains Base58 characters
        f.write_str(unsafe { core::str::from_utf8_unchecked(&self.0) })
    }
}

/// A container with a [`tracing::Span`] attached.
#[derive(Debug, Clone, Deref, DerefMut)]
pub struct Spanned<T> {
    #[deref]
    #[deref_mut]
    pub inner: T,
    pub span: tracing::Span,
}

impl<T> Spanned<T> {
    /// Create a spanned container using [`tracing::Span::current`] span.
    #[inline]
    pub fn current(inner: T) -> Self {
        Self { inner, span: tracing::Span::current() }
    }

    /// Create a container with a no-op [`tracing::Span::none`] span, to be eventually replaced
    /// with [`Self::with_span`]
    #[inline]
    pub fn new(inner: T) -> Self {
        Self { inner, span: tracing::Span::none() }
    }

    /// Replace the current [`tracing::Span`] with the provided one.
    #[inline]
    pub fn with_span(mut self, span: tracing::Span) -> Self {
        self.span = span;
        self
    }

    /// Break the spanned container into a tuple containing the inner object and the span.
    #[inline]
    pub fn into_parts(self) -> (T, tracing::Span) {
        (self.inner, self.span)
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: Future> Future for Spanned<T> {
    type Output = Spanned<T::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe {
            // SAFETY: we never move `inner` while polling, and span is `Unpin`.
            let this = self.get_unchecked_mut();
            let inner = Pin::new_unchecked(&mut this.inner);
            let span = &this.span;

            let _g = span.enter();

            if let Poll::Ready(val) = inner.poll(cx) {
                return Poll::Ready(Spanned::new(val).with_span(span.clone()));
            }

            Poll::Pending
        }
    }
}

pub trait SpannedExt<T> {
    fn with_span(self, span: tracing::Span) -> Spanned<T>;
}

impl<T> SpannedExt<T> for T {
    fn with_span(self, span: tracing::Span) -> Spanned<T> {
        Spanned { inner: self, span }
    }
}
