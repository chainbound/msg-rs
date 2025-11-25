use std::{
    pin::Pin,
    task::{Context, Poll},
};

use derive_more::{Deref, DerefMut};

/// A container with a [`tracing::Span`] attached.
///
/// If `T` is a `Future`, the span will be entered when polling the future, making this struct
/// almost equivalent to [`tracing::Instrument`]. The main difference is that the return value will
/// contain the span as well so it can be re-entered later.
#[derive(Debug, Clone, Deref, DerefMut)]
pub struct WithSpan<T> {
    #[deref]
    #[deref_mut]
    pub inner: T,
    pub span: tracing::Span,
}

impl<T> WithSpan<T> {
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

impl<T: Future> Future for WithSpan<T> {
    type Output = WithSpan<T::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe {
            // SAFETY: we never move `inner` while polling, and span is `Unpin`.
            let this = self.get_unchecked_mut();
            let inner = Pin::new_unchecked(&mut this.inner);
            let span = &this.span;

            let _g = span.enter();

            if let Poll::Ready(val) = inner.poll(cx) {
                return Poll::Ready(WithSpan::new(val).with_span(span.clone()));
            }

            Poll::Pending
        }
    }
}

/// A container with a [`tracing::span::EnteredSpan`] attached.
#[derive(Debug, Deref, DerefMut)]
pub struct WithEntered<T> {
    #[deref]
    #[deref_mut]
    pub inner: T,
    pub span: tracing::span::EnteredSpan,
}

/// Trait to convert [`WithSpan`] containers into [`WithEntered`] containers.
pub trait IntoEntered<T, S> {
    fn into_entered(self) -> S;
}

impl<T> IntoEntered<T, WithEntered<T>> for WithSpan<T> {
    fn into_entered(self) -> WithEntered<T> {
        let WithSpan { inner, span } = self;
        WithEntered { inner, span: span.entered() }
    }
}

impl<T> IntoEntered<T, Option<WithEntered<T>>> for Option<WithSpan<T>> {
    fn into_entered(self) -> Option<WithEntered<T>> {
        self.map(|v| v.into_entered())
    }
}

impl<T> IntoEntered<T, Poll<WithEntered<T>>> for Poll<WithSpan<T>> {
    fn into_entered(self) -> Poll<WithEntered<T>> {
        match self {
            Poll::Ready(val) => {
                let WithSpan { inner, span } = val;
                Poll::Ready(WithEntered { inner, span: span.entered() })
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> IntoEntered<T, Poll<Option<WithEntered<T>>>> for Poll<Option<WithSpan<T>>> {
    fn into_entered(self) -> Poll<Option<WithEntered<T>>> {
        match self {
            Poll::Ready(val) => {
                if let Some(val) = val {
                    let WithSpan { inner, span } = val;
                    Poll::Ready(Some(WithEntered { inner, span: span.entered() }))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub trait SpanExt<T> {
    fn with_span(self, span: tracing::Span) -> WithSpan<T>;
}

impl<T> SpanExt<T> for T {
    fn with_span(self, span: tracing::Span) -> WithSpan<T> {
        WithSpan { inner: self, span }
    }
}
