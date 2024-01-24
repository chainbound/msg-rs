use futures::{future::poll_fn, Future};
use std::{
    collections::HashSet,
    task::{ready, Context, Poll},
};
use tokio::task::{JoinError, JoinSet};

/// A collection of keyed tasks spawned on a Tokio runtime.
/// Hacky implementation of a join set that allows for a key to be associated with each task by having
/// the task return a tuple of (key, value).
#[derive(Debug, Default)]
pub struct JoinMap<K, V> {
    keys: HashSet<K>,
    joinset: JoinSet<(K, V)>,
}

impl<K, V> JoinMap<K, V> {
    /// Create a new `JoinSet`.
    pub fn new() -> Self {
        Self {
            keys: HashSet::new(),
            joinset: JoinSet::new(),
        }
    }

    /// Returns the number of tasks currently in the `JoinSet`.
    pub fn len(&self) -> usize {
        self.joinset.len()
    }

    /// Returns whether the `JoinSet` is empty.
    pub fn is_empty(&self) -> bool {
        self.joinset.is_empty()
    }
}

impl<K, V> JoinMap<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: 'static,
{
    /// Spawns a task onto the Tokio runtime that will execute the given future ONLY IF
    /// there is not already a task in the set with the same key.
    pub fn spawn<F>(&mut self, key: K, future: F)
    where
        F: Future<Output = (K, V)> + Send + 'static,
        V: Send,
    {
        if self.keys.insert(key) {
            self.joinset.spawn(future);
        }
    }

    /// Returns `true` if the `JoinSet` contains a task for the given key.
    pub fn contains_key(&self, key: &K) -> bool {
        self.keys.contains(key)
    }

    /// Waits until one of the tasks in the set completes and returns its output.
    ///
    /// Returns `None` if the set is empty.
    ///
    /// # Cancel Safety
    ///
    /// This method is cancel safe. If `join_next` is used as the event in a `tokio::select!`
    /// statement and some other branch completes first, it is guaranteed that no tasks were
    /// removed from this `JoinSet`.
    pub async fn join_next(&mut self) -> Option<Result<(K, V), JoinError>> {
        poll_fn(|cx| self.poll_join_next(cx)).await
    }

    /// Polls for one of the tasks in the set to complete.
    ///
    /// If this returns `Poll::Ready(Some(_))`, then the task that completed is removed from the set.
    ///
    /// When the method returns `Poll::Pending`, the `Waker` in the provided `Context` is scheduled
    /// to receive a wakeup when a task in the `JoinSet` completes. Note that on multiple calls to
    /// `poll_join_next`, only the `Waker` from the `Context` passed to the most recent call is
    /// scheduled to receive a wakeup.
    ///
    /// # Returns
    ///
    /// This function returns:
    ///
    ///  * `Poll::Pending` if the `JoinSet` is not empty but there is no task whose output is
    ///     available right now.
    ///  * `Poll::Ready(Some(Ok(value)))` if one of the tasks in this `JoinSet` has completed.
    ///     The `value` is the return value of one of the tasks that completed.
    ///  * `Poll::Ready(Some(Err(err)))` if one of the tasks in this `JoinSet` has panicked or been
    ///     aborted. The `err` is the `JoinError` from the panicked/aborted task.
    ///  * `Poll::Ready(None)` if the `JoinSet` is empty.
    ///
    /// Note that this method may return `Poll::Pending` even if one of the tasks has completed.
    /// This can happen if the [coop budget] is reached.
    pub fn poll_join_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(K, V), JoinError>>> {
        match ready!(self.joinset.poll_join_next(cx)) {
            Some(Ok((key, value))) => {
                self.keys.remove(&key);
                Poll::Ready(Some(Ok((key, value))))
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}
