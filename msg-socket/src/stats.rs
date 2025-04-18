use std::fmt::Debug;

#[derive(Debug)]
pub struct SocketStats<S> {
    pub(crate) specific: S,
}

impl<S: Default> Default for SocketStats<S> {
    fn default() -> Self {
        Self {
            specific: S::default(),
        }
    }
}

