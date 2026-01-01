#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

pub mod dynch;
pub mod ip;
pub mod namespace;
pub mod network;
pub mod tc;
pub mod wrappers;

pub trait TryClone: Sized {
    type Error;

    fn try_clone(&self) -> std::result::Result<Self, Self::Error>;
}
