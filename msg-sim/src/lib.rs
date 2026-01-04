#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

//! In-process network emulation for Linux, powered by `rtnetlink`.

pub mod dynch;
pub mod ip;
pub mod namespace;
pub mod network;
pub mod tc;
pub mod wrappers;
