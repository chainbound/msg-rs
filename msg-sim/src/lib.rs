#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod protocol;

pub use protocol::Protocol;

use crate::{
    ip::Subnet,
    network::{NetworkGraph, PeerId},
};

pub mod namespace;

pub mod assert;
pub mod command;
pub mod ip;
pub mod network;
pub mod tc;

#[derive(Debug)]
pub struct Simulator {
    pub network: NetworkGraph,
}

impl Simulator {
    pub fn new(subnet: Subnet) -> Self {
        Self {
            network: NetworkGraph { subnet, peers: Default::default(), links: Default::default() },
        }
    }

    pub fn add_peers(&mut self, peer_1: PeerId, peer_2: PeerId) {
        if let Err(e) = self.network.add_peers(peer_1, peer_2) {
            tracing::error!(?e, ?peer_1, ?peer_2, "failed to add peers");
        }
    }
}

//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
