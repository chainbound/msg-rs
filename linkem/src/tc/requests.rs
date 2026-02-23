//! Helpers to make traffic control requests, given a [`rtnetlink::Handle`].
//!
//! These functions handle the low-level netlink operations for configuring
//! Linux traffic control (tc) qdiscs, classes, and filters.

use std::net::IpAddr;

use futures::StreamExt as _;
use rtnetlink::packet_core::NetlinkPayload;
use rtnetlink::packet_route::tc::TcHandle;

use crate::network::PeerId;
use crate::tc::filter::{FlowerFilterRequest, U32CatchallFilterRequest};
use crate::tc::handle::{QdiscRequestInner, htb_class_handle, netem_handle, tbf_handle};
use crate::tc::htb::{HtbClassRequest, QdiscHtbRequest};
use crate::tc::impairment::LinkImpairment;
use crate::tc::netem::QdiscNetemRequest;
use crate::tc::tbf::QdiscTbfRequest;

/// Install HTB root qdisc with default class and catch-all filter.
///
/// This sets up the root qdisc hierarchy on a peer's veth interface:
/// - HTB root qdisc (1:0) with `defcls=1`
/// - Default class (1:1) for unimpaired traffic
/// - U32 catch-all filter to route unclassified packets to 1:1
pub async fn install_htb_root(
    handle: &mut rtnetlink::Handle,
    if_index: i32,
) -> std::result::Result<(), rtnetlink::Error> {
    tracing::debug!("installing htb root qdisc");

    let htb_request = QdiscHtbRequest::new(QdiscRequestInner::new(if_index)).build();

    let mut res = handle.request(htb_request)?;
    while let Some(res) = res.next().await {
        if let NetlinkPayload::Error(e) = res.payload {
            tracing::debug!(?e, "failed to create htb root qdisc");
            return Err(rtnetlink::Error::NetlinkError(e));
        }
    }

    // Create the default class (1:1) for unimpaired traffic.
    let default_class_request = HtbClassRequest::new(
        QdiscRequestInner::new(if_index)
            .with_parent(TcHandle::from(0x0001_0000)) // Parent: HTB root
            .with_handle(TcHandle::from(0x0001_0001)), // Handle: 1:1
    )
    .build();

    let mut res = handle.request(default_class_request)?;
    while let Some(res) = res.next().await {
        if let NetlinkPayload::Error(e) = res.payload {
            tracing::debug!(?e, "failed to create default htb class");
            return Err(rtnetlink::Error::NetlinkError(e));
        }
    }

    // Create a catch-all filter to route unclassified traffic to class 1:1.
    tracing::debug!("creating u32 catch-all filter for default class");
    let catchall_filter = U32CatchallFilterRequest::new(
        QdiscRequestInner::new(if_index).with_parent(TcHandle::from(0x0001_0000)),
    )
    .with_class_id(0x0001_0001)
    .build();

    let mut res = handle.request(catchall_filter)?;
    while let Some(res) = res.next().await {
        if let NetlinkPayload::Error(e) = res.payload {
            tracing::debug!(?e, "failed to create u32 catch-all filter");
            return Err(rtnetlink::Error::NetlinkError(e));
        }
    }

    tracing::debug!("htb root qdisc, default class, and catch-all filter installed");
    Ok(())
}

/// Create or replace an HTB class for a destination peer.
///
/// Each destination peer gets its own class with handle 1:(10 + peer_id).
pub async fn configure_htb_class(
    handle: &mut rtnetlink::Handle,
    if_index: i32,
    dst_peer_id: PeerId,
    is_replacement: bool,
) -> std::result::Result<(), rtnetlink::Error> {
    let class_handle = htb_class_handle(dst_peer_id);
    tracing::debug!(
        dst_peer_id,
        class_handle = format!("{:x}", class_handle),
        is_replacement,
        "creating htb class for destination"
    );

    let class_request = HtbClassRequest::new(
        QdiscRequestInner::new(if_index)
            .with_parent(TcHandle::from(0x0001_0000)) // Parent: HTB root (1:0)
            .with_handle(TcHandle::from(class_handle)),
    )
    .with_replace(is_replacement)
    .build();

    let mut res = handle.request(class_request)?;
    while let Some(res) = res.next().await {
        if let NetlinkPayload::Error(e) = res.payload {
            tracing::debug!(?e, "failed to create htb class");
            return Err(rtnetlink::Error::NetlinkError(e));
        }
    }

    Ok(())
}

/// Create or replace TBF qdisc for bandwidth limiting (if enabled).
///
/// Returns the parent handle that netem should attach to:
/// - If TBF is created: returns the TBF handle
/// - If no bandwidth limit: returns the HTB class handle
pub async fn configure_tbf(
    handle: &mut rtnetlink::Handle,
    if_index: i32,
    dst_peer_id: PeerId,
    impairment: &LinkImpairment,
    is_replacement: bool,
) -> std::result::Result<TcHandle, rtnetlink::Error> {
    let class_handle = htb_class_handle(dst_peer_id);

    let Some(mut tbf_request) = QdiscTbfRequest::try_new(
        QdiscRequestInner::new(if_index)
            .with_parent(TcHandle::from(class_handle))
            .with_handle(TcHandle::from(tbf_handle(dst_peer_id))),
        impairment,
    ) else {
        // No bandwidth limiting - netem attaches directly to HTB class
        return Ok(TcHandle::from(class_handle));
    };

    let tbf_h = tbf_handle(dst_peer_id);
    tracing::debug!(
        bandwidth_mbit = ?impairment.bandwidth_mbit_s,
        burst_kib = ?impairment.burst_kib,
        tbf_handle = format!("{}:{}", tbf_h >> 16, tbf_h & 0xFFFF),
        is_replacement,
        "creating tbf qdisc for bandwidth limiting"
    );

    if is_replacement {
        tbf_request = tbf_request.with_replace(true);
    }

    let mut res = handle.request(tbf_request.build())?;
    while let Some(res) = res.next().await {
        if let NetlinkPayload::Error(e) = res.payload {
            tracing::debug!(?e, "failed to create TBF qdisc");
            return Err(rtnetlink::Error::NetlinkError(e));
        }
    }

    // Netem's parent is the TBF qdisc
    Ok(TcHandle::from(tbf_h))
}

/// Create or replace netem qdisc for delay, loss, and jitter.
pub async fn configure_netem(
    handle: &mut rtnetlink::Handle,
    if_index: i32,
    dst_peer_id: PeerId,
    netem_parent: TcHandle,
    impairment: &LinkImpairment,
    is_replacement: bool,
) -> std::result::Result<(), rtnetlink::Error> {
    let netem_h = netem_handle(dst_peer_id);
    tracing::debug!(
        latency_us = impairment.latency,
        jitter_us = impairment.jitter,
        loss_pct = impairment.loss,
        duplicate_pct = impairment.duplicate,
        netem_handle = format!("{}:{}", netem_h >> 16, netem_h & 0xFFFF),
        is_replacement,
        "creating netem qdisc"
    );

    let netem_request = QdiscNetemRequest::from_impairment(
        QdiscRequestInner::new(if_index)
            .with_parent(netem_parent)
            .with_handle(TcHandle::from(netem_h)),
        impairment,
    )
    .with_replace(is_replacement)
    .build();

    let mut res = handle.request(netem_request)?;
    while let Some(res) = res.next().await {
        if let NetlinkPayload::Error(e) = res.payload {
            tracing::debug!(?e, "failed to create netem qdisc");
            return Err(rtnetlink::Error::NetlinkError(e));
        }
    }

    Ok(())
}

/// Create flower filter to classify traffic to a destination peer.
pub async fn configure_flower_filter(
    handle: &mut rtnetlink::Handle,
    if_index: i32,
    dst_peer_id: PeerId,
    dst_ip: IpAddr,
) -> std::result::Result<(), rtnetlink::Error> {
    let class_handle = htb_class_handle(dst_peer_id);
    tracing::debug!(
        dst_ip = %dst_ip,
        class_id = format!("1:{}", class_handle & 0xFFFF),
        "creating flower filter"
    );

    let filter_request = FlowerFilterRequest::new(
        QdiscRequestInner::new(if_index).with_parent(TcHandle::from(0x0001_0000)),
        dst_ip,
    )
    .with_class_id(class_handle)
    .build();

    let mut res = handle.request(filter_request)?;
    while let Some(msg) = res.next().await {
        if let NetlinkPayload::Error(e) = msg.payload {
            tracing::debug!(?e, "failed to create flower filter");
            return Err(rtnetlink::Error::NetlinkError(e));
        }
    }

    Ok(())
}
