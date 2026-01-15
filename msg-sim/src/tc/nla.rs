//! Netlink attribute building utilities.
//!
//! Netlink uses a TLV (Type-Length-Value) format for attributes. This module
//! provides helpers for constructing these attributes correctly.

use nix::libc::TCA_OPTIONS;
use rtnetlink::packet_core::{DefaultNla, NLA_HEADER_SIZE};

/// Build a nested NLA (Netlink Attribute) containing sub-attributes.
///
/// Netlink uses a TLV (Type-Length-Value) format. For qdisc options like HTB and TBF,
/// the `TCA_OPTIONS` attribute contains nested sub-attributes, each with their own type.
///
/// # NLA Header Format
///
/// ```text
/// ┌─────────────────────────────────────────┐
/// │  Length (2 bytes) │  Type (2 bytes)     │  <- NLA header (4 bytes)
/// ├─────────────────────────────────────────┤
/// │  Value (variable length, padded to 4)   │
/// └─────────────────────────────────────────┘
/// ```
///
/// Reference: <linux/netlink.h>
///
/// # Returns
///
/// A byte vector containing the complete NLA (header + value + padding).
pub(crate) fn build_nla(nla_type: u16, value: &[u8]) -> Vec<u8> {
    // NLA length includes the 4-byte header
    let nla_len = NLA_HEADER_SIZE + value.len();
    // Pad to 4-byte alignment
    let padded_len = (nla_len + 3) & !3;

    let mut buf = vec![0u8; padded_len];

    // Write length (2 bytes, native endian)
    buf[0..2].copy_from_slice(&(nla_len as u16).to_ne_bytes());
    // Write type (2 bytes, native endian)
    buf[2..4].copy_from_slice(&nla_type.to_ne_bytes());
    // Write value
    buf[NLA_HEADER_SIZE..NLA_HEADER_SIZE + value.len()].copy_from_slice(value);
    // Padding bytes are already zero from vec initialization

    buf
}

/// Build a nested TCA_OPTIONS attribute containing one or more sub-attributes.
///
/// This wraps sub-attributes in a `TCA_OPTIONS` container, which is what the kernel
/// expects for qdisc-specific configuration.
///
/// # Arguments
///
/// * `sub_attrs` - One or more NLA sub-attributes (already formatted with headers)
///
/// # Returns
///
/// A `DefaultNla` that can be added to a `TcMessage`.
pub(crate) fn build_nested_options(sub_attrs: Vec<u8>) -> DefaultNla {
    DefaultNla::new(TCA_OPTIONS, sub_attrs)
}
