//! Utilities and extension traits for dealing with IP addresses.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

pub trait IpAddrExt {
    fn to_ipv6_mapped(&self) -> Ipv6Addr;
    fn to_bits(&self) -> u128;
    fn from_bits(bits: u128) -> Self;
}

impl IpAddrExt for IpAddr {
    /// If it is a v4, it converts it to a v6, otherwise it is a no-op.
    fn to_ipv6_mapped(&self) -> Ipv6Addr {
        match self {
            IpAddr::V4(v4) => v4.to_ipv6_mapped(),
            IpAddr::V6(v6) => *v6,
        }
    }

    /// Returns the bits of the IP address, padding with zeros in case of an IPv4 address.
    fn to_bits(&self) -> u128 {
        match self {
            Self::V4(v4) => v4.to_bits().into(),
            Self::V6(v6) => v6.to_bits(),
        }
    }

    /// Create an IPv4 or IPv6 address depending on whether the value provided is greater than
    /// [`u32::MAX`].
    fn from_bits(bits: u128) -> Self {
        if bits < u32::MAX as u128 {
            Ipv4Addr::from_bits(bits as u32).into()
        } else {
            Ipv6Addr::from_bits(bits).into()
        }
    }
}

/// A subnet, composed of a base IP address and a netmask.
#[derive(Debug, Clone, Copy)]
pub struct Subnet {
    pub network_address: IpAddr,
    pub netmask: u8,
}

impl Subnet {
    pub fn new(address: IpAddr, mask: u8) -> Self {
        Self { network_address: address, netmask: mask }
    }
}
