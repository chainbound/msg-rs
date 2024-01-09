//! This module contains all the commands necessary to start and stop a network simulation
//! on MacOS.
//!
//! ## Implementation
//! Under the hood, this module builds a dummy network pipe with `dnctl` and `pfctl` and applies
//! the given configuration to it.

const PFCTL: &str = "pfctl";
const DNCTL: &str = "dnctl";
const IFCONFIG: &str = "ifconfig";

pub struct Pfctl;

pub struct Dnctl;

pub struct Ifconfig;
