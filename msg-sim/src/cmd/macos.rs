//! This module contains all the commands necessary to start and stop a network simulation
//! on MacOS.
//!
//! ## Implementation
//! Under the hood, this module builds a dummy network pipe with `dnctl` and `pfctl` and applies
//! the given configuration to it.
