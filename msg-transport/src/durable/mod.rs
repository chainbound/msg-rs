//! This package contains durable transport alternatives to the
//! standard ones. Thanks to craftytrickster for his work on [`stubborn-io`](https://github.com/craftytrickster/stubborn-io), which
//! this module is based on.
//!
//! A "durable" transport is one that can survive transient failures,
//! and is self-healing in the face of such failures.

mod config;
mod io;
mod strategies;
mod tcp;

pub use config::ReconnectOptions;
pub use tcp::DurableTcpStream;
