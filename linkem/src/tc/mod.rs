//! # Traffic Control Utilities
//!
//! This module provides utilities for creating Linux traffic control (tc) requests using
//! rtnetlink. It enables network emulation by configuring queue disciplines (qdiscs) that
//! can introduce latency, packet loss, bandwidth limits, and other impairments.
//!
//! ## Architecture Overview
//!
//! We use a hierarchical qdisc structure to enable per-destination impairments. This means
//! that peer A can have different network conditions when talking to peer B vs peer C.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                        DRR Root Qdisc (1:0)                                 │
//! │                                                                             │
//! │   Deficit Round Robin (DRR) serves as our root classifier. It allows us     │
//! │   to create an arbitrary number of classes, one per destination peer.       │
//! │   Each class can have its own chain of qdiscs for traffic shaping.          │
//! │                                                                             │
//! │   DRR is used purely for classification, not bandwidth shaping. With a      │
//! │   quantum equal to the MTU, it fairly round-robins between child qdiscs.   │
//! └─────────────────────────────────────────────────────────────────────────────┘
//!                                    │
//!            ┌───────────────────────┼───────────────────────┐
//!            │                       │                       │
//!            ▼                       ▼                       ▼
//! ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
//! │  Class 1:1       │    │  Class 1:11      │    │  Class 1:12      │
//! │  (default)       │    │  (dest=peer 1)   │    │  (dest=peer 2)   │
//! │  quantum=MTU     │    │  quantum=MTU     │    │  quantum=MTU     │
//! │                  │    │                  │    │                  │
//! │  Unimpaired      │    │  Impaired path   │    │  Impaired path   │
//! │  traffic         │    │  to peer 1       │    │  to peer 2       │
//! └──────────────────┘    └──────────────────┘    └──────────────────┘
//!                                    │                       │
//!                                    ▼                       ▼
//!                         ┌──────────────────┐    ┌──────────────────┐
//!                         │  TBF (11:0)      │    │  TBF (12:0)      │
//!                         │  (optional)      │    │  (optional)      │
//!                         │                  │    │                  │
//!                         │  Rate limiting   │    │  Rate limiting   │
//!                         │  if bandwidth    │    │  if bandwidth    │
//!                         │  is configured   │    │  is configured   │
//!                         └──────────────────┘    └──────────────────┘
//!                                    │                       │
//!                                    ▼                       ▼
//!                         ┌──────────────────┐    ┌──────────────────┐
//!                         │  Netem (21:0)    │    │  Netem (22:0)    │
//!                         │                  │    │                  │
//!                         │  Latency, loss,  │    │  Different       │
//!                         │  jitter, etc.    │    │  impairments     │
//!                         └──────────────────┘    └──────────────────┘
//! ```
//!
//! ## Handle Numbering Scheme
//!
//! TC handles are 32-bit values split into major:minor (16:16 bits). Our scheme:
//!
//! | Component        | Handle          | Example (peer_id=2) |
//! |------------------|-----------------|---------------------|
//! | DRR root         | `1:0`           | `N/A`               |
//! | Default class    | `1:1`           | `N/A`               |
//! | Per-dest class   | `1:(10+id)`     | `1:12`              |
//! | TBF qdisc        | `(10+id):0`     | `12:0`              |
//! | Netem qdisc      | `(20+id):0`     | `22:0`              |
//!
//! ## Packet Flow
//!
//! 1. Packet enters DRR root qdisc
//! 2. Flower filter examines destination IP
//! 3. If destination matches a configured peer → route to that peer's class
//! 4. Otherwise → route to default class (1:1, no impairment)
//! 5. In the peer's class: TBF applies rate limiting (if configured)
//! 6. Then netem applies delay, loss, jitter, etc.
//!
//! ## Why DRR?
//!
//! DRR (Deficit Round Robin) is the simplest classful qdisc that supports dynamic class
//! creation. Unlike HTB (Hierarchical Token Bucket), it doesn't impose bandwidth shaping
//! semantics at the classification layer.
//!
//! With a quantum equal to the MTU (1500 bytes), DRR fairly round-robins between
//! destination classes—each class dequeues one packet per round. This prevents a bursty
//! flow to one peer from starving traffic to other peers.
//!
//! This clean separation of concerns means:
//! - **DRR**: Fair classification (round-robin packets to the right child qdisc)
//! - **TBF**: Rate limiting (enforce bandwidth caps)
//! - **Netem**: Network emulation (add delay, loss, jitter)

pub mod core;
pub mod drr;
pub mod filter;
pub mod handle;
pub mod impairment;
pub mod netem;
pub mod nla;
pub mod requests;
pub mod tbf;
