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
//! │                        HTB Root Qdisc (1:0)                                │
//! │                                                                             │
//! │   Hierarchical Token Bucket (HTB) serves as our root classifier. It        │
//! │   allows us to create an arbitrary number of classes, one per destination   │
//! │   peer. Each class can have its own chain of qdiscs for traffic shaping.   │
//! │                                                                             │
//! │   HTB is used purely for classification, not bandwidth shaping. All         │
//! │   classes have effectively unlimited rate (~10 Gbit/s) and same priority,  │
//! │   so HTB fairly round-robins between child qdiscs.                         │
//! │                                                                             │
//! │   HTB was chosen over DRR because it correctly handles non-work-conserving │
//! │   children (netem with delay). When netem returns NULL, HTB tries the next │
//! │   class instead of blocking—preventing head-of-line blocking.              │
//! └─────────────────────────────────────────────────────────────────────────────┘
//!                                    │
//!            ┌───────────────────────┼───────────────────────┐
//!            │                       │                       │
//!            ▼                       ▼                       ▼
//! ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
//! │  Class 1:1       │    │  Class 1:11      │    │  Class 1:12      │
//! │  (default)       │    │  (dest=peer 1)   │    │  (dest=peer 2)   │
//! │  rate=unlimited  │    │  rate=unlimited  │    │  rate=unlimited  │
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
//! | HTB root         | `1:0`           | `N/A`               |
//! | Default class    | `1:1`           | `N/A`               |
//! | Per-dest class   | `1:(10+id)`     | `1:12`              |
//! | TBF qdisc        | `(10+id):0`     | `12:0`              |
//! | Netem qdisc      | `(20+id):0`     | `22:0`              |
//!
//! ## Packet Flow
//!
//! 1. Packet enters HTB root qdisc
//! 2. Flower filter examines destination IP
//! 3. If destination matches a configured peer -> route to that peer's class
//! 4. Otherwise -> route to default class (1:1, no impairment)
//! 5. In the peer's class: TBF applies rate limiting (if configured)
//! 6. Then netem applies delay, loss, jitter, etc.
//!
//! ## Why HTB?
//!
//! HTB (Hierarchical Token Bucket) was chosen over DRR (Deficit Round Robin) because
//! of a fundamental head-of-line blocking interaction between DRR and netem:
//!
//! DRR's `dequeue()` peeks at the first active class. When netem's peek returns NULL
//! (delay hasn't elapsed), DRR immediately returns without trying other classes. This
//! causes high-delay destinations to block low-delay destinations.
//!
//! HTB's `dequeue()` correctly handles this: when a child qdisc returns NULL, HTB
//! advances to the next class via `htb_next_rb_node()`. This prevents head-of-line
//! blocking entirely.
//!
//! With all classes at the same priority and effectively unlimited rate (~10 Gbit/s),
//! HTB acts as a fair classifier that correctly interleaves traffic across destinations
//! with different delays.
//!
//! This clean separation of concerns means:
//! - **HTB**: Fair classification (route packets to the right child qdisc)
//! - **TBF**: Rate limiting (enforce bandwidth caps)
//! - **Netem**: Network emulation (add delay, loss, jitter)

pub mod core;
pub mod htb;
pub mod filter;
pub mod handle;
pub mod impairment;
pub mod netem;
pub mod nla;
pub mod requests;
pub mod tbf;
