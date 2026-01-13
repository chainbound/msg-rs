# `msg-sim`

In-process network emulation for Linux, powered by `rtnetlink`.

This crate enables testing distributed systems under various network conditions
(latency, packet loss, bandwidth limits) using Linux network namespaces and
traffic control.

## Architecture

Each peer runs in an isolated network namespace, connected through a central hub:

```text
                      Hub Namespace (msg-sim-hub)
┌─────────────────────────────────────────────────────────────┐
│                    Bridge (msg-sim-br0)                     │
└─────────┬───────────────────┬───────────────────┬───────────┘
          │                   │                   │
     veth pair           veth pair           veth pair
          │                   │                   │
┌─────────┴─────────┐ ┌───────┴──────────┐ ┌──────┴───────────┐
│  Peer 1 Namespace │ │ Peer 2 Namespace │ │ Peer 3 Namespace │
│  IP: 10.0.0.1     │ │ IP: 10.0.0.2     │ │ IP: 10.0.0.3     │
│  TC: per-dest     │ │ TC: per-dest     │ │ TC: per-dest     │
└───────────────────┘ └──────────────────┘ └──────────────────┘
```

See [`src/network.rs`](src/network.rs) for detailed architecture documentation.

## Features

- **Per-destination impairments**: Different network conditions for each peer-to-peer link
- **Latency & jitter**: Configurable delay with random variation (netem)
- **Packet loss & duplication**: Percentage-based random effects
- **Bandwidth limiting**: Token bucket filter (TBF) rate limiting
- **Dynamic updates**: Change impairments at runtime without recreating the network

## Quick Example

```rust
use msg_sim::{network::{Network, Link}, tc::impairment::LinkImpairment, ip::Subnet};
use std::net::Ipv4Addr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subnet = Subnet::new(Ipv4Addr::new(10, 0, 0, 0).into(), 24);
    let mut network = Network::new(subnet).await?;

    let peer_1 = network.add_peer().await?;
    let peer_2 = network.add_peer().await?;

    // Simulate a slow, lossy link
    network.apply_impairment(
        Link::new(peer_1, peer_2),
        LinkImpairment {
            latency: 100_000,              // 100ms
            loss: 1.0,                     // 1% packet loss
            bandwidth_mbit_s: Some(10.0),  // 10 Mbit/s
            ..Default::default()
        },
    ).await?;

    // Run code in peer's network namespace
    network.run_in_namespace(peer_1, |_ctx| {
        Box::pin(async move {
            // Network operations here see the configured impairments
        })
    }).await?;

    Ok(())
}
```

## Known Limitations

### Packet Duplication Restriction

The Linux kernel prevents creating additional netem qdiscs on a network interface
once one with `duplicate > 0` exists. This means **packet duplication can only be
used on at most one outgoing link per peer**.

For example, if peer A has links to peers B, C, and D:
- You CAN set `duplicate > 0` on the A→B link
- You CANNOT then set any impairments on A→C or A→D (even without duplication)

**Workaround**: If you need multiple outgoing links from a peer, either use
`duplicate` on only one of them, or don't use `duplicate` at all from that peer.

This is enforced by the [`check_netem_in_tree()`][kernel-netem] function in the
Linux kernel, which returns:
> "netem: cannot mix duplicating netems with other netems in tree"

See [`LinkImpairment::duplicate`](src/tc/impairment.rs) for details.

[kernel-netem]: https://github.com/torvalds/linux/blob/master/net/sched/sch_netem.c

## Running Tests

Tests require root privileges to create network namespaces:

```bash
sudo env "PATH=$PATH" "HOME=$HOME" cargo test -p msg-sim --all-features -- --test-threads=1
```

The `--test-threads=1` flag is recommended to avoid conflicts between concurrent namespace operations.
