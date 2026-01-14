# msg-sim: Building a Linux Network Simulator from Scratch

When we started working on `msg-rs` we ran into the necessity of testing with real networks with latency, packet loss, and bandwidth constraints. We needed a way to test under realistic conditions without setting up actual distributed infrastructure.

**msg-sim** is what we ended up building. It's a Rust library that creates isolated network peers using Linux namespaces and injects network impairments between them using the kernel's traffic control subsystem. This article documents how it works and the decisions we made along the way.

## Existing Tools and the Gap

Before building something new, we looked at what already existed. Network emulation is a solved problem in many contexts—just not ours.

**tc/netem** is the foundation everything else builds on. The Linux kernel's traffic control subsystem can add delay, loss, bandwidth limits, and more to any network interface. The problem is that it operates at the system level. You run `tc` commands to modify interfaces, and those changes affect everything using that interface. There's no isolation between tests, no easy way to create multi-peer topologies, and integrating it into a Rust test suite means shelling out to external commands.

**[Mahimahi](http://mahimahi.mit.edu/)** is a set of composable shell tools from MIT. You wrap your command in shells like `mm-delay 50 mm-loss uplink 0.1` and it spawns containers with the specified impairments. It's elegant for benchmarking a single application against various network conditions, but it's designed around spawning subprocesses. You can't call it from inside a Rust async runtime and get back a handle to interact with. Each shell also represents a single link—you can't easily model "Peer A has 10ms latency to Peer B but 100ms to Peer C."

**[Mininet](https://github.com/mininet/mininet)** and its fork **[Containernet](https://containernet.github.io/)** are Python-based network emulators popular in SDN research. They can create complex topologies with switches, routers, and hosts. Containernet extends this to use Docker containers as hosts. These are powerful tools, but they require defining your topology upfront in Python, spinning up the emulated network, and then running your tests inside it. The workflow is "create topology first, then run code inside"—the opposite of what we wanted for Rust integration tests.

**[Toxiproxy](https://github.com/Shopify/toxiproxy)** from Shopify takes a different approach: it's a TCP proxy that sits between your application and its dependencies. You configure "toxics" (latency, timeouts, bandwidth limits) via a REST API. It's great for testing how your app handles a flaky database connection, but it's not network emulation—it's application-layer proxying. Your code has to connect through the proxy, which means changing connection strings. It also can't simulate things like packet loss at the IP level or test UDP-based protocols.

**[Pumba](https://github.com/alexei-led/pumba)** is a chaos testing tool for Docker containers. It can kill containers, pause them, or use `tc` to inject network faults. But it operates on running containers from the outside—you define your Docker Compose setup, start it, then point Pumba at containers to disrupt. Like Containernet, the topology exists before your test code runs.

**[netsim](https://github.com/canndrew/netsim)** is the closest to what we needed. It's a Rust library that uses Linux namespaces to isolate network stacks and lets you run async code inside them. We looked at it seriously. But it appears to be unmaintained (limited commit activity), and it doesn't provide the per-destination impairment control we needed. When Peer A sends to Peer B versus Peer C, we wanted different latency and loss characteristics—netsim's model didn't support that out of the box.

The common pattern across these tools: either they require external setup (Docker, Python scripts, CLI wrappers), or they model a single degraded link rather than a topology with per-peer-pair impairments. For e2e tests and benchmarks where you want to spin up a simulated network from Rust code, run some async tasks inside isolated peers, and tear it all down—there wasn't an obvious solution.

## What You Can Build

The core abstraction is simple: create a network, add peers, define impairments between them, and run async code inside each peer's isolated namespace.

```rust
let mut network = Network::new(subnet).await?;

// Add peers—each gets its own network namespace and IP
let frankfurt = network.add_peer().await?;
let tokyo = network.add_peer().await?;
let new_york = network.add_peer().await?;

// Define realistic cross-region conditions
network.apply_impairment(
    Link(frankfurt, tokyo),
    LinkImpairment::new()
        .latency_ms(120)
        .jitter_ms(5)
        .loss_percent(0.1)
).await?;

network.apply_impairment(
    Link(frankfurt, new_york),
    LinkImpairment::new()
        .latency_ms(40)
        .bandwidth_mbit(100.0)
).await?;

// Run your distributed system
network.run_in_namespace(frankfurt, || async {
    // This code sees frankfurt's network stack.
    // Connections to tokyo experience 120ms latency.
    // Connections to new_york experience 40ms latency.
    start_consensus_node(config).await
}).await?;
```

Impairments are directional and per-link. Frankfurt to Tokyo can have different characteristics than Tokyo to Frankfurt. You can model asymmetric links, regional variations, or specific failure scenarios.

### Testing Distributed Consensus

One of our main use cases is testing consensus protocols under degraded conditions. With msg-sim, you can:

- Verify your protocol handles network partitions correctly by setting 100% packet loss on specific links
- Test leader election under asymmetric latency conditions
- Ensure timeouts are tuned correctly by simulating cross-region delays
- Reproduce specific failure scenarios from production

### Benchmarking Protocol Performance

Because impairments run at the kernel level (not application-level proxying), you get accurate measurements. If you're comparing how two protocols perform under 50ms latency with 1% packet loss, the numbers reflect real kernel-level delays and drops.

### Chaos Testing

Impairments are dynamic—you can change them at runtime without recreating the network:

```rust
// Start with good conditions
network.apply_impairment(Link(a, b), LinkImpairment::default()).await?;

// Degrade the link mid-test
tokio::time::sleep(Duration::from_secs(10)).await;
network.apply_impairment(
    Link(a, b),
    LinkImpairment::new().latency_ms(500).loss_percent(10.0)
).await?;

// Restore it
tokio::time::sleep(Duration::from_secs(10)).await;
network.apply_impairment(Link(a, b), LinkImpairment::default()).await?;
```

This lets you test how your system responds to transient network issues, degradation during operation, or recovery from failures.

## How It Works

Under the hood, msg-sim creates a hub-and-spoke network topology using Linux namespaces:

```
┌──────────────────────────────────────────────────────┐
│              Hub Namespace (msg-sim-hub)             │
│              Bridge Device (msg-sim-br0)             │
│        ┌───────────┼───────────┐                     │
│    hub-veth-1  hub-veth-2  hub-veth-3                │
└────────┼───────────┼───────────┼─────────────────────┘
         │           │           │
   (veth pairs connect namespaces)
         │           │           │
┌────────┴────┐ ┌────┴────┐ ┌────┴────┐
│   Peer 1    │ │  Peer 2 │ │  Peer 3 │
│  10.0.0.1   │ │ 10.0.0.2│ │ 10.0.0.3│
│   tc rules  │ │ tc rules│ │ tc rules│
└─────────────┘ └─────────┘ └─────────┘
```

Each peer lives in its own network namespace with a virtual ethernet pair connecting it to a central bridge. Traffic control rules on each peer's interface apply impairments based on destination IP—so Peer 1 can have different latency to Peer 2 versus Peer 3.

The `tc` configuration uses a hierarchy of qdiscs: DRR for classification, TBF for bandwidth limiting, and netem for delay/loss/jitter. This is all managed through netlink, so there's no shelling out to external commands.

Running code inside a namespace is handled through an actor pattern. Each namespace has a dedicated thread running a single-threaded Tokio runtime. When you call `run_in_namespace`, your closure gets sent to that thread, executed in the isolated network context, and the result comes back through a channel. This keeps namespace isolation clean—`setns(2)` is thread-local, so we can't just switch namespaces in a multi-threaded runtime.

## Impairment Options

Each link can be configured with:

| Parameter | Description |
|-----------|-------------|
| `latency` | Base propagation delay (microseconds) |
| `jitter` | Random variation added to latency (±) |
| `loss` | Packet loss percentage (0-100) |
| `duplicate` | Packet duplication percentage |
| `bandwidth` | Rate limit in Mbit/s |
| `burst` | Burst allowance for bandwidth limiting |

Latency and jitter model propagation delay—the time it takes packets to travel the link. Bandwidth limiting models link capacity with a token bucket filter. These can be combined to simulate various network conditions: a satellite link (high latency, moderate bandwidth), a congested datacenter link (low latency, bandwidth constrained), or a flaky mobile connection (variable latency, packet loss).

## Limitations

**Linux only.** The implementation uses namespaces, netlink, and tc. No macOS or Windows support.

**Root required.** Creating namespaces and configuring tc needs `CAP_NET_ADMIN`.

**Single-threaded tests.** Parallel tests can conflict when manipulating namespaces. We run with `--test-threads=1`.

## Closing Notes

msg-sim exists because we needed it for testing msg-rs. It's alpha-quality and the API may change. The code is in the msg-rs repository—the tc module has extensive comments if you want to understand the qdisc hierarchy and netlink message construction.

The first time a test failed because of simulated network conditions, we found a real timeout bug that would have shown up in production. That made the whole effort worthwhile.
