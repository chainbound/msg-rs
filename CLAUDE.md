# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build and Test
```bash
# Primary commands (using just)
just check          # Check all workspace crates
just test           # Run tests with nextest
just clippy         # Run linting
just fmt            # Format code
just doc            # Generate documentation

# Alternative cargo commands
cargo build         # Build workspace
cargo nextest run   # Run tests (preferred test runner)
cargo test          # Standard test runner
cargo clippy        # Linting
cargo fmt           # Formatting
```

### Single Test Execution
```bash
# Run specific test file
cargo nextest run -p msg-socket --test integration_tests
# Run specific test case
cargo test test_name
```

### Benchmarks and Profiling
```bash
# Run benchmarks
cargo bench
# Generate flamegraphs (requires flamegraph tool)
cargo flamegraph --bin example_name
```

## Architecture Overview

**msg-rs** is a Rust messaging library inspired by ZeroMQ, built as a Cargo workspace with 6 crates:

### Core Crate Structure
- **`msg/`** - Main user-facing crate (facade pattern, re-exports everything)
- **`msg-socket/`** - Socket patterns (Req/Rep, Pub/Sub, with statistics and connection hooks)
- **`msg-transport/`** - Transport layer (TCP, QUIC, IPC with pluggable trait design)
- **`msg-wire/`** - Wire protocol (authentication, compression: gzip/lz4/snappy/zstd)
- **`msg-common/`** - Shared utilities (channels, task management, time utils)
- **`msg-sim/`** - Network simulation on Linux, powered by rtnetlink.

### Key Design Patterns
- **Trait-based extensibility** - Transport, ConnectionHook traits for pluggability
- **Async-first** - Built on Tokio, stream-based message handling
- **Statistics collection** - Built-in latency/throughput/drop metrics in sockets
- **Connection resilience** - Automatic reconnection with configurable backoff

### Socket Types Implementation Status
- **Request/Reply** (`ReqSocket`, `RepSocket`) - ✅ Fully implemented
- **Publish/Subscribe** (`PubSocket`, `SubSocket`) - ✅ Fully implemented  
- **Channel, Push/Pull, Survey/Respond** - 🚧 Planned future additions

## Configuration Details

### Build Profiles
- **dev** - Development with debug info
- **bench** - Benchmarking optimized
- **maxperf** - Maximum performance (release + specific flags)
- **debug-maxperf** - Performance with debug symbols

### Platform-Specific Features
- **Linux**: JeMalloc integration for memory performance and `msg-sim` support
- **MSRV**: Rust 1.86

## Testing Architecture

### Test Organization
- **Unit tests** - Distributed across each crate's `src/` directories
- **Integration tests** - Located in `msg-socket/tests/`
- **Examples** - Comprehensive examples in `msg/examples/` (also serve as integration tests)
- **Benchmarks** - Criterion-based in `msg/benches/`

### Network Simulation Testing
- Use `msg-sim` crate for chaos testing under various network conditions
- Supports latency injection, bandwidth limiting, packet loss simulation

## Documentation Structure

- **Main docs** - Generated via `just doc`, hosted at docs.rs
- **Book** - Comprehensive guide in `book/` directory, deployed to GitHub Pages
- **Examples** - Rich example set covering all major usage patterns in `msg/examples/`

## Important Implementation Notes

### Connection Hooks
- Implement `ConnectionHook` trait for custom authentication, handshakes, or protocol negotiation
- Built-in token-based auth via `hooks::token::ServerHook` and `hooks::token::ClientHook`

### Statistics Collection  
- All socket types collect latency, throughput, and packet drop metrics
- Access via socket statistics methods for monitoring

### Transport Selection
- **QUIC** - Preferred for modern applications (built-in encryption, better performance)
- **TCP** - Traditional, widely supported
- **IPC** - Inter-process communication when both processes on same machine

### Current Development Status
- **Alpha stage** - API may change, not production-ready
- **Active development** - Regular feature additions
- **Chainbound/Fiber integration** - Primary use case driving development
