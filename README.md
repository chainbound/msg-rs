<img src="./.github/assets/github-cover.png" alt="MSG-RS" width="100%" align="center">

<h4 align="center">
    A flexible and lightweight messaging library for distributed systems built with Rust and Tokio.
</h4>

<div align="center">

[![CI](https://github.com/chainbound/msg-rs/actions/workflows/ci.yml/badge.svg)][gh-ci]
[![License](https://img.shields.io/badge/License-MIT-orange.svg)][mit-license]
[![Book](https://img.shields.io/badge/Documentation_Book-MSG--RS-blue)][book]
[![Discord](https://img.shields.io/discord/1024661923737899058?style=flat&logo=discord&logoColor=white&color=lightgreen)][discord]

</div>

## Overview

`msg-rs` is a messaging library that was inspired by projects like [ZeroMQ](https://zeromq.org/) and [Nanomsg](https://nanomsg.org/).
It was built because we needed a Rust-native messaging library like those above.

> MSG is still in ALPHA and is not ready for production use.

## Documentation

The [MSG-RS Book][book] contains detailed information on how to use the library.

## Features

- [ ] Multiple socket types
  - [x] Request/Reply
  - [x] Publish/Subscribe
  - [ ] Channel
  - [ ] Push/Pull
  - [ ] Survey/Respond
- [ ] Stats (RTT, throughput, packet drops etc.)
- [x] Request/Reply basic stats
- [ ] Queuing
- [ ] Pluggable transport layer
  - [x] TCP
  - [ ] TLS
  - [ ] IPC
  - [ ] UDP
  - [ ] Inproc
- [x] Durable IO abstraction (built-in retries and reconnections)
- [ ] Simulation modes with [Turmoil](https://github.com/tokio-rs/turmoil)

## MSRV

The minimum supported Rust version is 1.70.

## Contributions & Bug Reports

If you are interested in contributing or have found a bug, please check out the [contributing guide][contributing].
Please report any bugs or doubts you encounter by [opening a Github issue][new-issue].

Additionally, you can reach out to us on [Discord][discord] if you have any questions or just want to chat.

## License

This project is licensed under the Open Source [MIT license][mit-license].

<!-- Links -->

[book]: https://chainbound.github.io/msg-rs/
[gh-ci]: https://github.com/chainbound/msg-rs/actions/workflows/ci.yml
[discord]: https://discord.gg/nhWcSWYpm9
[new-issue]: https://github.com/chainbound/msg-rs/issues/new
[mit-license]: https://github.com/chainbound/msg-rs/blob/main/LICENSE
[contributing]: https://github.com/chainbound/msg-rs/blob/main/CONTRIBUTING.md
