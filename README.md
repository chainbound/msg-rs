<img src="./.github/assets/github-cover.png" alt="MSG-RS" width="100%" align="center">

<h4 align="center">
    A flexible and lightweight messaging library for distributed systems built with Rust and Tokio.
</h4>

<div align="center">

[![CI](https://github.com/chainbound/msg-rs/actions/workflows/ci.yml/badge.svg)][gh-ci]
[![License](https://img.shields.io/badge/License-MIT-orange.svg)][mit-license]
[![Book](https://img.shields.io/badge/Documentation_Book-MSG--RS-blue)][book]
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/chainbound/msg-rs)

</div>

## Overview

`msg-rs` is a messaging library that was inspired by projects like [ZeroMQ](https://zeromq.org/) and [Nanomsg](https://nanomsg.org/).
It was built because we needed a Rust-native messaging library like those above.

## Documentation

The 📖 [MSG-RS Book][book] contains detailed information on how to use the library.

## Features

- Multiple socket types
  - [x] Request/Reply
  - [x] Publish/Subscribe
- Pluggable transport layers
  - [x] TCP (with TLS support)
  - [x] QUIC (with TLS support)
  - [x] IPC
- Useful stats: latency, throughput, packet drops
- Durable IO abstraction (built-in retries and reconnections)
- Custom wire protocol with support for authentication and compression
- Network simulation mode with dummynet & pfctl
- Extensive benchmarks
- Integration tests

<!-- TODO:
- Socket types
  - [ ] Channel
  - [ ] Push/Pull
  - [ ] Survey/Respond
- Queuing
- Transport layers
  - [ ] TLS
  - [ ] UDP
  - [ ] Inproc
-->

## MSRV

The minimum supported Rust version is 1.88 (see [rust-toolchain.toml](./rust-toolchain.toml)).

## Contributions & Bug Reports

If you are interested in contributing or have found a bug, please check out the [contributing guide][contributing].
Please report any bugs or doubts you encounter by [opening a Github issue][new-issue].

Additionally, you can reach out to us on [Discord][discord] if you have any questions or just want to chat.

## License

This project is licensed under the Open Source [MIT license][mit-license].

## Disclaimer

<sub>
This software is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose and noninfringement. In no event shall the authors or copyright holders be liable for any claim, damages or other liability, whether in an action of contract, tort or otherwise, arising from, out of or in connection with the software or the use or other dealings in the software.
</sub>

<!-- Links -->

[book]: https://chainbound.github.io/msg-rs/
[gh-ci]: https://github.com/chainbound/msg-rs/actions/workflows/ci.yml
[discord]: https://discord.gg/nhWcSWYpm9
[new-issue]: https://github.com/chainbound/msg-rs/issues/new
[mit-license]: https://github.com/chainbound/msg-rs/blob/main/LICENSE
[contributing]: https://github.com/chainbound/msg-rs/blob/main/CONTRIBUTING.md
