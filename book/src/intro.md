# MSG-RS

<a href="https://github.com/chainbound/msg-rs"><img src="https://img.shields.io/badge/GitHub%20Repo-msg-green?logo=github"></a>
<a href="https://discord.gg/nhWcSWYpm9"><img src="https://img.shields.io/badge/Discord-x?logo=discord&label=chainbound"></a>

## Introduction

> 📖 [MSG-RS][msg] is a flexible and lightweight messaging library for distributed
> systems built with Rust and Tokio, designed for performance and reliability.

This library is built and maintained by [Chainbound][chainbound], and is licensed
under the [MIT License][license].

## Overview

MSG-RS is inspired by projects like [ZeroMQ][zeromq] and [Nanomsg][nanomsg].
It was built because at Chainbound we needed a Rust-native messaging library
that was flexible and customizable enough without relying on C bindings or
external dependencies.

Another reason for building this library was to modularize the messaging
components of our [Fiber][fiber] project. Lastly, this was also used as
an opportunity to dive deeper into some async rust patterns to measure
optimizations in isolation from other distributed system components.

## Features

**1. Flexible and modular**

MSG-RS is designed to be modular by leveraging Rust's traits and generics. This
modularity can be seen in all aspects, from the transport layer to the socket patterns.

**2. Durable**

Durability is a critical aspect in distributed systems. By using smart abstractions
like the ones from [StubbornIO][stubborn-io], MSG is able to provide reliable
reconnections and retries out of the box.

**3. Extensible**

Even though MSG comes with different socket patterns and transport layers, it is also
designed to be easily extensible if you want to bring your own options like
authentication, encryption, and compression to the table.

## Development Status

**MSG is currently in ALPHA, and is not yet recommended for use in production.**

## Contributing

Contributors are welcome! Please see the [contributing guide][contributing] for
more information.

{{#include ./links.md}}
