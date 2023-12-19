# Contribution guidelines

Thanks for your interest in contributing to making MSG better!

## How to contribute

### Getting started

To get started with MSG, you will need the Rust toolchain installed on your machine.
You can find the installation instructions [here](https://www.rust-lang.org/tools/install).

Once you have the necessary tools installed, you can clone the repository and run the tests:

```shell
git clone git@github.com:chainbound/msg-rs.git
cd msg-rs

cargo test --all
```

### Development workflow

We use Github for all our development workflow. If you are not familiar with Github, you can find
a great guide [here](https://guides.github.com/activities/hello-world/).

We use Github issues to track all our work. If you want to contribute, you can find a list of
open issues [here](https://github.com/chainbound/msg-rs/issues). If you want to work on an issue,
please leave a comment on the specific issue so that we it can be assigned to you.

When testing your changes, please use the following commands and make sure that they all pass:

```
cargo check --all
cargo test --all
cargo +nightly fmt -- --check
cargo +nightly clippy --all --all-features -- -D warnings
```

Once you are done with your changes, you can open a pull request. We will review your changes
and provide feedback. Once the changes are approved, your pull request will be merged.

### Asking for help

If you have any questions, you can open a [new issue](https://github.com/chainbound/msg-rs/issues/new)
or join our [Discord server](https://discord.gg/nhWcSWYpm9).

### Code of conduct

MSG adheres to the [Rust Code of Conduct](https://www.rust-lang.org/policies/code-of-conduct).
This document describes the minimum behavior expected from all contributors.

### License

By contributing to MSG, you agree that your contributions will be licensed under its
[MIT license](https://github.com/chainbound/msg-rs/blob/main/LICENSE).
