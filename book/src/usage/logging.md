# Logging

MSG-RS uses the [tracing](https://docs.rs/tracing/latest/tracing/) ecosystem crate for logging.

The tracing targets are configured using the [tracing-subscriber](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/) crate.
They are named after the crates that they are used in, and can be configured using the `RUST_LOG` environment variable.

For example, to enable logging for the `msg_socket` crate in your program, simply set:

```bash
RUST_LOG=msg_socket=debug cargo run
```

## Metrics

MSG-RS doesn't currently expose any [metrics](https://docs.rs/metrics/latest/metrics/) by default.

If you are interested in this feature, please let us know by [opening an issue][new-issue].

{{#include ../links.md}}
