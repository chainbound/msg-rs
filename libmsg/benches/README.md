# `msg-rs` benchmarks
WIP

We use [criterion](https://github.com/bheisler/criterion.rs) for benchmarking.

## Running benchmarks
You can run all benchmarks using `cargo bench`, or individual benchmarks using `cargo bench --bench <bench_name>`.

## Profiling
To enable profiling, run `cargo bench <opts> -- --profile-time <secs>`. This will generate a flamegraph in `target/criterion/<bench_name>/profile/flamegraph.svg`.

