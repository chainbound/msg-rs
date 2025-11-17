default: check doc fmt clippy

check:
    cargo check --workspace --all-features --all-targets

doc:
    cargo doc --workspace --all-features --no-deps --document-private-items

clippy:
    cargo +nightly clippy --all --all-features -- -D warnings

fmt:
    cargo +nightly fmt --all -- --check

test:
    cargo nextest run --workspace --all-features --retries 3 
