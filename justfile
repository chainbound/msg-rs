default: check doc fmt

check:
    cargo check --workspace --all-features --all-targets

doc:
    cargo doc --workspace --all-features --no-deps --document-private-items


fmt:
    cargo +nightly fmt --all -- --check

test:
    cargo nextest run --workspace --retries 3