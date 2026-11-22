# Publishing the standalone libraries

Only these two packages are prepared for standalone publication:

1. `rusty-bot-messages`
2. `rusty-bot-providers` (depends on messages)

The legacy crates remain available for reference and compatibility. Do not run
`cargo publish --workspace`: that would select unrelated application and agent
packages. No repository URL is configured yet because this checkout has no Git
remote; add the actual public source URL when one is available.

## Before release

- Review and commit the intended release sources; tag the release commit.
- Confirm ownership/availability of both crate names on crates.io. Availability
  checks do not reserve names.
- Confirm the MIT license is appropriate for all included source and preserve
  any upstream attribution and license notices required by its provenance.
- Review the public API and version numbers. A published version cannot be
  overwritten; a correction requires a new version.
- Keep credentials outside packages. Publishing uses Cargo's credentials, not
  application API keys. Never copy the CLI `.env` into either library.
- Current manifests conservatively require Rust 1.98, the tested toolchain series.
  The earlier 1.85 declaration was not supported by the locked dependencies.
  Lowering the requirement needs a separate minimum-toolchain validation.

## Verification (does not upload)

From the workspace root, using Cargo 1.98 or later:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --all-targets --locked
cargo package -p rusty-bot-messages -p rusty-bot-providers --locked
cargo publish -p rusty-bot-messages -p rusty-bot-providers --dry-run --locked
```

Multi-package verification uses Cargo's local registry overlay, so messages does
not need to be publicly uploaded before providers can be verified. An active
network connection can be necessary for registry resolution and downloads; add
`--offline` only when dependencies and index entries are cached.

Inspect the generated `.crate` archives in `target/package/`, not only the source
tree. Both packages use an explicit include list: source, README, LICENSE, and
manifest (plus Cargo-generated package metadata). The manual paid-call probe is
not included. `--allow-dirty` is acceptable for local preflight while preparing a
release, but the release itself should be made from a reviewed clean commit.

## Publish later — only with explicit approval

After verification and credential setup, publish messages first, then providers:

```sh
cargo publish -p rusty-bot-messages --locked
cargo publish -p rusty-bot-providers --locked
```

Wait for messages to be available in the registry before publishing providers.
Do not publish the whole agent workspace. Once no active validation needs build
output, run `cargo clean` as required by `AGENTS.md`.
