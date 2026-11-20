# Agent instructions

## Rust builds and disk usage

- Never commit Rust build artifacts. Keep all `target/` directories ignored by Git.
- After every build, compilation, or test run (successful or failed), remove generated build artifacts that are no longer needed so they do not accumulate and waste disk space.
- Once validation is finished and no build or process is using the artifacts, run `cargo clean` from the workspace root to remove the workspace's build output. If artifacts are temporarily needed for active work, clean them as soon as that work is finished, before handing off.
- Do not delete artifacts used by another running build or process. Do not clean shared or external target directories without confirming they belong exclusively to this project.
- Preserve source files, `Cargo.toml`, and `Cargo.lock`; cleanup must only remove generated build output.
- Keep the existing low-disk-usage profiles (`debug = 0`, `incremental = false`) unless explicitly asked to change them.
