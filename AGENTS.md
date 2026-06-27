# Repository Guidelines

## Project Structure & Module Organization
This repository is a Rust workspace rooted at `Cargo.toml` with four primary crates:
`rusteron-code-gen`, `rusteron-client`, `rusteron-archive`, and `rusteron-media-driver`.
Each crate keeps core code in `src/`, build integration in `build.rs`, and generated-doc snapshots in `docs-rs/`.
Examples and benchmarks live in `rusteron-client/examples/` and `rusteron-client/benches/`.
Docker examples are isolated in `rusteron-docker-samples/` (excluded from the workspace build).
Each runtime crate vendors Aeron as a git submodule in `rusteron-*/aeron/`.

## Build, Test, and Development Commands
Prefer `just` targets for repeatable workflows:
- `just check` runs workspace check, `fmt --check`, and strict Clippy.
- `just build` builds all targets (`COPY_BINDINGS=true`).
- `just release` builds release artifacts.
- `just test` runs unit/integration/doc/all-target tests across the workspace.
- `just slow-tests` runs ignored archive slow-consumer tests.
- `just test-valgrind` runs workspace tests under Valgrind in Docker.

`just` commands require Java 17; set `JAVA_HOME` accordingly before running them (for example on macOS: `JAVA_HOME=$(/usr/libexec/java_home -v 17) just test`).

Direct cargo equivalents are also valid, e.g. `cargo test --workspace --all-targets --all-features -- --nocapture`.
If submodules are missing, run `git submodule update --init --recursive`.

## Coding Style & Naming Conventions
Use Rust 2021 with toolchain `1.95.0` (`rust-toolchain.toml`).
Formatting is mandatory via `cargo fmt --all`; lint with `cargo clippy --all -- --deny warnings`.
Follow standard Rust naming: `snake_case` for functions/modules, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
Do not hand-edit generated Aeron bindings in `OUT_DIR` outputs; implement custom behavior in crate `src/` or `rusteron-code-gen`.

## Testing Guidelines
Most tests are inline in `src/lib.rs` under `mod tests`; archive-specific tests also live in `rusteron-archive/src/slow_consumer_test.rs`, `testing.rs`, `persistent_subscription_tests.rs`, and `persistent_subscription_integration.rs`.
Keep tests deterministic and explicitly mark long-running scenarios with `#[ignore]`.
Before opening a PR, run at least `just check` and `just test`.

## Commit & Pull Request Guidelines
Use short, imperative commit subjects consistent with history (examples: `chore: Release`, `issue-29 fix clippy warnings`, `update aeron 1.49.1`).
PRs should be focused, linked to an issue when applicable, and include:
- what changed and why,
- crates/features affected (for example `static`, `precompile`),
- commands used for validation.
For Aeron/submodule updates, call out the exact upstream version and any regenerated artifacts.
