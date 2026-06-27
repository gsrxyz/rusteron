# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Rusteron** is a thin, high-performance Rust wrapper over the [Aeron](https://github.com/real-logic/aeron) C API, optimized for latency-sensitive (trading) environments. It is a Cargo workspace of four crates:

- `rusteron-code-gen` — code generation engine (build-dependency, not for standalone use).
- `rusteron-client` — core client (connect, publish, subscribe).
- `rusteron-archive` — extends the client with recording/replay; **includes** `rusteron-client`, so consumers never depend on both.
- `rusteron-media-driver` — launches an embedded or standalone Aeron Media Driver; ships a `media_driver` binary.

`rusteron-docker-samples/` is **excluded** from the workspace build. All work is licensed MIT OR Apache-2.0.

> **Unsafe by design.** This wraps C FFI. Misuse (e.g. using a publication after its `Aeron` client is closed) causes undefined behavior / segfaults. Lifetime/dependency correctness is the central safety concern — see *Resource management* below.

## Build Requirements

Building from source (the default feature) requires, in addition to Rust:

- **Java 17** (Aeron C is built with Gradle/cmake which invokes Java). On macOS: `JAVA_HOME=$(/usr/libexec/java_home -v 17)`.
- **CMake** and a **C/C++ compiler / Clang** (for `bindgen` and the C build).
- Aeron C is vendored as a git submodule at each `rusteron-*/aeron/`. If missing: `git submodule update --init --recursive`.

Toolchain is pinned to **1.95.0** via `rust-toolchain.toml`. `.cargo/config.toml` sets `target-cpu=native`.

## Commands

`just` is the primary command runner (`cargo install just` if missing). Java 17 must be on `JAVA_HOME` for the source builds.

- `just check` — workspace `cargo check` + `cargo fmt --check` + strict Clippy (`--deny warnings`). Run this before any PR.
- `just fix` — auto-format and auto-fix Clippy lints.
- `just build` — debug build with `COPY_BINDINGS=true` (regenerates committed binding snapshots).
- `just release` / `just build-artifacts` — release build / publish artifacts (`--features static,precompile`).
- `just test` — unit + doc + all-target tests across the workspace (runs `--nocapture`).
- `just slow-tests` — archive tests marked `#[ignore]` (slow consumer scenarios).
- `just test-valgrind` — runs the workspace tests under Valgrind inside Docker (builds the image first).
- `just docs` — builds local rustdoc (runs doc-tests first).
- `just clean` — removes `target/`, `artifacts/`, and resets the Aeron submodules.
- `just slow-tests`, `just bench`, `just run-aeron-media-driver-java`/`-rust`, `just update-to-latest-aeron-version`, `just show-aeron-version`.

Direct cargo is also fine, e.g. `cargo test --workspace --all-targets --all-features -- --nocapture`.

**Run a single test:**
```bash
cargo test --package rusteron-client --lib <test_name> -- --nocapture
# archive slow/ignored test:
cargo test -p rusteron-archive --lib --features "precompile static" <test_name> -- --ignored --nocapture
```

## Architecture: the code-generation pipeline (read this first)

Each runtime crate (`client`, `archive`, `media-driver`) is **mostly generated**, not hand-written. Understanding the generation flow is essential before editing anything:

1. The crate's `build.rs` feeds `bindings.h` into `bindgen` → raw FFI **`bindings.rs`** (in `OUT_DIR`).
2. `rusteron-code-gen::parse_bindings` reads that raw FFI and, using Aeron's predictable C patterns, emits higher-level Rust wrappers into **`aeron.rs`** (handlers, return-type wrappers, method generics).
3. A verbatim copy of the hand-maintained **`aeron_custom.rs`** is appended alongside it.
4. `src/lib.rs` pulls all three in: `pub mod bindings { include!(…/bindings.rs); }`, then `include!(…/aeron.rs)` and `include!(…/aeron_custom.rs)`.

So in each crate the three generated files form a strict layering:

| File | Source of truth | Edit policy |
|------|-----------------|-------------|
| `bindings.rs` (OUT_DIR / `docs-rs/bindings.rs`) | `bindgen` from Aeron C headers | **Never** hand-edit |
| `aeron.rs` (OUT_DIR / `docs-rs/aeron.rs`) | `rusteron-code-gen` `generator.rs` | **Never** hand-edit — change the generator |
| `aeron_custom.rs` | `rusteron-code-gen/src/aeron_custom.rs` + `common.rs` | Edit here; it is `include_str!`'d into all crates |

When you change `aeron_custom.rs` or the generator, rebuild with `COPY_BINDINGS=true` (or `just build`), which copies the regenerated `bindings.rs` back into the committed snapshots at `rusteron-code-gen/bindings/{client,archive,media-driver}.rs`. The `docs-rs/` directory holds committed copies of all three files so that **docs.rs builds do not need cmake/java** (build.rs detects `DOCS_RS` and skips the C build).

The `#![allow(clippy::all)]` / `non_snake_case` etc. at the top of every `lib.rs` exist precisely because the bulk of the code is generated; do not remove them. Do custom work in `aeron_custom.rs` (or `common.rs`), not in generated output.

## Architecture: features and linking modes

Each runtime crate shares the same feature set:

- `build-from-source` (default) — compiles Aeron C via cmake; needs Java 17 + C++ toolchain.
- `static` — statically link Aeron C (also propagates to `rusteron-media-driver/static` for the client).
- `precompile` / `precompile-rustls` — **macOS-only**, download precompiled static libs instead of cmake/java (requires `static`).
- `backtrace`, `extra-logging`, `log-c-bindings` — debugging/diagnostics toggles (e.g. log every FFI call).

`docs.rs` builds use `features = ["static", "precompile"]` (see `[package.metadata.docs.rs]`). The default dev build is dynamic linking + build-from-source.

## Architecture: resource management (the unsafe core)

Safe-ish lifetimes are layered onto the C objects in `rusteron-code-gen/src/common.rs` (included into every crate). The key types:

- `CResource<T>` — enum: `OwnedOnHeap` (an `Rc<ManagedCResource<T>>`), `OwnedOnStack` (`MaybeUninit<T>`, partial-stack init pattern), or `Borrowed` (raw `*mut T`, **not** freed on drop).
- `ManagedCResource<T>` — `Rc`-shared heap wrapper with **dependency tracking** and a `Drop` impl that calls the matching Aeron C free function. This dependency graph is how the bindings encode "a Subscription depends on its Aeron client" — destroying a parent before its children is the classic source of segfaults here.
- `Handler<T>` — callback wrapper that can `leak()` its inner value to hand a stable pointer to C FFI, and frees it on drop.

When fixing or extending behavior around object lifetimes, prefer the existing `CResource`/`ManagedCResource`/`Handler` machinery over reaching for raw pointers. `Send`/`Sync` impls for these C types are declared in `aeron_custom.rs`.

## Testing conventions

- Most tests are inline in each crate's `src/lib.rs` under `#[cfg(test)] mod tests`. Archive also has dedicated files: `slow_consumer_test.rs`, `testing.rs`, `persistent_subscription_tests.rs`, `persistent_subscription_integration.rs`.
- Tests that share media-driver state use `#[serial]` (the `serial_test` crate) and/or `fd-lock`; many tests spin up an **embedded** media driver via `AeronDriver::launch_embedded`.
- Long-running scenarios are marked `#[ignore]` and run via `just slow-tests`.
- Leak detection: the `test_alloc` harness (`current_allocs()`) is used in tests to assert no net allocation drift; memory correctness is also validated under Valgrind in CI (`just test-valgrind`).

## CI and checks

CI (`.github/workflows/ci.yml`) gates on: `cargo fmt --check`, `cargo clippy --all -- --deny warnings`, doc-tests, and a Valgrind memory-check job. Before opening a PR run at least `just check` and `just test`. Commits use short imperative subjects (e.g. `chore: Release`, `update aeron 1.50.2`); releases are driven by `cargo-release` (`[release]` table in the root `Cargo.toml`).

## Updating the vendored Aeron version

There are **three independent Aeron submodules** (`rusteron-{client,archive,media-driver}/aeron`) that must be advanced together. Use `just show-aeron-version` to inspect each, `just update-to-latest-aeron-version` (or `just update-aeron-version <tag>`) to bump, then rebuild with `COPY_BINDINGS=true` and commit the regenerated `rusteron-code-gen/bindings/*.rs` and `docs-rs/` snapshots. Call out the exact upstream version in the commit/PR.
