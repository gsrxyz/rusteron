# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Rusteron** is a thin, high-performance Rust wrapper over the [Aeron](https://github.com/real-logic/aeron) C API, sponsored by GSR and used in mission-critical real-time trading infrastructure. It exposes low-level C bindings with minimal abstraction, optimised for latency-sensitive environments.

## Build & Test Commands

Prerequisites: Rust 1.95.0 (pinned in `rust-toolchain.toml`), Java 17+ for building Aeron C from source, CMake + Clang.

```bash
# Build (debug) with code-gen binding copying
just build

# Release build
just release

# Full check: format, clippy, compile
just check

# Auto-fix formatting and simple clippy warnings
just fix

# Run all tests
just test

# Run single test in a specific crate
cargo test -p rusteron-client --lib <test_name> -- --nocapture

# Run doc tests
cargo test --workspace --doc

# Run slow consumer tests (normally ignored)
just slow-tests

# Memory checks via Valgrind in Docker
just test-valgrind

# Criterion benchmarks
just bench

# Generate docs locally
just docs
```

## Feature Flags

Each crate (`rusteron-client`, `rusteron-archive`, `rusteron-media-driver`) shares the same feature set:

| Feature | Purpose |
|---|---|
| `static` | Statically link Aeron C (default: dynamic linking) |
| `precompile` / `precompile-rustls` | Download precompiled C libs instead of building from source (avoids cmake/java requirement) |
| `build-from-source` | Build Aeron C from source (default; requires cmake, C++ compiler, Java) |
| `backtrace` | Log backtrace on each `AeronCError` |
| `extra-logging` | Log resource create/destroy (helpful for debugging segfaults) |
| `log-c-bindings` | Log every C binding call with args/return values |

## Workspace Architecture (5 crates)

```
rusteron/
├── rusteron-code-gen/        # Code generation engine — parses Aeron C headers & generates Rust wrappers
│   └── src/
│       ├── lib.rs            # Entry: parser + generator pipeline, using syn/proc-macro2/quote
│       ├── parser.rs         # Parses bindgen output into structured wrapper definitions
│       ├── generator.rs      # Generates Rust code for C struct wrappers (102KB — the core engine)
│       ├── common.rs         # Shared types: ManagedCResource, IntoCString, handler traits
│       └── aeron_custom.rs   # Hand-authored custom wrappers injected into generated code
├── rusteron-client/          # Core Aeron client — connect, publish, subscribe
│   └── src/lib.rs            # Generated + custom bindings, integration tests using embedded media driver
├── rusteron-archive/         # Stream recording and replay (includes rusteron-client concepts)
│   └── src/
│       ├── lib.rs            # Archive client, replay-merge, recording position helpers
│       ├── testing.rs        # EmbeddedArchiveMediaDriverProcess (spawns Java archive driver)
│       └── slow_consumer_test.rs
├── rusteron-media-driver/    # Launch embedded or standalone Media Driver from Rust
│   └── src/
│       ├── lib.rs            # AeronDriver::launch_embedded(), wait_for_previous_media_driver_to_timeout
│       └── bin/media_driver.rs  # Standalone media driver binary
└── rusteron-docker-samples/  # Docker/Podman/K8s sample setups (not production-ready)
```

## Code Generation Pipeline

1. Aeron C is pulled as git submodules (`rusteron-*/aeron/`)
2. Build scripts (`build.rs` in each crate) compile the C code via CMake
3. `bindgen` generates raw FFI bindings (`bindings.rs`)
4. `rusteron-code-gen` parses those bindings and emits higher-level Rust wrappers (`aeron.rs` + `aeron_custom.rs`)
5. Generated code is included via `include!(concat!(env!("OUT_DIR"), "/aeron.rs"))`

## Key Patterns & Conventions

- **`ManagedCResource`** — Wraps raw C pointers with RAII; close/cleanup is called on drop. Supports dependency tracking (`add_dependency`) so that, e.g., Aeron is dropped before its resources.
- **Handlers** — Reference-counted (`Arc`); retained-callback setters take the value (closure or trait impl), keep it alive via the registering resource, and return the `Handler` for optional state access. `_once` closure variants exist only for synchronous callbacks (poll etc.).
- **Close semantics** — `Aeron`/`AeronArchive` `close()` is deferred until the last reference (child or clone) drops; leaf closes are immediate and null clones' pointers; `unsafe close_now()` forces immediate close (children then dangle).
- **`#[serial]` tests** — All integration tests use `serial_test` with file locks because they spawn embedded media drivers that share resources. Tests must not run concurrently.
- **`.cargo/config.toml`** sets `rustflags = ["-C", "target-cpu=native"]` — builds are optimised for the host CPU.
- **Feature activation** — `static` implies `build-from-source` and propagates between crates (e.g. `rusteron-client/static` enables `rusteron-media-driver/static`).
- **Aeron version** — All three submodule copies of Aeron are kept in sync (see `just update-aeron-version` and `just show-aeron-version`).

## Safety Caveats

The library operates extensively in `unsafe` context. Improper usage (e.g., using a publication after the Aeron client is closed) can cause undefined behaviour or segfaults. All crates use `#![allow(improper_ctypes_definitions)]` and other FFI-related lint suppressions top-level.

## CI

GitHub Actions (`ci.yml`) runs:
- **Lint**: `cargo fmt --check`, `clippy --deny warnings`, doc tests
- **Build + Test**: Matrix across ubuntu-22.04 / macos-latest, with `default` and `static` features
- **Valgrind**: Docker-based memory checking via `just test-valgrind`
