# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Prerequisites

- **Rust 1.96.1** (pinned via `rust-toolchain.toml`)
- **Java 17+** — required for CMake-based Aeron C builds
- **CMake + Clang** — required for `build-from-source` (default) feature
- **just** — command runner (`cargo install just`)
- **Git submodules** — run `git submodule update --init --recursive` after cloning

## Commands

```bash
just build        # cargo build --release
just test         # cargo test
cargo fmt         # format
cargo clippy      # lint
```

Run a single test:
```bash
cargo test -p rusteron-client <test_name>
```

Run with `static` linking (links Aeron C statically):
```bash
cargo build --features static
```

Run with precompiled Aeron libs (macOS only, no Java/CMake needed):
```bash
cargo build --features "static,precompile"
```

## Architecture

This is a Cargo workspace with four crates:

| Crate | Role |
|---|---|
| `rusteron-code-gen` | Generates safe Rust wrappers from raw `bindgen` bindings |
| `rusteron-client` | Core Aeron client — connect, publish, subscribe |
| `rusteron-archive` | Recording/replay + persistent subscriptions; includes `rusteron-client` |
| `rusteron-media-driver` | Embeds or launches an Aeron Media Driver in-process |

### Code generation flow

The majority of the public API in `rusteron-client`, `rusteron-archive`, and `rusteron-media-driver` is **auto-generated** at build time via `build.rs` in each crate. The process:

1. `rusteron-code-gen/bindings/{client,archive,media-driver}.rs` — pre-generated `bindgen` output (raw C types)
2. `rusteron-code-gen/src/{parser,generator}.rs` — parse the raw bindings and emit idiomatic Rust wrappers
3. `rusteron-code-gen/src/aeron_custom.rs` and `common.rs` — hand-written code appended verbatim to generated output
4. Each crate's `build.rs` calls into `rusteron-code-gen` and writes `$OUT_DIR/aeron.rs` and `$OUT_DIR/aeron_custom.rs`

**Do not edit** the `include!(concat!(env!("OUT_DIR"), ...))` output directly. Hand-written extensions go in `rusteron-code-gen/src/aeron_custom.rs` or in the crate's own `src/lib.rs` after the `include!` lines.

### Linking model

Each crate supports two linking modes controlled by Cargo features:

- **`build-from-source`** (default) — compiles Aeron C from the `aeron/` submodule using CMake
- **`static`** — statically links; also enables `rusteron-media-driver/static`
- **`precompile`** / **`precompile-rustls`** — downloads pre-built libs (macOS only), bypasses Java/CMake

### Handler / callback pattern

Aeron's C API is callback-heavy. The generated code wraps handlers via `Handler<T>` and `Handlers` (static no-ops). Key lifecycle rule: a `Handler` must outlive the Aeron resource that holds it. Use `Handler::leak` for handlers with unbounded lifetime, and call `.release()` only after the associated resource is closed.

### Error handling

`AeronCError` wraps C return codes. Use `.kind()` to get `AeronErrorType`, then `.is_retryable()` / `.is_unrecoverable()` to classify. Never call `unwrap()`/`expect()` on results from Aeron APIs — propagate with `?`.

### Safety

All C interop is `unsafe`. Using a publication or subscription after its parent `Aeron` client is closed causes undefined behaviour / segfaults. Resource ordering matters: close children before parents.
