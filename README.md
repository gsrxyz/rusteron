# Rusteron

[![Crates.io](https://img.shields.io/crates/v/rusteron-archive)](https://crates.io/crates/rusteron-archive)
[![CI](https://github.com/gsrxyz/rusteron/actions/workflows/ci.yml/badge.svg)](https://github.com/gsrxyz/rusteron/actions/workflows/ci.yml)
[![API Docs](https://docs.rs/rusteron-archive/badge.svg)](https://docs.rs/rusteron-archive/)
[![github API Docs](https://custom-icon-badges.demolab.com/badge/githubdocs-blue.svg?logo=log\&logoSource=feather)](https://gsrxyz.github.io/rusteron)

> **Rusteron** is a thin, high-performance Rust wrapper over the [Aeron](https://github.com/real-logic/aeron) C API.
> It exposes low-level C bindings with minimal abstraction, optimized for production use in latency-sensitive environments.

---

## Sponsored by GSR

**Rusteron** is proudly sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It powers mission-critical infrastructure in GSR's real-time trading stack and is now developed under the official GSR GitHub organization as part of our commitment to open-source excellence and community collaboration.

We welcome contributions, feedback, and discussions. If you're interested in integrating or contributing, please open an issue or reach out directly.

---

## Project Overview

This project builds on a fork of [`libaeron-sys`](https://github.com/bspeice/libaeron-sys), offering Rust access to Aeron’s native C API. The API is **not fully idiomatic**, but is auto-generated for consistency and reliability. This tradeoff supports:

* Performance-sensitive trading environments
* Minimal runtime overhead
* Low maintenance costs

**Warning**: This library operates in an `unsafe` context and requires care. Improper usage (e.g., using a publisher after the Aeron client is closed) can lead to **undefined behavior or segmentation faults**.

---

## Module Overview

| Module                                                                                            | Description                                                                |
| ------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| [`rusteron-code-gen`](https://github.com/gsrxyz/rusteron/tree/main/rusteron-code-gen)             | Code generation engine to produce consistent Rust bindings for Aeron C.    |
| [`rusteron-client`](https://github.com/gsrxyz/rusteron/tree/main/rusteron-client)                 | Core Aeron client wrapper (connect, publish, subscribe).                   |
| [`rusteron-archive`](https://github.com/gsrxyz/rusteron/tree/main/rusteron-archive)               | Adds stream recording and replay features. Includes `rusteron-client`.     |
| [`rusteron-media-driver`](https://github.com/gsrxyz/rusteron/tree/main/rusteron-media-driver)     | Rust interface for launching an embedded or standalone Aeron Media Driver. |
| [`rusteron-docker-samples`](https://github.com/gsrxyz/rusteron/tree/main/rusteron-docker-samples) | Sample Docker setups for media driver + pub/sub flows. Not prod-ready.     |

Note: `rusteron-archive` includes `rusteron-client`, so you do **not** need both as dependencies.

---

## Installation

Choose the module and linking style appropriate for your project.

**Dynamic library:**

```toml
[dependencies]
rusteron-client = "0.1"
```

**Static library:**

```toml
[dependencies]
rusteron-client = { version = "0.1", features = ["static"] }
```

**macOS-only precompiled static libs:**

```toml
[dependencies]
rusteron-client = { version = "0.1", features = ["static", "precompile"] }
```

**macOS-only precompiled static libs with rustls downloader:**

```toml
[dependencies]
rusteron-client = { version = "0.1", features = ["static", "precompile-rustls"] }
```

Replace `rusteron-client` with `rusteron-archive` or `rusteron-media-driver` as needed.

For full build instructions, see [BUILD.md](./BUILD.md).

### Multi-threaded (`Sync`) handles

By default, handles (`AeronPublication`, `AeronSubscription`, …) are `Send` but **not
`Sync`** — they use `Rc` (non-atomic refcount) and are designed for single-thread ownership
(move to one thread, use exclusively there). Enable the `multi-threaded` feature to swap
`Rc` → `Arc` and add `unsafe impl Sync` so that `&AeronPublication` can be **shared across
threads** for the operations Aeron C documents as thread-safe
(`offer` / `try_claim` / `position` / `is_connected`):

```toml
[dependencies]
rusteron-client = { version = "0.2", features = ["multi-threaded"] }
```

```rust,ignore
// Share a publication across threads for concurrent offer (Aeron C documents
// aeron_publication_offer as thread-safe).
let pub_arc = Arc::new(publication);
let t1 = { let p = pub_arc.clone(); thread::spawn(move || { p.offer(b"hello")?; }) };
let t2 = { let p = pub_arc.clone(); thread::spawn(move || { p.offer(b"world")?; }) };
```

The Arc atomic refcount is **not on the hot path** — `offer` / `poll` read the inner C
pointer via a `Cell`/field load (no refcount), so the bench shows no measurable overhead
(Rc 2.84 ns vs Arc 2.82 ns per offer). The `unsafe impl Sync` follows the same "accepted
unsoundness" policy as the existing `unsafe impl Send`: the `UnsafeCell` fields inside
`ManagedCResource` are only mutated during construction and close (single-threaded), never
during the shared-read window. The deferred close + dependency graph (shipped in 0.2)
structurally prevents the close-while-shared race described in
[PR #50](https://github.com/gsrxyz/rusteron/pull/50) — children keep parents alive.

---

## Development

To simplify development, we use [`just`](https://github.com/casey/just), a command runner similar to `make`.

To view all available commands, run `just` in the command line.

> If you don’t have `just` installed, install it with: `cargo install just`

---

## Example: Pub/Sub

<details>
<summary>Expand for usage example</summary>

```rust,no_run
use rusteron_client::{
    Aeron, AeronContext, AeronErrorHandlerLogger, AeronHeader, Handler, Handlers, IntoCString,
};
use rusteron_media_driver::{AeronDriver, AeronDriverContext};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Embedded media driver. `launch_embedded_guard` returns a RAII guard that
    // stops the driver on drop — no manual `stop` flag / join needed.
    let driver_ctx = AeronDriverContext::new()?;
    let driver = AeronDriver::launch_embedded_guard(driver_ctx.clone(), false);

    let ctx = AeronContext::new()?;
    ctx.set_dir(&driver_ctx.get_dir().into_c_string())?;
    // Reuse the built-in logger for async client errors (Aeron samples always set one).
    ctx.set_error_handler(Some(AeronErrorHandlerLogger))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    // c"..." literals are compile-time &'static CStr — zero runtime cost.
    let channel = c"aeron:ipc";
    let publication = aeron
        .async_add_publication(channel, 123)?
        .poll_blocking(Duration::from_secs(5))?;
    let subscription = aeron
        .async_add_subscription(
            channel, 123,
            Handlers::NONE,
            Handlers::NONE,
        )?
        .poll_blocking(Duration::from_secs(5))?;

    // offer returns Ok(position) or a typed AeronOfferError; retry the
    // retryable ones (back-pressure / admin action / not connected).
    while publication.offer(b"Hello, Aeron!").is_err() {}

    // `poll_fn` runs the closure per fragment (zero allocation); use a fragment
    // assembler for messages larger than the MTU.
    subscription.poll_fn(
        |msg: &[u8], header: AeronHeader| {
            println!("received {} bytes at position {:?}", msg.len(), header.position());
        },
        10,
    )?;

    driver.join().ok();
    Ok(())
}
```

</details>

### C strings without hidden allocations

Every channel/URI argument is a `&CStr` (the C API's type), so allocations stay **explicit
and greppable** — important when reviewing latency-sensitive code. The recommended
three-tier pattern, cheapest first:

```rust,ignore
// 1. Constant channels: c"..." literals — compile-time &'static CStr, zero runtime cost.
aeron.async_add_publication(c"aeron:ipc", 10)?;

// 2. Dynamic URIs: cformat! — ONE named heap allocation (format + CString in one step).
let uri = cformat!("aeron:udp?endpoint=localhost:{port}");
aeron.async_add_publication(&uri, 10)?;

// 3. Repeated paths: build the CString once, store it, pass `&it` (zero-copy on reuse).
let chan: std::ffi::CString = cformat!("aeron:udp?endpoint={endpoint}");
for _ in 0..reconnect_attempts {
    aeron.async_add_publication(&chan, 10)?; // no allocation per call
}
```

Avoid `&"aeron:ipc".into_c_string()` for literals — it heap-allocates at runtime what
`c"aeron:ipc"` gets for free at compile time. (`into_c_string()` remains for converting
an owned `String` you already have.)

---

## What's new in 0.2

0.2 is a breaking release. The themes, briefly (deeper handler/close write-up in the
[rusteron-client README](./rusteron-client/README.md)):

**Memory safety — deferred close.** In 0.1.x, `aeron.close()` freed child resources
immediately: any surviving publication/subscription handle was a use-after-free. In 0.2,
close is deferred until the last reference (child or clone) drops — closing/dropping in any
order is structurally safe. The escape hatch for immediate teardown is `unsafe close_now()`.

**Handlers.** `Handler::leak()` + manual `release()` are gone; `Handler::new()` is
reference-counted (`Arc`) and freed automatically — resources that retain a callback keep a
clone alive, so the C side can never fire a freed callback. Retained-callback setters now
take the value (a closure or trait impl works directly) and return the `Handler` for
optional state access. For "no callback", one constant fits every parameter:
`Handlers::NONE`. Docs now spell out heap (retained `Handler`) vs stack
(`poll_fn`/`*_once` borrowed closures — zero allocation) semantics.

**Error handling.** `offer`/`try_claim` return `Result<i64, AeronOfferError>` — a typed
enum over Aeron's negative sentinels with `is_retryable()` (back-pressure / admin action /
not connected) vs fatal (closed / max position). `AeronCError` now snapshots
`aeron_errmsg()` **at the error site** for non-retryable codes, so `Display` shows the
message of the error that actually happened, not whatever a later error overwrote the
global buffer with. The archive client gains typed error codes (`AeronArchiveErrorCode`,
`archive.poll_for_error()`).

**Hot-path performance.** Generated FFI wrappers carry `#[inline]` and the workspace
release profile enables fat LTO. New `offer_parts(&[&header, &payload])` does a gathering
(vectored) publish with a stack-built iovec — no per-message `Vec` concat, no copy.
`poll_fn` is the zero-allocation closure poll. C-string arguments follow the explicit
three-tier pattern above (`c""` / `cformat!` / reuse) so allocations never hide.

**Convenience.** `Aeron::connect_dir(dir)`, `AeronDriver::launch_embedded_guard()` (RAII
stop+join on drop), `ChannelUri::add_session_id`, `AeronUriStringBuilder::ipc()/udp()`,
retained image accessors (`image_at_index`, `image_by_session_id`, `for_each_image`),
direct constant getters (`session_id()`, `stream_id()`, …), and ported canonical samples
(`basic_publisher`, `basic_subscriber`, ping/pong, file transfer, MDS, request/response).

## Migrating from 0.1.168 to 0.2

Old → new for every renamed/changed API (rows verified against the released 0.1.168):

| 0.1.168 | 0.2 | Notes |
|---|---|---|
| `Handler::leak(h)` + manual `handler.release()` | `Handler::new(h)` | Freed automatically when the last clone drops; registering resources keep clones. |
| `ctx.set_error_handler(Some(&handler))` (borrowed) | `ctx.set_error_handler(Some(handler_or_closure))` | Retained setters take the value; closures work directly; returns the `Handler`. |
| `aeron.close()` — freed children immediately | deferred close | Frees when the last reference drops; `unsafe close_now()` forces immediate. |
| `Handler` was `Sync` | `Send` only | The conductor thread invokes callbacks; sharing `&Handler` across threads raced. |
| `publication.offer(buf, supplier)` → raw `i64` | `publication.offer_raw(buf, supplier)` | Same branch-free sentinel return, renamed to make "raw" explicit. |
| `publication.offer_result(buf, supplier)` → `Result<_, AeronCError>` | `publication.offer_with_reserved_value(buf, supplier)` → `Result<_, AeronOfferError>` | Typed offer errors with `is_retryable()`. |
| `publication.offer_result_simple(buf)` | `publication.offer(buf)` | The common no-supplier case is now the flagship name. |
| `try_claim_result(len, claim)` / `try_claim_owned` → `AeronCError` | `try_claim(len, claim)` / `try_claim_owned` → `AeronOfferError` | Same RAII `AeronClaim`; typed error. |
| `subscription.poll_fn(f, limit)` | `subscription.poll_fn(f, limit)` | Same on `AeronImage` / `AeronArchiveReplayMerge`. `_once` read as "one fragment". |
| `subscription.for_each_fragment(limit, f)` | `subscription.poll_fn(f, limit)` | Removed (alias with the arguments in the opposite order). |
| `sub.poll(assembler.process(&mut ctx, f), limit)` | `assembler.poll(&sub, &mut ctx, f, limit)` | `process()` leaked a raw ctx pointer past the borrow (UAF hazard); the new form scopes it. |
| `Handlers::no_available_image_handler()`, `no_unavailable_image_handler()`, … | `Handlers::NONE` | One constant, any callback parameter, full inference. Old helpers removed. |
| `pub.add_destination(&aeron, dest, timeout)` (`&mut self`) | `pub.add_destination(dest, timeout)` (`&self`) | Owning `Aeron` comes from the handle's dependency graph. Same for subscriptions / exclusive publications. |
| `AeronCnc::new(dir)` | `AeronCnc::open(&CStr)` or `AeronCnc::read(&CStr, \|cnc\| { … })` | `new_on_heap`/`read_on_partial_stack` renamed; now accept `&CStr` (not `&str`/`&CString`). `read` = scoped (zero-alloc, preferred for one-shot), `open` = owned handle (for repeated polling). |
| `&"aeron:ipc".into_c_string()` (allocates at runtime) | `c"aeron:ipc"` | See "C strings without hidden allocations" above; `cformat!` for dynamic URIs. |

Behavioural notes:
- A panicking fragment handler aborts the process (panic cannot unwind across the C
  callback boundary) — return instead of panicking in handlers.
- `err.get_last_err_message()` now prefers the message captured when the error was
  created; only sentinel/retryable codes still read the live `aeron_errmsg()`.

For recording, replay, and **persistent subscriptions** (replay history, then seamlessly join a
live stream), see [`rusteron-archive`](./rusteron-archive/README.md#persistent-subscriptions).

---

## Benchmarks

For latency and throughput benchmarks, refer to [BENCHMARKS.md](./BENCHMARKS.md).

---

## Contributing

Contributions are more than welcome! Please:

* Submit bug reports, ideas, or improvements via GitHub Issues
* Propose changes via pull requests
* Read our [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md)

We’re especially looking for help with:

* API design reviews
* Safety and idiomatic improvements
* Dockerized and deployment examples

---

## License

Licensed under either [MIT License](https://opensource.org/licenses/MIT) or [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) at your option.

---

## Acknowledgments

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron - your work made this possible!
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for open protocol excellence
