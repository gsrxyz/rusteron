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
rusteron-client = "0.2"
```

**Static library:**

```toml
[dependencies]
rusteron-client = { version = "0.2", features = ["static"] }
```

**macOS-only precompiled static libs:**

```toml
[dependencies]
rusteron-client = { version = "0.2", features = ["static", "precompile"] }
```

**macOS-only precompiled static libs with rustls downloader:**

```toml
[dependencies]
rusteron-client = { version = "0.2", features = ["static", "precompile-rustls"] }
```

Replace `rusteron-client` with `rusteron-archive` or `rusteron-media-driver` as needed.

For full build instructions, see [BUILD.md](./BUILD.md).

### Multi-threaded (`Sync`) handles

Handles are `Send` but **not `Sync`** by default — they use `Rc` for single-thread
ownership. Enable `multi-threaded` to swap `Rc` → `Arc` and add `unsafe impl Sync`,
so `&Handle` can be shared across threads for the ops Aeron C documents as thread-safe
(`offer` / `try_claim` / `position` / `is_connected`):

```toml
[dependencies]
rusteron-client = { version = "0.2", features = ["multi-threaded"] }
```

```rust,ignore
// Each clone shares the same underlying C publication.
let t1 = { let p = publication.clone(); thread::spawn(move || { p.offer(b"hello")?; }) };
let t2 = { let p = publication.clone(); thread::spawn(move || { p.offer(b"world")?; }) };
```

> **The flag only lifts the Rust-side barrier — it does not make the underlying Aeron
> object thread-safe.** Sharing is correct only for objects Aeron C documents as
> thread-safe (`AeronPublication`, `AeronSubscription`, …). `AeronExclusivePublication`
> is single-producer by design and must not be shared across threads even under
> `multi-threaded`. It is the caller's responsibility to check the thread-safety of
> each object before sharing `&Handle`.

---

## Development

Build tasks use [`just`](https://github.com/casey/just). Run `just` to list commands, or `cargo install just` if needed.

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
    ctx.set_dir(&cformat!("{}", driver_ctx.get_dir()))?;
    // Error handler is Option (None = silently drop async client errors).
    // The Aeron samples always set a logger so failures are visible.
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
    // retryable ones (back-pressure / admin action / not connected),
    // surface the fatal ones (closed / max position exceeded).
    loop {
        match publication.offer(b"Hello, Aeron!") {
            Ok(_) => break,
            Err(e) if e.is_retryable() => continue,
            Err(e) => return Err(e.into()),
        }
    }

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

Every channel/URI argument is a `&CStr` (the C API's type), so every heap allocation is
visible at the call site. Recommended three-tier pattern, cheapest first:

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

---

## What's new in 0.2

0.2 is a breaking release (deeper write-up in the [rusteron-client README](./rusteron-client/README.md)):

- **Deferred close.** `aeron.close()` no longer frees child resources immediately (the 0.1.x behaviour was a use-after-free). Close is deferred until the last reference drops; drop order is arbitrary. `unsafe close_now()` forces immediate teardown.
- **Reference-counted handlers.** `Handler::leak()`/`release()` are gone; `Handler::new()` is `Arc`-backed and freed when the last clone drops. Retained-callback setters take the value (closure or trait impl) and return the `Handler`. `Handlers::NONE` covers "no callback".
- **Typed errors.** `offer`/`try_claim` return `Result<i64, AeronOfferError>` with `is_retryable()`. `AeronCError` construction is allocation-free (never reads `aeron_errmsg()`); `capture_errmsg()` opts into attaching the message. Archive control ops return `Result<_, AeronArchiveError>` (`From` keeps `?` working).
- **Hot path.** Generated wrappers are `#[inline]`; release profile uses fat LTO. `offer_parts(&[&header, &payload])` is a zero-copy vectored publish; `poll_fn` is the zero-alloc closure poll; C-string args follow the `c""`/`cformat!`/reuse pattern above.
- **Convenience.** `Aeron::connect_dir`, `AeronDriver::launch_embedded_guard` (RAII), `ChannelUri::add_session_id`, `AeronUriStringBuilder::ipc()/udp()`, retained-image accessors, direct constant getters, and ported samples (basic_publisher/subscriber, ping/pong, file transfer, MDS, request/response).

## Migrating from 0.1.168 to 0.2

Old → new for every renamed/changed API (rows verified against the released 0.1.168):

| 0.1.168 | 0.2 | Notes |
|---|---|---|
| `Handler::leak(h)` + manual `handler.release()` | `Handler::new(h)` | Freed automatically when the last clone drops; registering resources keep clones. |
| `ctx.set_error_handler(Some(&handler))` (borrowed) | `ctx.set_error_handler(Some(handler_or_closure))` | Retained setters take the value; closures work directly; returns the `Handler`. |
| `aeron.close()` — freed children immediately | deferred close | Frees when the last reference drops; `unsafe close_now()` forces immediate. |
| `Handler` was `Sync` | `Send` only | The conductor thread invokes callbacks; sharing `&Handler` across threads raced. |
| `AeronPublication` / `AeronSubscription` / … were `Sync` | `Send` only by default | Handles use `Rc` (single-thread ownership). Enable the `multi-threaded` feature (`Rc` → `Arc` + `unsafe impl Sync`) to share `&Handle` across threads for the ops Aeron C documents as thread-safe (`offer` / `try_claim` / `position` / `is_connected`). |
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
| `wrapper.get_inner_mut()` / `ManagedCResource::get_mut()` (safe) | `unsafe …()` | Both mint `&mut` from `&self`; the caller must now promise exclusive access. The only internal caller (`clone_struct`) is wrapped in `unsafe` already. |
| `ChannelUriStringBuilder::put_string(&CStr, &str)` / `put_strings(&str, &str)` | `put_str(&CStr, &str)` | Single name, single key type (`&CStr` — pair with `c"media"` or `CStr::from_bytes_until_nul`); `put_strings` removed (no external callers). |
| `archive.begin_replay(...)`, `start_recording(...)`, … → `Result<_, AeronCError>` | `Result<_, AeronArchiveError>` | Control ops on `AeronArchive` return the typed error (parseable code + message). Constructors, async-connect, and context setters still return `AeronCError`; `From<AeronArchiveError> for AeronCError` keeps `?` working across the boundary. |
| `async_add_exclusive_publication.poll(...).get_registration_id()` on the deprecated `exclusive_exclusive` alias | only `aeron_async_add_exclusive_publication_get_registration_id` is exposed | The deprecated `aeron_async_add_exclusive_exclusive_publication_get_registration_id` C alias is dropped (it collided with the canonical name); use the canonical `get_registration_id()`. |
| `DarwinPthread*`, `OpaquePthread*` wrapper structs in the generated API | removed | Bindgen pthread internals are no longer emitted as wrapper types; socket types the driver wrappers reference (`sockaddr_storage`, `iovec`, …) are retained. |

Behavioural notes:
- A panicking fragment handler aborts the process (panic cannot unwind across the C
  callback boundary) — return instead of panicking in handlers.
- `AeronCError` construction never reads or copies `aeron_errmsg()` — retry loops
  (e.g. polling that keeps returning `-1`) stay allocation-free. `Display` /
  `get_last_err_message()` read the live buffer; call `err.capture_errmsg()` at the
  error site to pin the text to the error when you store it or log it later.

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
