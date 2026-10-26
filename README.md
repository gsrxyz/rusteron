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

**Why generated bindings**: the main advantage of generating the bindings from the Aeron C headers is that the API is always complete and matches the Aeron documentation. Every C function and setting is available under a predictable name, with no wrapper lagging behind an Aeron release. Hand-written bindings can look nicer, but they are hard to maintain, and when something breaks you cannot tell whether the fault is in the wrapper or in Aeron itself. Because the generated code is a thin pass-through to the C API, a problem can be reproduced against Aeron directly, and Aeron's own docs, samples and issue tracker apply as-is. Aeron is specialised, and rusteron is aimed mainly at people who already know low-latency development and want the real Aeron API rather than an abstraction over it.

**Lifecycle**: since 0.2, closing or dropping handles in any order is safe. `aeron.close()` is deferred while any publication, subscription, counter or clone is alive, and those handles stay usable until the last one drops (see [Resource Lifecycle](./rusteron-client/README.md#resource-lifecycle--close)). The remaining sharp edges:
- `unsafe close_now()` frees children immediately.
- A panic in a handler aborts the process, because it cannot unwind through the C frames. If a handler can panic, wrap its body in `std::panic::catch_unwind` and report the failure through your own state.
- A retained image must be dropped before `subscription.close()`.
- `AeronArchiveContext::set_aeron()` does not keep the client alive. Connect with `AeronArchiveAsyncConnect::new_with_aeron(&ctx, &aeron)`, which does; with `set_aeron()` plus `new()` or `aeron_archive_connect()`, closing the client before the archive is a use-after-free.

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

**Precompiled static libs (macOS and Linux, no cmake or Java needed):**

```toml
[dependencies]
rusteron-client = { version = "0.2", features = ["static", "precompile"] }
```

**Precompiled static libs with rustls downloader (macOS and Linux):**

```toml
[dependencies]
rusteron-client = { version = "0.2", features = ["static", "precompile-rustls"] }
```

Replace `rusteron-client` with `rusteron-archive` or `rusteron-media-driver` as needed.

For full build instructions, see [BUILD.md](./BUILD.md).

### CPU target

A release build from source compiles the Aeron C code for the build machine's CPU
(`-march=native`). Set `RUSTERON_C_MARCH` (e.g. `x86-64-v3`) when binaries run on
machines other than the one that built them. The precompiled libraries (`precompile`)
target the architecture's baseline (`x86-64`, `armv8-a`), so they run on any CPU. On the
x86-64 VMs in [Tuning on x86-64 Linux](#tuning-on-x86-64-linux), `-march=native` for the
client's C code showed no consistent gain over `x86-64` or `x86-64-v3` on IPC; the driver's
build and UDP were not varied.

### Cross-language LTO (Linux)

A static build can let LLVM inline Aeron's C into the Rust that calls it: `try_claim` +
`commit` and the fragment handler collapse into their callers instead of crossing an FFI call
(and, for `poll`, an indirect call per fragment). Only the final binary can turn it on, since
rusteron's own build settings do not reach crates that depend on it.

1. Install clang, lld and llvm-ar with **the same LLVM major version as rustc** (`rustc -vV`
   prints `LLVM version: 22.x`). From [apt.llvm.org](https://apt.llvm.org):
   `wget https://apt.llvm.org/llvm.sh && sudo bash llvm.sh 22 && sudo apt install lld-22`.
   Upgrade them together with Rust: rustc's LLVM cannot read bitcode from another major, and
   the build stops with the fix when they differ.
2. Link Aeron statically and use fat LTO in the final binary's `Cargo.toml`:

   ```toml
   [dependencies]
   rusteron-client = { version = "0.2", features = ["static"] }

   [profile.release]
   lto = "fat"
   codegen-units = 1
   ```

3. Add to the final binary's `.cargo/config.toml` (use `aarch64-unknown-linux-gnu` on Arm):

   ```toml
   [target.x86_64-unknown-linux-gnu]
   linker = "clang-22"
   rustflags = ["-Clinker-plugin-lto", "-Clink-arg=-fuse-ld=lld-22", "-Ctarget-cpu=native",
                "-Clink-arg=-Wl,-mllvm,-locally-hot-callsite-threshold=3000"]

   [env]
   CC = "clang-22"
   ```

   `-Clinker-plugin-lto` is the switch: rusteron's build script sees it and compiles Aeron C
   to LLVM bitcode with `$CC` (or `RUSTERON_CLANG`), for the CPU named by `-Ctarget-cpu`.
   C and Rust must target the same CPU, or LLVM refuses to inline across them. `CC` also
   covers other C dependencies, which the `cc` crate compiles to bitcode under the same flag.
   C uses full LTO (`-flto`) to match Rust's `lto = "fat"`; with Rust `lto = "thin"`, set
   `RUSTERON_C_LTO=thin`. Mixing fat Rust with thin C inlines nothing.

   `-locally-hot-callsite-threshold` raises LLVM's inlining limit at call sites it estimates as
   hot: run at least 60 times as often as the entry of the function containing them, which in
   practice means a loop in that same function once earlier inlining is done. At its default
   (525), Aeron's `offer`, `offerv` and `try_claim` (585 to 1085 by LLVM's cost model) stay calls
   wherever the application calls them from more than one place; at 3000 they inline at the hot
   call sites, while every other call site keeps the normal threshold (250).

4. Check it worked. The build prints
   `cross-language LTO: Aeron C compiled to bitcode by clang-22 (LLVM 22) with -flto` for each
   rusteron crate (Cargo shows build-script warnings only for path dependencies). If the
   application calls the publish functions only from hot loops, they no longer exist in an
   unstripped binary:

   ```bash
   nm target/release/my-app | grep -E ' aeron_(exclusive_)?publication_(offerv?|try_claim)$| aeron_buffer_claim_commit$'   # prints nothing
   ```

   A call made anywhere else (start-up, a one-off message, a retry path) keeps a separate copy of
   that function even when the hot loop inlined it. To check the hot call sites themselves, link
   with LLVM's remarks (`-Clink-arg=-Wl,--opt-remarks-filename,remarks.yaml
   -Clink-arg=-Wl,--opt-remarks-passes,inline`): each call site is listed as inlined or as
   "too costly".

The `precompile` libraries are native code and cannot take part (the build warns). macOS is not
supported: rust-lld cannot read the macOS SDK, Apple's linker needs a matching `libLTO.dylib`
and rejects rustc's LTO flags, and clang marks Apple-silicon code with CPU features that rustc
does not, which blocks inlining.

### Multi-threaded (`Send` and `Sync`) handles

By default every handle uses `Rc` and is neither `Send` nor `Sync`: it stays on the thread
whose client created it. A publication or subscription holds an `Rc` clone of its `Aeron`
client, so moving it to another thread would race the client's reference count. To work on
another thread, give that thread a client of its own (`Aeron::connect_dir`), or enable
`multi-threaded`, which swaps `Rc` → `Arc`:
- **`Send`:** the client, and publication, exclusive publication, subscription, counter and
  counters-reader handles, may move to another thread.
- **`Sync`:** `&Handle` may be shared across threads for the ops Aeron C documents as
  thread-safe (`offer` / `try_claim` / `position` / `is_connected`).

```toml
[dependencies]
rusteron-client = { version = "0.2", features = ["multi-threaded"] }
```

```rust,no_run
# use rusteron_client::*;
# #[cfg(feature = "multi-threaded")]
# fn snippet(publication: &AeronPublication) -> Result<(), Box<dyn std::error::Error>> {
// AeronPublication is Sync under `multi-threaded`, so threads share `&publication`.
std::thread::scope(|s| -> Result<(), AeronOfferError> {
    let a = s.spawn(|| publication.offer(b"hello"));
    let b = s.spawn(|| publication.offer(b"world"));
    a.join().expect("publisher thread panicked")?;
    b.join().expect("publisher thread panicked")?;
    Ok(())
})?;
# Ok(()) }
```

> **The flag only lifts the Rust-side barrier — it does not make the underlying Aeron
> object thread-safe.** Sharing is correct only for objects Aeron C documents as
> thread-safe for concurrent use, e.g. `AeronPublication` (`ConcurrentPublication`),
> `AeronCounter`, and `Aeron` itself. `AeronSubscription` is documented by Aeron as
> **not** threadsafe and must not be shared between subscribers. Under `multi-threaded`
> it gains only `Send`, never `Sync`, so it can be moved to one other thread but never
> accessed concurrently from several. `AeronExclusivePublication` is single-producer by
> design and likewise gains only `Send`. It is the caller's responsibility to
> check the thread-safety of each object before sharing `&Handle`.

---

## Documentation & Guides

For detailed guides and code snippets on Aeron features in Rust, see:
- [Multi-Destination Subscription (MDC / MDS) Guide](./docs/mdc_mds_guide.md)
- [Media Driver Configuration & Back-pressure Guide](./docs/media_driver_configuration_and_backpressure_guide.md)

---

## Development

Build tasks use [`just`](https://github.com/casey/just). Run `just` to list commands, or `cargo install just` if needed.

---

## Example: Pub/Sub

<details>
<summary>Expand for usage example</summary>

```rust
use rusteron_client::{
    cformat, Aeron, AeronContext, AeronErrorHandlerLogger, AeronHeader, BackoffIdleStrategy,
    Handlers, IdleStrategy,
};
use rusteron_media_driver::testing::EmbeddedDriver;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Unique directory; stopped and joined when `driver` drops, after the client.
    let driver = EmbeddedDriver::launch()?;

    let ctx = AeronContext::new()?;
    ctx.set_dir(&cformat!("{}", driver.dir()))?;
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
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut idle = BackoffIdleStrategy::new();
    loop {
        match publication.offer(b"Hello, Aeron!") {
            Ok(_) => break,
            Err(e) if e.is_retryable() && Instant::now() < deadline => idle.idle(0),
            Err(e) => return Err(e.into()),
        }
    }

    // The subscription's image can appear just after the offer succeeds, so poll
    // until the message arrives. `poll_fn` runs the closure per fragment (zero
    // allocation); use a fragment assembler for messages larger than the MTU.
    let mut received = false;
    while !received {
        if Instant::now() >= deadline {
            return Err("no message received".into());
        }
        let fragments = subscription.poll_fn(
            |msg: &[u8], header: AeronHeader| {
                println!("received {} bytes at position {}", msg.len(), header.position());
                received = true;
            },
            10,
        )?;
        idle.idle(fragments);
    }
    Ok(())
}
```

> **`poll_blocking` / `add_*(.., timeout)` are for example brevity only** — they block the
> calling thread in a busy-poll loop. In production, drive the async poller's `poll()` from
> your own event loop. See
> [`rusteron-client/examples/non_blocking_publisher.rs`](rusteron-client/examples/non_blocking_publisher.rs)
> for the idiomatic pattern.

</details>

### C strings without hidden allocations

Every channel/URI argument is a `&CStr` (the C API's type), so every heap allocation is
visible at the call site. Recommended three-tier pattern, cheapest first:

```rust,no_run
# use rusteron_client::*;
# fn snippet(aeron: &Aeron, port: u16, endpoint: &str, reconnect_attempts: usize) -> Result<(), AeronCError> {
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
# Ok(()) }
```

---

## What's new in 0.2

0.2 is a breaking release (deeper write-up in the [rusteron-client README](./rusteron-client/README.md#general-patterns)):

- **Deferred close.** `aeron.close()` no longer frees child resources immediately (the 0.1.x behaviour was a use-after-free). Close is deferred until the last reference drops; drop order is arbitrary. `unsafe close_now()` forces immediate teardown.
- **Reference-counted handlers.** `Handler::leak()`/`release()` are gone; `Handler::new()` is `Arc`-backed and freed when the last clone drops. Retained-callback setters take the value (closure or trait impl) and return the `Handler`. `Handlers::NONE` covers "no callback".
- **Typed errors.** `offer`/`try_claim` return `Result<i64, AeronOfferError>` with `is_retryable()`. `AeronCError` construction is allocation-free (never reads `aeron_errmsg()`); `capture_errmsg()` opts into attaching the message. Archive control ops return `Result<_, AeronArchiveError>` (`From` keeps `?` working).
- **Hot path.** `offer_parts(&[&header, &payload])` publishes several buffers as one message with no intermediate Vec; C-string args follow the `c""`/`cformat!`/reuse pattern above.
- **After 0.2.10, handles are `Send` only under `multi-threaded`.** Up to 0.2.10, publication, subscription, counter and counters-reader handles were `Send` without it, which was unsound: each shares its client's `Rc` reference count. Code that moves one to another thread now gives that thread its own client (`Aeron::connect_dir`) or enables `multi-threaded`.
- **Convenience.** `Aeron::connect_dir`, `AeronDriver::launch_embedded_guard` (RAII), `ChannelUri::add_session_id`, `AeronUriStringBuilder::ipc()/udp()`, retained-image accessors, direct constant getters, and ported samples (basic_publisher/subscriber, ping/pong, file transfer, MDS, request/response).

## Migrating from 0.1.168 to 0.2

Old → new for every renamed/changed API

| 0.1.168 | 0.2 | Notes |
|---|---|---|
| `Handler::leak(h)` + manual `handler.release()` | `Handler::new(h)` | Freed automatically when the last clone drops; registering resources keep clones. |
| `ctx.set_error_handler(Some(&handler))` (borrowed) | `ctx.set_error_handler(Some(handler_or_closure))` | Retained setters take the value; closures work directly; returns the `Handler`. |
| `aeron.close()` — freed children immediately | deferred close | Frees when the last reference drops; `unsafe close_now()` forces immediate. |
| `Handler` was `Sync` | `Send` only | The conductor thread invokes callbacks; sharing `&Handler` across threads raced. |
| `AeronPublication` / `AeronSubscription` / … were `Send` and `Sync` | Neither by default; `Send` only under `multi-threaded`, and `Sync` only there and only for types Aeron C documents as thread-safe | Handles use `Rc` (single-thread ownership), and a handle shares its client's refcount, so moving one to another thread raced it. Give each thread its own client, or enable the `multi-threaded` feature (`Rc` → `Arc`): the handles become `Send`, and `AeronPublication`/`AeronCounter`/`Aeron` also `Sync` (share `&Handle` across threads for `offer` / `try_claim` / `position` / `is_connected`). `AeronSubscription` and `AeronExclusivePublication` are documented by Aeron as not safe to share and stay `Send`-only (never `Sync`). |
| `publication.offer(buf, supplier)` → raw `i64` | `publication.offer_raw(buf, supplier)` | Same branch-free sentinel return, renamed to make "raw" explicit. |
| `publication.offer_result(buf, supplier)` → `Result<_, AeronCError>` | `publication.offer_with_reserved_value(buf, supplier)` → `Result<_, AeronOfferError>` | Typed offer errors with `is_retryable()`. |
| `publication.offer_result_simple(buf)` | `publication.offer(buf)` | The common no-supplier case is now the flagship name. |
| `try_claim_result(len, claim)` / `try_claim_owned` → `AeronCError` | `try_claim(len, claim)` / `try_claim_owned` → `AeronOfferError` | Same RAII `AeronClaim`; typed error. |
| `subscription.poll_once(f, limit)` | `subscription.poll_fn(f, limit)` | Renamed (`_once` read as "one fragment"); same on `AeronImage` / `AeronArchiveReplayMerge`. |
| `subscription.for_each_fragment(limit, f)` | `subscription.poll_fn(f, limit)` | Removed (alias with the arguments in the opposite order). |
| `sub.poll(assembler.process(&mut ctx, f), limit)` | `assembler.poll(&sub, &mut ctx, f, limit)` | `process()` leaked a raw ctx pointer past the borrow (UAF hazard); the new form scopes it. |
| `Handlers::no_available_image_handler()`, `no_unavailable_image_handler()`, … | `Handlers::NONE` | One constant, any callback parameter, full inference. Old helpers removed. |
| `pub.add_destination(&aeron, dest, timeout)` (`&mut self`) | `pub.add_destination(dest, timeout)` (`&self`) | Owning `Aeron` comes from the handle's dependency graph. Same for subscriptions / exclusive publications. |
| `AeronCnc::new(dir)` | `AeronCnc::open(&CStr)` or `AeronCnc::read(&CStr, \|cnc\| { … })` | `new_on_heap`/`read_on_partial_stack` renamed; now accept `&CStr` (not `&str`/`&CString`). `read` = scoped (zero-alloc, preferred for one-shot), `open` = owned handle (for repeated polling). |
| `&"aeron:ipc".into_c_string()` (allocates at runtime) | `c"aeron:ipc"` | See "C strings without hidden allocations" above; `cformat!` for dynamic URIs. |
| `wrapper.get_inner_mut()` / `ManagedCResource::get_mut()` (safe) | `unsafe …()` | `&mut` from `&self`; the caller must now promise exclusive access. The only internal caller (`clone_struct`) is wrapped in `unsafe` already. |
| `AeronUriStringBuilder::put_string(&CStr, &str)` / `put_strings(&str, &str)` | `AeronUriStringBuilder::put_str(&CStr, &str)` | Single name, single key type (`&CStr` — pair with `c"media"` or `CStr::from_bytes_until_nul`); `put_strings` removed (no external callers). |
| `archive.start_replay(...)`, `start_recording(...)`, … → `Result<_, AeronCError>` | `Result<_, AeronArchiveError>` | Control ops on `AeronArchive` return the typed error (parseable code + message). Constructors, async-connect, and context setters still return `AeronCError`; `From<AeronArchiveError> for AeronCError` keeps `?` working across the boundary. |
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

## Tuning on x86-64 Linux

Measured on 2026-10-09 between two Azure `Standard_D8s_v6` VMs (Intel Xeon Platinum 8573C) in one proximity placement group, on Debian 13 with Linux 7.2.6: UDP round trips and throughput between the two hosts, and IPC on each host, with 32-byte messages. Each figure is a median of 5 interleaved runs. Other NICs and clouds will differ. Tables, method and open issues are in [BENCHMARKS.md](./BENCHMARKS.md#intel-d8s_v6-pair-on-azure-2026-10-09).

These were used in every run, and earlier runs on smaller VMs showed each to help:
- `AERON_TERM_BUFFER_SPARSE_FILE=false` (driver) and `AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY=true` (clients). Without them a new publication page-faults its way through its log.
- `AERON_DIR` on 2 MiB `hugetlbfs` with `AERON_FILE_PAGE_SIZE=2097152`.
- `noop` idle for the driver's sender and receiver.
- Every busy-spinning thread pinned to its own core, and everything else (interrupts, the driver's conductor, the clients' other threads) on a housekeeping CPU.

| Setting | Measured effect | Cost |
|---|---|---|
| Socket buffers and receiver window: `AERON_SOCKET_SO_SNDBUF`, `AERON_SOCKET_SO_RCVBUF` and `AERON_RCV_INITIAL_WINDOW_LENGTH` at 2 MiB, with `net.core.rmem_max`/`wmem_max` raised to allow them | UDP throughput between the hosts 1.9 → 13 M msgs/s. No effect on round-trip latency with one message in flight. | Memory per socket. |
| MTU 9000 inside the VNet and `AERON_MTU_LENGTH=8192` | UDP throughput 13 → 28.5 M msgs/s. | Azure allows it only inside a VNet and directly peered VNets; keep the default route at 1500 so traffic leaving the VNet still fits. |
| CPU isolation: `isolcpus=nohz,domain,managed_irq,<hot CPUs> nohz_full=<same> rcu_nocbs=<same> irqaffinity=<housekeeping>` | IPC p99.99 2.5 → 0.8–1.0 µs, and most runs' max 20–67 → 4–7 µs. UDP: no change on its own. | Isolated CPUs take no unpinned work, which then all crowds onto the housekeeping CPUs, so every busy thread must be pinned. |
| Socket busy reads, `net.core.busy_read=50`, with `napi_defer_hard_irqs=2` and `gro_flush_timeout=200000` on the NIC's VF, on isolated CPUs | UDP round trip p50 38.6 → 31 µs, p99.99 157 → 44–50 µs. `net.core.busy_poll` does nothing for Aeron's receiver, which calls `recvmmsg` directly. | The receiver's core polls the NIC queue itself. |
| The NIC's queue IRQs on the receiver's core, on isolated CPUs | UDP p99.99 157 → 49–57 µs; with the tuned kernel below, max 74 µs (62–84). | Re-pin them whenever Azure re-adds the VF (host servicing), which spreads them over all CPUs again. |
| The client's other threads (its conductor) off the busy-spinning CPUs | IPC p99.99 3.1–3.6 → 0.7–1.0 µs on isolated CPUs. | A CPU for them, or the conductor agent invoker. |
| A tuned kernel on top of isolation: `nosmt idle=poll rcu_nocb_poll nowatchdog nmi_watchdog=0 nosoftlockup skew_tick=1 transparent_hugepage=never audit=0`, all IRQs and workqueues on the housekeeping CPU, background services stopped | UDP p50 38.6 → 34.9 µs. IPC unchanged. | SMT off halves the CPUs; `idle=poll` keeps every idle CPU busy. |
| `mitigations=off` | UDP p50 34.9 → 33.9 µs and p99.99 156 → 121 µs. IPC unchanged. | Turns off the kernel's protection against CPU side-channel attacks. |
| Driver threading | Dedicated (pinned `noop` sender and receiver), SHARED_NETWORK and SHARED were within about 2 µs of each other at UDP p50. A SHARED driver costs IPC: p50 0.31 → 0.36–0.42 µs and half the throughput. | Dedicated spins two cores. |
| Across regions, socket buffers and initial window sized to the round trip: 16 MiB for 53 ms between West US 3 and North Central US, over 64 MiB terms, with `net.core.rmem_max`/`wmem_max` raised to match and kept in `/etc/sysctl.d` | Throughput 18 → 119 MB/s on a clean link. At 0.1% loss both carried 10–13 MB/s. | Memory per socket. If `rmem_max` resets (a reboot undoes `sysctl -w`), the driver refuses to start, as the window exceeds the receive buffer. |
| In Kubernetes: kubelet's static CPU manager and Guaranteed pods requesting whole CPUs | Each container gets its CPUs to itself, with no CFS quota. Under the default policy, containers limited to exactly their spinning threads' CPUs were throttled (23–55% of 100 ms periods for a 1-CPU driver); one CPU of headroom or no CPU limit avoided it. | See [BENCHMARKS.md](./BENCHMARKS.md#recommended-settings-for-kubernetes) for the kubelet and pod settings. |

Where these settings are recommended:
- **Aeron's settings:**
  - Aeron's [Best Practices Guide](https://github.com/aeron-io/aeron/wiki/Best-Practices-Guide) gives:
    - socket receive buffers of 2–4 MB and a receiver window of 2 MB or more for throughput;
    - DEDICATED threading when there are cores to spare, else SHARED_NETWORK or SHARED;
    - the low-latency driver's busy-spin conductor with `noop` sender and receiver.
  - Its [Configuration Options](https://github.com/aeron-io/aeron/wiki/Configuration-Options) explain sparse term files ("can cause latency pauses"), pre-touch ("to avoid soft page faults"), the file page size for huge pages, and the C driver's CPU affinity and cpuset options.
- **CPU isolation, thread pinning and IRQ affinity:** Erik Rigtorp's [Low Latency Tuning Guide](https://rigtorp.se/low-latency-guide/) covers these, along with SMT off (`nosmt`), transparent huge pages off with explicit huge pages, and `mitigations=off`.
- **The tuned kernel command line:** SUSE's [kernel arguments for low latency and high performance](https://documentation.suse.com/suse-edge/3.1/html/edge/atip-features.html) include `isolcpus=domain,nohz,managed_irq`, `nohz_full`, `rcu_nocbs`, `rcu_nocb_poll`, `irqaffinity`, `idle=poll`, `skew_tick=1`, `nosoftlockup`, `nowatchdog` and `nmi_watchdog=0`. The [Linux kernel parameters](https://docs.kernel.org/admin-guide/kernel-parameters.html) document every one.
- **Busy reads:** `net.core.busy_read` and the socket buffer limits are in the [kernel's net sysctls](https://docs.kernel.org/admin-guide/sysctl/net.html). The kernel's [NAPI documentation](https://docs.kernel.org/networking/napi.html) recommends setting `napi_defer_hard_irqs` and `gro_flush_timeout` together.
- **Jumbo frames on Azure:** Microsoft's [Configure MTU for virtual machines in Azure](https://learn.microsoft.com/en-us/azure/virtual-network/how-to-virtual-machine-mtu) gives MTU 9000 on the MANA NIC, but only within a VNet and directly peered VNets in the same region. Gateways, global peering and the internet stay at 1500.
- **Kubernetes:** [Control CPU Management Policies on the Node](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/) and [Manage HugePages](https://kubernetes.io/docs/tasks/manage-hugepages/scheduling-hugepages/).
- **On AWS (not measured here):**
  - AWS's [Improve network latency for Linux based EC2 instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-improve-network-latency-linux.html) recommends:
    - a cluster placement group;
    - `net.core.busy_read=50`, as here. Its `net.core.busy_poll=50` does nothing for Aeron, whose receiver calls `recvmmsg` directly;
    - limiting deeper C-states;
    - turning off the ENA NIC's interrupt moderation: `sudo ethtool -C <interface> adaptive-rx off rx-usecs 0 tx-usecs 0`.
  - [Processor state control](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/processor_state_control.html) lists the instance types whose guests may limit C-states, and the fewer that may also set P-states. Support varies by type.
  - The [Amazon Linux 2 guide](https://docs.aws.amazon.com/linux/al2/ug/processor_state_control.html) keeps idle cores in C1 with the kernel options `intel_idle.max_cstate=1 processor.max_cstate=1`. For the steadiest frequency it turns off Turbo Boost with `echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo`.
  - `idle=poll`, as in the tuned kernel above, goes further: idle CPUs never enter a C-state at all. Use one or the other.
  - On these Azure VMs the C-state options do nothing: the guest has no cpuidle driver, so there are no C-states to limit. Frequency control was not checked.

Recommended elsewhere, and not yet measured here (the lab's `tune8x` state and `ded-threads-msgs2` variant test the kernel and send-batch items):
- **Aeron's low-latency C driver script:**
  - [`low-latency-c-media-driver`](https://github.com/aeron-io/aeron/blob/master/aeron-samples/scripts/low-latency-c-media-driver) also sets `AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND=2` (the default is 4).
  - It also sets `AERON_CONDUCTOR_IDLE_STRATEGY=spin`, which spins a whole core.
- **Messages within the MTU:** Aeron's [Java Programming Guide](https://github.com/aeron-io/aeron/wiki/Java-Programming-Guide) says "For the lowest latency, keep messages within the MTU size."
- **Stalls and loss, watched:**
  - Lower `AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD`, `AERON_DRIVER_SENDER_CYCLE_THRESHOLD` and `AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD`. The default is 100 ms; Aeron's benchmark driver uses 1 ms.
  - Then watch AeronStat's "work cycle exceeded threshold count" counters, and the NAK counters and `loss-report.dat` ([Monitoring and Debugging](https://github.com/aeron-io/aeron/wiki/Monitoring-and-Debugging)).
  - On the host, watch `nstat -az UdpRcvbufErrors UdpInErrors`, the drops in `/proc/net/softnet_stat` and `ethtool -S` ([Red Hat's network tuning](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/monitoring_and_managing_system_status_and_performance/tuning-the-network-performance_monitoring-and-managing-system-status-and-performance)).
- **RCU, timers and kernel threads:**
  - tuned's [`network-latency` profile](https://github.com/redhat-performance/tuned/blob/master/profiles/network-latency/tuned.conf) adds `rcupdate.rcu_normal_after_boot=1`, `rcutree.nohz_full_patience_delay=1000` and `kernel.timer_migration=0`.
  - Its [`cpu-partitioning` profile](https://github.com/redhat-performance/tuned/blob/master/profiles/cpu-partitioning/tuned.conf) also moves the writeback workqueue (`/sys/bus/workqueue/devices/writeback/cpumask`) to the housekeeping CPUs.
  - The kernel's [NO_HZ documentation](https://docs.kernel.org/timers/no_hz.html) leaves pinning RCU's `rcuo` kthreads to userspace.
  - The kernel's [CPU isolation guide](https://docs.kernel.org/admin-guide/cpu-isolation.html) prefers cpuset isolated partitions, which can be changed at run time, to `isolcpus`.
- **Connection tracking:** if any firewall rule loads conntrack, a `notrack` rule at raw priority keeps Aeron's packets out of it ([nftables wiki](https://wiki.nftables.org/wiki-nftables/index.php/Setting_packet_connection_tracking_metainformation)).
- **AWS's ENA driver:**
  - Its [best practices guide](https://github.com/amzn/amzn-drivers/blob/master/kernel/linux/ena/ENA_Linux_Best_Practices.rst) suggests `busy_read` and `busy_poll` of 70.
  - It suggests keeping irqbalance but banning the hot CPUs (`IRQBALANCE_BANNED_CPUS`), and running the application on the vCPU that takes its queue's IRQ.
  - [ENA Express](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-express.html) can add "tens of microseconds" to median latency, so leave it off for latency.
- **Azure placement:** after host maintenance, check `az ppg show --include-colocation-status` ([co-location](https://learn.microsoft.com/en-us/azure/virtual-machines/co-location)). A VM moved out of alignment needs a full deallocate and start.

Packet loss costs far more than its share:
- 0.1% UDP loss cut same-zone throughput by a third, and 1% by 91%. Every lost packet was still retransmitted.
- In request/response traffic, a lost message waits for the sender's next heartbeat, 100 ms in the C driver.
- Find and fix the loss before tuning anything else ([details](./BENCHMARKS.md#packet-loss-and-distance-west-us-3-and-north-central-us-2026-10-10)).

```bash
# C media driver (a Java driver reads -D system properties instead)
export AERON_TERM_BUFFER_SPARSE_FILE=false
export AERON_SENDER_IDLE_STRATEGY=noop AERON_RECEIVER_IDLE_STRATEGY=noop
export AERON_CONDUCTOR_CPU_AFFINITY=0 AERON_SENDER_CPU_AFFINITY=4 AERON_RECEIVER_CPU_AFFINITY=6
export AERON_SOCKET_SO_SNDBUF=2m AERON_SOCKET_SO_RCVBUF=2m AERON_RCV_INITIAL_WINDOW_LENGTH=2m
export AERON_FILE_PAGE_SIZE=2097152 AERON_DIR=/mnt/huge/aeron
# every client
export AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY=true
# the host
sudo sysctl -w net.core.rmem_max=16777216 net.core.wmem_max=16777216 net.core.busy_read=50
# the NIC's VF (its netdev has eth0 as master), as measured with busy_read
vf=
for i in /sys/class/net/*; do [ "$(basename "$(readlink "$i/master")")" = eth0 ] && vf=$(basename "$i"); done
if [ -n "$vf" ]; then
  echo 2 | sudo tee /sys/class/net/$vf/napi_defer_hard_irqs
  echo 200000 | sudo tee /sys/class/net/$vf/gro_flush_timeout
else
  echo "no VF has eth0 as master: is accelerated networking on?" >&2
fi
sudo sysctl -w vm.nr_hugepages=1536
sudo mkdir -p /mnt/huge && sudo mount -t hugetlbfs -o pagesize=2M,size=2G,uid="$(id -u)" none /mnt/huge
```

### Archive disks

Measured on 2026-10-10 with a Java Archive on two Azure `Standard_D8ds_v6`, recording 1 KiB messages from up to 16 streams ([details](./BENCHMARKS.md#java-archive-on-disk-under-load-two-d8ds_v6-2026-10-10)).

- **Size the disk for sustained throughput, not IOPS.**
  - At file sync level 0 (the default) the archive writes into the page cache. RAM takes the bursts, so the disk only needs the average write rate plus replay reads.
  - Cloud disks share one budget between reads and writes.
  - Recordings are sequential, so the 3,000 IOPS a disk includes is plenty.
- **A durable network disk is enough.** On Azure that is Premium SSD v2: 125 MB/s included, more bought separately. AWS gp3 is priced the same way but wasn't measured here.
  - The VM caps its disks as well: 424 MB/s in total on a D8ds_v6.
  - New Premium SSD v2 disks, freshly formatted with a full-device discard (plain `mkfs.xfs`), ran well below their provisioned rate for their first 25 minutes or more. Test a fresh disk before relying on it.
- **Let the page cache hold larger bursts:** `sudo sysctl -w vm.dirty_bytes=17179869184 vm.dirty_background_bytes=536870912`. That allows 16 GiB of unwritten data, half of the 32 GiB VM, so scale it to your RAM.
  - With these, a 125 MB/s disk took 30 s bursts of 400 MB/s at full rate; with Linux's defaults it throttled after 15 s.
  - Data still in memory is lost if the host fails, which level 0 already accepts.
  - A full 16 GiB takes over 2 minutes to write out at 125 MB/s, at shutdown too.
- **Leave headroom for replays.** While recording ran flat out at the disk's limit, replays from that disk slowed from about 1 GB/s to 67–246 MB/s on local NVMe.
- **Local NVMe was the fastest and costs nothing extra:** 545 MB/s sustained, 1 GB/s of replays, and `fdatasync` in 0.03 ms against 0.8 ms on Premium SSD v2. Its data is gone when the VM stops or its host fails, though, so use it only for an archive that is replicated elsewhere.
- **File sync level 1 or 2** makes every write wait for the disk. That cost nothing sustained on local NVMe. On Premium SSD v2 it was measured only while the new disks were still slow, at 56–220 MB/s, so its cost at full speed is unknown.

Sources:
- **Aeron's [Archive guide](https://github.com/aeron-io/aeron/wiki/Aeron-Archive)** defines the file sync levels: 0 writes to the page cache, 1 forces data to disk, 2 forces data and metadata. It also notes that the archive's "major limitation is the performance of the storage for the recordings".
- **Disk performance models:**
  - Microsoft's [disk types](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-types#premium-ssd-v2): Premium SSD v2 includes 3,000 IOPS and 125 MB/s, with more throughput bought at 0.25 MB/s per IOPS. It is designed to deliver what was provisioned 99.9% of the time, and needs a zonal VM in most regions.
  - AWS's [gp3 volumes](https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html#gp3-ebs-volume-type) work the same way: 3,000 IOPS and 125 MiB/s included, up to 2,000 MiB/s.
- **The kernel's [vm sysctls](https://docs.kernel.org/admin-guide/sysctl/vm.html)** describe `dirty_bytes` and `dirty_background_bytes`. Setting one clears its `dirty_*ratio` counterpart.

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
