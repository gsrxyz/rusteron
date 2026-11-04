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

**Why generated bindings**: the main advantage of generating the bindings from the Aeron C headers is that the API is always complete and matches the Aeron documentation. Every C function and setting is available under a predictable name, with no wrapper lagging behind an Aeron release. Hand-written bindings can look nicer, but they are hard to maintain, and when something breaks you cannot tell whether the fault is in the wrapper or in Aeron itself. Because the generated code is a thin pass-through to the C API, a problem can be reproduced against Aeron directly, and Aeron's own docs, samples and issue tracker apply as-is. Aeron is specialised, and rusteron is aimed mainly at people who already know low-latency development and want the real Aeron API rather than an abstraction over it. As with any low-level API, that power cuts both ways: rusteron guards against the common mistakes, but it is still possible to shoot yourself in the foot if the API is misused, so it pays to know how Aeron expects each call to be used.

**Lifecycle**: since 0.2, closing or dropping owned handles in any order is safe. `aeron.close()` is deferred while any publication, subscription, counter or clone is alive, and those handles stay usable until the last one drops (see [Resource Lifecycle](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/README.md#resource-lifecycle--close)). The remaining sharp edges:
- `unsafe close_now()` frees children immediately.
- Borrowed handles do not keep their owner alive. For example, an `Aeron` from `archive_context.get_aeron()` or a descriptor poller from `AeronArchive` must not outlive the context or archive it came from.
- A buffer claim (`try_claim` / `try_claim_owned`) must be committed or aborted before its publication closes.
- A panic in a callback never aborts the process (unless you build with `panic = "abort"`). See [Panics in callbacks](#panics-in-callbacks) for what happens to the panic and to the message.
- A retained image must be dropped before `subscription.close()`.

### Panics in callbacks

Aeron calls your Rust code from C. A Rust panic is handled one of three ways, picked per C function when the bindings are generated:

| Callback | Runs on | What happens to the panic | What happens to the message |
|---|---|---|---|
| Fragment, controlled or block handler given to a subscription, image or replay-merge `poll` (and the `_fn` variants) | your thread, inside `poll` | **Unwinds** straight through C to the caller of `poll`, with its original payload | Nothing lost. Everything from the start of that poll (or the last `COMMIT`) up to the panicking fragment is delivered again on the next poll, so handlers must tolerate redelivery. Order is kept within each image; on a subscription with several images the next poll may start at a different image |
| Fragment assembler delegate (`Handler::with_fragment_assembler`, `AeronFragmentClosureAssembler`) | your thread, inside `poll` | **Caught**, the assembler finishes its bookkeeping, then the `poll` raises it on return | The panicking message is not delivered again; the rest of that poll still is |
| Controlled fragment assembler delegate | your thread, inside `poll` | **Caught**, then raised by the `poll` | The delegate returns `ABORT`, so the panicking message is delivered again on the next poll |
| Persistent subscription handler and listener (`on_live_joined`, `on_live_left`, `on_error`) | your thread, inside the persistent `poll` | **Caught**, then raised by the `poll` | The panicking fragment is not delivered again |
| Recording descriptor poller consumer (stored by `reset`, only on an owned poller: pollers from `AeronArchive` are borrowed, so `reset` returns an error) | your thread, inside the poller's `poll` | **Caught**, then raised by the poller's `poll` | The poller moves on, so that descriptor is not delivered again |
| `for_each_image` visitor | your thread | **Caught**, C releases the image list, then `for_each_image` raises it | The remaining images are still visited |
| Reserved-value supplier (`offer_with_reserved_value`) | your thread, inside `offer` | **Caught**, then the `offer` raises it on return | The message is still published, with reserved value `0` |
| Other callbacks that run during the call (`aeron_cnc_error_log_read`, counters `foreach`, loss report, URI parse, ...) | your thread | **Caught**, C sees a default return value, then the wrapper raises it on return | Depends on the C function; it sees `0` / null / `()` from the panicking call |
| Callbacks C keeps and fires later: available / unavailable image, error handler, new publication / subscription / counter, client close, archive and driver callbacks | usually the conductor thread | **Logged** with `log::error!` and `eprintln!` (`<callback> panicked inside an Aeron callback, continuing: <message>`), and the client carries on | That one event is dropped; later events still arrive |

**Why it works this way**

- Aeron C has no cleanup code for an exception passing through it. Unwinding through a C function is only safe when C holds no state between calling the handler and returning. An audit of the Aeron C source found that holds for the 9 poll functions listed in `UNWIND_POLL_FNS` (`rusteron-code-gen/src/generator.rs`): they commit the read position only after the handler returns, so an unwind just skips the commit and the fragments come back. These functions and their handler types are `extern "C-unwind"`, and Aeron C is built with `-fexceptions` so the unwinder can pass through its frames. This path adds no `catch_unwind` and no check to the poll loop.
- Everywhere else C does hold state across the call: the assembler's reassembly buffer, the image list refcount in `for_each_image`, the claimed term space in `offer`, the persistent subscription's state machine. Unwinding there would leak or corrupt it. So the trampoline catches the panic, returns a value C understands (`()`, `0`, null, `ABORT` for controlled handlers), lets C finish normally, and the Rust wrapper raises the panic once C has returned. You still get the panic, and Aeron's state stays valid.
- A callback C keeps and fires later, usually on the conductor thread, has no Rust caller to raise the panic to. Aborting the whole process for a bug in, say, an available-image handler is worse than losing that one event, so it is logged and the client continues.
- Before this, a panic in any callback except a poll handler aborted the process (since Rust 1.81 a panic cannot cross an `extern "C"` function).

**Edge cases**

- Only the first caught panic on a thread is kept until it is raised; a later one is reported by the panic hook and dropped.
- A caught panic is raised at the next resume point on the same thread. If you drive the image fragment assemblers through the raw bindings, or wrap an assembler in your own handler type, the panic waits until the next call that raises a caught panic. The same applies to a persistent subscription listener when the persistent `poll` is given no fragment handler.
- With `use_conductor_agent_invoker`, conductor callbacks run inside your `invoke` call, but are still logged rather than raised.
- Raw bindings: a handler you pass to one of the poll functions must be declared `unsafe extern "C-unwind" fn` (the compiler rejects `extern "C"`). Any other raw callback must not panic; Rust aborts at the `extern "C"` boundary.
- Want the old behaviour, where any panic stops the process? Set `panic = "abort"` in your Cargo profile. Every panic then aborts immediately, before any of the above runs.

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

For full build instructions, see [BUILD.md](https://github.com/gsrxyz/rusteron/blob/main/BUILD.md).

### CPU target

A release build from source compiles the Aeron C code for the build machine's CPU
(`-march=native`). Set `RUSTERON_C_MARCH` (e.g. `x86-64-v3`) when binaries run on
machines other than the one that built them. The precompiled libraries (`precompile`)
target the architecture's baseline (`x86-64`, `armv8-a`), so they run on any CPU. On the
x86-64 VMs in [Tuning on x86-64 Linux](https://github.com/gsrxyz/rusteron/blob/main/docs/tuning_x86_64_linux.md), `-march=native` for the
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
   rustflags = ["-Clinker-plugin-lto", "-Clink-arg=-fuse-ld=lld-22", "-Ctarget-cpu=native"]

   [env]
   CC = "clang-22"
   ```

   `-Clinker-plugin-lto` is the switch: rusteron's build script sees it and compiles Aeron C
   to LLVM bitcode with `$CC` (or `RUSTERON_CLANG`), for the CPU named by `-Ctarget-cpu`.
   C and Rust must target the same CPU, or LLVM refuses to inline across them. `CC` also
   covers other C dependencies, which the `cc` crate compiles to bitcode under the same flag.
   C uses full LTO (`-flto`) to match Rust's `lto = "fat"`; with Rust `lto = "thin"`, set
   `RUSTERON_C_LTO=thin`. Mixing fat Rust with thin C inlines nothing.

4. Check it worked. The build prints
   `cross-language LTO: Aeron C compiled to bitcode by clang-22 (LLVM 22) with -flto` for each
   rusteron crate (Cargo shows build-script warnings only for path dependencies).
   `aeron_buffer_claim_commit` is small enough to always inline, so it no longer exists in an
   unstripped binary:

   ```bash
   nm target/release/my-app | grep ' aeron_buffer_claim_commit$'   # prints nothing
   ```

   Whether larger functions such as `offer`, `offerv` and `try_claim` inline is left to LLVM's
   default cost model, so they may stay calls. To see the decision at each call site, link
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
- [Multi-Destination Subscription (MDC / MDS) Guide](https://github.com/gsrxyz/rusteron/blob/main/docs/mdc_mds_guide.md)
- [Media Driver Configuration & Back-pressure Guide](https://github.com/gsrxyz/rusteron/blob/main/docs/media_driver_configuration_and_backpressure_guide.md)
- [What's new in 0.2](https://github.com/gsrxyz/rusteron/blob/main/docs/whats_new_0.2.md)
- [Migrating from 0.1.168 to 0.2](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.1.168_to_0.2.md)
- [Tuning on x86-64 Linux](https://github.com/gsrxyz/rusteron/blob/main/docs/tuning_x86_64_linux.md)
- [Benchmarks](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md)

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
            |msg: &[u8], header: &AeronHeader| {
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

In the generated APIs every channel/URI argument is a `&CStr` (the C API's type), not
`&str`/`String`, so every heap allocation is visible at the call site. `&str`/`String` appear there
only where they add no allocation, e.g. strings borrowed back from C.

A few hand-written convenience helpers take `&str` and allocate on each call:
`AeronArchive::connect`, the persistent-subscription builder's `live_channel` / `replay_channel`,
and the replication params builder's `replication_channel` / `src_response_channel`. They are
meant for setup, not the hot path.

Recommended three-tier pattern, cheapest first:

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

0.2 is a breaking release: deferred close, reference-counted handlers, typed errors and more. See [What's new in 0.2](https://github.com/gsrxyz/rusteron/blob/main/docs/whats_new_0.2.md).

## Migrating from 0.1.168 to 0.2

Old → new for every renamed/changed API: [Migrating from 0.1.168 to 0.2](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.1.168_to_0.2.md).

---

## Tuning on x86-64 Linux

Measured settings for latency and throughput on x86-64 Linux (socket buffers, MTU, CPU isolation, busy reads, huge pages, archive disks): [Tuning on x86-64 Linux](https://github.com/gsrxyz/rusteron/blob/main/docs/tuning_x86_64_linux.md).

---

## Benchmarks

For latency and throughput benchmarks, refer to [BENCHMARKS.md](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md).

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
