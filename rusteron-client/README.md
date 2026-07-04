# rusteron-client

`rusteron-client` is a core component of the [Rusteron](https://github.com/gsrxyz/rusteron) project.  
It provides a Rust wrapper around the Aeron C client API, enabling high-performance, low-latency communication in distributed systems built with Rust.

This crate supports publishing, subscribing, and managing Aeron resources, while exposing a flexible and idiomatic interface over `unsafe` C bindings.  
Due to its reliance on raw FFI, developers must take care to manage resource lifetimes and concurrency correctly.

---

## Sponsored by GSR

**Rusteron** is proudly sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It powers mission-critical infrastructure in GSR's real-time trading stack and is now developed under the official GSR GitHub organization as part of our commitment to open-source excellence and community collaboration.

We welcome contributions, feedback, and discussions. If you're interested in integrating or contributing, please open an issue or reach out directly.

---

## Features

- **Client Setup** – Create and start an Aeron client using Rust.
- **Publications** – Send messages via `offer()` or `try_claim()`.
- **Subscriptions** – Poll for incoming messages and handle fragments.
- **Callbacks & Handlers** – React to driver events like availability, errors, and stream lifecycle changes.
- **Cloneable Wrappers** – All client types are cloneable and share ownership of the underlying C resources.
- **Automatic Resource Management** – Objects created with `.new()` automatically call `*_init` and `*_close`, where supported.
- **Result-Focused API** – Methods returning primitive C results return `Result<T, AeronCError>` for ergonomic error handling.
- **Efficient String Interop** – Inputs use `&CStr`, outputs return `&str`, giving developers precise allocation control.

---

## Installation

Add **rusteron-client** to your `Cargo.toml`:

```toml
# Dynamic linking (default)
rusteron-client = "0.1"

# Static linking
rusteron-client = { version = "0.1", features = ["static"] }

# Static linking with precompiled C libraries (best for Mac users, no Java/cmake needed)
rusteron-client = { version = "0.1", features = ["static", "precompile"] }

# Static linking with precompiled C libraries using rustls downloader
rusteron-client = { version = "0.1", features = ["static", "precompile-rustls"] }
```

When using the default dynamic configuration, you must ensure Aeron C libraries are available at runtime. The `static` option embeds them automatically into the binary.

---

## General Patterns

- **`new()` Initialization**: Automatically calls the corresponding `*_init` method.
- **One-liner connect**: `Aeron::connect(Some(dir))?` builds the context, client, and starts the conductor; build the `AeronContext` yourself for tuned setups. (`AeronArchive::connect(...)` in rusteron-archive.)
- **Retained images auto-release**: `subscription.image_at_index(i)` / `image_by_session_id(id)` return `Option<AeronImage>` that releases back to the subscription on drop; `for_each_image(|img| …)` borrows without bookkeeping.
- **Automatic Cleanup (Partial)**: When possible, `Drop` will invoke the appropriate `*_close` or `*_destroy` methods.
- **Manual Resource Responsibility**: For methods like `set_aeron()` or where lifetimes aren't managed internally, users are responsible for safety.
- **Handlers Are Reference-Counted**: Wrap callbacks with `Handler::new(...)`. Methods that register a callback the C client retains keep a clone alive inside the registering resource, so the value is freed automatically — no manual `release()`.

---

## Handlers and Callbacks

Two kinds of callbacks, two rules:

### 1. Retained callbacks (error handler, image/counter lifecycle, notifications)

The C client stores these and invokes them later from the conductor thread. Setters take the
callback **by value** — a closure or any type implementing the callback trait — heap-allocate
it internally, and keep it alive for as long as C can call it. Nothing to free:

```rust,ignore
ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
```

Need to read the callback's state later? Keep the returned [`Handler`]:

```rust,ignore
let counts = ctx.set_error_handler(Some(ErrorCounter::default()))?.unwrap();
// ... later: counts.errors
```

Registration methods that return a resource (e.g. `add_subscription` with image handlers) take
`Option<&Handler<T>>` instead and keep a clone — same lifetime guarantee.

### 2. Synchronous callbacks (`poll`, `controlled_poll`, log readers)

Only invoked *during* the call, so the `_once` variants borrow a stack closure — zero
allocation, may borrow local state:

```rust,ignore
subscription.poll_once(|buf: &[u8], header: AeronHeader| {
    println!("received {} bytes at position {:?}", buf.len(), header.position());
}, 10)?;
```

> For messages larger than the MTU use a fragment assembler with `subscription.poll(Some(&handler), limit)` — `poll_once` delivers raw fragments and does not reassemble.

No handler for an optional slot? Pass the typed `None` helper, e.g. `Handlers::no_available_image_handler()`.

---

## Building channel URIs

Prefer the typed [`AeronUriStringBuilder`] over hand-written URI strings — parameters are
typed setters, so misspelled keys and malformed values are caught before they reach the
driver:

```rust,ignore
let channel = AeronUriStringBuilder::udp("localhost:20121")?.build(256)?;
let mds_sub = AeronUriStringBuilder::udp_control("localhost:9998", ControlMode::Manual)?.build(256)?;
let response = AeronUriStringBuilder::udp_control("localhost:9999", ControlMode::Response)?
    .response_correlation_id(id)?
    .build(256)?;
```

Constructors: `ipc()`, `udp(endpoint)`, `udp_control(control, ControlMode)`; plus ~50 typed
setters (`session_id`, `mtu_length`, `term_length`, `fc`, `gtag`, `reliable`, …). See
`examples/multi_destination_subscription.rs` and `examples/request_response.rs`.

## Minimal Pub/Sub

```rust,ignore
use rusteron_client::*;
use std::time::Duration;

let ctx = AeronContext::new()?;
// Reuse the built-in logger for async client errors (Aeron samples always set one).
ctx.set_error_handler(Some(AeronErrorHandlerLogger))?;
let aeron = Aeron::new(&ctx)?;
aeron.start()?;

let channel = c"aeron:ipc"; // compile-time &CStr, zero runtime cost
let publication = aeron
    .async_add_publication(channel, 123)?
    .poll_blocking(Duration::from_secs(5))?;
let subscription = aeron
    .async_add_subscription(
        channel, 123,
        Handlers::no_available_image_handler(),
        Handlers::no_unavailable_image_handler(),
    )?
    .poll_blocking(Duration::from_secs(5))?;

// offer() returns the log position (>0) or a negative code — see "Errors & offer results".
while publication.offer_with_reserved_value(b"hello", Handlers::no_reserved_value_supplier_handler()) <= 0 {}
subscription.poll_once(|buf: &[u8], _hdr: AeronHeader| println!("got {} bytes", buf.len()), 10)?;
```

## AddressSanitizer

The `sanitize-address` feature compiles the Aeron C sources with `-fsanitize=address`
(implies `build-from-source`), so use-after-free / double-free across the FFI boundary are
caught in **your** tests:

```bash
# macOS — stable rustc; Apple's ASan runtime is linked automatically, it just needs to be
# on the loader path:
export DYLD_LIBRARY_PATH="$(clang -print-resource-dir)/lib/darwin"
ASAN_OPTIONS=detect_leaks=0 cargo test --features rusteron-client/sanitize-address

# Linux — nightly, additionally instruments the Rust side:
RUSTFLAGS="-Zsanitizer=address" cargo +nightly test -Zbuild-std \
  --target x86_64-unknown-linux-gnu --features rusteron-client/sanitize-address
```

Notes: leak detection is noisy with a media driver in-process — keep `detect_leaks=0`;
other sanitizers (`thread`, `undefined`) can be selected with the `RUSTERON_SANITIZE`
env var instead of the feature. In this repo, `just test-asan` runs the client suite this way.

## Resource Lifecycle & close()

The C client owns every child resource (publications, subscriptions, counters) and frees them
all in `aeron_close`. rusteron therefore reference-counts teardown:

- `aeron.close()` (and `drop`) is **deferred** while any child handle or clone is alive — the
  surviving handles remain fully usable; the C close runs when the last reference releases.
- Leaf resources (`publication.close()` etc.) close immediately; clones of that handle see a
  nulled pointer and become inert.
- `unsafe aeron.close_now()` forces the C close immediately. Every surviving child handle then
  dangles — even its `Drop` is a double free — so `mem::forget` survivors or never return.

Which structures get which behaviour:

| Behaviour | Types |
|---|---|
| **Deferred** `close()` + `unsafe close_now()` | `Aeron`, `AeronArchive` (rusteron-archive) |
| **Immediate** `close(self)` (+ `close_with_handler` for a close-complete notification); clones become inert | `AeronPublication`, `AeronExclusivePublication`, `AeronSubscription`, `AeronCounter` |
| **No public close** — freed when the last reference drops (the client holds an internal clone, so they always outlive it) | `AeronContext`, `AeronArchiveContext`, `AeronDriverContext` |
| **No close at all** — the C client frees them when their `poll()` completes (created, errored, or cancelled) | `AeronAsyncAdd*` pollers, `AeronAsyncDestination` |

Ordering is structurally safe in any combination: children hold references to the client, the
client holds its context, and closing/dropping in *any* order defers the C teardown until the
last handle releases (verified by the `complex_object_graph_close_is_safe_in_any_order` test).

## Migrating 0.1 → 0.2

Breaking changes, made because the old design allowed double frees and use-after-free
(see [PR #50](https://github.com/gsrxyz/rusteron/pull/50)):

| 0.1 | 0.2 |
|---|---|
| `Handler::leak(h)` + manual `release()` | `Handler::new(h)`; freed automatically (`Arc`-counted, resources keep clones) |
| `ctx.set_error_handler(Some(&handler))` | `ctx.set_error_handler(Some(h))` — takes the value (closures work), returns the `Handler` |
| `set_error_handler_once(closure)` (UB: C retained a stack closure) | removed; `_once` now exists only for synchronous callbacks like `poll_once` |
| `aeron.close()` freed children immediately (UAF on surviving handles) | deferred until the last reference drops; `unsafe close_now()` is the escape hatch |
| `Handler` was `Sync` | `Send` only (conductor thread may invoke callbacks) |
| `offer()`/`try_claim()` returned raw `i64` sentinels | return `Result<i64, AeronOfferError>` with `is_retryable()`; raw variants: `offer_raw()`/`try_claim_raw()` |

## Examples

- [`examples/basic_pub_sub.rs`](./examples/basic_pub_sub.rs) — minimal pub/sub with fragment assembly
- [`examples/streaming_rate.rs`](./examples/streaming_rate.rs) — streaming publisher + rate-reporting subscriber (port of `streaming_publisher.c` + `rate_subscriber.c`)
- [`examples/multi_destination_subscription.rs`](./examples/multi_destination_subscription.rs) — MDS: one manual-control subscription aggregating several endpoints (port of `basic_mds_subscriber.c`)
- [`examples/driver_stats.rs`](./examples/driver_stats.rs) — CnC tooling: counters, distinct error log, loss report (ports of `aeron_stat.c` / `error_stat.c` / `loss_stat.c`)
- [`examples/embedded_ping_pong.rs`](./examples/embedded_ping_pong.rs) — RTT ping/pong with `try_claim` (port of `cping`/`cpong`)
- [`examples/embedded_exclusive_ipc_throughput.rs`](./examples/embedded_exclusive_ipc_throughput.rs) — exclusive-publication IPC throughput
- [`examples/request_response.rs`](./examples/request_response.rs) — response channels (aeron 1.44+): request/response wiring via `control-mode=response` + `response-correlation-id` (port of `response_server.c`/`response_client.c`)
- [`examples/file_transfer.rs`](./examples/file_transfer.rs) — chunked file transfer with fragment reassembly and verification (port of `FileSender`/`FileReceiver`)

## Errors & offer results

- **Client errors**: install an error handler on the context (`ctx.set_error_handler(Some(handler))`) so async errors aren't silently lost — Aeron's samples always do.
- **`offer()` / `try_claim()` results**: `Result<i64, AeronOfferError>` — `Ok` is the new log position; the error is a typed sentinel with `is_retryable()` (back-pressured, admin action, not connected — retry, ideally with an idle strategy) vs fatal (closed, max position exceeded). This mirrors Aeron's `BasicPublisher.checkResult` without magic numbers:
  ```rust,ignore
  match publication.offer(msg) {
      Ok(_) => {}
      Err(e) if e.is_retryable() => idle.idle(), // retry
      Err(e) => return Err(e.into()),            // publication gone
  }
  ```
  For branch-free hot paths, `offer_raw()` / `try_claim_raw()` return the raw `i64` sentinel —
  though measured on `benches/offer_claim_poll.rs` the typed path is within noise of raw
  (~2.9ns vs ~2.8ns per offer): the error enum only materialises on the error path.
- **Image handlers**: `Handlers::no_available_image_handler()` / `no_unavailable_image_handler()` are fine as a default, but real apps usually react to image availability (logging, synchronization) — Aeron's `Ping` sample uses one as a latch.

## Idle strategies

Poll loops should back off when a cycle does no work. Rusteron ports Aeron's `IdleStrategy`
(`idle(work_count)` returns immediately when work was done, otherwise backs off):

```rust,ignore
use rusteron_client::{BackoffIdleStrategy, IdleStrategy};

let mut idle = BackoffIdleStrategy::new(); // spin → yield → sleep, Aeron's default
loop {
    let fragments = subscription.poll(Some(&handler), 10)?;
    if fragments == 0 { /* break when done */ }
    idle.idle(fragments);
}
```

Available: [`BusySpinIdleStrategy`] (lowest latency, pins a core), [`YieldingIdleStrategy`],
[`SleepingIdleStrategy`] (fixed sleep), [`BackoffIdleStrategy`] (adaptive, general-purpose),
[`NoOpIdleStrategy`]. Latency benchmarks should keep busy-spin.

Need exact parity with Aeron's C/Java implementations (e.g. cross-language latency
comparisons)? [`CIdleStrategy`] wraps the C reference strategies behind the same trait:
`CIdleStrategy::backoff()?` / `busy_spinning()` / `yielding()` / `noop()`. The conductor's
own idle strategy is configured on the context — use the typed enum
(`ctx.set_idle_strategy_kind(AeronIdleStrategyKind::Backoff)?`, which also sets coherent
init args) — and likewise the media driver's per-agent strategies
(`set_conductor_idle_strategy_kind` etc. on `AeronDriverContext`).

For recording, replay, and **persistent subscriptions** (replay history, then seamlessly join a live stream), see [`rusteron-archive`](../rusteron-archive/README.md#persistent-subscriptions).

---

## Contributing & License

See the root [README](https://github.com/gsrxyz/rusteron#readme) and [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md).
Dual-licensed under MIT or Apache-2.0.

---

## Acknowledgments

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron - your work made this possible!
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for open protocol excellence
