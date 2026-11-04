# rusteron-client

`rusteron-client` is a core component of the [Rusteron](https://github.com/gsrxyz/rusteron) project.
It provides a Rust wrapper around the Aeron C client API for low-latency communication in distributed systems built with Rust.

This crate supports publishing, subscribing and managing Aeron resources through an idiomatic interface over `unsafe` C bindings.
Because it relies on raw FFI, you must manage resource lifetimes and concurrency correctly.

---

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It runs in GSR's real-time trading stack and is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

---

## Features

- Create and start an Aeron client from Rust.
- Send messages with `offer()`, `offer_parts()` (gathered) or zero-copy `try_claim_owned()`.
- Poll for incoming messages and handle fragments.
- React to driver events such as availability, errors and stream lifecycle changes.
- All client types are cloneable and share ownership of the underlying C resources.
- Objects created with `.new()` call `*_init` and `*_close` automatically, where supported.
- Methods that return primitive C results return `Result<T, AeronCError>`.
- Inputs use `&CStr` and outputs return `&str`, so you control allocation.

---

## Installation

Add rusteron-client to your `Cargo.toml`:

```toml
# Dynamic linking (default)
rusteron-client = "0.2"

# Static linking
rusteron-client = { version = "0.2", features = ["static"] }

# Static linking with precompiled C libraries (no Java/cmake needed; macOS and Linux)
rusteron-client = { version = "0.2", features = ["static", "precompile"] }

# Static linking with precompiled C libraries using rustls downloader
rusteron-client = { version = "0.2", features = ["static", "precompile-rustls"] }
```

With the default dynamic configuration, the Aeron C libraries must be available at runtime. The `static` option embeds them in the binary.

---

## General patterns

- `new()` calls the corresponding `*_init` method.
- `Aeron::connect(Some(dir))?` builds the context and client and starts the conductor. Build the `AeronContext` yourself for tuned setups. rusteron-archive has `AeronArchive::connect(...)`.
- `subscription.image_at_index(i)` and `image_by_session_id(id)` return `Option<AeronImage>`, which releases back to the subscription on drop. `for_each_image(|img| …)` borrows without bookkeeping. Drop these handles before calling `subscription.close()`, because a handle must not be used after the close and its image may stay mapped until the client closes. See [`examples/retained_images.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/retained_images.rs).
- Cleanup is partial. Where possible, `Drop` calls the appropriate `*_close` or `*_destroy` method.
- For methods like `set_aeron()`, or where lifetimes are not managed internally, you are responsible for safety.
- Handlers are reference-counted. Wrap callbacks with `Handler::new(...)`. Methods that register a callback the C client retains keep a clone alive inside the registering resource, so the value is freed automatically and you never call `release()`.

---

## Handlers and callbacks

Two kinds of callbacks:

### 1. Retained (error handler, image/counter lifecycle, notifications)

The C client stores these and invokes them later from the conductor thread. Setters take the
callback by value (a closure or any `impl Trait`), heap-allocate it into a reference-counted
`Handler`, and keep a clone inside the registering resource. It is freed when that resource
drops, so there is no manual `release()`:

```rust,no_run
# use rusteron_client::*;
# fn snippet(ctx: &AeronContext) -> Result<(), AeronCError> {
ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
# Ok(()) }
```

Reading state through the returned `Handler` is only safe when the callback runs on the
reading thread (an agent-invoker client, or synchronous callbacks); otherwise share it
explicitly:

```rust,no_run
# use rusteron_client::*;
# fn snippet(ctx: &AeronContext) -> Result<(), AeronCError> {
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// The conductor thread runs this callback, so share state through an Arc rather than reading the Handler.
let errors = Arc::new(AtomicUsize::new(0));
let counted = errors.clone();
ctx.set_error_handler(Some(move |_code: i32, _msg: &str| {
    counted.fetch_add(1, Ordering::Relaxed);
}))?;
// ... later, from the application thread
println!("{} errors", errors.load(Ordering::Relaxed));
# Ok(()) }
```

Registration methods that return a resource (e.g. `add_subscription` with image handlers) take
`Option<&Handler<T>>` and clone it into the resource, with the same lifetime guarantee.

### Callback arguments are borrowed

Wrapper arguments (`&AeronHeader`, `&AeronImage`, `&AeronArchiveRecordingDescriptor`, ...) are
borrowed for the call only. Aeron may free or reuse the struct once the callback returns, so the
compiler stops you keeping them. Copy out the fields you need, e.g. `d.recording_id` or
`d.original_channel().to_owned()`. Structs only ever handed to callbacks (header, recording
descriptors and signal, publication error values) do not implement `Clone` or `From` a raw pointer
or struct value. Their `unsafe fn from_ptr` and `unsafe fn clone_reference` cover the rare case
where you need a second handle and can promise it is not used after the callback. `clone_struct`
copies the struct, and is `unsafe` when it has pointer fields such as strings, as those still point
at the original memory.

### 2. Synchronous (`poll`, `controlled_poll`, log readers)

They are invoked only during the call, so they take a stack closure. This allocates nothing and may borrow
local state:

```rust,no_run
# use rusteron_client::*;
# fn snippet(subscription: &AeronSubscription) -> Result<(), AeronCError> {
subscription.poll_fn(|buf: &[u8], header: &AeronHeader| {
    println!("received {} bytes", buf.len());
}, 10)?;
# Ok(()) }
```

For messages larger than the MTU, wrap the delegate in an `AeronFragmentAssembler`, which
reassembles fragments before calling back. `poll_fn` delivers raw fragments
only. `Handler::with_fragment_assembler` builds the reference-counted pair (it replaces the
deprecated `Handler::leak_with_fragment_assembler`):

```rust,no_run
# use rusteron_client::AeronSubscription;
# fn snippet(subscription: &AeronSubscription) -> Result<(), Box<dyn std::error::Error>> {
use rusteron_client::{AeronFragmentHandlerCallback, AeronHeader, Handler};

struct OnMessage { bytes: u64 }
impl AeronFragmentHandlerCallback for OnMessage {
    fn handle_aeron_fragment_handler(&mut self, msg: &[u8], _header: &AeronHeader) {
        self.bytes += msg.len() as u64; // msg is the whole reassembled message
    }
}

// The assembler keeps the delegate alive; keep `on_message` to read its state on this thread.
let (assembler, on_message) = Handler::with_fragment_assembler(OnMessage { bytes: 0 })?;
let fragments = subscription.poll(Some(&assembler), 10)?;
println!("{} bytes so far", on_message.bytes);
# Ok(()) }
```

To pass state through a stack context instead, use `AeronFragmentClosureAssembler`; its `poll` borrows a `&mut T`
context for the call (the callback is a `fn` pointer, not a closure, so pass state through
the context):

```rust,no_run
# use rusteron_client::AeronSubscription;
# fn snippet(subscription: AeronSubscription) -> Result<(), Box<dyn std::error::Error>> {
use rusteron_client::{AeronFragmentClosureAssembler, AeronHeader};

struct Stats { bytes: u64 }
fn on_msg(stats: &mut Stats, buf: &[u8], _hdr: &AeronHeader) { stats.bytes += buf.len() as u64; }

let mut assembler = AeronFragmentClosureAssembler::new()?;
let mut stats = Stats { bytes: 0 };
assembler.poll(&subscription, &mut stats, on_msg, 10)?; // 10 = fragment limit
// stats.bytes now holds the reassembled payload sizes
# Ok(()) }
```

Pass `Handlers::NONE` for any optional callback parameter you do not need.

---

## Building channel URIs

Prefer the typed [`AeronUriStringBuilder`] over hand-written URI strings. Parameters are
typed setters, so misspelled keys and malformed values are caught before they reach the
driver:

```rust,no_run
# use rusteron_client::*;
# fn snippet(id: i64) -> Result<(), AeronCError> {
let channel = AeronUriStringBuilder::udp("localhost:20121")?.build(256)?;
let mds_sub = AeronUriStringBuilder::udp_control("localhost:9998", ControlMode::Manual)?.build(256)?;
let response = AeronUriStringBuilder::udp_control("localhost:9999", ControlMode::Response)?
    .response_correlation_id(id)?
    .build(256)?;
# Ok(()) }
```

Constructors are `ipc()`, `udp(endpoint)` and `udp_control(control, ControlMode)`. There are also ~50 typed
setters (`session_id`, `mtu_length`, `term_length`, `fc`, `gtag`, `reliable`, …). See
`examples/multi_destination_subscription.rs` and `examples/request_response.rs`.

## Minimal pub/sub

```rust,no_run
# fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        Handlers::NONE,
        Handlers::NONE,
    )?
    .poll_blocking(Duration::from_secs(5))?;

// offer() returns Ok(position) or a typed AeronOfferError, see "Errors & offer results".
let mut idle = BackoffIdleStrategy::new();
loop {
    match publication.offer(b"hello") {
        Ok(_) => break,
        Err(e) if e.is_retryable() => idle.idle(0),
        Err(e) => return Err(e.into()),
    }
}
// The image can appear just after the offer succeeds, so poll until the message arrives.
let mut received = false;
while !received {
    let fragments = subscription.poll_fn(|buf: &[u8], _hdr: &AeronHeader| {
        println!("got {} bytes", buf.len());
        received = true;
    }, 10)?;
    idle.idle(fragments);
}
# Ok(()) }
```

> `poll_blocking` and `add_*(.., timeout)` block the calling thread in a
> busy-poll loop and exist for example and test brevity. In production, drive the async poller's
> `poll()` from your own event loop instead. See
> [`examples/non_blocking_publisher.rs`](examples/non_blocking_publisher.rs) for the idiomatic
> pattern.

## AddressSanitizer

The `sanitize-address` feature compiles the Aeron C sources with `-fsanitize=address`
(implies `build-from-source`), so use-after-free / double-free across the FFI boundary are
caught in your tests:

```bash
# macOS: stable rustc; Apple's ASan runtime is linked automatically, it just needs to be
# on the loader path:
export DYLD_LIBRARY_PATH="$(clang -print-resource-dir)/lib/darwin"
ASAN_OPTIONS=detect_leaks=0 cargo test --features rusteron-client/sanitize-address

# Linux: nightly, additionally instruments the Rust side:
RUSTFLAGS="-Zsanitizer=address" cargo +nightly test -Zbuild-std \
  --target x86_64-unknown-linux-gnu --features rusteron-client/sanitize-address
```

Leak detection is noisy with a media driver in-process, so keep `detect_leaks=0`.
Other sanitizers (`thread`, `undefined`) can be selected with the `RUSTERON_SANITIZE`
env var instead of the feature. In this repo, `just test-asan` runs the client suite this way.

## Resource lifecycle & close()

The C client owns every child resource (publications, subscriptions, counters) and frees them
all in `aeron_close`. rusteron therefore reference-counts teardown:

- `aeron.close()` (and `drop`) is deferred while any child handle or clone is alive. The
  surviving handles remain usable, and the C close runs when the last reference releases.
- Leaf resources (`publication.close()` etc.) close immediately; clones of that handle see a
  nulled pointer and become inert.
- `unsafe aeron.close_now()` forces the C close immediately. Every surviving child handle then
  dangles, and even its `Drop` is a double free, so `mem::forget` survivors or never return.

Which structures get which behaviour:

| Behaviour | Types |
|---|---|
| Deferred `close()` + `unsafe close_now()` | `Aeron`, `AeronArchive` (rusteron-archive) |
| Immediate `close(self)` (+ `close_with_handler` for a close-complete notification); clones become inert | `AeronPublication`, `AeronExclusivePublication`, `AeronSubscription`, `AeronCounter` |
| No public close. Freed when the last reference drops (the client holds an internal clone, so they always outlive it) | `AeronContext`, `AeronArchiveContext`, `AeronDriverContext` |
| No close at all. The C client frees them when their `poll()` completes (created, errored or cancelled) | `AeronAsyncAdd*` pollers, `AeronAsyncDestination` |

Ordering is structurally safe in any combination. Children hold references to the client, the
client holds its context, and closing or dropping in any order defers the C teardown until the
last handle releases (verified by the `complex_object_graph_close_is_safe_in_any_order` test).
This covers owned handles. Two exceptions:

- Borrowed handles (wrapping a pointer C gave out, e.g. `AeronArchiveContext::get_aeron()`) hold
  no reference, so they must not outlive their owner.
- A buffer claim from `try_claim` / `try_claim_owned` points into the publication's log buffer
  and must be committed or aborted before the publication closes.

## Migrating 0.1 → 0.2

Breaking changes, made because the old design allowed double frees and use-after-free
(see [PR #50](https://github.com/gsrxyz/rusteron/pull/50)):

| 0.1 | 0.2 |
|---|---|
| `Handler::leak(h)` + manual `release()` | `Handler::new(h)`; freed automatically (`Arc`-counted, resources keep clones) |
| `ctx.set_error_handler(Some(&handler))` | `ctx.set_error_handler(Some(h))`, which takes the value (closures work) and returns the `Handler` |
| `set_error_handler_fn(closure)` (UB: C retained a stack closure) | removed; `_once` now exists only for synchronous callbacks |
| `aeron.close()` freed children immediately (UAF on surviving handles) | deferred until the last reference drops; `unsafe close_now()` is the escape hatch |
| `Handler` was `Sync` | `Send` only (conductor thread may invoke callbacks) |
| `offer(buf, supplier)` returned a raw `i64` sentinel | `offer(buf)` → `Result<i64, AeronOfferError>` with `is_retryable()`. The supplier form is `offer_with_reserved_value` and the raw sentinel form is `offer_raw` |
| `offer_result` / `offer_result_simple` / `try_claim_result` (→ `AeronCError`) | `offer_with_reserved_value` / `offer` / `try_claim` (→ typed `AeronOfferError`) |
| `poll_once(f, limit)` / `for_each_fragment(limit, f)` | `poll_fn(f, limit)` (`for_each_fragment` removed) |
| `Handlers::no_xxx_handler()` per callback | `Handlers::NONE` for any callback parameter |

The full old → new table (destinations, fragment assembler, C strings, driver guard) lives
in the [migration guide](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.1.168_to_0.2.md).

## Examples

- [`examples/basic_publisher.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/basic_publisher.rs): typed-offer retry loop (port of `basic_publisher.c`)
- [`examples/basic_subscriber.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/basic_subscriber.rs): image lifecycle handlers and the zero-allocation `poll_fn` (port of `basic_subscriber.c`)
- [`examples/non_blocking_publisher.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/non_blocking_publisher.rs): drives `async_add_publication(..).poll()` from its own loop instead of `poll_blocking`
- [`examples/basic_pub_sub.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/basic_pub_sub.rs): minimal pub/sub with fragment assembly
- [`examples/streaming_rate.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/streaming_rate.rs): streaming publisher + rate-reporting subscriber (port of `streaming_publisher.c` + `rate_subscriber.c`)
- [`examples/multi_destination_subscription.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/multi_destination_subscription.rs): MDS with one manual-control subscription aggregating several endpoints (port of `basic_mds_subscriber.c`)
- [`examples/driver_stats.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/driver_stats.rs): CnC tooling covering counters, the distinct error log and the loss report (ports of `aeron_stat.c` / `error_stat.c` / `loss_stat.c`)
- [`examples/embedded_ping_pong.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/embedded_ping_pong.rs): RTT ping/pong with `try_claim` (port of `cping`/`cpong`); starts its own driver unless `AERON_DIR` is set
- [`examples/embedded_exclusive_ipc_throughput.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/embedded_exclusive_ipc_throughput.rs): exclusive-publication IPC throughput; starts its own driver unless `AERON_DIR` is set
- [`examples/request_response.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/request_response.rs): response channels (aeron 1.44+), with request/response wiring via `control-mode=response` + `response-correlation-id` (port of `response_server.c`/`response_client.c`)
- [`examples/file_transfer.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/file_transfer.rs): chunked file transfer with fragment reassembly and verification (port of `FileSender`/`FileReceiver`)
- [`examples/zero_copy_claim.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/zero_copy_claim.rs): zero-copy publish with `try_claim_owned` (commit, abort on drop), a send timestamp in `set_reserved_value`, and header reads on the subscriber
- [`examples/retained_images.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/retained_images.rs): retained image lifecycle, covering `for_each_image`, `image_by_session_id`, polling the image directly, `is_closed()` once the publisher goes, and dropping the handle before `subscription.close()`

---

## Documentation & guides

For detailed guides and code snippets on Aeron features in Rust, see:
- [Multi-Destination Subscription (MDC / MDS) Guide](https://github.com/gsrxyz/rusteron/blob/main/docs/mdc_mds_guide.md)
- [Media Driver Configuration & Back-pressure Guide](https://github.com/gsrxyz/rusteron/blob/main/docs/media_driver_configuration_and_backpressure_guide.md)

---

## Errors & offer results

- For client errors, install an error handler on the context (`ctx.set_error_handler(Some(handler))`) so async errors are not silently lost. Aeron's samples always do.
- `offer()` and `try_claim()` return `Result<i64, AeronOfferError>`. `Ok` is the new log position. The error is a typed sentinel, and `is_retryable()` separates retryable errors (back-pressured, admin action, not connected) from fatal ones (closed, max position exceeded). Retry the first kind, ideally with an idle strategy. This mirrors Aeron's `BasicPublisher.checkResult` without magic numbers:
  ```rust,no_run
  # use rusteron_client::*;
  # fn snippet(publication: &AeronPublication, msg: &[u8], idle: &mut BackoffIdleStrategy) -> Result<(), Box<dyn std::error::Error>> {
  match publication.offer(msg) {
      Ok(_) => {}
      Err(e) if e.is_retryable() => idle.idle(0), // no work done: back off, then retry
      Err(e) => return Err(e.into()),             // publication gone
  }
  # Ok(()) }
  ```
  Zero-copy `try_claim_owned()` returns an RAII `AeronClaim`; an uncommitted claim aborts on drop:
  ```rust,no_run
  # use rusteron_client::*;
  # fn snippet(publication: &AeronPublication, msg: &[u8], idle: &mut BackoffIdleStrategy) -> Result<(), Box<dyn std::error::Error>> {
  match publication.try_claim_owned(msg.len()) {
      Ok(mut claim) => {
          claim.data().copy_from_slice(msg);
          claim.set_reserved_value(Aeron::nano_clock()); // optional; read with header.reserved_value()
          claim.commit()?;
      }
      Err(e) if e.is_retryable() => idle.idle(0),
      Err(e) => return Err(e.into()),
  }
  # Ok(()) }
  ```
  For branch-free hot paths, `offer_raw()` / `try_claim_raw()` return the raw `i64` sentinel.
  The typed path costs the same on the happy path. The error enum only materialises on the
  error path.
- For image handlers, `Handlers::NONE` for each image slot is a fine default, but real apps usually react to image availability (logging, synchronisation). Aeron's `Ping` sample uses one as a latch.

## Idle strategies

Poll loops should back off when a cycle does no work. Rusteron ports Aeron's `IdleStrategy`
(`idle(work_count)` returns immediately when work was done, otherwise backs off):

```rust,no_run
# use rusteron_client::{AeronFragmentHandlerCallback, AeronSubscription, AeronCError, Handler};
# fn snippet<H: AeronFragmentHandlerCallback>(subscription: &AeronSubscription, handler: Handler<H>) -> Result<(), AeronCError> {
use rusteron_client::{BackoffIdleStrategy, IdleStrategy};

let mut idle = BackoffIdleStrategy::new(); // spin → yield → sleep, Aeron's default
loop {
    let fragments = subscription.poll(Some(&handler), 10)?;
    if fragments == 0 { /* break when done */ }
    idle.idle(fragments);
}
# }
```

Available strategies are [`BusySpinIdleStrategy`] (lowest latency, pins a core), [`YieldingIdleStrategy`],
[`SleepingIdleStrategy`] (fixed sleep), [`BackoffIdleStrategy`] (adaptive, general-purpose),
[`NoOpIdleStrategy`]. Latency benchmarks should keep busy-spin.

`BackoffIdleStrategy` is the default and matches Aeron's C/Java backoff (10 spins,
20 yields, park 1µs..1ms), so there is no need for a C-backed idle strategy to get parity.
The conductor's own idle strategy is set on the context. Use the typed enum
(`ctx.set_idle_strategy_kind(AeronIdleStrategyKind::Backoff)?`, which also sets coherent
init args). The media driver's per-agent strategies work the same way
(`set_conductor_idle_strategy_kind` etc. on `AeronDriverContext`).

For recording, replay and persistent subscriptions (replay history, then join a live stream), see [`rusteron-archive`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/README.md#persistent-subscriptions).

---

## Contributing & license

See the root [README](https://github.com/gsrxyz/rusteron#readme) and [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md).
Dual-licensed under MIT or Apache-2.0.

---

## Acknowledgments

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron. Your work made this possible.
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for the open protocol
