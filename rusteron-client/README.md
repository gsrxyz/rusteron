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
- **Automatic Cleanup (Partial)**: When possible, `Drop` will invoke the appropriate `*_close` or `*_destroy` methods.
- **Manual Resource Responsibility**: For methods like `set_aeron()` or where lifetimes aren't managed internally, users are responsible for safety.
- **Handlers Must Be Leaked and Released**: Callbacks passed to the C layer require explicit memory management using `Handlers::leak(...)` and `Handlers::release(...)`.

---

## Handlers and Callbacks

Handlers allow users to customize responses to Aeron events (errors, image availability, etc). There are two ways to use them:

### 1. Implementing a Trait (Recommended)

This is the most performant and idiomatic approach for long-lived handlers (e.g. an error handler installed on the context).

```rust,no_ignore
use rusteron_client::*;

pub struct MyErrorHandler;

impl AeronErrorHandlerCallback for MyErrorHandler {
    fn handle_aeron_error_handler(&mut self, code: i32, msg: &str) {
        eprintln!("Aeron error ({}): {}", code, msg);
    }
}
```

Wrap it with `Handler::leak(...)` to pass it into the C layer, and call `release()` once it's no longer needed:

```rust,ignore
let handler = Handler::leak(MyErrorHandler);
ctx.set_error_handler(Some(&handler))?;
// ...when done:
handler.release();
```

### 2. Short-lived Polls: Closures

For one-off message consumption, skip the trait and pass a closure directly to `poll_once` — no leak/release bookkeeping needed:

```rust,ignore
subscription.poll_once(
    |buf: &[u8], header: AeronHeader| {
        println!("received {} bytes at position {:?}", buf.len(), header.position());
    },
    10,
)?;
```

> For messages larger than the MTU that need reassembling, use a fragment assembler with `subscription.poll(Some(&handler), limit)` — `poll_once` delivers raw fragments and does not reassemble.

No handler needed for an optional slot? Use the typed `None` helpers, e.g. `Handlers::no_error_handler_handler()`.

---

## Minimal Pub/Sub

```rust,ignore
use rusteron_client::*;
use std::time::Duration;

let ctx = AeronContext::new()?;
// Reuse the built-in logger for async client errors (Aeron samples always set one).
let mut error_handler = Handler::leak(AeronErrorHandlerLogger);
ctx.set_error_handler(Some(&error_handler))?;
let aeron = Aeron::new(&ctx)?;
aeron.start()?;

let channel = &"aeron:ipc".into_c_string();
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
while publication.offer(b"hello", Handlers::no_reserved_value_supplier_handler()) <= 0 {}
subscription.poll_once(|buf: &[u8], _hdr: AeronHeader| println!("got {} bytes", buf.len()), 10)?;
error_handler.release();
```

## Errors & offer results

- **Client errors**: install an error handler on the context (`ctx.set_error_handler(Some(&handler))`) so async errors aren't silently lost — Aeron's samples always do. Leaked handlers must be `release()`d.
- **`offer()` / `try_claim()` results**: a positive value is the resulting log position; the negatives are classified by the `PUBLICATION_*` constants. `PUBLICATION_CLOSED`, `PUBLICATION_MAX_POSITION_EXCEEDED`, and `PUBLICATION_ERROR` are **fatal** (stop offering); `PUBLICATION_BACK_PRESSURED`, `PUBLICATION_NOT_CONNECTED`, and `PUBLICATION_ADMIN_ACTION` are **transient** (retry, ideally with an idle strategy). This mirrors Aeron's `BasicPublisher.checkResult`.
  ```rust,ignore
  let r = publication.offer(msg, Handlers::no_reserved_value_supplier_handler());
  if r == PUBLICATION_CLOSED || r == PUBLICATION_MAX_POSITION_EXCEEDED {
      return Err("publication gone".into());
  }
  ```
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
