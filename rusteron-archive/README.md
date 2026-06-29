# rusteron-archive

**rusteron-archive** is a module within the **[rusteron](https://github.com/gsrxyz/rusteron)** project that provides functionality for interacting with Aeron's archive system in a Rust environment. This module builds on **rusteron-client**, adding support for recording, managing, and replaying archived streams.

---

## Sponsored by GSR

**Rusteron** is proudly sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It powers mission-critical infrastructure in GSR's real-time trading stack and is now developed under the official GSR GitHub organization as part of our commitment to open-source excellence and community collaboration.

We welcome contributions, feedback, and discussions. If you're interested in integrating or contributing, please open an issue or reach out directly.

---

## Overview

The **rusteron-archive** module enables Rust developers to leverage Aeron's archive functionality, including recording and replaying messages with minimal friction.

For **MacOS users**, the easiest way to get started is by using the static library with precompiled C dependencies. This avoids the need for `cmake` or `Java`:

```toml
rusteron-archive = { version = "0.1", features = ["static", "precompile"] }
````

If you prefer a rustls-only downloader dependency:

```toml
rusteron-archive = { version = "0.1", features = ["static", "precompile-rustls"] }
````

---

## Installation

Add **rusteron-archive** to your `Cargo.toml` depending on your setup:

```toml
# Dynamic linking (default)
rusteron-archive = "0.1"

# Static linking
rusteron-archive = { version = "0.1", features = ["static"] }

# Static linking with precompiled C libraries (best for Mac users, no Java/cmake needed)
rusteron-archive = { version = "0.1", features = ["static", "precompile"] }

# Static linking with precompiled C libraries using rustls downloader
rusteron-archive = { version = "0.1", features = ["static", "precompile-rustls"] }
```

When using the default dynamic configuration, you must ensure Aeron C libraries are available at runtime. The `static` option embeds them automatically into the binary.

---

## Development

To simplify development, we use [`just`](https://github.com/casey/just), a command runner similar to `make`.

To view all available commands, run `just` in the command line.

> If you don’t have `just` installed, install it with: `cargo install just`

---

## Features

* **Stream Recording** – Record Aeron streams for replay or archival.
* **Replay Handling** – Replay previously recorded messages.
* **Persistent Subscriptions** – Replay recorded history, then seamlessly join the live stream (Aeron Archive 1.51.0). See [below](#persistent-subscriptions).
* **Publication/Subscription** – Publish to and subscribe from Aeron channels.
* **Callbacks** – Receive events such as new publications, subscriptions, and errors.
* **Automatic Resource Management** (via `new()` only) – Constructors automatically call `*_init` and clean up with `*_close` or `*_destroy` when dropped.
* **String Handling** – `new()` and setter methods accept `&CStr`; getter methods return `&str`.

---

## General Patterns

### Cloneable Wrappers

All wrapper types in **rusteron-archive** implement `Clone` and share the same underlying Aeron C resource. For shallow copies of raw structs, use `.clone_struct()`.

### Mutable and Immutable APIs

Most methods use `&self`, allowing mutation without full ownership transfer.

### Resource Management Caveats

Automatic cleanup applies **only** to `new()` constructors. Other methods (e.g. `set_aeron()`) require manual lifetime and validity tracking to prevent resource misuse.

### Manual Handler Management

Handlers must be passed into C bindings using `Handlers::leak(...)` and explicitly cleaned up using `release()` when no longer needed.

For short-lived operations such as polling, closures can be used directly:

```rust,ignore
subscription.poll_once(|msg, header| {
    println!("msg={:?}, header={:?}", msg, header)
});
```

---

## Handlers and Callbacks

There are two primary patterns for defining callbacks:

### 1. Trait-Based Handlers (Recommended)

The preferred and most efficient approach is to define a trait and implement it for a struct:

```rust,no_ignore
use rusteron_archive::*;

pub trait AeronErrorHandlerCallback {
    fn handle_aeron_error_handler(&mut self, errcode: ::std::os::raw::c_int, message: &str);
}

pub struct AeronErrorHandlerLogger;

impl AeronErrorHandlerCallback for AeronErrorHandlerLogger {
    fn handle_aeron_error_handler(&mut self, errcode: ::std::os::raw::c_int, message: &str) {
        eprintln!("Error {}: {}", errcode, message);
    }
}
```

You then wrap the implementation in a handler using `Handlers::leak(...)`.

### 2. Wrapping Callbacks with `Handler`

Regardless of approach, callbacks must be wrapped in a `Handler` to interact with Aeron's C bindings. Use `Handlers::leak(...)` to pass it into the system, and call `release()` once cleanup is needed.

---

### Handler Convenience Methods

You can pass `None` if a handler isn't required, but dealing with typed `Option`s can be awkward. **rusteron-archive** offers helpers like:

```rust,ignore
pub fn no_error_handler_handler() -> Option<&'static Handler<AeronErrorHandlerLogger>> {
    None::<&Handler<AeronErrorHandlerLogger>>
}
```

These helpers return `None` with the correct generic type to reduce boilerplate.

---

## Error Handling with Aeron C Bindings

Operations in **rusteron-archive** return `Result<i32, AeronCError>`, using idiomatic Rust error types.

### AeronErrorType Enum

| Variant                              | Description                   |
| ------------------------------------ | ----------------------------- |
| `NullOrNotConnected`                 | Null value or unconnected     |
| `ClientErrorDriverTimeout`           | Driver timed out              |
| `ClientErrorClientTimeout`           | Client timed out              |
| `ClientErrorConductorServiceTimeout` | Conductor service timeout     |
| `ClientErrorBufferFull`              | Buffer full                   |
| `PublicationBackPressured`           | Publication is back-pressured |
| `PublicationAdminAction`             | Admin action in progress      |
| `PublicationClosed`                  | Publication has closed        |
| `PublicationMaxPositionExceeded`     | Max position exceeded         |
| `PublicationError`                   | Generic publication error     |
| `TimedOut`                           | Timeout occurred              |
| `Unknown(i32)`                       | Unrecognized error code       |

The `AeronCError` struct exposes these enums alongside descriptive messages.

---

## Safety Considerations

1. **Aeron Lifetime** – The `AeronArchive` depends on an external `Aeron` instance. Ensure `Aeron` outlives all references to the archive.
2. **Unsafe Bindings** – The module interfaces directly with Aeron’s C API. Improper resource handling can cause undefined behavior.
3. **Manual Cleanup** – Handlers and other leaked objects must be manually cleaned up using `.release()`.
4. **Thread Safety** – Use care when accessing Aeron objects across threads. Synchronize access appropriately.

---

## Typical Workflow

1. **Initialize** client and archive contexts.
2. **Start Recording** a specific channel and stream.
3. **Publish Messages** to the stream.
4. **Stop Recording** once complete.
5. **Locate the Recording** using archive queries.
6. **Replay Setup**: Configure replay target/channel.
7. **Subscribe and Receive** replayed messages.

---

## Persistent Subscriptions

A **persistent subscription** replays a recording from a start position, then seamlessly merges into the live stream — so a consumer catches up on history without missing new messages and without a gap at the handover. Introduced in Aeron Archive 1.51.0.

- **What it is**: [Aeron — Persistent Subscriptions (replay-to-live)](https://aeron.io/software-release/persistent-subscriptions-replay-to-live-transitions/)
- **How it works**: [Aeron Wiki — Persistent Subscriptions](https://github.com/aeron-io/aeron/wiki/Persistent-Subscriptions)
- **Background on publications/subscriptions**: [Aeron docs](https://aeron.io/docs/aeron/publications-subscriptions/)

Rusteron exposes it via `persistent_subscription_builder()` and the `PersistentSubscriptionListener` trait — a 1:1 wrapper over the Aeron C API (`aeron_archive_persistent_subscription_*`), mirroring Aeron's `PersistentSubscription.Context` field-for-field.

```rust,ignore
use rusteron_archive::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// `archive` is a connected AeronArchive; record + publish history first, then resolve recording_id.
let live_channel = "aeron:ipc";
let stream_id = 1001;

struct MyListener { live_joined: Arc<AtomicUsize> }
impl PersistentSubscriptionListener for MyListener {
    fn on_live_joined(&self) { self.live_joined.fetch_add(1, Ordering::SeqCst); }
    fn on_live_left(&self)  { /* fell back to replay */ }
    fn on_error(&self, code: i32, msg: &str) { eprintln!("ps error {code}: {msg}"); }
}
let live_joined = Arc::new(AtomicUsize::new(0));

let ps = persistent_subscription_builder()?
    .aeron(&aeron)?
    .archive_context(&archive_context)?
    .live_channel(live_channel)?        // the live stream to join
    .live_stream_id(stream_id)?
    .replay_channel("aeron:udp?endpoint=localhost:0")?  // scratch channel for the replay
    .replay_stream_id(stream_id + 1)?
    .start_from_beginning()?            // replay from the start (or .start_from_live())
    .recording_id(recording_id)?        // which recording to replay
    .listener(MyListener { live_joined: live_joined.clone() })?
    .build()?;

// Drive it: replay runs, then it joins live. `ps.poll_once()` drives the archive
// client internally, so no `archive.poll_for_recording_signals()` is needed. Check
// `has_failed()` each iteration (terminal failure) and stop once `is_live()`.
while !ps.is_live() {
    if ps.has_failed() {
        panic!("persistent subscription failed: {:?}", ps.get_failure_reason());
    }
    let _ = publication.offer(b"live", Handlers::no_reserved_value_supplier_handler());
    ps.poll_once(|buf, _hdr| { /* an assembled replayed or live message */ }, 100)?;
}
ps.close()?;
```

**Polling & errors.** `ps.poll_once()` drives the PS state machine *and* the archive async client internally, so — unlike a manual replay loop — you do **not** call `archive.poll_for_recording_signals()` here. Drive the loop on `ps.is_live()` and check `ps.has_failed()` each iteration (read the reason with `ps.get_failure_reason()`); the listener's `on_error` fires for non-terminal errors too, and `on_live_left`/`on_live_joined` can fire repeatedly as it falls back to replay and rejoins. The poll handler receives **assembled** messages — do not wrap it in a fragment assembler.

For a fully runnable version, see the example and integration tests:
- [`examples/persistent_subscription.rs`](./examples/persistent_subscription.rs) — standalone demo (run with `cargo run --release --features "static precompile" --example persistent_subscription`)
- `persistent_subscription_tests::test_persistent_subscription_listener_live_joined` (callback wiring)
- `persistent_subscription_integration::test_end_to_end_persistent_subscription` (record → replay → live)

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
