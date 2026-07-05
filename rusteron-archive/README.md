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
rusteron-archive = { version = "0.2", features = ["static", "precompile"] }
```

If you prefer a rustls-only downloader dependency:

```toml
rusteron-archive = { version = "0.2", features = ["static", "precompile-rustls"] }
```

---

## Installation

Add **rusteron-archive** to your `Cargo.toml` depending on your setup:

```toml
# Dynamic linking (default)
rusteron-archive = "0.2"

# Static linking
rusteron-archive = { version = "0.2", features = ["static"] }

# Static linking with precompiled C libraries (best for Mac users, no Java/cmake needed)
rusteron-archive = { version = "0.2", features = ["static", "precompile"] }

# Static linking with precompiled C libraries using rustls downloader
rusteron-archive = { version = "0.2", features = ["static", "precompile-rustls"] }
```

When using the default dynamic configuration, you must ensure Aeron C libraries are available at runtime. The `static` option embeds them automatically into the binary.

---

## Development

Build tasks use [`just`](https://github.com/casey/just). Run `just` to list commands, or `cargo install just` if needed.

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

### Handlers and errors

Retained-callback setters take the callback by value (a closure or trait impl), keep it
alive inside the registering resource, and return the `Handler` for optional state access.
For synchronous polling, pass a stack closure:

```rust,ignore
// retained (e.g. an error handler on the archive context)
archive_context.set_error_handler(Some(|code: i32, msg: &str| eprintln!("archive error {code}: {msg}")))?;

// synchronous poll — note the fragment-limit argument
subscription.poll_fn(|buf: &[u8], header: AeronHeader| println!("{} bytes", buf.len()), 10)?;
```

`Handlers::NONE` fits any optional callback slot.

For comprehensive details on how handler registration, callbacks, error checking, and idle strategies work in the `rusteron` ecosystem (which are fully applicable here as well), please refer to the corresponding sections in the **rusteron-client** documentation:
- [rusteron-client: Handlers and Callbacks](../rusteron-client/README.md#handlers-and-callbacks)
- [rusteron-client: Errors & Offer Results](../rusteron-client/README.md#errors--offer-results)
- [rusteron-client: Idle Strategies](../rusteron-client/README.md#idle-strategies)

Archive control operations (`begin_replay`, `start_recording`, …) return
`Result<_, AeronArchiveError>` — a typed code (`AeronArchiveErrorCode`) plus the archive's
message. Constructors, async-connect, and context setters return `AeronCError`;
`From<AeronArchiveError> for AeronCError` keeps `?` working across both.

---

## Documentation & Guides

For detailed guides and code snippets on Aeron features in Rust, see:
- [Multi-Destination Subscription (MDC / MDS) Guide](../docs/mdc_mds_guide.md)
- [Media Driver Configuration & Back-pressure Guide](../docs/media_driver_configuration_and_backpressure_guide.md)

---

## Safety Considerations

1. **Aeron Lifetime** – The `AeronArchive` depends on an external `Aeron` instance. Ensure `Aeron` outlives all references to the archive.
2. **Unsafe Bindings** – The module interfaces directly with Aeron’s C API. Improper resource handling can cause undefined behavior.
3. **Automatic Handler Cleanup** – Handlers are reference-counted; registered callbacks live as long as the resource that registered them and are freed automatically.
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

// Drive it: replay runs, then it joins live. `ps.poll_fn()` drives the archive
// client internally, so no `archive.poll_for_recording_signals()` is needed. Check
// `has_failed()` each iteration (terminal failure) and stop once `is_live()`.
while !ps.is_live() {
    if ps.has_failed() {
        panic!("persistent subscription failed: {:?}", ps.get_failure_reason());
    }
    let _ = publication.offer_with_reserved_value(b"live", Handlers::NONE);
    ps.poll_fn(|buf, _hdr| { /* an assembled replayed or live message */ }, 100)?;
}
ps.close()?;
```

**Polling & errors.** `ps.poll_fn()` drives the PS state machine *and* the archive async client, so you do not call `archive.poll_for_recording_signals()` separately. Loop on `ps.is_live()`, checking `ps.has_failed()` each iteration (reason via `get_failure_reason()`). The listener's `on_error` covers non-terminal errors; `on_live_left`/`on_live_joined` may fire repeatedly as it falls back and rejoins.

**Fragment assembly.** `poll_fn` delivers **raw fragments**; messages larger than the MTU arrive in pieces. Reassemble with `AeronFragmentClosureAssembler` (or `AeronControlledFragmentClosureAssembler` for flow-controlled polling) — both work with a persistent subscription the same way as with `AeronSubscription`:

```rust,ignore
let mut assembler = AeronFragmentClosureAssembler::new()?;
let mut ctx = Collector::default();
loop {
    ps.poll_fn(|_buf, _hdr| {}, 10)?;          // advance the PS state machine
    assembler.poll(&ps, &mut ctx, Collector::on_msg, 100)?;  // reassembled messages only
    if ctx.done { break; }
}
```

For a fully runnable version, see the example and integration tests:
- [`examples/persistent_subscription.rs`](./examples/persistent_subscription.rs) — standalone demo (run with `cargo run --release --features "static precompile" --example persistent_subscription`)
- [`examples/archive_error_handling.rs`](./examples/archive_error_handling.rs) — error handlers on both contexts, recording signals, typed control-session errors via `archive.poll_for_error()` / `AeronArchiveError::parse` (the archive's `errorCode=N` recovered from the message text), and detecting/reconnecting after the archive goes down
- [`examples/persistent_subscription_failover.rs`](./examples/persistent_subscription_failover.rs) — failure modes: live stream dies → automatic fallback to replay (`on_live_left`), then rejoins live when it returns
- [`examples/replay_merge.rs`](./examples/replay_merge.rs) — late-joiner catch-up: replay recorded history, then merge seamlessly onto the live MDC stream (`AeronArchiveReplayMerge`)
- [`examples/recording_throughput.rs`](./examples/recording_throughput.rs) — recording throughput measurement (publish rate vs archiver catch-up) and `list_recordings` descriptor enumeration
- [`examples/recording_replication.rs`](./examples/recording_replication.rs) — archive-to-archive replication (`archive.replicate`): a destination archive pulls a finished recording from a source archive and the copy is verified (port of `RecordingReplicator`)
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
