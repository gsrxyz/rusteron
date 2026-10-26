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

Automatic cleanup applies **only** to `new()` constructors. Setters that take another wrapper (e.g. `set_aeron()`) keep it alive for as long as the context.

### Handlers and errors

Retained-callback setters take the callback by value (a closure or trait impl), keep it
alive inside the registering resource, and return the `Handler` for optional state access.
For synchronous polling, pass a stack closure:

```rust,no_run
# use rusteron_archive::*;
# fn snippet(archive_context: &AeronArchiveContext, subscription: &AeronSubscription) -> Result<(), AeronCError> {
// retained (e.g. an error handler on the archive context)
archive_context.set_error_handler(Some(|code: i32, msg: &str| eprintln!("archive error {code}: {msg}")))?;

// synchronous poll — note the fragment-limit argument
subscription.poll_fn(|buf: &[u8], header: AeronHeader| println!("{} bytes", buf.len()), 10)?;
# Ok(()) }
```

`Handlers::NONE` fits any optional callback slot.

For comprehensive details on how handler registration, callbacks, error checking, and idle strategies work in the `rusteron` ecosystem (which are fully applicable here as well), please refer to the corresponding sections in the **rusteron-client** documentation:
- [rusteron-client: Handlers and Callbacks](../rusteron-client/README.md#handlers-and-callbacks)
- [rusteron-client: Errors & Offer Results](../rusteron-client/README.md#errors--offer-results)
- [rusteron-client: Idle Strategies](../rusteron-client/README.md#idle-strategies)

Archive control operations (`start_recording`, `start_replay`, `stop_recording_subscription`, …) return
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

1. **Aeron Lifetime** – Connect with `AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)`: the archive then keeps the client alive, and the two close in any order. `set_aeron()` also keeps the client alive, so `AeronArchiveAsyncConnect::new()` and `aeron_archive_connect()` are safe after the caller drops its `Aeron`. A client obtained with `get_aeron()` is borrowed: publications and subscriptions made from it must drop before the context.
2. **Persistent Subscription Lifetime** – A persistent subscription keeps the `Aeron` client and archive context given to its builder open until it closes. Building it also points the archive context at the subscription's client, so build with `PersistentSubscriptionBuilder::new_with_aeron(&archive_context, &aeron)`, which sets one client on both. Without a client the subscription makes its own and closes it, and the archive context must not be used again afterwards.
3. **Deferred Close** – `archive.close()` is deferred while a clone is alive and does not close the `Aeron` client. `unsafe close_now()` frees it immediately, so any surviving handle then dangles.
4. **Automatic Handler Cleanup** – Handlers are reference-counted; registered callbacks live as long as the resource that registered them and are freed automatically. A panic in an archive callback is logged and aborts the process; a panic in a `ReplayMerge` poll handler unwinds to the caller and the fragments are delivered again on the next poll.
5. **Thread Safety** – Handles use `Rc` and stay on the thread that created them, unless the `multi-threaded` feature is enabled (see the [root README](../README.md#multi-threaded-send-and-sync-handles)).

---

## Typical Workflow

1. **Initialize** client and archive contexts.
2. **Start Recording** a specific channel and stream.
3. **Publish Messages** to the stream.
4. **Stop Recording** once complete.
5. **Locate the Recording** using archive queries.
6. **Replay Setup**: Configure replay target/channel.
7. **Subscribe and Receive** replayed messages.

See [`examples/record_and_replay.rs`](./examples/record_and_replay.rs) for this workflow end to end (`cargo run --release --features "static precompile" --example record_and_replay`).

---

## Duty Cycle

Each object is driven by its own call, once a cycle:

| Object | Call | Notes |
|---|---|---|
| `Aeron` client | Nothing with its conductor thread (the default). With the agent invoker, `aeron.main_do_work()`. | `archive.do_work()` makes this call for you. |
| `AeronArchive` | `archive.do_work()` | Runs an agent-invoker client's conductor, then hands one recording signal to the context's consumer, or one archive error to its error handler. With a conductor thread only the second part is left, and it can be skipped if you use neither a signal consumer nor errors for requests no longer awaited: blocking calls dispatch the signals they meet. |
| Persistent subscription | `ps.poll_fn(..)` | Drives its own archive client. With the agent invoker every poll also runs the client's conductor, so many persistent subscriptions on one client should use its conductor thread. |
| `AeronArchiveReplayMerge` | `merge.poll_fn(..)` | Until it has merged or failed, poll only the merge and leave its archive client alone: `archive.do_work()`, `poll_for_error` and blocking calls read the same responses, skip the merge's, and stall it until its progress timeout. Archive errors come back as `Err` from the merge's poll. |

```rust,no_run
# use rusteron_archive::*;
# fn snippet(archive: &AeronArchive, ps: &AeronArchivePersistentSubscription) -> Result<(), AeronCError> {
loop {
    archive.do_work()?;
    ps.poll_fn(|message, _header| { /* a replayed or live message */ }, 100)?;
}
# }
```

Persistent subscriptions share none of their per-poll work: each has its own archive client, and every poll checks its control session before it reads the live image. Idle, on two 4-vCPU Azure VMs on 2026-10-09 (an earlier run than the one in BENCHMARKS.md), a poll cost 22–24 ns on both with the client's conductor thread, against 8–12 ns for a plain subscription, and 35 ns (AMD) or 43 ns (Intel) with 100 of them. With the agent invoker every poll also runs the conductor: 54–62 ns, and 69 ns (AMD) or 107 ns (Intel) with 100. The conductor thread was slower to bring 100 of them to LIVE, though (3.6–3.8 s against about 0.8 s), possibly because it sleeps 16 ms when idle (`AERON_CLIENT_IDLE_SLEEP_DURATION`).

Blocking archive calls idle with the C client's default backoff strategy between polls (Aeron C++ yields instead); `archive_context.set_idle_strategy(..)` replaces it.

`archive.do_work()` fails only on an archive error its context has no error handler for. Client faults go to the client's error handler, and a lost archive shows as `archive.get_control_response_subscription().is_connected()` turning false.

---

## Persistent Subscriptions

A **persistent subscription** replays a recording from a start position, then seamlessly merges into the live stream — so a consumer catches up on history without missing new messages and without a gap at the handover. Introduced in Aeron Archive 1.51.0.

- **What it is**: [Aeron — Persistent Subscriptions (replay-to-live)](https://aeron.io/software-release/persistent-subscriptions-replay-to-live-transitions/)
- **How it works**: [Aeron Wiki — Persistent Subscriptions](https://github.com/aeron-io/aeron/wiki/Persistent-Subscriptions)
- **Background on publications/subscriptions**: [Aeron docs](https://aeron.io/docs/aeron/publications-subscriptions/)

Rusteron exposes it via `PersistentSubscriptionBuilder::new_with_aeron(&archive_context, &aeron)` (one client for the subscription and its archive context), `build()`, and the `PersistentSubscriptionListener` trait — a 1:1 wrapper over the Aeron C API (`aeron_archive_persistent_subscription_*`), mirroring Aeron's `PersistentSubscription.Context` field-for-field.

```rust,no_run
# use rusteron_archive::{Aeron, AeronArchiveContext, AeronPublication};
# fn snippet(archive_context: &AeronArchiveContext, aeron: &Aeron, publication: &AeronPublication, recording_id: i64) -> Result<(), Box<dyn std::error::Error>> {
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

// one client for the subscription and its archive context
let ps = PersistentSubscriptionBuilder::new_with_aeron(&archive_context, &aeron)?
    .live_channel(live_channel)?        // the live stream to join
    .live_stream_id(stream_id)?
    .replay_channel("aeron:udp?endpoint=localhost:0")?  // scratch channel for the replay
    .replay_stream_id(stream_id + 1)?
    .start_from_beginning()?            // replay from the start (or .start_from_live())
    .recording_id(recording_id)?        // which recording to replay
    .listener(MyListener { live_joined: live_joined.clone() })?
    .build()?;

// Drive it: replay runs, then it joins live. `ps.poll_fn()` drives its own archive
// client, so it needs nothing from `archive` (a control loop still calls
// `archive.do_work()` each cycle; see Duty Cycle). Check `has_failed()` each
// iteration (terminal failure) and stop once `is_live()`.
while !ps.is_live() {
    if ps.has_failed() {
        return Err(format!("persistent subscription failed: {:?}", ps.get_failure_reason()).into());
    }
    let _ = publication.offer(b"live"); // NotConnected/BackPressured are fine to skip in a demo
    ps.poll_fn(|buf, _hdr| { /* an assembled replayed or live message */ }, 100)?;
}

ps.close()?;
# Ok(()) }
```

**Polling & errors.** `ps.poll_fn()` drives the PS state machine *and* its own archive client, so it needs nothing from your `AeronArchive`; see [Duty Cycle](#duty-cycle) for what else to call each cycle. Loop on `ps.is_live()`, checking `ps.has_failed()` each iteration (reason via `get_failure_reason()`). The listener's `on_error` covers non-terminal errors; `on_live_left`/`on_live_joined` may fire repeatedly as it falls back and rejoins.

**Fragment assembly (already done for you).** Unlike `AeronSubscription`, the persistent subscription **reassembles fragments internally** — the C `aeron_archive_persistent_subscription_poll` routes each image through `aeron_image_fragment_assembler_handler`, so your handler receives whole messages directly. Just poll:

```rust,no_run
# use rusteron_archive::*;
# fn snippet(ps: &AeronArchivePersistentSubscription) -> Result<(), AeronCError> {
loop {
    // handler receives whole messages; no assembler needed
    ps.poll_fn(|buf, _hdr| { /* handle reassembled message */ }, 100)?;
}
# }
```

If you prefer the shared assembler API (e.g. to reuse a collector across subscription types), `AeronFragmentClosureAssembler` works too — it polls the PS internally, so it advances the state machine and delivers messages in one call. **Do not also call `ps.poll_fn(…)` separately**: that consumes the messages before the assembler sees them.

```rust,no_run
# use rusteron_archive::*;
# #[derive(Default)]
# struct Collector { done: bool }
# impl Collector { fn on_msg(&mut self, _msg: &[u8], _header: AeronHeader) { self.done = true; } }
# fn snippet(ps: AeronArchivePersistentSubscription) -> Result<(), Box<dyn std::error::Error>> {
let mut assembler = AeronFragmentClosureAssembler::new()?;
let mut ctx = Collector::default();
loop {
    assembler.poll(&ps, &mut ctx, Collector::on_msg, 100)?;  // polls the PS internally
    if ctx.done { break; }
}
# Ok(()) }
```

For a fully runnable version, see the example and integration tests:
- [`examples/record_and_replay.rs`](./examples/record_and_replay.rs) — the Typical Workflow end to end: record a stream, find the recording and replay it
- [`examples/persistent_subscription.rs`](./examples/persistent_subscription.rs) — standalone demo (run with `cargo run --release --features "static precompile" --example persistent_subscription`)
- [`examples/archive_error_handling.rs`](./examples/archive_error_handling.rs) — error handlers on both contexts, recording signals, typed control-session errors (blocking calls return `AeronArchiveError` with `e.code`; `archive.poll_for_error()` drains unsolicited ones, always with `Generic` code), and reconnecting after the archive goes down
- [`examples/persistent_subscription_failover.rs`](./examples/persistent_subscription_failover.rs) — failure modes: the live stream dies (`on_live_left`), and the subscription rejoins it (`on_live_joined`) once the publisher resumes the same session where it stopped
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
