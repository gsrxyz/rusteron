# rusteron-archive

`rusteron-archive` wraps the Aeron C Archive client. It includes the client APIs from
`rusteron-client` and adds recording, replay, replication and persistent subscriptions.

---

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It runs in GSR's real-time trading stack and is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

---

## Overview

On macOS, precompiled static libraries let you build without `cmake` or Java:

```toml
rusteron-archive = { version = "0.2", features = ["static", "precompile"] }
```

If you prefer a rustls-only downloader dependency:

```toml
rusteron-archive = { version = "0.2", features = ["static", "precompile-rustls"] }
```

---

## Installation

Add rusteron-archive to your `Cargo.toml`, depending on your setup:

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

With the default dynamic configuration, the Aeron C libraries must be available at runtime. The `static` option embeds them in the binary.

---

## Development

Build tasks use [`just`](https://github.com/casey/just). Run `just` to list commands, or `cargo install just` if needed.

---

## Features

* Record Aeron streams for replay or archival.
* Replay previously recorded messages.
* Persistent subscriptions replay recorded history, then join the live stream (Aeron Archive 1.51.0). See [below](#persistent-subscriptions).
* Publish to and subscribe from Aeron channels.
* Receive events such as new publications, subscriptions and errors.
* Objects built with `new()` call `*_init` and clean up with `*_close` or `*_destroy` when dropped. Other objects are not cleaned up automatically.
* `new()` and setter methods accept `&CStr`; getter methods return `&str`.

---

## General patterns

### Cloneable wrappers

Wrapper types in rusteron-archive implement `Clone` and share the same underlying Aeron C resource. The exceptions are structs Aeron only hands to callbacks, such as `AeronArchiveRecordingDescriptor`; see [Callback arguments are borrowed](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/README.md#callback-arguments-are-borrowed). For shallow copies of raw structs, use `.clone_struct()` (`unsafe` when the struct holds pointers such as strings).

### Mutable and immutable APIs

Most methods take `&self` and update the C resource without transferring ownership.

### Resource management caveats

Automatic cleanup applies only to `new()` constructors. Setters that take another wrapper (e.g. `set_aeron()`) keep it alive for as long as the context.

`archive.get_archive_context()`, the pollers and other getters return a `Ref`, which
borrows the archive. The compiler stops you from dropping or closing the archive while
you still use the result. Borrowed wrappers do not retain callbacks. Keep any returned
`Handler` alive while C can use it.

### Handlers and errors

Retained-callback setters take the callback by value (a closure or trait impl), keep it
alive inside the registering resource, and return the `Handler` for optional state access.
For synchronous polling, pass a stack closure:

```rust,no_run
# use rusteron_archive::*;
# fn snippet(archive_context: &AeronArchiveContext, subscription: &AeronSubscription) -> Result<(), AeronCError> {
// retained (e.g. an error handler on the archive context)
archive_context.set_error_handler(Some(|code: i32, msg: &str| eprintln!("archive error {code}: {msg}")))?;

// synchronous poll, note the fragment-limit argument
subscription.poll_fn(|buf: &[u8], header: &AeronHeader| println!("{} bytes", buf.len()), 10)?;
# Ok(()) }
```

`Handlers::NONE` fits any optional callback slot.

### Why replaced handlers stay alive

`AeronArchiveAsyncConnect::new_with_aeron()` copies the C archive context, including
its handler pointers, before you poll for the connection. Replacing a handler on the
original context only changes that context. The connection keeps using the earlier
handler. Configure handlers before starting a connection if it should use them.

Rusteron keeps every registered handler until the last owner of the registering
context drops. Async-connect handles and connected archives own that context, so
dropping your own context variable does not free its handlers while a connection
needs them. Dropping the returned `Handler`, replacing it, or setting it to `None`
does not release the earlier closure or its captured values. A `Handler` clone held
by your code keeps it alive longer.

This example assumes `ctx` contains the channels of a running archive and uses `aeron`:

```rust,no_run
# use rusteron_archive::*;
# use std::time::Duration;
# fn snippet(ctx: &AeronArchiveContext, aeron: &Aeron) -> Result<(), AeronCError> {
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

let old_calls = Arc::new(AtomicUsize::new(0));
let old_capture = old_calls.clone();
ctx.set_error_handler(Some(move |_, _: &str| {
    old_capture.fetch_add(1, Ordering::SeqCst);
}))?;

// Aeron copies the old handler pointer here, before poll_blocking().
let connecting = AeronArchiveAsyncConnect::new_with_aeron(ctx, aeron)?;
let new_calls = Arc::new(AtomicUsize::new(0));
let new_capture = new_calls.clone();
ctx.set_error_handler(Some(move |_, _: &str| {
    new_capture.fetch_add(1, Ordering::SeqCst);
}))?;
let archive = connecting.poll_blocking(Duration::from_secs(5))?;

// Invoke each context's handler directly to check which callback it holds.
archive.get_archive_context().invoke_error_handler(1, 0, c"connected context");
ctx.invoke_error_handler(2, 0, c"original context");
assert_eq!(old_calls.load(Ordering::SeqCst), 1);
assert_eq!(new_calls.load(Ordering::SeqCst), 1);
# Ok(()) }
```

The [regression test](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/src/lib.rs) starts an archive and checks
that both closures stay alive until the last context owner drops, then get freed.

Handler registration, callbacks, error checking and idle strategies work the same way here as in rusteron-client. See these sections of its documentation:
- [rusteron-client: Handlers and callbacks](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/README.md#handlers-and-callbacks)
- [rusteron-client: Errors and offer results](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/README.md#errors--offer-results)
- [rusteron-client: Idle strategies](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/README.md#idle-strategies)

Archive control operations (`start_recording`, `start_replay`, `stop_recording_subscription`, …) return
`Result<_, AeronArchiveError>`, which holds a typed code (`AeronArchiveErrorCode`) and the archive's
message. Constructors, async-connect and context setters return `AeronCError`;
`From<AeronArchiveError> for AeronCError` keeps `?` working across both.

---

<a id="documentation--guides"></a>

## Documentation and guides

See these guides for configuration and examples:

- [Multi-destination subscription guide](https://github.com/gsrxyz/rusteron/blob/main/docs/mdc_mds_guide.md)
- [Media Driver configuration and back-pressure guide](https://github.com/gsrxyz/rusteron/blob/main/docs/media_driver_configuration_and_backpressure_guide.md)
- [What's new in 0.2](https://github.com/gsrxyz/rusteron/blob/main/docs/whats_new_0.2.md)
- [Migrating from 0.1.168 to 0.2](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.1.168_to_0.2.md)
- [Tuning on x86-64 Linux](https://github.com/gsrxyz/rusteron/blob/main/docs/tuning_x86_64_linux.md)
- [Benchmarks](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md)

---

## Safety considerations

1. **Aeron lifetime.** Connect with `AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)`. The archive then keeps the client alive, and the two close in any order. `set_aeron()` also keeps the client alive, so `AeronArchiveAsyncConnect::new()` and `aeron_archive_connect()` are safe after the caller drops its `Aeron`. A client obtained with `get_aeron()` is borrowed. Publications and subscriptions made from it must drop before the context.
2. **Persistent subscription lifetime.** A persistent subscription keeps the `Aeron` client and archive context given to its builder open until it closes. Building it also points the archive context at the subscription's client, so build with `PersistentSubscriptionBuilder::new_with_aeron(&archive_context, &aeron)`, which sets one client on both. Without a client the subscription makes its own and closes it, and the archive context must not be used again afterwards.
3. **Deferred close.** `archive.close()` is deferred while a clone is alive and does not close the `Aeron` client. `unsafe close_now()` frees it immediately, so any surviving handle then dangles.
4. **Automatic handler cleanup.** Handlers are reference-counted. Owned contexts keep registered callbacks alive through C cleanup. Replacing a handler on an owned context keeps the earlier closure alive too. See [Why replaced handlers stay alive](#why-replaced-handlers-stay-alive). A panic in a `ReplayMerge` poll handler unwinds to the caller and the fragments are delivered again on the next poll. A panic in a persistent subscription handler or listener is caught and raised by its `poll`. A panic in an archive callback fired later is logged and the client continues. See [Panics in callbacks](https://github.com/gsrxyz/rusteron/blob/main/README.md#panics-in-callbacks).
5. **Thread safety.** Handles use `Rc` and stay on the thread that created them, unless the `multi-threaded` feature is enabled (see the [root README](https://github.com/gsrxyz/rusteron/blob/main/README.md#multi-threaded-send-and-sync-handles)).

---

## Typical workflow

1. Initialise the client and archive contexts.
2. Start recording a specific channel and stream.
3. Publish messages to the stream.
4. Stop recording once complete.
5. Locate the recording using archive queries.
6. Configure the replay target and channel.
7. Subscribe and receive the replayed messages.

See [`examples/record_and_replay.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/record_and_replay.rs) for this workflow end to end (`cargo run --release --features "static precompile" --example record_and_replay`).

---

## Replaying an empty recording

`AeronArchiveReplayParams` uses `position(-1)` to start at the recording's start position.
Use `length(-2)` to replay all available data and stop. An empty recording returns
`AeronArchiveErrorCode::EmptyRecording`, whether it is still recording or has stopped.
Use `length(-1)` to follow a live recording. That replay waits for new messages when the
recording is empty. These values come from `aeron_archive.h`.

```rust
use rusteron_archive::AeronArchiveReplayParams;

let replay_available = AeronArchiveReplayParams::builder()
    .position(-1)
    .length(-2)
    .build()?;
let follow_live = AeronArchiveReplayParams::builder()
    .position(-1)
    .length(-1)
    .build()?;
# Ok::<(), rusteron_archive::AeronCError>(())
```

Truncating a recording stops its active replays. Stop recording before calling
`truncate_recording`.

An Archive context can create its own Aeron client. `set_client_name` sets that client's
name and the name in the Archive control session counter. Names must fit in 100 UTF-8
bytes. If you supply an Aeron client with `set_aeron`, set that client's name on its own
`AeronContext`.

---

## Duty cycle

Each object is driven by its own call, once a cycle:

| Object | Call | Notes |
|---|---|---|
| `Aeron` client | Nothing with its conductor thread (the default). With the agent invoker, `aeron.main_do_work()`. | `archive.do_work()` makes this call for you. |
| `AeronArchive` | `archive.do_work()` | Runs an agent-invoker client's conductor, then hands one recording signal to the context's consumer, or one archive error to its error handler. With a conductor thread only the second part is left, and it can be skipped if you use neither a signal consumer nor errors for requests no longer awaited, because blocking calls dispatch the signals they meet. |
| Persistent subscription | `ps.poll_fn(..)` | Drives its own archive client. With the agent invoker every poll also runs the client's conductor, so many persistent subscriptions on one client should use its conductor thread. |
| `AeronArchiveReplayMerge` | `merge.poll_fn(..)` | Until it has merged or failed, poll only the merge and leave its archive client alone. `archive.do_work()`, `poll_for_error` and blocking calls read the same responses, skip the merge's, and stall it until its progress timeout. Archive errors come back as `Err` from the merge's poll. |

```rust,no_run
# use rusteron_archive::*;
# fn snippet(archive: &AeronArchive, ps: &AeronArchivePersistentSubscription) -> Result<(), AeronCError> {
loop {
    archive.do_work()?;
    ps.poll_fn(|message, _header| { /* a replayed or live message */ }, 100)?;
}
# }
```

Each persistent subscription has its own Archive client. Every poll checks its
control session before reading the live image. On two 4-vCPU Azure VMs on
2026-10-09, an idle poll took 22 to 24 ns with the client's conductor thread,
compared with 8 to 12 ns for a plain subscription. With 100 persistent subscriptions,
the per-subscription poll took 35 ns on AMD and 43 ns on Intel.

With the agent invoker, every poll also runs the conductor. An idle poll then took
54 to 62 ns, or 69 ns on AMD and 107 ns on Intel with 100 subscriptions. The
conductor thread took longer to bring 100 subscriptions to LIVE, 3.6 to 3.8 s
compared with about 0.8 s for the invoker. Its 16 ms idle sleep,
`AERON_CLIENT_IDLE_SLEEP_DURATION`, may explain that difference. These measurements
precede the run in BENCHMARKS.md.

Blocking archive calls idle with the C client's default backoff strategy between polls (Aeron C++ yields instead); `archive_context.set_idle_strategy(..)` replaces it.

`archive.do_work()` fails only on an archive error its context has no error handler for. Client faults go to the client's error handler, and a lost archive shows as `archive.get_control_response_subscription().is_connected()` turning false.

---

## Persistent subscriptions

A persistent subscription replays a recording from a start position, then merges into the live stream. A consumer catches up on history without missing new messages and without a gap at the handover. It was introduced in Aeron Archive 1.51.0.

- What it is: [Aeron persistent subscriptions (replay-to-live)](https://aeron.io/software-release/persistent-subscriptions-replay-to-live-transitions/)
- How it works: [Aeron wiki on persistent subscriptions](https://github.com/aeron-io/aeron/wiki/Persistent-Subscriptions)
- Background on publications and subscriptions: [Aeron docs](https://aeron.io/docs/aeron/publications-subscriptions/)

Rusteron exposes it through `PersistentSubscriptionBuilder::new_with_aeron(&archive_context, &aeron)` (one client for the subscription and its archive context), `build()` and the `PersistentSubscriptionListener` trait. It is a 1:1 wrapper over the Aeron C API (`aeron_archive_persistent_subscription_*`) and mirrors Aeron's `PersistentSubscription.Context` field for field.

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

`ps.poll_fn()` runs the persistent subscription state machine and its Archive client. It does not need calls on your `AeronArchive`. See [Duty cycle](#duty-cycle) for what else to call each cycle. Loop on `ps.is_live()`, checking `ps.has_failed()` each iteration (reason via `get_failure_reason()`). The listener's `on_error` covers non-terminal errors; `on_live_left`/`on_live_joined` may fire repeatedly as it falls back and rejoins.

A persistent subscription reassembles fragments internally. The C `aeron_archive_persistent_subscription_poll` routes each image through `aeron_image_fragment_assembler_handler`, so your handler receives whole messages directly. Poll for complete messages:

```rust,no_run
# use rusteron_archive::*;
# fn snippet(ps: &AeronArchivePersistentSubscription) -> Result<(), AeronCError> {
loop {
    // handler receives whole messages; no assembler needed
    ps.poll_fn(|buf, _hdr| { /* handle reassembled message */ }, 100)?;
}
# }
```

If you prefer the shared assembler API (e.g. to reuse a collector across subscription types), `AeronFragmentClosureAssembler` works too. It polls the PS internally, so it advances the state machine and delivers messages in one call. Do not also call `ps.poll_fn(…)` separately, because that consumes the messages before the assembler sees them.

```rust,no_run
# use rusteron_archive::*;
# #[derive(Default)]
# struct Collector { done: bool }
# impl Collector { fn on_msg(&mut self, _msg: &[u8], _header: &AeronHeader) { self.done = true; } }
# fn snippet(ps: AeronArchivePersistentSubscription) -> Result<(), Box<dyn std::error::Error>> {
let mut assembler = AeronFragmentClosureAssembler::new()?;
let mut ctx = Collector::default();
loop {
    assembler.poll(&ps, &mut ctx, Collector::on_msg, 100)?;  // polls the PS internally
    if ctx.done { break; }
}
# Ok(()) }
```

The examples and integration tests cover these cases:

- [`examples/record_and_replay.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/record_and_replay.rs): the typical workflow end to end, which records a stream, finds the recording and replays it
- [`examples/persistent_subscription.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/persistent_subscription.rs): standalone demo (run with `cargo run --release --features "static precompile" --example persistent_subscription`)
- [`examples/archive_error_handling.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/archive_error_handling.rs): error handlers on both contexts, recording signals, typed control-session errors (blocking calls return `AeronArchiveError` with `e.code`; `archive.poll_for_error()` drains unsolicited ones, always with `Generic` code), and reconnecting after the archive goes down
- [`examples/persistent_subscription_failover.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/persistent_subscription_failover.rs): failure modes, where the live stream dies (`on_live_left`) and the subscription rejoins it (`on_live_joined`) once the publisher resumes the same session where it stopped
- [`examples/replay_merge.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/replay_merge.rs): late-joiner catch-up that replays recorded history, then merges onto the live MDC stream (`AeronArchiveReplayMerge`)
- [`examples/recording_throughput.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/recording_throughput.rs): recording throughput measurement (publish rate vs archiver catch-up) and `list_recordings` descriptor enumeration
- [`examples/recording_replication.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/recording_replication.rs): archive-to-archive replication (`archive.replicate`), where a destination archive pulls a finished recording from a source archive and the copy is verified (port of `RecordingReplicator`)
- `persistent_subscription_tests::test_persistent_subscription_listener_live_joined` (listener callbacks)
- `persistent_subscription_integration::test_end_to_end_persistent_subscription` (recording, replay and live delivery)

---

## Benchmarks

See [BENCHMARKS.md](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md) for latency and throughput benchmarks.

---

## Contributing

Report bugs and propose changes through GitHub Issues and pull requests.
See [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md)
for the workflow and required checks. Contributions can cover API design, safety,
Rust conventions and deployment examples.

---

## License

Licensed under either [MIT License](https://opensource.org/licenses/MIT) or [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) at your option.

---

## Acknowledgments

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron. Your work made this possible.
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for the open protocol
