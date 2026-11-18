# Archive guide

This guide covers ownership, callbacks, replay and persistent subscriptions.
See the [Archive README](../rusteron-archive/README.md) for setup and examples.

[Ownership](#general-patterns) · [Callbacks](#handlers-and-errors) ·
[Empty recordings](#replaying-an-empty-recording) · [Duty cycle](#duty-cycle) ·
[Persistent subscriptions](#persistent-subscriptions)

## General patterns

### Cloneable wrappers

Supported owning wrappers implement `Clone` and share the same underlying Aeron
C resource. Borrowed and inline storage do not implement `Clone`.

`AeronArchiveContext` has no `Clone` implementation. A `Ref` returned by an archive
borrows that archive and cannot be cloned. Use `retain_archive_context()` or
`aeron_owned()` to retain the archive while using its child through `as_ref()`.
If setup code needs to share an owning context, create it inside `Rc` and clone
the `Rc`.

Native fragment assemblers and callback-only structs such as
`AeronArchiveRecordingDescriptor` are also not cloneable. See
[Callback arguments are borrowed](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/README.md#callback-arguments-are-borrowed).
For plain data structs, `.clone_struct()` makes a shallow copy; it is `unsafe`
when the struct holds pointers such as strings.

### Mutable and immutable APIs

Most methods take `&self` and update the C resource without transferring ownership.

### Resource management caveats

Owning constructors and completed asynchronous registrations manage native
cleanup. Setters such as `set_aeron()` retain the dependencies needed by the
context. A borrowed getter does not create an independent native owner.

`archive.get_archive_context()`, the pollers and other getters return a `Ref`, which
borrows the archive. The compiler stops you from dropping or closing the archive while
you still use the result. The archive context has no `Clone` or safe raw-pointer/native-struct
conversion that can turn this borrow into an owned context. Retained-callback setters
and new archive connections require an owning context and reject a borrowed one. See
[the migration guide](migrating_0.2_to_0.3.md#archive-ownership-and-strings).

### Handlers and errors

Retained-callback setters accept a closure or callback implementation by value.
The registering resource keeps it alive. The setter returns a `SharedHandler`
for synchronized state access.
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

`SharedHandler` uses `Arc`, but C copies raw pointers. Those copies do not increase the
Rust reference count or tell Rust when C has finished using the handler.

The error, recording-signal, delegating-invoker and custom idle-strategy setters
replace the context's retained handler after a successful C call. Replacing or
clearing an unused handler releases its captured values. Each pending connection
keeps a snapshot of the registrations it copied. On success, that snapshot moves
to the connected archive. Explicit shared-handler clones can keep a callback alive longer.

Connections still retain the source context because C borrows its idle-strategy
state. That source context keeps its current handler, even if only an earlier
connection retains the context. This does not keep a history of replaced handlers.
Credential-supplier registrations and transport-client callbacks retain their
existing cleanup rules.

An archive-owned context is borrowed. Its retained-callback setters and new
connection methods return an error before changing C. Configure an owning context
before connecting. Replacing a running or borrowed callback returns an error;
change it after the call finishes. A native idle strategy cannot be replaced after initialization,
because existing connections may still use its state.

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
that a connection uses its original handler, unused replacements are freed, and
closing the connection releases its old handler while the source context remains alive.

### Replacing callback state through its handler

Keep the `SharedHandler` returned by the original setter when updates can wait
for a running callback. `handler.lock()` gives exclusive access to its state.
Changing that state also affects connections that copied the same native handler
pointer. It does not register another callback.

This shared handler uses a mutex for state updates and each callback invocation.
Keep updates short to limit callback delays. Transport receive loops can use
`poll_fn` or an owned assembler with direct Rust borrowing and no callback mutex.
Local `Handler::lock()` and `borrow()` use a `RefCell` check without a mutex or
atomic operation. See [synchronization costs](thread-safety.md#hot-path-behaviour).

A concrete callback struct can update its fields without dynamic dispatch. To
replace it with different closure types, choose a boxed callback when registering:

```rust,no_run
use rusteron_archive::*;

# fn replace(context: &AeronArchiveContext) -> Result<(), AeronCError> {
type ErrorCallback = Box<dyn FnMut(i32, &str) + Send>;
let callback: ErrorCallback = Box::new(|code, message| {
    eprintln!("first: {code}: {message}");
});
let handler = context.set_error_handler(Some(callback))?.unwrap();

let replacement: ErrorCallback = Box::new(|code, message| {
    eprintln!("second: {code}: {message}");
});
let previous = std::mem::replace(&mut *handler.lock(), replacement);
drop(previous); // Drop captured values after releasing the handler lock.
# Ok(()) }
```

The boxed form adds a dynamic callback call. Both forms use the existing handler
lock without cloning a callback owner on each invocation. Keep the returned
handler; passing another handler as the setter's callback value adds a wrapper
and another lock.

Replacement waits for an active callback, and must not be attempted from inside
that same callback. Use a slot when replacement must finish while an earlier
call is still running, or when a callback needs to replace or clear itself.

### Changing a callback without waiting for an active call

<a id="changing-a-callback-without-registering-another-handler"></a>

Use `AeronErrorHandlerSlot` for replacement that does not wait for the previous
callback to finish. Register the slot once, then use `set` to replace its callback
or `clear` to remove it. The C handler stays registered.

```rust
use rusteron_archive::{AeronArchiveContext, AeronErrorHandlerSlot};
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let ctx = AeronArchiveContext::new()?;
let handler = AeronErrorHandlerSlot::new();
ctx.set_error_handler(Some(handler.clone()))?;

let label = String::from("first");
handler.set(move |code, message| eprintln!("{label}: {code}: {message}"));
handler.set(|code, message| eprintln!("second: {code}: {message}"));
handler.clear();
# Ok(())
# }
```

Old callbacks and their captured values are freed after their last call finishes.
Archive connections share the slot, so they also use the new callback.

Callbacks can change captured values. Captures must be owned and safe to move
between threads. Calls to the same callback run one at a time. It can replace
or clear itself, but calling itself again will deadlock. A non-empty slot takes
two internal mutex locks and clones an `Arc` per invocation. Calling it through
a retained handler adds the handler's own lock, for three lock acquisitions in total.

The [runnable example](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/replaceable_error_handler.rs)
needs no archive server:

```sh
cargo run -p rusteron-archive --features precompile,static --example replaceable_error_handler
```

The client, archive and media driver crates provide named slots for callbacks.
For callbacks that return a value, choose what to return when empty. This slot
returns `AERON_ACTION_ABORT` until you set a callback:

```rust
use rusteron_archive::{
    AeronControlledFragmentHandlerSlot,
    bindings::aeron_controlled_fragment_handler_action_t::AERON_ACTION_ABORT,
};

let handler = AeronControlledFragmentHandlerSlot::new(|| AERON_ACTION_ABORT);
```

Handler registration, callbacks, error checking and idle strategies work the
same way as in `rusteron-client`. See these sections of its documentation:

- [Client handlers and callbacks](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/README.md#handlers-and-callbacks)
- [Client errors and offer results](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/README.md#errors--offer-results)
- [Client idle strategies](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/README.md#idle-strategies)

Archive control operations such as `start_recording`, `start_replay` and
`stop_recording_subscription` return `Result<_, AeronArchiveError>`.
The error contains an `AeronArchiveErrorCode` and the archive's message.
Constructors, async-connect and context setters return `AeronCError`.
`From<AeronArchiveError> for AeronCError` keeps `?` working across both and
preserves the native code and captured message.

Rusteron reads `aeron_errcode()` after a failed control request. It does not
parse the message to identify the error. Known codes use Aeron's public
`AERON_ARCHIVE_ERROR_CODE_*` and `AERON_ARCHIVE_CLIENT_ERROR_*` constants.
Other errors retain their numeric code and sign in `Unknown(code)`.

Archive error handlers receive positive native codes. Convert them with
`AeronArchiveErrorCode::from_native_code(code)`. This also accepts negative
thread-local Archive codes. `as_native_code()` returns the negative code for a
known variant and preserves the original value for an unknown code.

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
| `Aeron` client | Nothing with its conductor thread (the default). Manual `main_do_work()` requires `unsafe` and coordination with all native users. | Archive operations can also invoke an agent-invoker conductor. |
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

Blocking archive calls use the C client's default backoff strategy between
polls. Aeron C++ yields instead. Use `archive_context.set_idle_strategy(..)` to
replace the default strategy.

`archive.do_work()` fails only on an archive error its context has no error handler for. Client faults go to the client's error handler, and a lost archive shows as `archive.get_control_response_subscription().is_connected()` turning false.

---

## Persistent subscriptions

A persistent subscription replays a recording from a start position, then merges into the live stream. A consumer catches up on history without missing new messages and without a gap at the handover. It was introduced in Aeron Archive 1.51.0.

- [Aeron persistent subscription release notes](https://aeron.io/software-release/persistent-subscriptions-replay-to-live-transitions/)
- [Aeron wiki on persistent subscriptions](https://github.com/aeron-io/aeron/wiki/Persistent-Subscriptions)
- [Aeron publications and subscriptions](https://aeron.io/docs/aeron/publications-subscriptions/)

Create a persistent subscription with
`PersistentSubscriptionBuilder::new_with_aeron(&archive_context, &aeron)` and
`build()`. The subscription and its archive context share one client.
Implement `PersistentSubscriptionListener` to receive state changes and errors.
These APIs wrap the C `aeron_archive_persistent_subscription_*` functions and
expose the fields in Aeron's `PersistentSubscription.Context`.

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

`ps.poll_fn()` runs the persistent subscription state machine and its Archive
client. It does not need calls on your `AeronArchive`. See [Duty cycle](#duty-cycle)
for what else to call each cycle. Loop until `ps.is_live()` returns true.
Check `ps.has_failed()` each iteration and use `get_failure_reason()` on failure.
The listener's `on_error` reports non-terminal errors. `on_live_left` and
`on_live_joined` can run repeatedly if the subscription loses and rejoins the
live stream.

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

Use `AeronFragmentClosureAssembler` to reuse a collector across subscription
types. It polls the persistent subscription and advances its state machine.
Do not also call `ps.poll_fn(...)` separately. That would consume messages before
the assembler receives them.

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

The examples cover these cases:

| Example | Behaviour |
| --- | --- |
| [`record_and_replay.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/record_and_replay.rs) | Record a stream, find the recording and replay it |
| [`persistent_subscription.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/persistent_subscription.rs) | Replay history and join a live stream |
| [`archive_error_handling.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/archive_error_handling.rs) | Handle context errors, recording signals and reconnects |
| [`persistent_subscription_failover.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/persistent_subscription_failover.rs) | Rejoin a live stream after its publisher resumes the same session |
| [`replay_merge.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/replay_merge.rs) | Replay history, then merge into a live multi-destination stream |
| [`recording_throughput.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/recording_throughput.rs) | Measure publishing and recording rates and list recording descriptors |
| [`recording_replication.rs`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/recording_replication.rs) | Replicate a finished recording between archives and verify the copy |

Run the standalone subscription example with
`cargo run --release --features "static precompile" --example persistent_subscription`.

Blocking control requests return `AeronArchiveError` with a typed `code`.
`archive.poll_for_error()` reads unsolicited errors with their typed code and
full message from the native response poller. It returns `Some(error)` even
when the message is empty. Rusteron creates no temporary message buffer for the poll.
The error-handling example shows both forms. The replication example follows
Aeron's `RecordingReplicator` sample.

The `persistent_subscription_tests::test_persistent_subscription_listener_live_joined`
test checks listener callbacks. The
`persistent_subscription_integration::test_end_to_end_persistent_subscription`
test checks recording, replay and live delivery.

---
