# Client guide

This guide covers callbacks, channels, errors and polling in `rusteron-client`.
See the [client README](../rusteron-client/README.md) for setup and a first example.

[Callbacks](#handlers-and-callbacks) · [Channel URIs](#building-channel-uris) ·
[Send errors](#errors-and-offer-results) · [Idle strategies](#idle-strategies) ·
[Sanitizers](#addresssanitizer)

## Handlers and callbacks

The client accepts retained and synchronous callbacks.

### Retained callbacks

The C client stores these and can invoke them later from the conductor thread.
Owned setters accept a closure or callback implementation and put it in a
`SharedHandler`. The registering resource retains it through native cleanup.
Dropping the returned handler does not unregister the callback. You do not need
to call `release()`.

For example:

```rust,no_run
# use rusteron_client::*;
# fn snippet(ctx: &AeronContext) -> Result<(), AeronCError> {
ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
# Ok(()) }
```

Use the returned shared handler's `lock()` for synchronized state access. Another
option is to capture shared application state:

```rust,no_run
# use rusteron_client::*;
# fn snippet(ctx: &AeronContext) -> Result<(), AeronCError> {
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// Share the count between the conductor callback and the application thread.
let errors = Arc::new(AtomicUsize::new(0));
let counted = errors.clone();
ctx.set_error_handler(Some(move |_code: i32, _msg: &str| {
    counted.fetch_add(1, Ordering::Relaxed);
}))?;
// ... later, from the application thread
println!("{} errors", errors.load(Ordering::Relaxed));
# Ok(()) }
```

Registration methods such as `add_subscription` accept
`Option<&SharedHandler<T>>` for retained image callbacks and keep their own
reference. Use `SharedHandler::new_shared(...)` to create one yourself.

### Why handlers stay alive until the client closes

Earlier versions left handler cleanup to developers. Releasing a handler while C
can still call it causes undefined behaviour. Rusteron now keeps handlers alive
through client cleanup.

Closing a publication, subscription or counter asks the conductor to close it.
The conductor can still call its handlers after `close()` returns. Rusteron therefore
also keeps these handlers on the owning `Aeron` client. It releases them after C
cleanup when the last owning client reference drops, even if their subscription or
other resource closed earlier. Child handles keep the client alive too.

Each registration adds a retained handler reference. Repeated registrations on a
long-lived client keep their closures and captured values allocated until that client
closes. Share large application state through an `Arc` rather than copying it into
each closure. Dropping your own `Handler` reference does not free the registered copy.

This costs memory, but avoids tracking exactly when C stops using each handler.

Archive connections retain the registrations they copied when connecting.
Replacing an unused handler on its context releases it. See
[Archive callback ownership](archive-guide.md#why-replaced-handlers-stay-alive).

For state updates that can wait for an active callback, keep the setter's returned
`SharedHandler` and use `handler.lock()`. This acquires a mutex; keep state updates
short so callbacks can proceed. Local `Handler::lock()` and `borrow()` instead
use `RefCell` borrow checks without a mutex or atomic operation.
`poll(&mut callback, limit)`, `poll_fn` and owned assemblers use direct Rust
borrowing without either guard. See the
[synchronization costs](thread-safety.md#hot-path-behaviour) and
[direct-handler example](archive-guide.md#replacing-callback-state-through-its-handler).
Use `AeronErrorHandlerSlot` with `AeronContext::set_error_handler` when replacement
must not wait for an active callback. Old callbacks are freed after their last
call. See the [example](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/replaceable_error_handler.rs)
and [callback docs](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/README.md#changing-a-callback-without-registering-another-handler).

### Callback arguments are borrowed

Wrapper arguments such as `&AeronHeader`, `&AeronImageRef` and
`&AeronArchiveRecordingDescriptorRef` are borrowed for the call only. Aeron may
free or reuse the struct once the callback returns. The compiler prevents you
from keeping their references. Copy the fields you need, such
as `d.recording_id()` or `d.original_channel().to_owned()`. Borrowed wrapper
specializations have no `Clone` or safe conversion from raw pointers or native
structs. `unsafe from_ptr()` requires an external lifetime guarantee.
`clone_struct()` makes a shallow copy and is unsafe for pointer-bearing structs.
The copied pointers still refer to the original memory.

### Synchronous callbacks

Synchronous callbacks run during the call. They accept a stack closure that can borrow
local state without allocating:

```rust,no_run
# use rusteron_client::*;
# fn snippet(subscription: &AeronSubscription) -> Result<(), AeronCError> {
subscription.poll_fn(|buf: &[u8], header: &AeronHeader| {
    println!("received {} bytes", buf.len());
}, 10)?;
# Ok(()) }
```

`poll_fn` delivers raw fragments. To receive complete messages, create a
`FragmentAssembler` once and poll through it. The callback can borrow local
state, and polling needs no lock or reference count:

```rust,no_run
# use rusteron_client::*;
# fn snippet(subscription: &AeronSubscription) -> Result<(), AeronCError> {
let mut bytes = 0;
let mut assembler = FragmentAssembler::new(|msg: &[u8], _header: &AeronHeader| {
    bytes += msg.len() as u64;
})?;
let fragments = assembler.poll(subscription, 10)?;
drop(assembler); // release the callback's borrow before reading bytes
println!("{fragments} fragments, {bytes} bytes received");
# Ok(()) }
```

For controlled polling, use `ControlledFragmentAssembler` with a callback that
returns an Aeron action. `ABORT` leaves the last fragment available for the
next poll:

```rust,no_run
# use rusteron_client::*;
# fn snippet(subscription: &AeronSubscription) -> Result<(), AeronCError> {
use rusteron_client::bindings::aeron_controlled_fragment_handler_action_t::AERON_ACTION_CONTINUE;

let mut assembler = ControlledFragmentAssembler::new(
    |msg: &[u8], _header: &AeronHeader| {
        println!("received {} bytes", msg.len());
        AERON_ACTION_CONTINUE
    },
)?;
assembler.poll(subscription, 10)?;
# Ok(()) }
```

Both assemblers expose `handler()` and `handler_mut()` to inspect or update
callback state between polls. Their `delete_session_buffer(session_id)` method
releases an unfinished message for a session. The native `Aeron*FragmentAssembler`
types and `Handler::with_*_fragment_assembler` remain available when a callback
must be shared with another owner.

Pass `Handlers::NONE` for any optional callback parameter you do not need.

---

## Building channel URIs

Prefer the typed [`AeronUriStringBuilder`] over hand-written URI strings. Parameters are
typed setters, so misspelled keys and malformed values are caught before they reach the
driver:

```rust,no_run
# use rusteron_client::*;
# fn snippet(id: i64) -> Result<(), AeronCError> {
let channel = AeronUriStringBuilder::udp("localhost:20121")?
    .socket_tos(0x28)?
    .build(256)?;
let untethered = AeronUriStringBuilder::ipc()?
    .tether(false)?
    .untethered_linger_timeout(1_000_000_000)?
    .build(256)?;
let mds_sub = AeronUriStringBuilder::udp_control("localhost:9998", ControlMode::Manual)?.build(256)?;
let response = AeronUriStringBuilder::udp_control("localhost:9999", ControlMode::Response)?
    .response_correlation_id(id)?
    .build(256)?;
# Ok(()) }
```

`socket_tos` sets the UDP traffic class byte, including DSCP and ECN bits. The driver
context's `set_socket_tos` sets the default for channels without `so-tos`.
`untethered_linger_timeout` takes nanoseconds and configures an untethered subscription's
linger timeout. It is separate from publication `linger`.

Use `ipc()`, `udp(endpoint)` or `udp_control(control, ControlMode)` to create a builder.
It also has about 50 typed setters, including `session_id`, `mtu_length`,
`term_length`, `fc`, `gtag` and `reliable`. See
`examples/multi_destination_subscription.rs` and `examples/request_response.rs`.

The `*_duration` URI helpers, such as `linger_timeout_duration`, accept
`Duration` and check that the nanosecond value fits Aeron's signed 64-bit range.

Response channels can also set `session_id`. Use the same session ID on the response
subscription and publication. `examples/request_response.rs` uses session ID 71 and
checks the received header. The tests cover both IPC and UDP response channels.

## Errors and offer results

- Install a client error handler with `ctx.set_error_handler(Some(handler))` to receive asynchronous errors.
- `offer()` and `try_claim()` return `Result<i64, AeronOfferError>`. `Ok` contains the new log position. Use `is_retryable()` to check the error. Retry after back pressure, an administrative action or a missing connection. A closed publication or an exceeded maximum position is a fatal error. Use an idle strategy between retries:

  ```rust,no_run
  # use rusteron_client::*;
  # fn snippet(publication: &AeronPublication, msg: &[u8], idle: &mut BackoffIdleStrategy) -> Result<(), Box<dyn std::error::Error>> {
  match publication.offer(msg) {
      Ok(_) => idle.reset(),
      Err(e) if e.is_retryable() => idle.idle(0), // no work done: back off, then retry
      Err(e) => return Err(e.into()),             // publication gone
  }
  # Ok(()) }
  ```
  Zero-copy `try_claim_owned()` returns an `AeronClaim<'_>` that borrows the
  publication. An uncommitted claim aborts when dropped. Safe close of another
  owning clone keeps the claim's owner alive. For example:

  ```rust,no_run
  # use rusteron_client::*;
  # fn snippet(publication: &AeronPublication, msg: &[u8], idle: &mut BackoffIdleStrategy) -> Result<(), Box<dyn std::error::Error>> {
  match publication.try_claim_owned(msg.len()) {
      Ok(mut claim) => {
          claim.data().copy_from_slice(msg);
          claim.set_reserved_value(Aeron::nano_clock()); // optional; read with header.reserved_value()
          claim.commit()?;
          idle.reset();
      }
      Err(e) if e.is_retryable() => idle.idle(0),
      Err(e) => return Err(e.into()),
  }
  # Ok(()) }
  ```
  `offer_raw()` and `try_claim_raw()` return the raw `i64` sentinel without converting it to a Rust error.
  A successful typed call returns the log position. Only a failed call constructs the
  error enum.
- For image handlers, pass `Handlers::NONE` when you do not need image callbacks. Applications can use them to log image availability or synchronise startup. Aeron's `Ping` sample uses one as a latch.

## Idle strategies

Poll loops should back off when a cycle does no work. Rusteron's `IdleStrategy`
matches Aeron's behaviour. `idle(work_count)` returns immediately when work was
done and backs off otherwise. For example:

```rust,no_run
# use rusteron_client::{AeronFragmentHandlerCallback, AeronSubscription, AeronCError, Handler};
# fn snippet<H: AeronFragmentHandlerCallback>(subscription: &AeronSubscription, mut handler: H) -> Result<(), AeronCError> {
use rusteron_client::{BackoffIdleStrategy, IdleStrategy};

let mut idle = BackoffIdleStrategy::new(); // Aeron's default spins, yields, then sleeps.
loop {
    let fragments = subscription.poll(&mut handler, 10)?;
    if fragments == 0 { /* break when done */ }
    idle.idle(fragments);
}
# }
```

Use an idle strategy in retry and poll loops rather than calling CPU pause hints
directly. Keep a strategy instance for each loop or thread. Pass the work count
after each cycle; when idling only on retries, call `reset()` after a successful
operation so backoff does not carry over to the next message.

Available strategies are [`BusySpinIdleStrategy`], [`YieldingIdleStrategy`],
[`SleepingIdleStrategy`], [`BackoffIdleStrategy`] and [`NoOpIdleStrategy`].
`BusySpinIdleStrategy` occupies a core. Measure its effect on your deployment and
use the same strategy in every benchmark being compared.
`SleepingIdleStrategy` sleeps for a fixed interval, and
`BackoffIdleStrategy` progresses through spinning, yielding and sleeping.

`BackoffIdleStrategy` is the default. It matches Aeron's C and Java backoff with
10 spins, 20 yields and parks from 1 µs to 1 ms.
Set the conductor's idle strategy on the context with
`ctx.set_idle_strategy_kind(AeronIdleStrategyKind::Backoff)?`.
This also sets the matching initialisation arguments. The media driver's
per-agent setters, such as `AeronDriverContext::set_conductor_idle_strategy_kind`,
work the same way.

For recording, replay and persistent subscriptions, see [`rusteron-archive`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/README.md#persistent-subscriptions).
Persistent subscriptions replay history, then join a live stream.

---

## C strings without hidden allocations

Generated channel and URI arguments use `&CStr`, as the C API does. Constructing
a dynamic C string allocates at the call site. Reusing a `&CStr` does not allocate.

A few hand-written convenience helpers take `&str` and allocate on each call:
`AeronArchive::connect`, the persistent-subscription builder's `live_channel` and
`replay_channel`, and the replication params builder's `replication_channel` and
`src_response_channel`. Call these during setup, outside the hot path.

String getters return borrowed `&str`. Immutable native text has safe getters.
Borrowing replaceable native text requires `unsafe` because a setter or native
operation can invalidate it. Keep its owner alive and prevent changes for every
use of the borrow. Use an explicit `_owned` getter when a copy must survive those
changes. See [borrowed strings and allocations](migrating_0.2_to_0.3.md#borrowed-strings-and-allocations).

Choose the allocation pattern for your use case:

```rust,no_run
# use rusteron_client::*;
# fn snippet(aeron: &Aeron, port: u16, endpoint: &str, reconnect_attempts: usize) -> Result<(), AeronCError> {
// 1. Constant channels: c"..." literals are compile-time &'static CStr, zero runtime cost.
aeron.async_add_publication(c"aeron:ipc", 10)?;

// 2. Format a dynamic URI into one CString allocation with cformat!.
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

## AddressSanitizer

The `sanitize-address` feature enables `build-from-source` and compiles the Aeron
C sources with `-fsanitize=address`. It helps tests detect use-after-free and
double-free errors across the FFI boundary. For example:

```bash
# macOS: stable rustc; Apple's ASan runtime is linked automatically, it just needs to be
# on the loader path:
export DYLD_LIBRARY_PATH="$(clang -print-resource-dir)/lib/darwin"
ASAN_OPTIONS=detect_leaks=0 cargo test --features rusteron-client/sanitize-address

# Linux: nightly also instruments the Rust side:
RUSTFLAGS="-Zsanitizer=address" cargo +nightly test -Zbuild-std \
  --target x86_64-unknown-linux-gnu --features rusteron-client/sanitize-address
```

Leak detection is noisy with a media driver in-process, so keep `detect_leaks=0`.
Select other sanitizers, such as `thread` or `undefined`, with the
`RUSTERON_SANITIZE` environment variable. In this repository, `just test-asan`
runs the client suite with AddressSanitizer.
