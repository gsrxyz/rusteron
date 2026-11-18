# Client guide

This guide covers callbacks, channels, errors and polling in `rusteron-client`.
See the [client README](../rusteron-client/README.md) for setup and a first example.

[Callbacks](#handlers-and-callbacks) · [Threads](#sharing-across-threads) · [Channel URIs](#building-channel-uris) ·
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

<a id="why-handlers-stay-alive-until-the-client-closes"></a>

### Handler lifetime

The conductor can still call a handler after `close()` returns on a publication,
subscription or counter. Rusteron therefore keeps retained handlers on the owning
`Aeron` client and releases them after C cleanup, when the last owning client
reference drops. Child handles keep the client alive.

Each registration adds a retained handler. On a long-lived client, repeated
registrations keep their closures and captured values allocated until the client
closes. Share large application state through an `Arc` rather than copying it into
each closure. Dropping your own `Handler` reference does not free the registered copy.

Archive connections retain the registrations they copied when connecting.
Replacing an unused handler on its context releases it. See
[replacing handlers](archive-guide.md#why-replaced-handlers-stay-alive).

To update callback state while a callback may be running, keep the setter's returned
`SharedHandler` and use `handler.lock()`. This takes a mutex, so keep updates short.
Local `Handler::lock()` and `borrow()` use a `RefCell` check instead.
`poll(&mut callback, limit)`, `poll_fn` and owned assemblers use direct Rust
borrowing with no guard. See [callback costs](thread-safety.md#hot-path-behaviour) and
[replacing callback state](archive-guide.md#replacing-callback-state-through-its-handler).

Use `AeronErrorHandlerSlot` with `AeronContext::set_error_handler` when replacement
must not wait for an active callback. See the
[example](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/replaceable_error_handler.rs)
and [callback docs](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/README.md#changing-a-callback-without-registering-another-handler).

### Callback arguments are borrowed

Wrapper arguments such as `&AeronHeader`, `&AeronImageRef` and
`&AeronArchiveRecordingDescriptorRef` are valid for the call only. Aeron may
free or reuse the struct once the callback returns, and the compiler stops you
keeping the references. Copy the fields you need, such
as `d.recording_id()` or `d.original_channel().to_owned()`. Borrowed wrappers
have no `Clone` and no safe conversion from raw pointers or native structs.
`unsafe from_ptr()` needs a lifetime guarantee from you.
`clone_struct()` makes a shallow copy and is unsafe for structs that hold pointers,
because the copied pointers still refer to the original memory.

### Synchronous callbacks

Synchronous callbacks run during the call. They accept a stack closure that can borrow
local state without allocating:

```rust,no_run
# use rusteron_client::*;
# fn snippet(subscription: &mut AeronSubscription) -> Result<(), AeronCError> {
subscription.poll_fn(|buf: &[u8], header: &AeronHeader| {
    println!("received {} bytes", buf.len());
}, 10)?;
# Ok(()) }
```

`poll_fn` delivers raw fragments. To receive complete messages, create an
assembler once and poll through it. Polling takes no lock and changes no reference count.

`AeronFragmentClosureAssembler` takes a new closure on each poll. The closure can
borrow local state for that call only:

```rust,no_run
# use rusteron_client::*;
# fn snippet(subscription: &mut AeronSubscription) -> Result<(), AeronCError> {
let mut bytes = 0;
let mut assembler = AeronFragmentClosureAssembler::new()?;
let fragments = assembler.poll(subscription, |msg, _header| bytes += msg.len() as u64, 10)?;
println!("{fragments} fragments, {bytes} bytes received");
# Ok(()) }
```

`FragmentAssembler` owns one fixed callback, such as a struct with its
own state. Read that state between polls with `handler()`:

```rust,no_run
# use rusteron_client::*;
# fn snippet(subscription: &mut AeronSubscription) -> Result<(), AeronCError> {
#[derive(Default)]
struct Bytes(u64);

impl AeronFragmentHandlerCallback for Bytes {
    fn handle_aeron_fragment_handler(&mut self, msg: &[u8], _header: &AeronHeader) {
        self.0 += msg.len() as u64;
    }
}

let mut assembler = FragmentAssembler::new(Bytes::default())?;
assembler.poll(subscription, 10)?;
println!("{} bytes received", assembler.handler().0);
# Ok(()) }
```

To poll an image or use a bounded poll, pass `&mut assembler` for the fixed
form, or `&mut assembler.with(closure)` for the closure form.

For controlled polling, use `ControlledFragmentAssembler` or
`AeronControlledFragmentClosureAssembler` with a callback that returns an Aeron action. `ABORT` leaves the last fragment available for the
next poll:

```rust,no_run
# use rusteron_client::*;
# fn snippet(subscription: &mut AeronSubscription) -> Result<(), AeronCError> {
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
releases an unfinished message for a session.

To poll one image, use `AeronImageFragmentAssembler` or
`AeronImageControlledFragmentAssembler` with `assembler.poll(&image, limit)`. They
keep one buffer, allocated at creation. The subscription assemblers allocate a
buffer the first time each session sends a fragmented message.

Pass `Handlers::NONE` for any optional callback parameter you do not need.

---

## Sharing across threads

`Aeron`, `AeronPublication`, `AeronCounter` and `AeronCountersReader` are
`Send + Sync`. Put one in an `Arc` to share it with other threads, or clone it.
A clone shares the same native object, and native cleanup runs when the last
owner drops. Several threads can offer on one regular publication at once.

`AeronSubscription` is `Send` but not `Sync`, so one thread at a time can poll it.
`AeronExclusivePublication` and `AeronImage` are neither `Send` nor `Sync`, so the
compiler rejects moving them to another thread. Move the client into the thread
instead and create the exclusive publication there. Polls take `&mut self`, see
[re-entrant calls](thread-safety.md#re-entrant-calls).

```rust,no_run
use rusteron_client::*;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let client = Aeron::connect_default()?;
let timeout = Duration::from_secs(5);

// One regular publication, shared by every producer thread
let publication = Arc::new(client.add_publication(c"aeron:ipc", 1001, timeout)?);
let producers: Vec<_> = (0..4u8)
    .map(|id| {
        let publication = Arc::clone(&publication);
        thread::spawn(move || loop {
            // Wait for the subscriber so no message is dropped
            if !publication.is_connected() {
                std::hint::spin_loop();
                continue;
            }
            match publication.offer(&[id]) {
                Ok(_) => return Ok(()),
                Err(error) if error.is_retryable() => std::hint::spin_loop(),
                Err(error) => return Err(error),
            }
        })
    })
    .collect();

// The subscription is created on the thread that polls it
let consumer_client = client.clone();
let consumer = thread::spawn(move || -> Result<usize, AeronCError> {
    let mut subscription = consumer_client
        .async_add_subscription(c"aeron:ipc", 1001, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(timeout)?;
    let mut received = 0;
    while received < 4 {
        received += subscription.poll_fn(|_message, _header| {}, 10)? as usize;
    }
    Ok(received)
});

for producer in producers {
    producer.join().expect("producer panicked")?;
}
consumer.join().expect("consumer panicked")?;
# Ok(()) }
```

Put callback state shared with another thread in a `SharedHandler`, which
locks a mutex on each access. State used only by the polling thread
can stay on that thread's stack and be passed as `&mut callback`, with no lock.
See [retained callbacks](#retained-callbacks) and
[thread safety](thread-safety.md).

## Building channel URIs

Prefer the typed [`AeronUriStringBuilder`] over hand-written URI strings. Typed
setters catch misspelled keys and malformed values before they reach the driver:

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
`untethered_linger_timeout` takes nanoseconds and sets an untethered subscription's
linger timeout. It is separate from publication `linger`.

Use `ipc()`, `udp(endpoint)` or `udp_control(control, ControlMode)` to create a builder.
It also has about 50 typed setters, including `session_id`, `mtu_length`,
`term_length`, `fc`, `gtag` and `reliable`. See
`examples/multi_destination_subscription.rs` and `examples/request_response.rs`.

The `*_duration` URI helpers, such as `linger_timeout_duration`, accept
`Duration` and check that the nanosecond value fits Aeron's signed 64-bit range.

Response channels can also set `session_id`. Use the same session ID on the response
subscription and publication. `examples/request_response.rs` shows this.

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
  A successful typed call allocates nothing. Only a failed call builds the error enum.
- For image handlers, pass `Handlers::NONE` when you do not need image callbacks. Applications can use them to log image availability or synchronise startup. Aeron's `Ping` sample uses one as a latch.

## Idle strategies

Poll loops should back off when a cycle does no work. `idle(work_count)` returns
immediately when work was done and backs off otherwise. For example:

```rust,no_run
# use rusteron_client::{AeronFragmentHandlerCallback, AeronSubscription, AeronCError, Handler};
# fn snippet<H: AeronFragmentHandlerCallback>(subscription: &mut AeronSubscription, mut handler: H) -> Result<(), AeronCError> {
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
directly. Keep one strategy instance per loop or thread and pass the work count
after each cycle. When idling only on retries, call `reset()` after a successful
operation so backoff does not carry over to the next message.

Available strategies are [`BusySpinIdleStrategy`], [`YieldingIdleStrategy`],
[`SleepingIdleStrategy`], [`BackoffIdleStrategy`] and [`NoOpIdleStrategy`].
`BusySpinIdleStrategy` occupies a core.
`SleepingIdleStrategy` sleeps for a fixed interval, and
`BackoffIdleStrategy` progresses through spinning, yielding and sleeping.

`BackoffIdleStrategy` is the default. It uses Aeron's C and Java backoff:
10 spins, 20 yields, then parks from 1 µs to 1 ms.
Set the conductor's idle strategy on the context with
`ctx.set_idle_strategy_kind(AeronIdleStrategyKind::Backoff)?`.
This also sets the matching initialisation arguments. The media driver's
per-agent setters, such as `AeronDriverContext::set_conductor_idle_strategy_kind`,
work the same way.

For recording, replay and persistent subscriptions, see [`rusteron-archive`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/README.md#persistent-subscriptions).

---

## C strings without hidden allocations

Generated channel and URI arguments use `&CStr`, as the C API does. Constructing
a dynamic C string allocates at the call site. Reusing a `&CStr` does not allocate.

A few hand-written convenience helpers take `&str` and allocate on each call:
`AeronArchive::connect`, the persistent-subscription builder's `live_channel` and
`replay_channel`, and the replication params builder's `replication_channel` and
`src_response_channel`. Call these during setup, outside the hot path.

String getters return borrowed `&str`. Immutable native text has safe getters.
Borrowing replaceable native text requires `unsafe`, because a setter or native
operation can invalidate it. Keep its owner alive and unchanged for every
use of the borrow. Use the `_owned` getter when you need a copy. See [borrowed strings and allocations](migrating_0.2_to_0.3.md#borrowed-strings-and-allocations).

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
C sources with `-fsanitize=address`. It detects use-after-free and
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

Keep `detect_leaks=0` when a media driver runs in the same process.
Select other sanitizers, such as `thread` or `undefined`, with the
`RUSTERON_SANITIZE` environment variable. In this repository, `just test-asan`
runs the client suite with AddressSanitizer.
