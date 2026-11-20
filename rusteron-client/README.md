# rusteron-client

Fast, idiomatic Rust for Aeron IPC and UDP messaging, using the Aeron C API
directly. Connect to a Media Driver, publish messages and poll subscriptions.

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.
Rusteron has been running in production in GSR's low-latency trading system since the beginning of 2025. It is mature and battle tested.
It is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

[Quick start](#minimal-pubsub) · [Sending](#errors-and-offer-results) ·
[Callbacks](#handlers-and-callbacks) ·
[Migration to 0.3](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.2_to_0.3.md)

## Installation

Add a static client dependency:

```toml
[dependencies]
rusteron-client = { version = "0.3", features = ["static"] }
```

Use the default features for dynamic linking. On macOS and Linux, add `precompile`
or `precompile-rustls` alongside `static` to use prebuilt C libraries without CMake
or Java. See [BUILD.md](https://github.com/gsrxyz/rusteron/blob/main/BUILD.md)
for source-build and native link requirements.

## Minimal pub/sub

Start a Media Driver before running this example.

```rust,no_run
use rusteron_client::*;
use std::time::{Duration, Instant};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let context = AeronContext::new()?;
context.set_error_handler(Some(AeronErrorHandlerLogger))?;
let client = Aeron::new(&context)?;
client.start()?;

let timeout = Duration::from_secs(5);
let subscription = client
    .async_add_subscription(c"aeron:ipc", 123, Handlers::NONE, Handlers::NONE)?
    .poll_blocking(timeout)?;
let publication = client
    .async_add_publication(c"aeron:ipc", 123)?
    .poll_blocking(timeout)?;

let deadline = Instant::now() + timeout;
let mut idle = BackoffIdleStrategy::new();
loop {
    match publication.offer(b"hello") {
        Ok(_) => break,
        Err(error) if error.is_retryable() && Instant::now() < deadline => idle.idle(0),
        Err(error) => return Err(error.into()),
    }
}

let mut received = false;
while !received {
    if Instant::now() >= deadline {
        return Err("timed out waiting for a message".into());
    }
    let fragments = subscription.poll_fn(|message, _header| {
        println!("received {} bytes", message.len());
        received = true;
    }, 10)?;
    idle.idle(fragments);
}
# Ok(()) }
```

`poll_blocking` busy-polls the calling thread until the registration completes. It
keeps setup code and examples short. In an event loop, call the async
registration's `poll()` yourself. See the
[non-blocking publisher](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/non_blocking_publisher.rs).

`Aeron::connect_default()` and `Aeron::connect_dir(dir)` build the context and
client and start the conductor in one call. Build the `AeronContext` yourself to
set an error handler, driver timeout or idle strategy. Without an error handler,
Aeron C prints an asynchronous client error to stderr and exits the process.

## Errors and offer results

<a id="errors--offer-results"></a>

| Send API | Result | Use |
| --- | --- | --- |
| `offer(message)` | `Result<i64, AeronOfferError>` | Copy one message into the log |
| `offer_parts(parts)` | `Result<i64, AeronOfferError>` | Send several slices as one message |
| `try_claim_owned(length)` | `Result<AeronClaim<'_>, AeronOfferError>` | Write directly into a claimed log region |
| `offer_raw` / `try_claim_raw` | Native `i64` position or error sentinel | Handle native status codes yourself |

For typed errors, retry when `error.is_retryable()` is true. Use an idle strategy
when no progress is made. Set a client error handler to receive asynchronous faults.

A claim borrows its publication and aborts on drop unless committed:

```rust,no_run
# use rusteron_client::*;
# fn write(publication: &AeronPublication) -> Result<(), Box<dyn std::error::Error>> {
let mut claim = publication.try_claim_owned(5)?;
claim.data().copy_from_slice(b"hello");
claim.commit()?;
# Ok(()) }
```

See the [sending and error guide](https://github.com/gsrxyz/rusteron/blob/main/docs/client-guide.md#errors-and-offer-results)
for retry loops and reserved values.

## Handlers and callbacks

| Need | API |
| --- | --- |
| Reusable callback, borrowed exclusively | `subscription.poll(&mut callback, fragment_limit)` |
| Raw fragments, with a stack closure | `subscription.poll_fn(callback, fragment_limit)` |
| Complete messages | `FragmentAssembler::new(callback)` |
| Controlled message delivery | `ControlledFragmentAssembler::new(callback)` |
| Callback state shared with code outside the poll loop | `Handler::new(callback)`; read through `borrow()` or `lock()`. Not used by polls |
| Conductor callback | Owned callback setter, or `SharedHandler::new_shared(callback)` |
| No optional callback | `Handlers::NONE` |

**The poll path uses no `Rc`, `Arc`, `RefCell` or `Mutex`.** `poll(&mut callback, limit)`,
`poll_fn` and the owned assemblers pass Aeron C a plain `&mut` to your callback as
`clientd`. For each fragment, the trampoline casts `clientd` back to `&mut F` and calls
it. Rusteron adds no lock, borrow check, reference count or allocation per fragment or
per poll. `Handler` (`Rc<RefCell<T>>`) and `SharedHandler` (`Arc<Mutex<T>>`) exist only for
callbacks that C keeps after the call returns, such as error, available image,
unavailable image and close handlers. The conductor calls those on events, not per
message.

On latency-sensitive receive threads, use `poll(&mut callback, limit)`,
`poll_fn` or an owned assembler. Local `Handler::lock()` and `borrow()` use `RefCell`
borrow checks, with no mutex or atomic operation. Polls never take a `Handler`.
`SharedHandler::lock()` takes a mutex and can wait for an active callback.
Shared callbacks take the same mutex when invoked. See [synchronisation costs](https://github.com/gsrxyz/rusteron/blob/main/docs/thread-safety.md#hot-path-behaviour).

Create an assembler once and reuse it. `AeronFragmentClosureAssembler` takes a new
closure on each poll, and `FragmentAssembler::new(callback)` owns one fixed callback.
This example uses the closure assembler:

```rust,no_run
# use rusteron_client::*;
# fn receive(subscription: &AeronSubscription) -> Result<(), AeronCError> {
let mut assembler = AeronFragmentClosureAssembler::new()?;
assembler.poll(subscription, |message, _header| {
    println!("received a complete {}-byte message", message.len());
}, 64)?;
# Ok(()) }
```

### Callback arguments are borrowed

Callback buffers, headers and image views are valid only during the callback.
Copy the fields you need to keep. See the
[callback guide](https://github.com/gsrxyz/rusteron/blob/main/docs/client-guide.md#callback-arguments-are-borrowed)
for the borrowed wrapper types.

### Why handlers stay alive until the client closes

The conductor can call a registered handler after its subscription closes.
Rusteron keeps the handler alive through native client cleanup. Dropping your
handler does not unregister it. For retention rules and replaceable callbacks,
see the [handler lifetime guide](https://github.com/gsrxyz/rusteron/blob/main/docs/client-guide.md#why-handlers-stay-alive-until-the-client-closes).

## General patterns

<a id="resource-lifecycle--close"></a>

### Resource lifecycle and close()

- Owning handles share native ownership where `Clone` is supported. `close(self)`
  releases that owner; native cleanup waits for the last owner, including children.
- Getters return lifetime-bound `Ref` views. These views cannot be cloned or outlive
  their parent. `as_ref()` creates another short borrow.
- Contexts are configured before native initialisation. Later changes are rejected.
- `close_with_handler(handler)` closes like `close(self)` and calls the handler
  from the conductor once the close finishes.
- `image_at_index(i)` and `image_by_session_id(id)` return a retained
  `Option<AeronImage>`. It keeps reading after its publisher goes away, when
  `is_closed()` turns true, and after the subscription handle drops. Drop the two in
  any order.
- Raw-pointer construction, native struct references and forced cleanup require
  `unsafe` and the documented caller guarantees.

See [owning and borrowed handles](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.2_to_0.3.md#owning-and-borrowed-handles)
and [thread safety](https://github.com/gsrxyz/rusteron/blob/main/docs/thread-safety.md).

## Building channel URIs

Use `c"aeron:ipc"` for constant channels, `cformat!` for dynamic C strings, or
`AeronUriStringBuilder` for typed URI parameters. Build dynamic strings outside
the polling loop and reuse them. Duration helpers check for overflow.

See [channel examples](https://github.com/gsrxyz/rusteron/blob/main/docs/client-guide.md#building-channel-uris).

## Idle strategies

Call `idle.idle(work_count)` after a polling cycle. `BackoffIdleStrategy` moves
through spinning, yielding and sleeping. `BusySpinIdleStrategy`,
`YieldingIdleStrategy`, `SleepingIdleStrategy` and `NoOpIdleStrategy` support other
workloads. Use these strategies in retry and poll loops instead of direct CPU
pause hints. Keep one instance per loop or thread. When idling only on retries, reset
backoff after a successful operation. Match the strategy and CPU placement when
comparing benchmarks.

See the [poll-loop guide](https://github.com/gsrxyz/rusteron/blob/main/docs/client-guide.md#idle-strategies).

## Examples

| Example | Shows |
| --- | --- |
| [Basic pub/sub](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/basic_pub_sub.rs) | Publishing and fragment assembly |
| [Non-blocking publisher](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/non_blocking_publisher.rs) | Event-loop setup |
| [Zero-copy claim](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/zero_copy_claim.rs) | Claim, write and commit |
| [Request/response](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/request_response.rs) | Response channels |
| [File transfer](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/file_transfer.rs) | Chunking and reassembly |
| [Driver stats](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/driver_stats.rs) | Counters and error logs |
| [Basic publisher](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/basic_publisher.rs) and [subscriber](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/basic_subscriber.rs) | Ports of Aeron's `BasicPublisher` and `BasicSubscriber` |
| [Streaming rate](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/streaming_rate.rs) | Streaming publisher and rate-reporting subscriber |
| [Ping/pong](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/embedded_ping_pong.rs) | Round-trip latency histogram, as in Aeron's `cping` and `cpong` |
| [C ping/pong workload](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/aeron_ping_pong.rs) | The `cping`/`cpong` workload for comparing with Aeron C |
| [Exclusive IPC throughput](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/embedded_exclusive_ipc_throughput.rs) | Exclusive publication throughput over IPC |
| [Retained images](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/retained_images.rs) | Keeping and polling an image beyond one poll |
| [Multi-destination subscription](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/examples/multi_destination_subscription.rs) | Manual MDS |

## Documentation and guides

<a id="documentation--guides"></a>
<a id="migrating-01--02"></a>

- [Migration from 0.2 to 0.3](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.2_to_0.3.md)
- [Detailed client guide](https://github.com/gsrxyz/rusteron/blob/main/docs/client-guide.md), including AddressSanitizer
- [Archive recording and replay](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/README.md)
- [Build and LTO settings](https://github.com/gsrxyz/rusteron/blob/main/docs/cross-language-lto.md)
- [How to benchmark](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md#how-to-benchmark)
- [Earlier 0.1-to-0.2 migration](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.1.168_to_0.2.md)

## Contributing and license

<a id="contributing--license"></a>

See [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md).
Dual-licensed under MIT or Apache-2.0. Thanks to Rusteron's creator
[@mimran1980](https://github.com/mimran1980),
[@bspeice](https://github.com/bspeice) for the original
[libaeron-sys](https://github.com/bspeice/libaeron-sys), and the Aeron community.
