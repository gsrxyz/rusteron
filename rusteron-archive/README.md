# rusteron-archive

Fast, idiomatic Rust for the Aeron transport client and Aeron Archive, using the C
API directly. Publish and subscribe over IPC or UDP, record streams, replay
history, replicate recordings, and join live traffic with persistent subscriptions.

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.
Rusteron has been running in production in GSR's low-latency trading system since the beginning of 2025. It is mature and battle tested.
It is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

[Getting started](#getting-started) · [Examples](#examples) ·
[Persistent subscriptions](#persistent-subscriptions) ·
[Migration to 0.3](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.2_to_0.3.md)

## Installation

The Archive crate includes the transport APIs,
so you do not need a separate `rusteron-client` dependency.

```toml
[dependencies]
rusteron-archive = { version = "0.3", features = ["static"] }
```

Use the default features for dynamic linking. On macOS and Linux, add `precompile`
or `precompile-rustls` alongside `static` to use prebuilt C libraries without CMake
or Java. See [BUILD.md](https://github.com/gsrxyz/rusteron/blob/main/BUILD.md)
for native link and source-build requirements.

## Getting started

Run an Archive server and its Media Driver first. Configure the control channels
to match that server:

```rust,no_run
use rusteron_archive::*;
use std::time::Duration;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let client_context = AeronContext::new()?;
client_context.set_error_handler(Some(AeronErrorHandlerLogger))?;
let client = Aeron::new(&client_context)?;
client.start()?;

let context = AeronArchiveContext::new()?;
context.set_control_request_channel(c"aeron:udp?endpoint=localhost:8010")?;
context.set_control_response_channel(c"aeron:udp?endpoint=localhost:0")?;
context.set_error_handler(Some(AeronErrorHandlerLogger))?;
let archive = AeronArchiveAsyncConnect::new_with_aeron(&context, &client)?
    .poll_blocking(Duration::from_secs(5))?;

let client_id = archive.aeron().client_id();
println!("connected with transport client {client_id}");
# Ok(()) }
```

The archive retains the supplied client. Dropping your original client handle
does not invalidate the connection. Use the original owning client when creating
publications or subscriptions. `archive.aeron()` is a borrowed view.

## Typical workflow

1. Start recording a channel and stream.
2. Publish messages, then wait for the recording position to catch up.
3. Stop recording and find its recording ID.
4. Start a replay and poll the replay subscription.

The [record-and-replay example](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/record_and_replay.rs)
shows the complete workflow:

```sh
cargo run -p rusteron-archive --release --features static,precompile --example record_and_replay
```

## General patterns

| Topic | Rule |
| --- | --- |
| Owning handles | Clones share native ownership where supported; cleanup waits for the last owner |
| Archive context | Not cloneable; configure it before connecting |
| Borrowed getters | `get_archive_context()`, `aeron()` and poller getters return lifetime-bound `Ref` views |
| Retained children | `aeron_owned()` or `retain_archive_context()` retain the archive; call `as_ref()` to use the child |
| Threading | Archive handles stay local; the transport client's shared types retain their own `Send + Sync` rules |
| Errors | Control requests return `AeronArchiveError`; setup and context operations return `AeronCError` |

See [owning and borrowed handles](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.2_to_0.3.md#owning-and-borrowed-handles)
and the [Archive guide](https://github.com/gsrxyz/rusteron/blob/main/docs/archive-guide.md).

## Handlers and errors

Pass an owned callback to a context setter. For synchronous transport polling,
use `poll_fn` with a stack closure. `Handlers::NONE` fills an unused callback slot.

Archive control errors use native numeric codes. Match on `error.code` or use
`AeronArchiveErrorCode::from_native_code(code)` in an Archive error handler.
`poll_for_error()` returns unsolicited errors with their typed code and full message.
A blocking Archive request fails after the message timeout, 10 s by default.
The setting is `AERON_ARCHIVE_MESSAGE_TIMEOUT`. Change it with
`AeronArchiveContext::set_message_timeout(duration)`.

### Why replaced handlers stay alive

A connection keeps the handlers that its context held when the connection started.
Replacing an unused context handler releases it, but does not change an existing
connection. Configure an owning context before connecting. See
[handler ownership](https://github.com/gsrxyz/rusteron/blob/main/docs/archive-guide.md#why-replaced-handlers-stay-alive).

### Changing a callback without registering another handler

For simple updates, keep the setter's returned `SharedHandler` and use
`handler.lock()`. This acquires a mutex shared with callback invocations, so keep
updates short. Transport `poll_fn` and owned assemblers use direct Rust borrowing
without a callback mutex. See
[synchronisation costs](https://github.com/gsrxyz/rusteron/blob/main/docs/thread-safety.md#hot-path-behaviour).
Use `AeronErrorHandlerSlot` when replacement must not wait for an active callback:

```rust
use rusteron_archive::{AeronArchiveContext, AeronErrorHandlerSlot};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let context = AeronArchiveContext::new()?;
let slot = AeronErrorHandlerSlot::new();
context.set_error_handler(Some(slot.clone()))?;
slot.set(|code, message| eprintln!("archive {code}: {message}"));
slot.clear();
# Ok(()) }
```

Slots synchronise callback access. For their costs and replacement rules, see the
[callback guide](https://github.com/gsrxyz/rusteron/blob/main/docs/archive-guide.md#changing-a-callback-without-registering-another-handler).
The [direct-handler alternative](https://github.com/gsrxyz/rusteron/blob/main/docs/archive-guide.md#replacing-callback-state-through-its-handler)
uses fewer locks when waiting for an active callback is acceptable.

## Duty cycle

| Object | Drive it with |
| --- | --- |
| Transport client | Its background conductor by default |
| Archive | `archive.do_work()` for signals and unsolicited errors |
| Persistent subscription | `subscription.poll_fn(...)`; it drives its own Archive client |
| Replay merge | `merge.poll_fn(...)`; do not also drain responses from its Archive client while merging |

Call the manual conductor entry points, such as `main_do_work()`, from one thread
at a time. See the [duty-cycle guide](https://github.com/gsrxyz/rusteron/blob/main/docs/archive-guide.md#duty-cycle).

## Persistent subscriptions

A persistent subscription replays recorded history, then joins the live stream.
Create one with `PersistentSubscriptionBuilder::new_with_aeron(&context, &client)`.
Poll it until it is live, and check `has_failed()` for terminal failures.
`PersistentSubscriptionBuilder::new()` without a client makes the subscription
create its own client, set it on the archive context and close it when the
subscription closes. Do not use that archive context again afterwards.

It reassembles fragments itself, so poll it with `ps.poll_fn(closure, limit)` or
`ps.controlled_poll_fn(closure, limit)`. The closure gets whole messages. Do not
wrap it in a fragment assembler. Wrapping still works, for example
`ps.poll(&mut assembler.with(closure), limit)`, but it only adds a call per message.
See the [complete guide](https://github.com/gsrxyz/rusteron/blob/main/docs/archive-guide.md#persistent-subscriptions)
for the builder, listener and failure handling.

## Examples

| Example | Shows |
| --- | --- |
| [Record and replay](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/record_and_replay.rs) | Record, find and replay a stream |
| [Persistent subscription](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/persistent_subscription.rs) | Replay history and join live traffic |
| [Persistent subscription failover](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/persistent_subscription_failover.rs) | Leave and rejoin a live stream |
| [Replay merge](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/replay_merge.rs) | Merge replayed history onto a live stream |
| [Archive errors](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/archive_error_handling.rs) | Typed errors, signals and reconnects |
| [Replication](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/recording_replication.rs) | Copy a recording between archives |
| [Replaceable callback](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/examples/replaceable_error_handler.rs) | Change callbacks without registering again |

## Documentation and guides

<a id="documentation--guides"></a>

- [Migration from 0.2 to 0.3](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.2_to_0.3.md)
- [Detailed Archive guide](https://github.com/gsrxyz/rusteron/blob/main/docs/archive-guide.md), including replay limits and empty recordings
- [Transport client](https://github.com/gsrxyz/rusteron/blob/main/rusteron-client/README.md)
- [Thread safety](https://github.com/gsrxyz/rusteron/blob/main/docs/thread-safety.md) and [callback panics](https://github.com/gsrxyz/rusteron/blob/main/docs/callback-panics.md)
- [How to benchmark and measured results](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md)

## Contributing and license

See [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md).
Dual-licensed under MIT or Apache-2.0. Thanks to Rusteron's creator
[@mimran1980](https://github.com/mimran1980),
[@bspeice](https://github.com/bspeice) for the original
[libaeron-sys](https://github.com/bspeice/libaeron-sys), and the Aeron community.
