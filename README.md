# Rusteron

[![Crates.io](https://img.shields.io/crates/v/rusteron-archive)](https://crates.io/crates/rusteron-archive)
[![CI](https://github.com/gsrxyz/rusteron/actions/workflows/ci.yml/badge.svg)](https://github.com/gsrxyz/rusteron/actions/workflows/ci.yml)
[![API Docs](https://docs.rs/rusteron-archive/badge.svg)](https://docs.rs/rusteron-archive/)
[![GitHub API Docs](https://custom-icon-badges.demolab.com/badge/githubdocs-blue.svg?logo=log&logoSource=feather)](https://gsrxyz.github.io/rusteron)

Rusteron provides fast, idiomatic Rust wrappers over the [Aeron](https://github.com/real-logic/aeron) C API
for IPC and UDP messaging, recording, replay and replication. It exposes the C API with
minimal abstraction, optimised for production use in latency-sensitive environments,
and manages resource ownership and callback lifetimes.

## Design goals

- Match the safety and cost of the Aeron Java and C++ clients. Prefer compile-time
  checks such as lifetimes and borrow rules. Avoid runtime validation, locks and
  copies that those clients do not have.
- Pass calls through to Aeron C. If Aeron C accepts an invalid argument or state,
  report it to [Aeron](https://github.com/real-logic/aeron/issues) so every client
  gets the fix, rather than adding a workaround here.
- Document the rules Aeron leaves to the caller, such as not polling an image from
  inside a poll on the same image.
- Keep Aeron's names, so Aeron's own documentation applies. Applications still
  follow Aeron's ownership, thread-safety and polling rules. To check whether an
  issue is in a wrapper, reproduce it against the C API.

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.
Rusteron has been running in production in GSR's low-latency trading system since the beginning of 2025. It is mature and battle tested.
It is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

[Getting started](#getting-started) · [Examples](#examples) ·
[Migration to 0.3](docs/migrating_0.2_to_0.3.md) · [Guides](#documentation-and-guides)

## Choose a crate

| Crate | Use it for |
| --- | --- |
| [rusteron-client](rusteron-client/README.md) | Connect, publish and subscribe over IPC or UDP |
| [rusteron-archive](rusteron-archive/README.md) | The transport client plus recording, replay, replication and persistent subscriptions |
| [rusteron-media-driver](rusteron-media-driver/README.md) | Run an embedded or standalone C media driver, and extend it in Rust |
| [rusteron-code-gen](rusteron-code-gen/README.md) | Generate bindings and wrappers; an internal build dependency |

`rusteron-archive` includes the transport APIs, so applications using it do not
need a separate `rusteron-client` dependency.

## Getting started

The examples use the 0.3 API. Existing 0.2 applications should
follow the [migration guide](docs/migrating_0.2_to_0.3.md).

For a static build:

```toml
[dependencies]
rusteron-client = { version = "0.3", features = ["static"] }
```

| Build | Features | Requirements |
| --- | --- | --- |
| Dynamic, from source | Default | C build tools; Java for Archive; shared libraries available at runtime |
| Static, from source | `static` | C build tools; Java for Archive |
| Precompiled static | `static`, `precompile` | Available for macOS and Linux; no CMake or Java |
| Precompiled static, rustls download | `static`, `precompile-rustls` | Same bundles, with a rustls downloader |

Linux also needs the native link dependencies listed in [BUILD.md](BUILD.md).
Replace `rusteron-client` with another crate as needed.

This example launches an embedded Media Driver, then publishes and receives one
message. The embedded driver comes from `rusteron-media-driver`. In production, run
the driver as its own process and connect with `Aeron::connect_dir(dir)`.
It also needs `rusteron-media-driver = { version = "0.3", features = ["static"] }`.

```rust
use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::time::Duration;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let driver = EmbeddedDriver::launch()?;
let context = AeronContext::new()?;
context.set_dir(&driver.dir().into_c_string())?;
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

let mut idle = BackoffIdleStrategy::new();
loop {
    match publication.offer(b"hello") {
        Ok(position) => break println!("published at {position}"),
        Err(error) if error.is_retryable() => idle.idle(0), // not connected yet or back pressure
        Err(error) => return Err(error.into()),
    }
}

let mut received = None;
while received.is_none() {
    let fragments = subscription.poll_fn(|message, _header| received = Some(message.to_vec()), 10)?;
    idle.idle(fragments);
}
assert_eq!(received.as_deref(), Some(&b"hello"[..]));
# Ok(()) }
```

Without `set_error_handler`, Aeron C's default handler prints an asynchronous
client error to stderr and exits the process.

`poll_blocking` busy-polls the calling thread until the registration completes.
It keeps setup code and examples short. In an event loop, call the async
registration's `poll()` yourself. See the
[non-blocking publisher](rusteron-client/examples/non_blocking_publisher.rs).

## Ownership and threading

| Topic | Rule |
| --- | --- |
| Owning handles | Clones share native ownership where `Clone` is supported; final cleanup waits for the last owner |
| Borrowed getters | `Ref<'_, T>` borrows its parent and cannot be cloned or outlive that parent |
| Buffer claims | `try_claim_owned()` returns a lifetime-bound guard; commit it or let drop abort it |
| Shared handles | `Aeron`, regular publications, counters and counter readers are `Send + Sync`; share one through `Arc` or a clone ([example](docs/client-guide.md#sharing-across-threads)) |
| Local handles | Exclusive publications, images, contexts, async pollers, assemblers, Archive and driver handles stay local; create them on the thread that uses them |
| Subscriptions | `Send` but not `Sync` and not `Clone`; move one to the thread that polls it |
| Configuration | Configure contexts before `Aeron::new` or `AeronDriver::new` |

There is no `multi-threaded` feature. Storage and thread traits are selected at
compile time. Publishing and raw polling do not clone an `Arc` or dispatch through
a Rust trait object.

See [owning and borrowed handles](docs/migrating_0.2_to_0.3.md#owning-and-borrowed-handles) and
[thread safety](docs/thread-safety.md) for the full rules.

## Callbacks

| Need | API |
| --- | --- |
| Poll fragments with local state | `subscription.poll_fn(closure, limit)` |
| Receive complete messages | `FragmentAssembler::new(callback)` for a fixed callback, or `AeronFragmentClosureAssembler::new()` for a closure per poll, then `assembler.poll(...)` |
| Share callback state outside the poll loop | `Handler::new(callback)`. Not used by polls |
| Register a callback that may run on the conductor | Pass an owned callback to its setter, or use `SharedHandler::new_shared(callback)` |

**The poll path uses no `Rc`, `Arc`, `RefCell` or `Mutex`.** `poll(&mut callback, limit)`,
`poll_fn` and the owned assemblers pass Aeron C a plain `&mut` to your callback as
`clientd`. For each fragment, the trampoline casts `clientd` back to `&mut F` and calls
it. Rusteron adds no lock, borrow check, reference count or allocation per fragment or
per poll. `Handler` (`Rc<RefCell<T>>`) and `SharedHandler` (`Arc<Mutex<T>>`) exist only for
callbacks that C keeps after the call returns, such as error, available image,
unavailable image and close handlers. The conductor calls those on events, not per
message.

Registered callbacks remain alive while C can call them. Dropping your handler
does not unregister it. See the [client callback guide](docs/client-guide.md#handlers-and-callbacks).

Fragment assemblers allocate a buffer for fragmented messages and grow it when a
larger message arrives. Create them once and reuse them.

### Panics in callbacks

Synchronous polls return panics to the polling caller. Retained callbacks log
panics and continue. Media driver extensions abort on panic. Message redelivery
depends on the callback type. See
[callback panics](docs/callback-panics.md) before relying on recovery behaviour.

## Examples

| Example | Shows |
| --- | --- |
| [Basic pub/sub](rusteron-client/examples/basic_pub_sub.rs) | Connect, publish and assemble messages |
| [Non-blocking publisher](rusteron-client/examples/non_blocking_publisher.rs) | Drive setup from an event loop |
| [Zero-copy claim](rusteron-client/examples/zero_copy_claim.rs) | Claim, write and commit a message |
| [Record and replay](rusteron-archive/examples/record_and_replay.rs) | Record a stream and replay its history |
| [Persistent subscription](rusteron-archive/examples/persistent_subscription.rs) | Replay history and join live traffic |
| [Docker samples](rusteron-docker-samples/README.md) | Run a driver, publisher and subscriber |

## Documentation and guides

- [Migrate from 0.2 to 0.3](docs/migrating_0.2_to_0.3.md)
- [Client guide](docs/client-guide.md) and [Archive guide](docs/archive-guide.md)
- [Build requirements](BUILD.md) and [CPU targets and cross-language LTO](docs/cross-language-lto.md)
- [Media Driver configuration and back pressure](docs/media_driver_configuration_and_backpressure_guide.md)
- [Media driver extensions in Rust](docs/driver-extensions.md)
- [Multi-destination subscriptions](docs/mdc_mds_guide.md)
- [Tuning on x86-64 Linux](docs/tuning_x86_64_linux.md)
- [How to benchmark and measured results](BENCHMARKS.md)
- Earlier releases: [What's new in 0.2](docs/whats_new_0.2.md), [0.1-to-0.2 migration](docs/migrating_0.1.168_to_0.2.md)

## Development

Run `just` to list build and test tasks. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the contribution workflow and required checks.

## License

Dual-licensed under MIT or Apache-2.0.

## Acknowledgements

- [@mimran1980](https://github.com/mimran1980), Rusteron's original creator at GSR.
- [@bspeice](https://github.com/bspeice), for the original [libaeron-sys](https://github.com/bspeice/libaeron-sys).
- The [Aeron](https://github.com/real-logic/aeron) community.
