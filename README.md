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

- Be as safe as the Aeron Java and C++ clients, at the same cost. Rusteron adds a
  check only when it costs nothing at run time, such as a lifetime or a borrow rule.
  It does not add run-time validation, locks or copies that Java and C++ do not have
- Pass calls through to Aeron C. If Aeron C accepts an invalid argument or state,
  report it to [Aeron](https://github.com/real-logic/aeron/issues) so every client
  gets the fix, rather than adding a workaround here
- Document the rules Aeron leaves to the caller, such as not polling an image from
  inside a poll on the same image

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
| [rusteron-media-driver](rusteron-media-driver/README.md) | Run an embedded or standalone C Media Driver |
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
| Dynamic, from source | Default | C build tools and Java; shared libraries available at runtime |
| Static, from source | `static` | C build tools and Java |
| Precompiled static | `static`, `precompile` | Available for macOS and Linux; no CMake or Java |
| Precompiled static, rustls download | `static`, `precompile-rustls` | Same bundles, with a rustls downloader |

Linux also needs the native link dependencies listed in [BUILD.md](BUILD.md).
Replace `rusteron-client` with another crate as needed.

With a Media Driver running, connect and create a publication:

```rust,no_run
use rusteron_client::*;
use std::time::Duration;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let context = AeronContext::new()?;
context.set_error_handler(Some(AeronErrorHandlerLogger))?;
let client = Aeron::new(&context)?;
client.start()?;

let publication = client
    .async_add_publication(c"aeron:ipc", 123)?
    .poll_blocking(Duration::from_secs(5))?;

match publication.offer(b"hello") {
    Ok(position) => println!("published at {position}"),
    Err(error) if error.is_retryable() => { /* retry in the next duty cycle */ }
    Err(error) => return Err(error.into()),
}
# Ok(()) }
```

`poll_blocking` waits for registration during setup. In an event loop, call the
async registration's `poll()` yourself. See the [client quick start](rusteron-client/README.md#minimal-pubsub)
for both publishing and receiving.

## Ownership and threading

| Topic | Rule |
| --- | --- |
| Owning handles | Clones share native ownership where `Clone` is supported; final cleanup waits for the last owner |
| Borrowed getters | `Ref<'_, T>` borrows its parent and cannot be cloned or outlive that parent |
| Buffer claims | `try_claim_owned()` returns a lifetime-bound guard; commit it or let drop abort it |
| Shared handles | `Aeron`, regular publications, counters and counter readers are `Send + Sync`; share one through `Arc` or a clone ([example](docs/client-guide.md#sharing-across-threads)) |
| Local handles | Exclusive publications, images, contexts, async pollers, assemblers, Archive and driver handles stay local; create them on the thread that uses them |
| Subscriptions | `Send` but not `Sync` and not `Clone`; move one to the thread that polls it |
| Configuration | Configure contexts before creating their clients or starting their drivers |

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
| Share a local callback | `Handler::new(callback)` |
| Register a callback that may run on the conductor | Pass an owned callback to its setter, or use `SharedHandler::new_shared(callback)` |

Registered callbacks remain alive while C can call them. Dropping your handler
does not unregister it. See the [client callback guide](docs/client-guide.md#handlers-and-callbacks).

### Panics in callbacks

Synchronous polls return panics to the polling caller. Retained callbacks log
panics and continue. Message redelivery depends on the callback type; see
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
