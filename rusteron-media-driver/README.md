# rusteron-media-driver

`rusteron-media-driver` is a Rust API over the Aeron C media driver, which manages
low-latency messaging between producers and consumers.

- Run it as a standalone process or embed it in a Rust application.
- Add custom driver behaviour in Rust: flow control, congestion control, packet
  interceptors, name resolution, idle strategies, duty-cycle trackers, port managers
  and native UDP transports. See [Custom extensions](#custom-extensions).
- Start an embedded driver for integration tests with `testing::EmbeddedDriver`.

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.
Rusteron has been running in production in GSR's low-latency trading system since the beginning of 2025. It is mature and battle tested.
It is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

[Installation](#installation) · [Examples](#usage-examples) ·
[UDP tuning](#udp-polling-and-send-batches) · [Extensions](#custom-extensions) ·
[Migration to 0.3](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.2_to_0.3.md)

## Installation

```toml
[dependencies]
rusteron-media-driver = { version = "0.3", features = ["static"] }
```

- `static` embeds the Aeron C libraries. Default features link them dynamically.
- Add `precompile` or `precompile-rustls` to use prebuilt C libraries on macOS and Linux.
- Build prerequisites are in [BUILD.md](https://github.com/gsrxyz/rusteron/blob/main/BUILD.md).

## Usage examples

<details>
<summary>Standalone media driver</summary>

```rust,no_run
// `cargo run -p rusteron-media-driver --bin media_driver` ships the same
use rusteron_media_driver::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = AeronDriverContext::new()?;
    context.set_dir(c"target/aeron")?;
    context.set_threading_mode(AeronThreadingMode::Shared)?;
    let mut driver = AeronDriver::new(&context)?.start_manual()?;
    // SAFETY: printing copies the text before any setter can change it
    println!("media driver running in {}", unsafe { context.get_dir() });
    loop {
        let work = driver.do_work()?;
        driver.idle(work);
    }
}
```

</details>

<details>
<summary>Embedded media driver</summary>

```rust
// Unique directory, stopped and joined on drop
use rusteron_media_driver::testing::EmbeddedDriver;
use rusteron_media_driver::Aeron;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = EmbeddedDriver::launch()?; // or launch_with(|ctx| Ok(()))
    let aeron = Aeron::connect_dir(driver.dir())?;
    drop(aeron); // close the client before the driver stops
    Ok(())
}
```

For your own `AeronDriverContext`, `AeronDriver::launch_embedded_guard(ctx, false)` stops
and joins the driver on drop.

</details>

## UDP polling and send batches

Set these before creating the driver.

```rust
use rusteron_media_driver::{AeronCError, AeronDriverContext};

let context = AeronDriverContext::new()?;
context.set_udp_channel_transport_poller_iteration_threshold(5)?;
context.set_udp_channel_transport_poller_socket_poll_limit(4)?;
context.set_sender_io_vector_capacity(64)?;
context.set_network_publication_max_messages_per_send(4)?;
# Ok::<(), AeronCError>(())
```

- With at most the iteration threshold of sockets, the driver polls each one directly instead
  of using the platform poller.
- The socket poll limit (default 4) caps polls per ready socket per duty cycle.
- A send batch must fit in `sender_io_vector_capacity`, at most 64 messages.

## Custom extensions

Implement a trait or pass a closure, and install it on the context before `AeronDriver::new`.
Callbacks are monomorphised `extern "C"` functions with no allocation, lock or dynamic
dispatch. See [driver extensions](../docs/driver-extensions.md) and the
[examples](examples/extensions/README.md).

```rust,no_run
use rusteron_media_driver::{AeronDriverContext, extensions::*};
let context = AeronDriverContext::new()?;
context.set_idle_strategy(IdleStrategyRole::Shared, |work| {
    if work <= 0 {
        std::hint::spin_loop();
    }
})?;
# Ok::<(), rusteron_media_driver::AeronCError>(())
```

## Custom UDP transports in C

A C transport, such as kernel bypass, fills in `aeron_udp_channel_transport_bindings_t`
and is installed with `NativeTransportBindings::from_callbacks` and
`set_native_transport_bindings`. Its build script gets the vendored Aeron header paths
from `links = "aeron_driver"`:

- `DEP_AERON_DRIVER_INCLUDE`: the media driver headers
- `DEP_AERON_DRIVER_CLIENT_INCLUDE`: the client headers they include
- `DEP_AERON_DRIVER_AERON_ROOT`: the Aeron source tree

```rust,no_run
// build.rs of a crate that depends on rusteron-media-driver
fn main() {
    let dep = |key: &str| std::env::var(format!("DEP_AERON_DRIVER_{key}")).unwrap();
    cc::Build::new()
        .file("transport.c")
        .include(dep("INCLUDE"))
        .include(dep("CLIENT_INCLUDE"))
        .compile("my_transport");
}
```

Cargo allows one package per `links` name, so two semver-incompatible versions of this
crate cannot share a build. See
[`links-test`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-media-driver/links-test)
for a minimal C file.

<a id="contributing--license"></a>

## Contributing and license

See the root [README](https://github.com/gsrxyz/rusteron#readme) and [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md). Build requirements are in [BUILD.md](https://github.com/gsrxyz/rusteron/blob/main/BUILD.md).
Dual-licensed under MIT or Apache-2.0.

---

## Acknowledgements

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron. Your work made this possible.
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for the open protocol
