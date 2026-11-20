# rusteron-media-driver

`rusteron-media-driver` provides a fast, idiomatic Rust API over the Aeron C Media
Driver, which manages low-latency messaging between producers and consumers. Run
it as a standalone process or embed it in a Rust application.

> For production deployments, we recommend the Aeron Java or C media driver.
> The embedded version here is best suited to integration tests and lightweight environments.

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.
Rusteron has been running in production in GSR's low-latency trading system since the beginning of 2025. It is mature and battle tested.
It is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

[Installation](#installation) · [Examples](#usage-examples) ·
[UDP tuning](#udp-polling-and-send-batches) ·
[Migration to 0.3](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.2_to_0.3.md)

## Installation

For a static build:

```toml
[dependencies]
rusteron-media-driver = { version = "0.3", features = ["static"] }
```

Use default features for dynamic linking. Add `precompile` or `precompile-rustls`
alongside `static` to use prebuilt C libraries on macOS and Linux.

The default source build compiles the Aeron C libraries. Dynamic builds also need
those shared libraries available at runtime; `static` embeds them in the binary.
See [BUILD.md](https://github.com/gsrxyz/rusteron/blob/main/BUILD.md) for prerequisites.

---

## Usage examples

<details>
<summary>Standalone media driver</summary>

```rust,no_run
// A standalone media driver; `cargo run -p rusteron-media-driver --bin media_driver` ships the same.
use rusteron_media_driver::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = AeronDriverContext::new()?;
    context.set_dir(c"target/aeron")?;
    // Native enum setters take the Rust names directly
    context.set_threading_mode(AeronThreadingMode::Shared)?;
    let driver = AeronDriver::new(&context)?;
    driver.start(true)?; // run the conductor duty cycle on this thread
    // SAFETY: printing copies the text before any setter can change it
    println!("media driver running in {}", unsafe { context.get_dir() });
    loop {
        driver.main_idle_strategy(driver.main_do_work()?);
    }
}
```

</details>

<details>
<summary>Embedded media driver</summary>

```rust
// Embeds the media driver in this process: unique directory, stopped and joined on drop.
use rusteron_media_driver::testing::EmbeddedDriver;
use rusteron_media_driver::Aeron;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = EmbeddedDriver::launch()?; // or EmbeddedDriver::launch_with(|ctx| { /* tune ctx */ Ok(()) })
    // Applications usually connect with rusteron_client::Aeron::connect_dir(driver.dir()).
    let aeron = Aeron::connect_dir(driver.dir())?;
    // add publications / subscriptions on `aeron` ...
    drop(aeron); // close the client before the driver stops
    Ok(())
}
```

For a caller-built `AeronDriverContext`, use
`AeronDriver::launch_embedded_guard(ctx, false)` to stop and join the driver on drop.

</details>

---

## UDP polling and send batches

Aeron 1.54 lets the C driver configure the threshold for iterating over sockets directly
instead of using the platform poller. It also limits how many times each ready socket is
polled in one duty cycle. Configure these before creating the driver.

```rust
use rusteron_media_driver::{AeronCError, AeronDriverContext};

let context = AeronDriverContext::new()?;
context.set_udp_channel_transport_poller_iteration_threshold(5)?;
context.set_udp_channel_transport_poller_socket_poll_limit(4)?;
context.set_sender_io_vector_capacity(64)?;
context.set_network_publication_max_messages_per_send(4)?;
# Ok::<(), AeronCError>(())
```

The socket poll limit defaults to 4. A send batch must fit within
`sender_io_vector_capacity`. Driver initialisation rejects a larger batch. The
maximum send batch is 64 messages.

---

## Building C against Aeron's headers

Some driver extensions are C compiled against Aeron's own headers. The main one is a custom UDP
transport, such as kernel bypass with DPDK or ef_vi. It fills in Aeron's
`aeron_udp_channel_transport_bindings_t` and is installed with
`AeronDriverContext::set_udp_channel_transport_bindings`. That C must see the headers of the
Aeron this crate builds, and they exist only in this crate's vendored Aeron sources. The
crate declares `links = "aeron_driver"`, so Cargo passes their paths to the build script of
every crate that depends on it directly:

| Variable | Directory |
|---|---|
| `DEP_AERON_DRIVER_INCLUDE` | the media driver's headers |
| `DEP_AERON_DRIVER_CLIENT_INCLUDE` | the client headers they include |
| `DEP_AERON_DRIVER_AERON_ROOT` | the Aeron source tree |

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

Cargo allows one package per `links` name in a build, so two semver-incompatible versions
of `rusteron-media-driver` cannot share a dependency graph. The workspace crate
[`links-test`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-media-driver/links-test)
compiles a C file against these paths.

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
