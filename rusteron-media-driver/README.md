# rusteron-media-driver

rusteron-media-driver is a Rust interface to the Aeron Media Driver, which manages low-latency messaging between producers and consumers. It is part of the [Rusteron](https://github.com/gsrxyz/rusteron) project and supports both standalone and embedded drivers.

> For production deployments, we recommend the Aeron Java or C media driver.
> The embedded version here is best suited to integration tests and lightweight environments.

---

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It runs in GSR's real-time trading stack and is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

---

## Installation

To use `rusteron-media-driver`, add the appropriate dependency to your `Cargo.toml`:

<details>
<summary>Dynamic</summary>

```toml
[dependencies]
rusteron-media-driver = "0.2"
```

</details>

<details>
<summary>Static</summary>

```toml
[dependencies]
rusteron-media-driver = { version = "0.2", features = ["static"] }
```

</details>

<details>
<summary>Static with precompiled C libs (macOS / Linux)</summary>

```toml
[dependencies]
rusteron-media-driver = { version = "0.2", features = ["static", "precompile"] }
```

```toml
[dependencies]
rusteron-media-driver = { version = "0.2", features = ["static", "precompile-rustls"] }
```

</details>

Make sure the Aeron C libraries are installed and available on your system.

---

## Usage examples

<details>
<summary>Standard Media Driver</summary>

```rust,no_run
// A standalone media driver; `cargo run -p rusteron-media-driver --bin media_driver` ships the same.
use rusteron_media_driver::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = AeronDriverContext::new()?;
    context.set_dir(c"target/aeron")?;
    let driver = AeronDriver::new(&context)?;
    driver.start(true)?; // run the conductor duty cycle on this thread
    println!("media driver running in {}", context.get_dir());
    loop {
        driver.main_idle_strategy(driver.main_do_work()?);
    }
}
```

</details>

<details>
<summary>Embedded Media Driver</summary>

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

For a caller-built `AeronDriverContext`, `AeronDriver::launch_embedded_guard(ctx, false)` is the RAII form (stops and joins on drop).

</details>

---

## Building C against Aeron's headers

Some driver extensions are C compiled against Aeron's own headers. The main one is a custom
UDP transport, such as kernel bypass with DPDK or ef_vi. It fills in Aeron's
`aeron_udp_channel_transport_bindings_t` and is installed with
`AeronDriverContext::set_udp_channel_transport_bindings`. That C must see the headers of the
Aeron this crate builds, or the structs it fills in will not match the driver's, and those
headers exist only in this crate's vendored Aeron sources. So the crate declares
`links = "aeron_driver"`, and Cargo hands their paths to the build script of every crate that
depends on it directly:

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

The key has one cost. Cargo allows a single package per `links` name in a build, so two
semver-incompatible versions of `rusteron-media-driver` cannot share a dependency graph. The
workspace crate [`links-test`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-media-driver/links-test) compiles a C file against these paths, so the
export cannot break unnoticed.

## Contributing & license

See the root [README](https://github.com/gsrxyz/rusteron#readme) and [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md). Build requirements are in [BUILD.md](https://github.com/gsrxyz/rusteron/blob/main/BUILD.md).
Dual-licensed under MIT or Apache-2.0.

---

## Acknowledgments

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron. Your work made this possible.
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for the open protocol
