# Rusteron

[![Crates.io](https://img.shields.io/crates/v/rusteron-archive)](https://crates.io/crates/rusteron-archive)
[![CI](https://github.com/gsrxyz/rusteron/actions/workflows/ci.yml/badge.svg)](https://github.com/gsrxyz/rusteron/actions/workflows/ci.yml)
[![API Docs](https://custom-icon-badges.demolab.com/badge/githubdocs-blue.svg?logo=log\&logoSource=feather)](https://gsrxyz.github.io/rusteron)

> **Rusteron** is a thin, high-performance Rust wrapper over the [Aeron](https://github.com/real-logic/aeron) C API.
> It exposes low-level C bindings with minimal abstraction, optimized for production use in latency-sensitive environments.

---

## Sponsored by GSR

**Rusteron** is proudly sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.

It powers mission-critical infrastructure in GSR's real-time trading stack and is now developed under the official GSR GitHub organization as part of our commitment to open-source excellence and community collaboration.

We welcome contributions, feedback, and discussions. If you're interested in integrating or contributing, please open an issue or reach out directly.

---

## Project Overview

This project builds on a fork of [`libaeron-sys`](https://github.com/bspeice/libaeron-sys), offering Rust access to Aeron’s native C API. The API is **not fully idiomatic**, but is auto-generated for consistency and reliability. This tradeoff supports:

* Performance-sensitive trading environments
* Minimal runtime overhead
* Low maintenance costs

**Warning**: This library operates in an `unsafe` context and requires care. Improper usage (e.g., using a publisher after the Aeron client is closed) can lead to **undefined behavior or segmentation faults**.

---

## Module Overview

| Module                                                                                            | Description                                                                |
| ------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| [`rusteron-code-gen`](https://github.com/gsrxyz/rusteron/tree/main/rusteron-code-gen)             | Code generation engine to produce consistent Rust bindings for Aeron C.    |
| [`rusteron-client`](https://github.com/gsrxyz/rusteron/tree/main/rusteron-client)                 | Core Aeron client wrapper (connect, publish, subscribe).                   |
| [`rusteron-archive`](https://github.com/gsrxyz/rusteron/tree/main/rusteron-archive)               | Adds stream recording and replay features. Includes `rusteron-client`.     |
| [`rusteron-media-driver`](https://github.com/gsrxyz/rusteron/tree/main/rusteron-media-driver)     | Rust interface for launching an embedded or standalone Aeron Media Driver. |
| [`rusteron-docker-samples`](https://github.com/gsrxyz/rusteron/tree/main/rusteron-docker-samples) | Sample Docker setups for media driver + pub/sub flows. Not prod-ready.     |

Note: `rusteron-archive` includes `rusteron-client`, so you do **not** need both as dependencies.

---

## Installation

Choose the module and linking style appropriate for your project.

**Dynamic library:**

```toml
[dependencies]
rusteron-client = "0.1"
```

**Static library:**

```toml
[dependencies]
rusteron-client = { version = "0.1", features = ["static"] }
```

**macOS-only precompiled static libs:**

```toml
[dependencies]
rusteron-client = { version = "0.1", features = ["static", "precompile"] }
```

Replace `rusteron-client` with `rusteron-archive` or `rusteron-media-driver` as needed.

For full build instructions, see [BUILD.md](./BUILD.md).

---

## Development

To simplify development, we use [`just`](https://github.com/casey/just), a command runner similar to `make`.

To view all available commands, run `just` in the command line.

> If you don’t have `just` installed, install it with: `cargo install just`

---

## Example: Aeron Client

<details>
<summary>Expand for usage example</summary>

```rust
use rusteron::client::{Aeron, AeronContext, IntoCString};
use rusteron_media_driver::{AeronDriverContext, AeronDriver};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start embedded media driver
    let media_driver_ctx = AeronDriverContext::new()?;
    let (stop, driver_handle) = AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

    let ctx = AeronContext::new()?;
    ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    // Create subscription and publication
    let subscription = aeron
        .async_add_subscription(&"aeron:ipc".into_c_string(), 123,                
                                Handlers::no_available_image_handler(),
                                Handlers::no_unavailable_image_handler())?
        .poll_blocking(Duration::from_secs(5))?;

    let publisher = aeron
        .async_add_publication(&"aeron:ipc".into_c_string(), 123)?
        .poll_blocking(Duration::from_secs(5))?;

    let message = "Hello, Aeron!".as_bytes();
    let result = publisher.offer(message, Handlers::no_reserved_value_supplier_handler());

    // Fragment handler example
    struct FragmentHandler;
    impl AeronFragmentHandlerCallback for FragmentHandler {
        fn handle_aeron_fragment_handler(
            &mut self,
            msg: &[u8],
            header: AeronHeader,
        ) {
            println!(
                "received a message from aeron {:?}, msg length:{}",
                header.position(),
                msg.len()
            );
        }
    }

    let (closure, _inner) = Handler::leak_with_fragment_assembler(FragmentHandler)?;

    let mut count = 0;
    while count < 10000 {
        subscription.poll(Some(&closure), 128)?;
        count += 1;
    }
    Ok(())
}
```

</details>

---

## Benchmarks

For latency and throughput benchmarks, refer to [BENCHMARKS.md](./BENCHMARKS.md).

---

## Contributing

Contributions are more than welcome! Please:

* Submit bug reports, ideas, or improvements via GitHub Issues
* Propose changes via pull requests
* Read our [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md)

We’re especially looking for help with:

* API design reviews
* Safety and idiomatic improvements
* Dockerized and deployment examples

---

## License

Licensed under either [MIT License](https://opensource.org/licenses/MIT) or [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) at your option.

---

## Acknowledgments

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron - your work made this possible!
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for open protocol excellence
