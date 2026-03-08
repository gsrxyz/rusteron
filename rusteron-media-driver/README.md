# rusteron-media-driver

**rusteron-media-driver** is a Rust interface to the Aeron Media Driver, responsible for managing low-latency messaging infrastructure between producers and consumers. It's part of the [Rusteron](https://github.com/gsrxyz/rusteron) project and provides both standalone and embedded driver support.

> For production deployments, we recommend using the Aeron **Java** or **C** media driver.  
> The embedded version provided here is best suited for integration tests or lightweight environments.

---

## Installation

To use `rusteron-media-driver`, add the appropriate dependency to your `Cargo.toml`:

<details>
<summary>Dynamic</summary>

```toml
[dependencies]
rusteron-media-driver = "0.1"
````

</details>

<details>
<summary>Static</summary>

```toml
[dependencies]
rusteron-media-driver = { version = "0.1", features = ["static"] }
```

</details>

<details>
<summary>Static with precompiled C libs (macOS only)</summary>

```toml
[dependencies]
rusteron-media-driver = { version = "0.1", features = ["static", "precompile"] }
```

```toml
[dependencies]
rusteron-media-driver = { version = "0.1", features = ["static", "precompile-rustls"] }
```

</details>

Ensure the Aeron C libraries are properly installed and available on your system.

---

## Usage Examples

<details>
<summary>Standard Media Driver</summary>

```rust
// Launches a standalone Aeron Media Driver
use rusteron_media_driver::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let aeron_context = AeronDriverContext::new()?;
    aeron_context.set_dir(&"target/test".into_c_string())?;

    let aeron_driver = AeronDriver::new(&aeron_context)?;
    aeron_driver.start(false)?;
    println!("Aeron Media Driver started");

    Ok(())
}
```

</details>

<details>
<summary>Embedded Media Driver</summary>

```rust
// Embeds the media driver directly into the current process
use rusteron_media_driver::*;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let media_driver_ctx = AeronDriverContext::new()?;
    let (stop, driver_handle) = AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

    let ctx = AeronContext::new()?;
    ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;

    thread::sleep(Duration::from_secs(3)); // Simulated workload

    stop.store(true, Ordering::SeqCst);
    driver_handle.join().expect("Failed to join driver thread");
    println!("Embedded Aeron Media Driver stopped");

    Ok(())
}
```

</details>
