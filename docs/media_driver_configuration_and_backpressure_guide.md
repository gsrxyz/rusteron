# Media Driver configuration and back-pressure guide

Aeron uses a Media Driver to handle UDP and IPC transport. Low-latency performance
depends on configuring the Media Driver and handling back-pressure correctly.
Configure its threads and buffers for your workload, and retry publication offers
when subscribers apply back-pressure.

---

## Official Aeron documentation
For tuning guides, system properties, and thread models:
- [Aeron Wiki: Configuration Options](https://github.com/aeron-io/aeron/wiki/Configuration-Options)
- [Aeron Wiki: Monitoring and Debugging](https://github.com/aeron-io/aeron/wiki/Monitoring-and-Debugging)
- [Aeron Wiki: Performance Tuning](https://github.com/aeron-io/aeron/wiki/Performance-Tuning)
- [Aeron Cookbook (aeron.io)](https://aeron.io/docs/)

---

## Rust configuration and tuning snippets

### 1. Launching an embedded C Media Driver

Use `rusteron-media-driver` to run the C Media Driver in your Rust process:

```rust,no_run
use rusteron_media_driver::bindings::aeron_threading_mode_t;
use rusteron_media_driver::{AeronDriver, AeronDriverContext};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = AeronDriverContext::new()?;

    // DEDICATED (the default): conductor, sender and receiver each get a thread, for the lowest latency.
    // SHARED: all three share one thread, for dev boxes and tests.
    ctx.set_threading_mode(aeron_threading_mode_t::AERON_THREADING_MODE_SHARED)?;
    ctx.set_dir(c"/tmp/aeron-rust")?;
    ctx.set_term_buffer_length(16 * 1024 * 1024)?;

    // runs the driver's duty cycle on a background thread; stops and joins on drop
    let _driver = AeronDriver::launch_embedded_guard(ctx, false);

    // application logic here; clients connect with Aeron::connect_dir("/tmp/aeron-rust")
    Ok(())
}
```

For tests, `rusteron_media_driver::testing::EmbeddedDriver::launch_with(|ctx| { ctx.set_threading_mode(..)?; Ok(()) })` also picks a unique directory.

### 2. Handling publication back-pressure (`BACK_PRESSURED`)

`offer` returns `Err(AeronOfferError::BackPressured)` when the publication has reached its publisher limit, because the slowest subscriber (or the sender's flow control) has not consumed enough of the term buffer. `is_retryable()` is true for it, for `AdminAction` (term rotation) and for `NotConnected`; the other errors are fatal. Idle and retry until your deadline:

```rust,no_run
use rusteron_client::{AeronPublication, BackoffIdleStrategy, IdleStrategy};
use std::time::{Duration, Instant};

fn publish_message(publication: &AeronPublication, data: &[u8]) -> Result<i64, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    // pick the idle by latency budget: BusySpinIdleStrategy, YieldingIdleStrategy, BackoffIdleStrategy, ...
    let mut idle = BackoffIdleStrategy::new();
    loop {
        match publication.offer(data) {
            Ok(position) => return Ok(position),
            Err(e) if e.is_retryable() && Instant::now() < deadline => idle.idle(0),
            // a fatal error, or still not accepted at the deadline
            Err(e) => return Err(e.into()),
        }
    }
}
```
