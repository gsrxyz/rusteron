# Media Driver Configuration & Back-pressure Guide

Aeron is brokerless but requires a Media Driver to handle the transport protocol (UDP or IPC). Managing the Media Driver configuration and handling back-pressure correctly is essential for low-latency performance.

---

## Official Aeron Documentation
For tuning guides, system properties, and thread models:
- [Aeron Wiki: Configuration Options](https://github.com/aeron-io/aeron/wiki/Configuration-Options)
- [Aeron Wiki: Monitoring and Debugging](https://github.com/aeron-io/aeron/wiki/Monitoring-and-Debugging)
- [Aeron Wiki: Performance Tuning](https://github.com/aeron-io/aeron/wiki/Performance-Tuning)
- [Aeron Cookbook (aeron.io)](https://aeron.io/docs/)

---

## Rust Configuration & Tuning Snippets

### 1. Launching an Embedded C Media Driver

With `rusteron-media-driver`, you can embed the C-based Media Driver directly in your Rust process:

```rust
use rusteron_media_driver::{AeronDriver, AeronDriverContext};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = AeronDriverContext::new()?;
    
    // Set threading model:
    // - "SHARED": Conductor, Sender, and Receiver run on a single thread (default/dev friendly)
    // - "DEDICATED": Conductor, Sender, and Receiver each run on their own thread (low latency)
    use rusteron_media_driver::bindings::aeron_threading_mode_t;
    ctx.set_threading_mode(aeron_threading_mode_t::AERON_THREADING_MODE_SHARED)?;
    
    // Customize directory path and term buffer sizes
    ctx.set_dir("/tmp/aeron-rust")?;
    
    // Launch driver
    let driver = AeronDriver::new(&ctx)?;
    driver.start(true)?; // true = block until started

    // Application logic here...

    Ok(())
}
```

### 2. Handling Publication Back-Pressure (`BACK_PRESSURED`)

When calling `.offer()`, Aeron returns `BACK_PRESSURED` (error code `-2`) if the client's internal ring buffers are full (meaning the publisher is faster than the subscriber or the driver). The idiomatic pattern is to use an idle strategy to back off before retrying:

```rust
use rusteron_client::{Aeron, AeronContext, Handlers};
use std::thread::sleep;
use std::time::{Duration, Instant};

fn publish_message(publication: &AeronPublication, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    
    loop {
        match publication.offer(data) {
            Ok(position) => {
                // Message successfully offered at the returned position
                return Ok(());
            }
            Err(err) if err.is_retryable() => {
                if Instant::now() >= deadline {
                    return Err("Publishing timed out due to persistent back-pressure".into());
                }
                
                // Back off. You can use different idle strategies:
                // - BusySpin: loop {} (lowest latency, 100% CPU usage)
                // - Yielding: std::thread::yield_now()
                // - Sleeping: std::thread::sleep(...)
                sleep(Duration::from_millis(1));
            }
            Err(err) => {
                return Err(format!("Fatal publication offer error: {:?}", err).into());
            }
        }
    }
}
```
