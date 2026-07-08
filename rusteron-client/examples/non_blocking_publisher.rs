//! # Non-blocking publisher (the production pattern)
//!
//! Companion to [`basic_publisher`], showing the **idiomatic non-blocking** way to add a
//! publication. Every other example uses [`Aeron::add_publication`] / [`poll_blocking`] for
//! brevity — those block the calling thread in a busy-poll loop, which is fine for a terse
//! example but **not** what you want in production.
//!
//! This example drives the async poller's [`poll()`](AeronAsyncAddPublication::poll) from a
//! hand-written loop, interleaving other work while the publication comes up. The same shape
//! applies to subscriptions (`async_add_subscription` + `poll()`) and to the archive client.
//!
//! Run it:
//!
//! ```bash
//! cargo run --release --features examples --example non_blocking_publisher
//! ```
//!
//! [`add_publication`]: Aeron::add_publication
//! [`poll_blocking`]: AeronAsyncAddPublication::poll_blocking

use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CHANNEL: &std::ffi::CStr = c"aeron:udp?endpoint=localhost:40124";
const STREAM_ID: i32 = 11;
const MESSAGE: &[u8] = b"Hello World! ";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let _ = ctrlc::set_handler(move || r.store(false, Ordering::SeqCst));

    let driver = EmbeddedDriver::launch()?;
    let ctx = AeronContext::new()?;
    ctx.set_dir(&cformat!("{}", driver.dir()))?;
    ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    // ── Non-blocking publication setup ──────────────────────────────────────
    // `add_publication(.., timeout)` would block here until the publication is ready.
    // Instead we take the async poller and drive it ourselves, so the thread is free to do
    // other things while the driver sets up the publication.
    let poller = aeron.async_add_publication(CHANNEL, STREAM_ID)?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let publication = loop {
        if let Some(publication) = poller.poll()? {
            break publication;
        }
        if Instant::now() > deadline {
            return Err("timed out waiting for publication".into());
        }
        // This is the point: while the publication is coming up we can service other work —
        // other subscriptions, timers, a duty cycle, telemetry, etc. — instead of blocking.
        do_other_work();
    };
    println!("publishing to {} on stream id {STREAM_ID}", CHANNEL.to_str().unwrap());

    // ── Publication duty cycle ──────────────────────────────────────────────
    let mut count: u64 = 0;
    while running.load(Ordering::SeqCst) {
        let mut buf = MESSAGE.to_vec();
        buf.extend_from_slice(count.to_string().as_bytes());
        match publication.offer(&buf) {
            Ok(_pos) => count += 1,
            Err(e) if e.is_retryable() => std::hint::spin_loop(),
            Err(e) => return Err(format!("publication gone: {e}").into()),
        }
        // Interleave the non-publication work with the publish loop.
        do_other_work();
        std::thread::sleep(Duration::from_millis(100));
    }

    println!("published {count} messages; shutting down");
    drop(publication);
    drop(aeron);
    Ok(())
}

/// Placeholder for whatever else your application does — servicing other subscriptions,
/// timers, telemetry, etc. In a real app this is your main duty cycle.
fn do_other_work() {
    std::hint::spin_loop();
}
