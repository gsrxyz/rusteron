//! # Streaming publisher + rate subscriber
//!
//! Port of Aeron's `streaming_publisher.c` + `rate_subscriber.c` samples, self-contained
//! with an embedded media driver:
//!
//! - the publisher streams messages flat out, classifying every failed offer with
//!   [`AeronOfferError`] (back-pressure retried with an idle strategy; fatal errors abort);
//! - the subscriber polls through a fragment assembler and reports msgs/sec + MB/sec once
//!   a second, like the C sample's rate reporter.
//!
//! ```bash
//! cargo run --release --features "static precompile" --example streaming_rate
//! ```

use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 1002;
const MESSAGE_LENGTH: usize = 256;
const MESSAGES: u64 = 5_000_000;
const FRAGMENT_LIMIT: usize = 256;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running = Arc::new(AtomicBool::new(true));
    let running_ctrl_c = Arc::clone(&running);
    ctrlc::set_handler(move || running_ctrl_c.store(false, Ordering::SeqCst))?;

    // Embedded media driver (the C samples assume an external `aeronmd`).
    // embedded media driver with RAII teardown (stops + joins on drop)
    let driver = EmbeddedDriver::launch()?;

    let ctx = AeronContext::new()?;
    ctx.set_dir(&driver.dir().into_c_string())?;
    ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    let publication = aeron
        .async_add_publication(AERON_IPC_STREAM, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let subscription = aeron
        .async_add_subscription(
            AERON_IPC_STREAM,
            STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?
        .poll_blocking(Duration::from_secs(5))?;

    // ── rate subscriber (rate_subscriber.c) ─────────────────────────────
    let msgs = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let msgs_poll = msgs.clone();
    let bytes_poll = bytes.clone();
    let running_sub = running.clone();
    let subscriber = thread::spawn(move || -> Result<(), AeronCError> {
        // fragment assembler so messages larger than the MTU are reassembled
        let mut assembler = AeronFragmentClosureAssembler::new()?;
        let mut counters = (msgs_poll, bytes_poll);
        let mut last_report = Instant::now();
        let (mut last_msgs, mut last_bytes) = (0u64, 0u64);
        while running_sub.load(Ordering::Acquire) {
            let fragments = subscription.poll(
                assembler.process(&mut counters, |c, buf, _hdr| {
                    c.0.fetch_add(1, Ordering::Relaxed);
                    c.1.fetch_add(buf.len() as u64, Ordering::Relaxed);
                }),
                FRAGMENT_LIMIT,
            )?;
            if fragments == 0 {
                std::hint::spin_loop();
            }
            // once-a-second rate report, like the C sample's rate reporter thread
            if last_report.elapsed() >= Duration::from_secs(1) {
                let (m, b) = (counters.0.load(Ordering::Relaxed), counters.1.load(Ordering::Relaxed));
                let secs = last_report.elapsed().as_secs_f64();
                println!(
                    "{:.03} msgs/sec, {:.03} MB/sec, totals {} messages {} MB",
                    (m - last_msgs) as f64 / secs,
                    (b - last_bytes) as f64 / secs / (1024.0 * 1024.0),
                    m,
                    b / (1024 * 1024),
                );
                (last_msgs, last_bytes) = (m, b);
                last_report = Instant::now();
            }
        }
        Ok(())
    });

    // ── streaming publisher (streaming_publisher.c) ─────────────────────
    let message = vec![42u8; MESSAGE_LENGTH];
    let mut back_pressure = 0u64;
    let mut not_connected_spins = 0u64;
    let start = Instant::now();
    let mut sent = 0u64;
    'publish: while sent < MESSAGES && running.load(Ordering::Acquire) {
        match publication.offer_simple(&message) {
            Ok(_) => sent += 1,
            Err(e) if e.is_retryable() => {
                // back-pressure/admin-action/not-connected: idle and retry
                match e {
                    AeronOfferError::BackPressured => back_pressure += 1,
                    AeronOfferError::NotConnected => not_connected_spins += 1,
                    _ => {}
                }
                std::hint::spin_loop();
            }
            Err(e) => {
                eprintln!("fatal offer error after {sent} messages: {e}");
                break 'publish;
            }
        }
    }
    let elapsed = start.elapsed();
    println!(
        "published {sent} messages in {elapsed:?} ({:.0} msgs/sec), {back_pressure} back-pressure events, {not_connected_spins} not-connected spins",
        sent as f64 / elapsed.as_secs_f64(),
    );

    // drain, then stop
    thread::sleep(Duration::from_millis(200));
    running.store(false, Ordering::Release);
    subscriber.join().expect("subscriber thread panicked")?;

    let received = msgs.load(Ordering::Relaxed);
    println!("received {received} messages");
    assert_eq!(sent, received, "subscriber must observe every published message");

    drop(publication);
    drop(aeron);
    Ok(())
}
