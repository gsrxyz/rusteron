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
//! cargo run --release --features examples --example streaming_rate
//! ```

use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    ctx.set_dir(&cformat!("{}", driver.dir()))?;
    ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    let publication = aeron
        .async_add_publication(AERON_IPC_STREAM, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;

    // rate subscriber (rate_subscriber.c)
    // The subscriber keeps plain counters (single writer, no atomics on the hot path) and
    // returns them when it stops. It stops once it has seen everything the publisher sent.
    // It has its own client: without `multi-threaded` a handle stays on the thread whose
    // client created it. Offers report NotConnected until its subscription joins.
    let target = Arc::new(AtomicU64::new(u64::MAX));
    let target_sub = target.clone();
    let dir = driver.dir().to_string();
    let subscriber = thread::spawn(move || -> Result<(u64, u64), AeronCError> {
        let aeron = Aeron::connect_dir(&dir)?;
        let subscription = aeron
            .async_add_subscription(AERON_IPC_STREAM, STREAM_ID, Handlers::NONE, Handlers::NONE)?
            .poll_blocking(Duration::from_secs(5))?;
        // fragment assembler so messages larger than the MTU are reassembled
        let mut assembler = AeronFragmentClosureAssembler::new()?;
        let mut counters = (0u64, 0u64);
        let mut last_report = Instant::now();
        let (mut last_msgs, mut last_bytes) = (0u64, 0u64);
        let mut polls = 0u32;
        let mut drain_deadline = None;
        loop {
            let fragments = assembler.poll(
                &subscription,
                &mut counters,
                |c, buf, _hdr| {
                    c.0 += 1;
                    c.1 += buf.len() as u64;
                },
                FRAGMENT_LIMIT,
            )?;
            polls = polls.wrapping_add(1);
            // read the clock when idle, or now and then so a saturated poll still reports
            if fragments > 0 && !polls.is_multiple_of(1024) {
                continue;
            }
            std::hint::spin_loop();
            let (m, b) = counters;
            let target = target_sub.load(Ordering::Acquire);
            if target != u64::MAX {
                let deadline = *drain_deadline.get_or_insert_with(|| Instant::now() + Duration::from_secs(5));
                if m >= target || Instant::now() >= deadline {
                    return Ok(counters);
                }
            }
            // once-a-second rate report, like the C sample's rate reporter thread
            let secs = last_report.elapsed().as_secs_f64();
            if secs >= 1.0 {
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
    });

    // streaming publisher (streaming_publisher.c)
    let message = vec![42u8; MESSAGE_LENGTH];
    let mut back_pressure = 0u64;
    let mut not_connected_spins = 0u64;
    let start = Instant::now();
    let mut sent = 0u64;
    'publish: while sent < MESSAGES && running.load(Ordering::Acquire) {
        match publication.offer(&message) {
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

    // tell the subscriber how many to expect; it drains up to that (or 5 s), then returns
    target.store(sent, Ordering::Release);
    let (received, _bytes) = subscriber.join().map_err(|_| "subscriber thread panicked")??;
    println!("received {received} messages");
    if received != sent {
        return Err(format!("sent {sent} but received {received}").into());
    }

    drop(publication);
    drop(aeron);
    Ok(())
}
