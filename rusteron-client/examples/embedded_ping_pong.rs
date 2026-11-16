//! # Ping/pong RTT
//!
//! Port of Aeron's `cping`/`cpong` (and `Ping.cpp`/`Pong.cpp`): ping stamps a time into each
//! message, pong echoes it back, and ping records the round trip in a histogram.
//!
//! Set `AERON_DIR` to use a running media driver; otherwise one is embedded. As in Aeron's
//! `embedded-ping-pong` script, term buffers are non-sparse and the clients pre-touch them. Use this setting for latency-sensitive streams.
//!
//! ```bash
//! cargo run --release --features examples --example embedded_ping_pong
//! ```

mod benchmark_support;

use benchmark_support::{MessageCounts, StopOnDrop, print_histogram};
use hdrhistogram::Histogram;
use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const PING_CHANNEL: &std::ffi::CStr = c"aeron:udp?endpoint=localhost:20123";
const PONG_CHANNEL: &std::ffi::CStr = c"aeron:udp?endpoint=localhost:20124";
const PING_STREAM_ID: i32 = 1002;
const PONG_STREAM_ID: i32 = 1003;
const MESSAGE_LENGTH: usize = 32;
const FRAGMENT_COUNT_LIMIT: usize = 10;

fn main() -> Result<(), Box<dyn Error>> {
    let counts = MessageCounts::parse(1_000_000)?;
    let driver = match std::env::var_os("AERON_DIR") {
        Some(_) => None,
        None => Some(EmbeddedDriver::launch()?),
    };
    let dir = match &driver {
        Some(driver) => driver.dir().to_string(),
        None => std::env::var("AERON_DIR")?,
    };
    println!(
        "latency workload: message_length={MESSAGE_LENGTH} poll_limit={FRAGMENT_COUNT_LIMIT} warmup={} messages={} clients=2",
        counts.warmup, counts.measured
    );
    let running = Arc::new(AtomicBool::new(true));
    let pong = {
        let running = running.clone();
        let dir = dir.clone();
        thread::Builder::new().name("pong".into()).spawn(move || {
            let _stop = StopOnDrop(&running);
            run_pong(&running, &dir).expect("pong failed");
        })?
    };
    thread::sleep(Duration::from_millis(500));
    let result = run_ping(&running, &dir, &counts);
    running.store(false, Ordering::Release);
    pong.join().map_err(|_| "pong thread panicked")?;
    let histogram = result?;
    if histogram.len() != counts.measured as u64 {
        return Err(format!("expected {} RTT samples, got {}", counts.measured, histogram.len()).into());
    }
    print_histogram(&histogram);
    Ok(())
}

fn client(dir: &str) -> Result<Aeron, AeronCError> {
    let context = AeronContext::new()?;
    context.set_dir(&cformat!("{dir}"))?;
    context.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    context.set_pre_touch_mapped_memory(true)?;
    let client = Aeron::new(&context)?;
    client.start()?;
    Ok(client)
}

fn connected(publication: &AeronPublication) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !publication.is_connected() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    if !publication.is_connected() {
        return Err("publisher failed to connect".into());
    }
    Ok(())
}

fn run_pong(running: &AtomicBool, dir: &str) -> Result<(), Box<dyn Error>> {
    let client = client(dir)?;
    let subscription = client
        .async_add_subscription(PONG_CHANNEL, PONG_STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;
    let publication = client
        .async_add_publication(PING_CHANNEL, PING_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    connected(&publication)?;
    while running.load(Ordering::Acquire) {
        subscription.poll_fn(
            |data, _| {
                let result = publication.try_claim_owned(data.len()).map(|mut claim| {
                    claim.data().copy_from_slice(data);
                    claim.commit().expect("claim commit failed");
                });
                if !sent(result) {
                    while !sent(publication.offer(data)) && running.load(Ordering::Acquire) {}
                }
            },
            FRAGMENT_COUNT_LIMIT,
        )?;
    }
    Ok(())
}

fn run_ping(running: &AtomicBool, dir: &str, counts: &MessageCounts) -> Result<Histogram<u64>, Box<dyn Error>> {
    let client = client(dir)?;
    let publication = client
        .async_add_publication(PONG_CHANNEL, PONG_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let subscription = client
        .async_add_subscription(PING_CHANNEL, PING_STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;
    connected(&publication)?;
    thread::sleep(Duration::from_millis(500));

    let mut histogram = Histogram::<u64>::new(3)?;
    let mut buffer = [0u8; MESSAGE_LENGTH];
    for _ in 0..counts.warmup {
        round_trip(running, &publication, &subscription, &mut buffer, &mut histogram)?;
    }
    histogram.reset();
    println!("warmed up");
    for _ in 0..counts.measured {
        round_trip(running, &publication, &subscription, &mut buffer, &mut histogram)?;
    }
    Ok(histogram)
}

fn round_trip(
    running: &AtomicBool,
    publication: &AeronPublication,
    subscription: &AeronSubscription,
    buffer: &mut [u8],
    histogram: &mut Histogram<u64>,
) -> Result<(), Box<dyn Error>> {
    buffer[..8].copy_from_slice(&nanos().to_le_bytes());
    while !sent(publication.offer(buffer)) {
        if !running.load(Ordering::Acquire) {
            return Err("pong stopped".into());
        }
    }
    let mut received = false;
    while !received {
        if !running.load(Ordering::Acquire) {
            return Err("pong stopped".into());
        }
        subscription.poll_fn(
            |reply, _| {
                let sent = i64::from_le_bytes(reply[..8].try_into().expect("timestamp"));
                let rtt = nanos() - sent;
                if rtt >= 0 {
                    histogram.record(rtt as u64).expect("RTT histogram overflow");
                }
                received = true;
            },
            FRAGMENT_COUNT_LIMIT,
        )?;
    }
    Ok(())
}

fn nanos() -> i64 {
    use std::sync::OnceLock;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as i64
}

fn sent<T>(result: Result<T, AeronOfferError>) -> bool {
    match result {
        Ok(_) => true,
        Err(e) if e.is_retryable() => false,
        Err(e) => panic!("offer failed: {e}"),
    }
}
