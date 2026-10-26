//! # Ping/pong RTT
//!
//! Port of Aeron's `cping`/`cpong` (and `Ping.cpp`/`Pong.cpp`): ping stamps a time into each
//! message, pong echoes it back, and ping records the round trip in a histogram.
//!
//! Set `AERON_DIR` to use a running media driver; otherwise one is embedded. As in Aeron's
//! `embedded-ping-pong` script, term buffers are non-sparse and the clients pre-touch them, the setting to use for latency-sensitive streams.
//!
//! ```bash
//! cargo run --release --features examples --example embedded_ping_pong
//! ```

use hdrhistogram::Histogram;
use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::error::Error;
use std::hint::spin_loop;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

const PING_STREAM_ID: i32 = 1002;
const PONG_STREAM_ID: i32 = 1003;
const PING_CHANNEL: &std::ffi::CStr = c"aeron:udp?endpoint=localhost:20123";
const PONG_CHANNEL: &std::ffi::CStr = c"aeron:udp?endpoint=localhost:20124";
// cping sends 10M; 1M keeps a loopback UDP run short
const NUMBER_OF_MESSAGES: usize = 1_000_000;
const WARMUP_NUMBER_OF_MESSAGES: usize = 100_000;
const MESSAGE_LENGTH: usize = 32;
const FRAGMENT_COUNT_LIMIT: usize = 10;

fn main() -> Result<(), Box<dyn Error>> {
    // declared first so the driver outlives both clients
    let driver = match std::env::var_os("AERON_DIR") {
        Some(_) => None,
        None => Some(EmbeddedDriver::launch_with(|context| {
            context.set_term_buffer_sparse_file(false)?;
            Ok(())
        })?),
    };
    let dir = match &driver {
        Some(driver) => driver.dir().to_string(),
        None => std::env::var("AERON_DIR")?,
    };

    let running = Arc::new(AtomicBool::new(true));
    let pong_thread = {
        let running = Arc::clone(&running);
        let dir = dir.clone();
        thread::Builder::new()
            .name("pong".to_string())
            .spawn(move || run_pong(&running, &dir))?
    };

    let ping = run_ping(&dir);
    running.store(false, Ordering::Release);
    // a pong failure is usually why ping failed, so report it first
    pong_thread
        .join()
        .expect("pong thread panicked")
        .map_err(|e| -> Box<dyn Error> { e })?;
    let hist = ping?;

    println!("message length {MESSAGE_LENGTH} bytes\n");
    println!("Histogram of RTT latencies:");
    println!("# of samples: {}", hist.len());
    println!("min: {:?}", Duration::from_nanos(hist.min()));
    println!(
        "50th percentile: {:?}",
        Duration::from_nanos(hist.value_at_quantile(0.50))
    );
    println!(
        "99th percentile: {:?}",
        Duration::from_nanos(hist.value_at_quantile(0.99))
    );
    println!(
        "99.9th percentile: {:?}",
        Duration::from_nanos(hist.value_at_quantile(0.999))
    );
    println!(
        "99.99th percentile: {:?}",
        Duration::from_nanos(hist.value_at_quantile(0.9999))
    );
    println!("max: {:?}", Duration::from_nanos(hist.max()));
    println!("avg: {:?}", Duration::from_nanos(hist.mean() as u64));

    Ok(())
}

fn client_context(dir: &str) -> Result<AeronContext, AeronCError> {
    let context = AeronContext::new()?;
    context.set_dir(&cformat!("{dir}"))?;
    context.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    context.set_pre_touch_mapped_memory(true)?;
    Ok(context)
}

fn run_pong(running: &AtomicBool, dir: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let context = client_context(dir)?;
    let aeron = Aeron::new(&context)?;
    aeron.start()?;
    let ping_publication = aeron
        .async_add_exclusive_publication(PING_CHANNEL, PING_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let pong_subscription = aeron
        .async_add_subscription(PONG_CHANNEL, PONG_STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;

    println!("PONG: ping publisher {PING_CHANNEL:?} {PING_STREAM_ID}");
    println!("PONG: pong subscriber {PONG_CHANNEL:?} {PONG_STREAM_ID}");

    // the poll callback cannot return an error, so it parks the first one here
    let mut failure = None;
    while running.load(Ordering::Acquire) {
        let fragments = pong_subscription.poll_fn(
            |buffer, _header| {
                if failure.is_none() {
                    failure = echo(&ping_publication, buffer).err();
                }
            },
            FRAGMENT_COUNT_LIMIT,
        )?;
        if let Some(e) = failure.take() {
            return Err(e);
        }
        if fragments == 0 {
            spin_loop();
        }
    }
    println!("Shutting down pong thread");
    Ok(())
}

#[inline]
fn echo(publication: &AeronExclusivePublication, buffer: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
    loop {
        match publication.try_claim_owned(buffer.len()) {
            // pings never fragment, so the claim's own BEGIN|END flags are the right ones
            Ok(mut claim) => {
                claim.data().copy_from_slice(buffer);
                claim.commit()?;
                return Ok(());
            }
            Err(e) if e.is_retryable() => spin_loop(),
            Err(e) => return Err(e.into()),
        }
    }
}

fn run_ping(dir: &str) -> Result<Histogram<u64>, Box<dyn Error>> {
    let context = client_context(dir)?;
    let aeron = Aeron::new(&context)?;
    aeron.start()?;

    let pong_publication = aeron
        .async_add_exclusive_publication(PONG_CHANNEL, PONG_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let ping_subscription = aeron
        .async_add_subscription(PING_CHANNEL, PING_STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;

    println!("PING: pong publisher {PONG_CHANNEL:?} {PONG_STREAM_ID}");
    println!("PING: ping subscriber {PING_CHANNEL:?} {PING_STREAM_ID}");

    // both directions must be up: our pings reach pong, and pong's echoes reach our image
    let deadline = Instant::now() + Duration::from_secs(5);
    let image = loop {
        if pong_publication.is_connected()
            && let Some(image) = ping_subscription.image_at_index(0)
        {
            break image;
        }
        if Instant::now() > deadline {
            return Err("pong never connected".into());
        }
        sleep(Duration::from_millis(10));
    };

    let mut buffer = [0u8; MESSAGE_LENGTH];
    // bounded, so recording never resizes inside the measured loop
    let mut histogram = Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3)?;
    for _ in 0..WARMUP_NUMBER_OF_MESSAGES {
        record_rtt(&pong_publication, &image, &mut buffer, &mut histogram)?;
    }
    println!("warmed up");
    histogram.reset();
    for _ in 0..NUMBER_OF_MESSAGES {
        record_rtt(&pong_publication, &image, &mut buffer, &mut histogram)?;
    }
    println!("finished sending all pings");
    Ok(histogram)
}

#[inline]
fn record_rtt(
    publication: &AeronExclusivePublication,
    image: &AeronImage,
    buffer: &mut [u8],
    histogram: &mut Histogram<u64>,
) -> Result<(), Box<dyn Error>> {
    let position = loop {
        // stamp each attempt, so time spent back-pressured is not counted as RTT
        buffer[0..8].copy_from_slice(&Aeron::nano_clock().to_le_bytes());
        match publication.offer(buffer) {
            Ok(position) => break position,
            Err(e) if e.is_retryable() => spin_loop(),
            Err(e) => return Err(e.into()),
        }
    };

    // pong echoes at the same length, so the echo has been read once the image reaches our position
    while image.position() < position {
        let fragments = image.poll_fn(
            |reply, _header| {
                let rtt = Aeron::nano_clock() - read_i64(reply);
                histogram.saturating_record(rtt as u64);
            },
            FRAGMENT_COUNT_LIMIT,
        )?;
        if fragments == 0 {
            if image.is_closed() {
                return Err("pong went away".into());
            }
            spin_loop();
        }
    }
    Ok(())
}

fn read_i64(buffer: &[u8]) -> i64 {
    i64::from_le_bytes(buffer[0..8].try_into().expect("ping shorter than 8 bytes"))
}
