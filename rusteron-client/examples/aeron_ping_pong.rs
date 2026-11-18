//! Aeron C cping/cpong workload: exclusive publications, 32-byte messages,
//! retained image polls limited to 10 fragments, image fragment assemblers on
//! both sides, and an offer echo. Neither application callback reads headers

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

const PING_STREAM_ID: i32 = 1002;
const PONG_STREAM_ID: i32 = 1003;
const PING_CHANNEL: &std::ffi::CStr = c"aeron:udp?endpoint=localhost:20123";
const PONG_CHANNEL: &std::ffi::CStr = c"aeron:udp?endpoint=localhost:20124";
const MESSAGE_LENGTH: usize = 32;
const FRAGMENT_COUNT_LIMIT: usize = 10;

fn main() -> Result<(), Box<dyn Error>> {
    let counts = MessageCounts::parse(10_000_000)?;
    let driver = match std::env::var_os("AERON_DIR") {
        Some(_) => None,
        None => Some(EmbeddedDriver::launch()?),
    };
    let dir = match &driver {
        Some(driver) => driver.dir().to_string(),
        None => std::env::var("AERON_DIR")?,
    };
    println!(
        "Aeron C latency workload: message_length={MESSAGE_LENGTH} poll_limit={FRAGMENT_COUNT_LIMIT} warmup={} messages={} clients=2",
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
    // cping/cpong leave pre-touch at its default (false)
    context.set_pre_touch_mapped_memory(false)?;
    let client = Aeron::new(&context)?;
    client.start()?;
    Ok(client)
}

fn image(subscription: &AeronSubscription, running: &AtomicBool) -> Result<AeronImage, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(image) = subscription.image_at_index(0) {
            return Ok(image);
        }
        if !running.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err("peer did not connect".into());
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn run_pong(running: &Arc<AtomicBool>, dir: &str) -> Result<(), Box<dyn Error>> {
    let client = client(dir)?;
    let subscription = client
        .async_add_subscription(PING_CHANNEL, PING_STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;
    let publication = client
        .async_add_exclusive_publication(PONG_CHANNEL, PONG_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let mut image = image(&subscription, running)?;
    let mut assembler = AeronImageFragmentAssembler::new(Echo {
        publication,
        running: running.clone(),
    })?;
    let mut idle = BusySpinIdleStrategy::new();
    while running.load(Ordering::Acquire) {
        idle.idle(assembler.poll(&mut image, FRAGMENT_COUNT_LIMIT)?);
    }
    Ok(())
}

struct Echo {
    publication: AeronExclusivePublication,
    running: Arc<AtomicBool>,
}

impl AeronFragmentHandlerCallback for Echo {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _: &AeronHeader) {
        let mut idle = BusySpinIdleStrategy::new();
        loop {
            match self.publication.offer(buffer) {
                Ok(_) => break,
                Err(e) if e.is_retryable() => {
                    if !self.running.load(Ordering::Acquire) {
                        break;
                    }
                    idle.idle(0);
                }
                Err(e) => panic!("echo failed: {e}"),
            }
        }
    }
}

struct Measure {
    histogram: Histogram<u64>,
    received: usize,
}

impl AeronFragmentHandlerCallback for Measure {
    fn handle_aeron_fragment_handler(&mut self, reply: &[u8], _: &AeronHeader) {
        self.received += 1;
        let end = Aeron::nano_clock();
        let start = i64::from_le_bytes(reply[..8].try_into().expect("timestamp"));
        self.histogram
            .record((end - start) as u64)
            .expect("RTT histogram overflow");
    }
}

fn run_ping(running: &AtomicBool, dir: &str, counts: &MessageCounts) -> Result<Histogram<u64>, Box<dyn Error>> {
    let client = client(dir)?;
    let subscription = client
        .async_add_subscription(PONG_CHANNEL, PONG_STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;
    let publication = client
        .async_add_exclusive_publication(PING_CHANNEL, PING_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let mut image = image(&subscription, running)?;
    let mut assembler = AeronImageFragmentAssembler::new(Measure {
        histogram: Histogram::new_with_bounds(1, 10_000_000_000, 3)?,
        received: 0,
    })?;
    let mut buffer = [0u8; MESSAGE_LENGTH];
    let mut idle = BusySpinIdleStrategy::new();
    for count in [counts.warmup, counts.measured] {
        let state = assembler.handler_mut();
        state.histogram.reset();
        state.received = 0;
        for expected in 1..=count {
            loop {
                // cping refreshes the timestamp on every offer attempt
                buffer[..8].copy_from_slice(&Aeron::nano_clock().to_le_bytes());
                match publication.offer(&buffer) {
                    Ok(_) => break,
                    Err(e) if e.is_retryable() => {
                        if !running.load(Ordering::Acquire) {
                            return Err("pong stopped".into());
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            loop {
                // The synchronous poll callback updates this count
                if assembler.handler().received >= expected {
                    break;
                }
                if !running.load(Ordering::Acquire) {
                    return Err("pong stopped".into());
                }
                idle.idle(assembler.poll(&mut image, FRAGMENT_COUNT_LIMIT)?);
            }
        }
    }
    Ok(assembler.handler().histogram.clone())
}
