use hdrhistogram::Histogram;
use rusteron_client::*;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

const PING_STREAM_ID: i32 = 1002;
const PONG_STREAM_ID: i32 = 1003;
const PING_CHANNEL: &std::ffi::CStr = c"aeron:udp?endpoint=localhost:20123";
const PONG_CHANNEL: &std::ffi::CStr = c"aeron:udp?endpoint=localhost:20124";
const NUMBER_OF_MESSAGES: usize = 10_000_000;
const WARMUP_NUMBER_OF_MESSAGES: usize = 100_000;
const MESSAGE_LENGTH: usize = 32;
const FRAGMENT_COUNT_LIMIT: usize = 10;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running = Arc::new(AtomicBool::new(true));
    let running_pong = Arc::clone(&running);

    // Set up the pong thread
    let pong_thread = thread::Builder::new()
        .name("pong".to_string())
        .spawn(move || run_pong(running_pong).unwrap())?;

    let hist = run_ping(running, pong_thread)?;
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

fn run_pong(running_pong: Arc<AtomicBool>) -> Result<(), Box<dyn std::error::Error>> {
    let context = AeronContext::new()?;
    let error_handler = Handler::new(AeronErrorHandlerLogger);
    context.set_error_handler(Some(error_handler.clone()))?;
    let dir = std::env::var("AERON_DIR").expect("AERON_DIR must be set");
    context.set_dir(&cformat!("{dir}"))?;
    context.set_idle_sleep_duration_ns(0)?;
    let aeron = Aeron::new(&context)?;
    aeron.start()?;
    let ping_publication = aeron
        .async_add_publication(PING_CHANNEL, PING_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let pong_subscription = aeron
        .async_add_subscription(PONG_CHANNEL, PONG_STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(4))?;

    println!("PONG: ping publisher {PING_CHANNEL:?} {PING_STREAM_ID}");
    println!("PONG: pong subscriber {PONG_CHANNEL:?} {PONG_STREAM_ID}");

    println!("Starting pong thread");
    pub struct PongRoundTripHandler {
        publisher: AeronPublication,
        buffer_claim: AeronBufferClaim,
        header_values: AeronHeaderValues,
    }

    impl AeronFragmentHandlerCallback for PongRoundTripHandler {
        #[inline]
        fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
            header.values(&self.header_values).unwrap();
            let flags = self.header_values.frame.flags;

            loop {
                match self.publisher.try_claim(buffer.len(), &self.buffer_claim) {
                    Ok(_) => break,
                    Err(e) if e.is_retryable() => continue,
                    // Fatal -> publication gone; abandon this fragment rather than spin forever.
                    Err(_) => return,
                }
            }
            self.buffer_claim.frame_header_mut().flags = flags;
            self.buffer_claim.data_mut().copy_from_slice(buffer);
            self.buffer_claim.commit().unwrap();
        }
    }

    let handler = Handler::new(PongRoundTripHandler {
        publisher: ping_publication,
        buffer_claim: Default::default(),
        header_values: Default::default(),
    });
    while running_pong.load(Ordering::Acquire) {
        let _ = pong_subscription.poll(Some(&handler), FRAGMENT_COUNT_LIMIT);
    }
    println!("Shutting down pong thread");
    Ok(())
}

fn run_ping(running: Arc<AtomicBool>, pong_thread: JoinHandle<()>) -> Result<Histogram<u64>, Box<dyn Error>> {
    let context = AeronContext::new()?;
    let error_handler = Handler::new(AeronErrorHandlerLogger);
    context.set_error_handler(Some(error_handler.clone()))?;
    let dir = std::env::var("AERON_DIR").expect("AERON_DIR must be set");
    println!("idle sleep {}", context.get_idle_sleep_duration_ns());
    context.set_idle_sleep_duration_ns(0)?;
    context.set_dir(&cformat!("{dir}"))?;
    let aeron = Aeron::new(&context)?;
    aeron.start()?;

    let pong_publication = aeron
        .async_add_publication(PONG_CHANNEL, PONG_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let ping_subscription = aeron
        .async_add_subscription(PING_CHANNEL, PING_STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(4))?;

    println!("PING: pong publisher {PONG_CHANNEL:?} {PONG_STREAM_ID}");
    println!("PING: ping subscriber {PING_CHANNEL:?} {PING_STREAM_ID}");

    let mut buffer = vec![0u8; MESSAGE_LENGTH];

    let (mut handler, inner_handler) = Handler::with_fragment_assembler(PingRoundTripHandler {
        histogram: Histogram::new(3)?,
    })
    .unwrap();
    sleep(Duration::from_secs(1));
    for _ in 0..WARMUP_NUMBER_OF_MESSAGES {
        record_rtt(&pong_publication, &ping_subscription, &mut buffer, &mut handler);
    }
    println!("warmed up");
    unsafe {
        inner_handler.get_mut().histogram.reset();
    }
    for _ in 0..NUMBER_OF_MESSAGES {
        record_rtt(&pong_publication, &ping_subscription, &mut buffer, &mut handler);
    }

    println!("finished sending all pings");
    running.store(false, Ordering::SeqCst);
    pong_thread.join().expect("Failed to join pong thread");

    let hist = unsafe { &inner_handler.get_mut().histogram };
    Ok(hist.clone())
}

pub struct PingRoundTripHandler {
    histogram: Histogram<u64>,
}

impl AeronFragmentHandlerCallback for PingRoundTripHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        let time = read_i64(buffer);
        let rtt = Aeron::nano_clock() - time;
        debug_assert!(rtt >= 0);
        self.histogram.record(rtt as u64).unwrap();
    }
}

fn read_i64(buffer: &[u8]) -> i64 {
    i64::from_le_bytes(buffer[0..8].try_into().expect("Slice with incorrect length"))
}

#[inline]
fn record_rtt(
    pong_publication: &AeronPublication,
    ping_subscription: &AeronSubscription,
    buffer: &mut [u8],
    handler: &mut Handler<AeronFragmentAssembler>,
) {
    let now = Aeron::nano_clock();
    write_i64(buffer, &now);
    loop {
        match pong_publication.offer(buffer) {
            Ok(_) => break,
            Err(e) if e.is_retryable() => continue,
            // Fatal -> publication gone; stop trying to send this ping.
            Err(_) => return,
        }
    }

    while ping_subscription
        .poll(Some(handler), FRAGMENT_COUNT_LIMIT)
        .unwrap_or_default()
        == 0
    {}
}

fn write_i64(buffer: &mut [u8], now: &i64) {
    buffer[0..8].copy_from_slice(&now.to_le_bytes());
}
