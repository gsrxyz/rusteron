use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const BURST_LENGTH: u64 = 1_000_000;
const MESSAGE_LENGTH: usize = 32;
const FRAGMENT_COUNT_LIMIT: usize = 32;
const STREAM_ID: i32 = 1001;

fn main() -> Result<(), Box<dyn Error>> {
    let driver = match std::env::var_os("AERON_DIR") {
        Some(_) => None,
        None => Some(EmbeddedDriver::launch()?),
    };
    let dir = match &driver {
        Some(driver) => driver.dir().to_string(),
        None => std::env::var("AERON_DIR")?,
    };
    println!("throughput workload: message_length={MESSAGE_LENGTH} poll_limit={FRAGMENT_COUNT_LIMIT} clients=2");
    let running = Arc::new(AtomicBool::new(true));
    let stop = running.clone();
    ctrlc::set_handler(move || stop.store(false, Ordering::SeqCst))?;
    let publisher = {
        let running = running.clone();
        let dir = dir.clone();
        thread::Builder::new().name("publisher".into()).spawn(move || {
            let outcome = publish(&running, &dir).map_err(|error| error.to_string());
            // A failed publisher stops the subscriber as well as Ctrl-C does
            running.store(false, Ordering::Release);
            outcome
        })?
    };
    let result = subscribe(&running, &dir);
    running.store(false, Ordering::Release);
    publisher
        .join()
        .map_err(|_| "publisher thread panicked")?
        .map_err(|error| format!("publisher: {error}"))?;
    result
}

fn client(dir: &str) -> Result<Aeron, AeronCError> {
    let context = AeronContext::new()?;
    context.set_dir(&cformat!("{dir}"))?;
    context.set_pre_touch_mapped_memory(false)?;
    let client = Aeron::new(&context)?;
    client.start()?;
    Ok(client)
}

fn publish(running: &AtomicBool, dir: &str) -> Result<(), Box<dyn Error>> {
    let client = client(dir)?;
    let publication = client
        .async_add_exclusive_publication(AERON_IPC_STREAM, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    // Offers before the subscriber joins would count as back pressure. Check every 10 ms, up
    // to 500 times
    let mut checks = 0;
    while !publication.is_connected() {
        checks += 1;
        if checks > 500 {
            return Err("no subscriber joined the publication within 5 seconds".into());
        }
        thread::sleep(Duration::from_millis(10));
    }

    let buffer = [0u8; MESSAGE_LENGTH];
    let mut retries = 0u64;
    let mut messages = 0u64;
    'publish: while running.load(Ordering::Acquire) {
        loop {
            match publication.offer(&buffer) {
                Ok(_) => break,
                Err(e) if e.is_retryable() => {
                    retries += 1;
                    if !running.load(Ordering::Acquire) {
                        break 'publish;
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        messages += 1;
    }
    if messages > 0 {
        println!("Publisher back pressure ratio: {:.6}", retries as f64 / messages as f64);
    }
    Ok(())
}

fn subscribe(running: &AtomicBool, dir: &str) -> Result<(), Box<dyn Error>> {
    let client = client(dir)?;
    let subscription = client
        .async_add_subscription(AERON_IPC_STREAM, STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;
    let mut messages = 0u64;
    let mut start = Instant::now();
    while running.load(Ordering::Acquire) {
        subscription.poll_fn(|_, _| messages += 1, FRAGMENT_COUNT_LIMIT)?;
        if messages >= BURST_LENGTH && start.elapsed() >= Duration::from_secs(1) {
            let rate = messages as f64 / start.elapsed().as_secs_f64();
            let megabytes = rate * MESSAGE_LENGTH as f64 / 1e6;
            println!("rate: {rate:.0} msg/s ({megabytes:.1} MB/s)");
            messages = 0;
            start = Instant::now();
        }
    }
    Ok(())
}
