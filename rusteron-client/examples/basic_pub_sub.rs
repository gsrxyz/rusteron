//! # Basic pub/sub
//!
//! One process publishes and subscribes on an IPC stream through an embedded media driver.
//! Messages are 1 MiB, larger than the MTU, so the subscriber reassembles them with a
//! fragment assembler.
//!
//! ```bash
//! cargo run --release --example basic_pub_sub
//! ```

use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const STREAM_ID: i32 = 123;
const MESSAGE_LEN: usize = 1024 * 1024;
const MESSAGES: usize = 100;

struct Counter {
    count: usize,
    bad: usize,
}

impl AeronFragmentHandlerCallback for Counter {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: &AeronHeader) {
        // count bad messages instead of asserting: a panic cannot unwind into C and aborts
        if buffer.len() != MESSAGE_LEN || buffer.iter().any(|&b| b != b'1') {
            self.bad += 1;
        }
        self.count += 1;
        println!(
            "received message at position {} ({} bytes)",
            header.position(),
            buffer.len()
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = EmbeddedDriver::launch()?;
    let ctx = AeronContext::new()?;
    ctx.set_dir(&cformat!("{}", driver.dir()))?;
    ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    let mut subscription = aeron
        .async_add_subscription(AERON_IPC_STREAM, STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;

    let running = Arc::new(AtomicBool::new(true));
    let publisher = {
        let running = running.clone();
        let dir = driver.dir().to_string();
        // This publisher uses its own client on the sending thread.
        std::thread::spawn(move || -> Result<(), AeronCError> {
            let aeron = Aeron::connect_dir(&dir)?;
            // offer() reports NotConnected until the image links; the loop below retries it
            let publication = aeron
                .async_add_publication(AERON_IPC_STREAM, STREAM_ID)?
                .poll_blocking(Duration::from_secs(5))?;
            let message = vec![b'1'; MESSAGE_LEN];
            let mut idle = BackoffIdleStrategy::new();
            while running.load(Ordering::Acquire) {
                match publication.offer(&message) {
                    Ok(_) => idle.reset(),
                    Err(e) if e.is_retryable() => idle.idle(0),
                    Err(e) => {
                        eprintln!("publication gone: {e}");
                        break;
                    }
                }
            }
            Ok(())
        })
    };

    // For messages under the MTU, poll_fn receives fragments without an assembler.
    let mut assembler = FragmentAssembler::new(Counter { count: 0, bad: 0 })?;
    let mut idle = BackoffIdleStrategy::new();
    loop {
        idle.idle(assembler.poll(&mut subscription, 10)?);
        if assembler.handler().count >= MESSAGES {
            break;
        }
    }

    running.store(false, Ordering::Release);
    publisher.join().map_err(|_| "publisher thread panicked")??;
    if assembler.handler().bad > 0 {
        return Err(format!("{} corrupt messages", assembler.handler().bad).into());
    }
    println!("received {} messages", assembler.handler().count);
    Ok(())
}
