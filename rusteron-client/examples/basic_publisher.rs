//! # Basic publisher
//!
//! Port of Aeron's `basic_publisher.c` / `BasicPublisher.java`: publish messages on a
//! channel+stream until interrupted. Showcases the typed-offer retry pattern — retry the
//! retryable errors (back-pressure / admin action / not-connected), stop on fatal ones.
//!
//! Run standalone against [`basic_subscriber`]:
//!
//! ```bash
//! cargo run --release --features examples --example basic_publisher
//! ```

use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const CHANNEL: &std::ffi::CStr = c"aeron:udp?endpoint=localhost:40123";
const STREAM_ID: i32 = 10;
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

    let publication = aeron
        .async_add_publication(CHANNEL, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    println!("publishing to {} on stream id {STREAM_ID}", CHANNEL.to_str().unwrap());

    let mut count: u64 = 0;
    while running.load(Ordering::SeqCst) {
        let mut buf = MESSAGE.to_vec();
        buf.extend_from_slice(count.to_string().as_bytes());
        // offer() returns Ok(position) or a typed AeronOfferError.
        match publication.offer(&buf) {
            Ok(_pos) => count += 1,
            Err(e) if e.is_retryable() => std::hint::spin_loop(),
            Err(e) => return Err(format!("publication gone: {e}").into()),
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    println!("published {count} messages; shutting down");
    drop(publication);
    drop(aeron);
    Ok(())
}
