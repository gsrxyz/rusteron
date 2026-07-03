//! # Basic subscriber
//!
//! Port of Aeron's `basic_subscriber.c` / `BasicSubscriber.java`: subscribe to a
//! channel+stream and print fragments until interrupted. Showcases per-subscription
//! image lifecycle handlers and the zero-allocation `poll_fn`.
//!
//! Run standalone against [`basic_publisher`]:
//!
//! ```bash
//! cargo run --release --features examples --example basic_subscriber
//! ```

use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const CHANNEL: &str = "aeron:udp?endpoint=localhost:40123";
const STREAM_ID: i32 = 10;
const FRAGMENT_COUNT_LIMIT: usize = 10;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let _ = ctrlc::set_handler(move || r.store(false, Ordering::SeqCst));

    let driver = EmbeddedDriver::launch()?;
    let ctx = AeronContext::new()?;
    ctx.set_dir(&driver.dir().into_c_string())?;
    ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    // Image lifecycle handlers are per-subscription in the C API (unlike Java's
    // Context-level handlers). Each fires with the subscription + the new image.
    let on_avail = Handler::new(|_sub: AeronSubscription, _image: AeronImage| {
        println!(">> available image");
    });
    let on_unavail = Handler::new(|_sub: AeronSubscription, _image: AeronImage| {
        println!("<< unavailable image");
    });

    let channel = CHANNEL.into_c_string();
    let subscription = aeron
        .async_add_subscription(&channel, STREAM_ID, Some(&on_avail), Some(&on_unavail))?
        .poll_blocking(Duration::from_secs(5))?;
    println!("subscribing to {CHANNEL} on stream id {STREAM_ID}");

    while running.load(Ordering::SeqCst) {
        // poll_fn: zero-allocation closure poll for non-fragmented messages.
        let fragments = subscription.poll_fn(
            |buf, header| {
                let pos = header.position();
                let text = std::str::from_utf8(buf).unwrap_or("<non-utf8>");
                println!("received (pos {pos}, {} bytes): {text}", buf.len());
            },
            FRAGMENT_COUNT_LIMIT,
        )?;
        if fragments == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    println!("shutting down");
    drop(subscription);
    drop(aeron);
    Ok(())
}
