//! # Retained images
//!
//! The lifecycle of an image handle kept beyond a poll: `for_each_image` borrows the
//! images with no bookkeeping, `image_by_session_id` retains one, which is polled directly,
//! stays valid after its publisher goes (`is_closed` turns true), and is dropped before the
//! subscription closes.
//!
//! ```bash
//! cargo run --release --example retained_images
//! ```

use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::thread::sleep;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 1201;
const MESSAGES: usize = 5;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = EmbeddedDriver::launch()?;
    let ctx = AeronContext::new()?;
    ctx.set_dir(&cformat!("{}", driver.dir()))?;
    ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    // the image passed here is valid only during the call; retain one from the subscription
    let on_available = Handler::new(|_sub: &AeronSubscription, image: &AeronImage| {
        if let Ok(constants) = image.get_constants() {
            println!(">> image available: session {}", constants.session_id);
        }
    });
    let on_unavailable = Handler::new(|_sub: &AeronSubscription, image: &AeronImage| {
        println!("<< image unavailable at position {}", image.position());
    });
    let subscription = aeron
        .async_add_subscription(AERON_IPC_STREAM, STREAM_ID, Some(&on_available), Some(&on_unavailable))?
        .poll_blocking(Duration::from_secs(5))?;
    let publication = aeron
        .async_add_publication(AERON_IPC_STREAM, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;

    let deadline = Instant::now() + Duration::from_secs(5);
    let image = loop {
        if let Some(image) = subscription.image_by_session_id(publication.session_id()) {
            break image;
        }
        if Instant::now() > deadline {
            return Err("the publication's image never appeared".into());
        }
        sleep(Duration::from_millis(1));
    };
    println!("retained the image of session {}", publication.session_id());

    // borrowed for the closure only: no retain or release
    subscription.for_each_image(|img| println!("for_each_image: image at position {}", img.position()));

    for i in 0..MESSAGES {
        let message = format!("message {i}");
        loop {
            match publication.offer(message.as_bytes()) {
                Ok(_) => break,
                Err(e) if e.is_retryable() => sleep(Duration::from_millis(1)),
                Err(e) => return Err(e.into()),
            }
        }
    }

    // poll the retained image directly, as a subscription poll would
    let mut received = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while received < MESSAGES {
        if Instant::now() > deadline {
            return Err(format!("read only {received} of {MESSAGES} messages").into());
        }
        received += image.poll_fn(
            |buf, _header| println!("image.poll_fn: {}", String::from_utf8_lossy(buf)),
            10,
        )? as usize;
    }
    println!("image consumed to position {}", image.position());

    drop(publication);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !image.is_closed() {
        if Instant::now() > deadline {
            return Err("the image did not close after its publisher went".into());
        }
        // the client's conductor delivers the unavailable-image callback
        sleep(Duration::from_millis(10));
    }
    let after_close = image.poll_fn(|_, _| {}, 10)?;
    println!(
        "publisher gone: the retained handle is still valid, is_closed() is true, a poll read {after_close} fragments"
    );

    // a handle must not be used after the subscription closes, so release it first
    drop(image);
    subscription.close()?;
    println!("dropped the image handle, then closed the subscription");
    Ok(())
}
