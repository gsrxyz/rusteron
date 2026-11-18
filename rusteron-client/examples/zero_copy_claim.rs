//! # Zero-copy claim
//!
//! Publishes by writing straight into the publication's term buffer with
//! `try_claim_owned`, stamps each frame's reserved value with a send timestamp, and reads
//! the frame header fields on the subscriber side. One claim is dropped without a
//! commit: it is aborted, and the subscriber never sees it.
//!
//! ```bash
//! cargo run --release --example zero_copy_claim
//! ```

use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 1101;
const MESSAGE_LEN: usize = 16;
const MESSAGES: u64 = 10;
/// Written into the claim that is dropped, so a delivery of it would show up.
const ABORTED: u64 = u64::MAX;

/// Claims a slot, retrying only the retryable errors.
fn claim_slot<'a>(
    publication: &'a AeronExclusivePublication,
    idle: &mut BackoffIdleStrategy,
) -> Result<AeronClaim<'a>, AeronOfferError> {
    loop {
        match publication.try_claim_owned(MESSAGE_LEN) {
            Ok(claim) => {
                idle.reset();
                return Ok(claim);
            }
            Err(e) if e.is_retryable() => idle.idle(0),
            Err(e) => return Err(e),
        }
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
    let publication = aeron
        .async_add_exclusive_publication(AERON_IPC_STREAM, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;

    let mut idle = BackoffIdleStrategy::new();
    for seq in 0..MESSAGES {
        if seq == MESSAGES / 2 {
            // no commit: the claim aborts on drop, and its slot becomes padding at once
            // rather than after the publication unblock timeout
            let mut dropped = claim_slot(&publication, &mut idle)?;
            dropped.data()[..8].copy_from_slice(&ABORTED.to_le_bytes());
            println!("dropped an uncommitted claim at position {}", dropped.position());
        }
        let mut claim = claim_slot(&publication, &mut idle)?;
        claim.data()[..8].copy_from_slice(&seq.to_le_bytes());
        claim.set_reserved_value(Aeron::nano_clock());
        let position = claim.commit()?;
        println!("committed seq {seq} at position {position}");
    }

    let mut received = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while received.len() < MESSAGES as usize {
        if Instant::now() > deadline {
            return Err(format!("received only {} of {MESSAGES} messages", received.len()).into());
        }
        let fragments = subscription.poll_fn(
            |buf, header| {
                let latency = Aeron::nano_clock() - header.reserved_value().unwrap_or(0);
                println!(
                    "session {:?} stream {:?} term {:?} offset {:?} position {} sent {latency} ns ago",
                    header.session_id(),
                    header.stream_id(),
                    header.term_id(),
                    header.term_offset(),
                    header.position()
                );
                received.push(buf.get(..8).and_then(|b| b.try_into().ok()).map(u64::from_le_bytes));
            },
            10,
        )?;
        idle.idle(fragments);
    }

    let expected: Vec<_> = (0..MESSAGES).map(Some).collect();
    if received != expected {
        return Err(format!("expected {expected:?}, received {received:?}").into());
    }
    println!("received {MESSAGES} messages in order; the dropped claim was skipped as padding");
    Ok(())
}
