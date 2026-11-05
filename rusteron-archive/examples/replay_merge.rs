//! # Replay merge for late-joiner catch-up
//!
//! Port of Aeron's `ReplayMergeSubscriber` sample: a subscriber that starts *after* the
//! stream began replays the recorded history from the archive and, once it has caught up,
//! **merges onto the live multicast/MDC stream** (`is_merged`, `is_live_added`).
//! This is the standard late-joiner pattern for market-data / order-flow feeds.
//!
//! Flow: publisher on an MDC channel → archive records it (remote source) → late joiner
//! drives an [`AeronArchiveReplayMerge`] over a manual-control subscription.
//!
//! Requires `java` on PATH (an embedded Java Archive is started for you).
//!
//! ```bash
//! cargo run --release --features "static precompile" --example replay_merge
//! ```

use rusteron_archive::testing::{
    EmbeddedArchiveMediaDriverProcess, find_counter_id_by_session_in_archive_blocking, find_unused_udp_port,
};
use rusteron_archive::*;
use std::thread::sleep;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 1042;
const HISTORY_MESSAGES: u64 = 10_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

    let id = Aeron::nano_clock();
    let aeron_dir = format!("target/aeron/{id}_rm/shm");
    let archive_dir = format!("target/aeron/{id}_rm/archive");
    let req_port = find_unused_udp_port(9400).expect("no free port");
    let resp_port = find_unused_udp_port(req_port + 1).expect("no free port");
    let events_port = find_unused_udp_port(resp_port + 1).expect("no free port");
    let control_port = find_unused_udp_port(events_port + 1).expect("no free port");
    let recording_port = find_unused_udp_port(control_port + 1).expect("no free port");
    let live_port = find_unused_udp_port(recording_port + 1).expect("no free port");
    let control_endpoint = format!("localhost:{control_port}");
    let process = EmbeddedArchiveMediaDriverProcess::build_and_start(
        &aeron_dir,
        &archive_dir,
        &format!("aeron:udp?endpoint=localhost:{req_port}"),
        &format!("aeron:udp?endpoint=localhost:{resp_port}"),
        &format!("aeron:udp?endpoint=localhost:{events_port}"),
    )?;

    let aeron_context = AeronContext::new()?;
    aeron_context.set_dir(&cformat!("{aeron_dir}"))?;
    aeron_context.set_error_handler(Some(|code: i32, msg: &str| eprintln!("[client error] {code}: {msg}")))?;
    let aeron = Aeron::new(&aeron_context)?;
    aeron.start()?;

    let archive_context = process.archive_context(&aeron)?;
    let archive =
        AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(Duration::from_secs(20))?;
    println!("connected to archive");

    // Publisher on an MDC (multi-destination-cast) channel, recorded remotely
    let publication = aeron.add_publication(
        &cformat!("aeron:udp?control={control_endpoint}|control-mode=dynamic|term-length=65536"),
        STREAM_ID,
        Duration::from_secs(5),
    )?;
    let session_id = publication.session_id();
    archive.start_recording(
        &cformat!("aeron:udp?endpoint=localhost:{recording_port}|control={control_endpoint}|session-id={session_id}"),
        STREAM_ID,
        SOURCE_LOCATION_REMOTE,
        true,
    )?;

    // Find the recording's counter once, rather than scanning every counter on each check.
    let counters = aeron.counters_reader();
    let counter_id = find_counter_id_by_session_in_archive_blocking(
        &counters,
        session_id,
        archive.get_archive_id(),
        Duration::from_secs(10),
    )?;
    let recording_id = RecordingPos::get_recording_id_block(&counters, counter_id, Duration::from_secs(5))?;

    // Publish the history as fast as flow control allows. Everything runs on this thread:
    // without `multi-threaded` a publication stays on the thread whose client created it.
    let mut published = 0u64;
    while published < HISTORY_MESSAGES {
        let message = format!("message-{published}");
        loop {
            match publication.offer(message.as_bytes()) {
                Ok(_) => break,
                Err(e) if e.is_retryable() => sleep(Duration::from_millis(1)),
                Err(e) => return Err(e.into()),
            }
        }
        published += 1;
    }
    println!("{HISTORY_MESSAGES} historical messages recorded; late joiner starting");

    // The late joiner: replay history, then merge onto the live stream

    let subscription = aeron.add_subscription(
        &cformat!("aeron:udp?control-mode=manual|session-id={session_id}"),
        STREAM_ID,
        Handlers::NONE,
        Handlers::NONE,
        Duration::from_secs(5),
    )?;
    let replay_merge = AeronArchiveReplayMerge::new(
        &subscription,
        &archive,
        &cformat!("aeron:udp?session-id={session_id}"),
        c"aeron:udp?endpoint=localhost:0", // replay destination (ephemeral)
        &cformat!("aeron:udp?endpoint=localhost:{live_port}|control={control_endpoint}"),
        recording_id,
        0, // start position: from the beginning
        Aeron::epoch_clock(),
        10_000, // merge progress timeout ms
    )?;

    let mut received = 0u64;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !replay_merge.is_merged() {
        if replay_merge.has_failed() {
            return Err("replay merge failed".into());
        }
        if Instant::now() > deadline {
            return Err("timed out waiting for replay merge".into());
        }
        // the live stream goes on while the joiner catches up
        publish_live(&publication, &counters, (counter_id, recording_id), &mut published)?;
        // Archive errors arrive as Err from poll_fn; polling the archive here would take
        // the merge's own responses and stall it.
        if replay_merge.poll_fn(|_buf, _hdr| received += 1, 256)? == 0 {
            sleep(Duration::from_millis(1));
        }
    }
    assert!(replay_merge.is_live_added());
    println!("merged onto live after {received} replayed messages (published so far: {published})");

    // Once merged, the plain subscription IS the live stream. Poll it directly.
    let mut live_received = 0u64;
    let deadline = Instant::now() + Duration::from_secs(10);
    while live_received < 1_000 && Instant::now() < deadline {
        publish_live(&publication, &counters, (counter_id, recording_id), &mut published)?;
        subscription.poll_fn(|_buf, _hdr| live_received += 1, 256)?;
    }
    assert!(live_received >= 1_000, "expected live traffic after the merge");
    println!("received {live_received} further messages live; replay-merge complete");

    drop(replay_merge);
    drop(subscription);
    Ok(())
}

/// Offers the next live message once the archiver has recorded everything published so far,
/// so it stays caught up; does nothing until then.
fn publish_live(
    publication: &AeronPublication,
    counters: &AeronCountersReader,
    (counter_id, recording_id): (i32, i64),
    published: &mut u64,
) -> Result<(), Box<dyn std::error::Error>> {
    if counters.get_counter_value(counter_id)? < publication.position() {
        // a stopped recording never catches up, and its counter id may be reused
        if !RecordingPos::is_active(counters, counter_id, recording_id)? {
            return Err(format!("recording {recording_id} stopped").into());
        }
        return Ok(());
    }
    match publication.offer(format!("message-{published}").as_bytes()) {
        Ok(_) => *published += 1,
        Err(e) if e.is_retryable() => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
