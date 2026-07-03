//! # Replay merge — late-joiner catch-up
//!
//! Port of Aeron's `ReplayMergeSubscriber` sample: a subscriber that starts *after* the
//! stream began replays the recorded history from the archive and, once it has caught up,
//! **merges seamlessly onto the live multicast/MDC stream** (`is_merged`, `is_live_added`).
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

use rusteron_archive::testing::{find_unused_udp_port, EmbeddedArchiveMediaDriverProcess};
use rusteron_archive::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
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
    let _process = EmbeddedArchiveMediaDriverProcess::build_and_start(
        &aeron_dir,
        &archive_dir,
        &format!("aeron:udp?endpoint=localhost:{req_port}"),
        &format!("aeron:udp?endpoint=localhost:{resp_port}"),
        &format!("aeron:udp?endpoint=localhost:{events_port}"),
    )?;

    let aeron_context = AeronContext::new()?;
    aeron_context.set_dir(&aeron_dir.clone().into_c_string())?;
    aeron_context.set_error_handler(Some(|code: i32, msg: &str| eprintln!("[client error] {code}: {msg}")))?;
    let aeron = Aeron::new(&aeron_context)?;
    aeron.start()?;

    let archive_context = AeronArchiveContext::new()?;
    archive_context.set_aeron(&aeron)?;
    archive_context.set_control_request_channel(&format!("aeron:udp?endpoint=localhost:{req_port}").into_c_string())?;
    archive_context
        .set_control_response_channel(&format!("aeron:udp?endpoint=localhost:{resp_port}").into_c_string())?;
    archive_context
        .set_recording_events_channel(&format!("aeron:udp?endpoint=localhost:{events_port}").into_c_string())?;
    let archive =
        AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(Duration::from_secs(20))?;
    println!("connected to archive");

    // ── Publisher on an MDC (multi-destination-cast) channel, recorded remotely ──
    let publication = aeron.add_publication(
        &format!("aeron:udp?control={control_endpoint}|control-mode=dynamic|term-length=65536").into_c_string(),
        STREAM_ID,
        Duration::from_secs(5),
    )?;
    let session_id = publication.session_id();
    archive.start_recording(
        &format!("aeron:udp?endpoint=localhost:{recording_port}|control={control_endpoint}|session-id={session_id}")
            .into_c_string(),
        STREAM_ID,
        SOURCE_LOCATION_REMOTE,
        true,
    )?;

    // Publish history, throttled so the archiver keeps up (`is_archive_position_with`).
    let published = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let publisher = {
        let published = published.clone();
        let running = running.clone();
        std::thread::spawn(move || {
            let mut n = 0u64;
            while running.load(Ordering::Acquire) {
                let message = format!("message-{n}");
                loop {
                    match publication.offer(message.as_bytes()) {
                        Ok(_) => break,
                        Err(e) if e.is_retryable() => sleep(Duration::from_millis(1)),
                        Err(e) => {
                            eprintln!("publisher stopping: {e}");
                            return;
                        }
                    }
                }
                n += 1;
                published.store(n, Ordering::Release);
                if n > HISTORY_MESSAGES {
                    // live phase: pace it and let the archiver stay caught up
                    while !publication.is_archive_position_with(0) {
                        sleep(Duration::from_micros(300));
                    }
                }
            }
        })
    };
    while published.load(Ordering::Acquire) < HISTORY_MESSAGES {
        sleep(Duration::from_millis(10));
    }
    println!("{HISTORY_MESSAGES} historical messages recorded; late joiner starting");

    // ── The late joiner: replay history, then merge onto the live stream ──
    let counters = aeron.counters_reader();
    let mut counter_id = -1;
    while counter_id < 0 {
        counter_id = RecordingPos::find_counter_id_by_session(&counters, session_id);
    }
    let recording_id = RecordingPos::get_recording_id_block(&counters, counter_id, Duration::from_secs(5))?;

    let subscription = aeron.add_subscription(
        &format!("aeron:udp?control-mode=manual|session-id={session_id}").into_c_string(),
        STREAM_ID,
        Handlers::no_available_image_handler(),
        Handlers::no_unavailable_image_handler(),
        Duration::from_secs(5),
    )?;
    let replay_merge = AeronArchiveReplayMerge::new(
        &subscription,
        &archive,
        &format!("aeron:udp?session-id={session_id}").into_c_string(),
        &"aeron:udp?endpoint=localhost:0".into_c_string(), // replay destination (ephemeral)
        &format!("aeron:udp?endpoint=localhost:{live_port}|control={control_endpoint}").into_c_string(),
        recording_id,
        0, // start position: from the beginning
        Aeron::epoch_clock(),
        10_000, // merge progress timeout ms
    )?;

    let mut received = 0u64;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !replay_merge.is_merged() {
        assert!(!replay_merge.has_failed(), "replay merge failed");
        if Instant::now() > deadline {
            return Err("timed out waiting for replay merge".into());
        }
        if replay_merge.poll_fn(|_buf, _hdr| received += 1, 256)? == 0 {
            if let Some(err) = archive.poll_for_error()? {
                return Err(format!("archive error during merge: {err}").into());
            }
            sleep(Duration::from_millis(1));
        }
    }
    assert!(replay_merge.is_live_added());
    println!(
        "merged onto live after {received} replayed messages (published so far: {})",
        published.load(Ordering::Acquire)
    );

    // Once merged, the plain subscription IS the live stream — poll it directly.
    let mut live_received = 0u64;
    let deadline = Instant::now() + Duration::from_secs(10);
    while live_received < 1_000 && Instant::now() < deadline {
        subscription.poll_fn(|_buf, _hdr| live_received += 1, 256)?;
    }
    assert!(live_received >= 1_000, "expected live traffic after the merge");
    println!("received {live_received} further messages live; replay-merge complete");

    running.store(false, Ordering::Release);
    publisher.join().ok();
    drop(replay_merge);
    drop(subscription);
    Ok(())
}
