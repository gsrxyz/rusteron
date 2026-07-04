//! # Recording throughput + listing recordings
//!
//! Port of Aeron's `EmbeddedRecordingThroughput` sample plus a recording listing: publish a
//! burst to an IPC stream being recorded, measure publish throughput and how quickly the
//! archiver catches up, then enumerate the recordings (`list_recordings`) like the
//! `RecordingDescriptorCollector` utilities do.
//!
//! Requires `java` on PATH (an embedded Java Archive is started for you).
//!
//! ```bash
//! cargo run --release --features "static precompile" --example recording_throughput
//! ```

use rusteron_archive::testing::{find_unused_udp_port, EmbeddedArchiveMediaDriverProcess};
use rusteron_archive::*;
use std::thread::sleep;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 1050;
const MESSAGE_LENGTH: usize = 256;
const MESSAGES: u64 = 500_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

    let id = Aeron::nano_clock();
    let aeron_dir = format!("target/aeron/{id}_rt/shm");
    let archive_dir = format!("target/aeron/{id}_rt/archive");
    let req_port = find_unused_udp_port(9500).expect("no free port");
    let resp_port = find_unused_udp_port(req_port + 1).expect("no free port");
    let events_port = find_unused_udp_port(resp_port + 1).expect("no free port");
    let _process = EmbeddedArchiveMediaDriverProcess::build_and_start(
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

    let archive_context = AeronArchiveContext::new()?;
    archive_context.set_aeron(&aeron)?;
    archive_context.set_control_request_channel(&cformat!("aeron:udp?endpoint=localhost:{req_port}"))?;
    archive_context
        .set_control_response_channel(&cformat!("aeron:udp?endpoint=localhost:{resp_port}"))?;
    archive_context
        .set_recording_events_channel(&cformat!("aeron:udp?endpoint=localhost:{events_port}"))?;
    let archive =
        AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(Duration::from_secs(20))?;
    println!("connected to archive");

    // ── record an IPC stream and blast a burst through it ────────────────
    let subscription_id = archive.start_recording(AERON_IPC_STREAM, STREAM_ID, SOURCE_LOCATION_LOCAL, true)?;
    let publication = aeron
        .async_add_exclusive_publication(AERON_IPC_STREAM, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let start_wait = Instant::now();
    while !publication.is_connected() && start_wait.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(10));
    }

    let message = vec![7u8; MESSAGE_LENGTH];
    let mut back_pressure = 0u64;
    let publish_start = Instant::now();
    let mut sent = 0u64;
    while sent < MESSAGES {
        match publication.offer(&message) {
            Ok(_) => sent += 1,
            Err(e) if e.is_retryable() => {
                back_pressure += 1;
                std::hint::spin_loop();
            }
            Err(e) => return Err(format!("fatal offer error after {sent} messages: {e}").into()),
        }
    }
    let publish_elapsed = publish_start.elapsed();
    let publish_rate = sent as f64 / publish_elapsed.as_secs_f64();
    let mb = (sent as usize * MESSAGE_LENGTH) as f64 / (1024.0 * 1024.0);
    println!(
        "published {sent} x {MESSAGE_LENGTH}B in {publish_elapsed:.2?} — {publish_rate:.0} msgs/sec, {:.1} MB/sec ({back_pressure} back-pressure events)",
        mb / publish_elapsed.as_secs_f64()
    );

    // ── how long until the archiver has everything on disk? ──────────────
    let session_id = publication.get_constants()?.session_id;
    let counters = aeron.counters_reader();
    let counter_id = RecordingPos::find_counter_id_by_session(&counters, session_id);
    let recording_id = RecordingPos::get_recording_id_block(&counters, counter_id, Duration::from_secs(5))?;
    let target = publication.position();
    let catchup_start = Instant::now();
    while counters.get_counter_value(counter_id) < target {
        if catchup_start.elapsed() > Duration::from_secs(30) {
            return Err("archiver did not catch up".into());
        }
        sleep(Duration::from_millis(1));
    }
    println!(
        "archiver caught up to position {target} {:.2?} after publishing finished",
        catchup_start.elapsed()
    );

    archive.stop_recording_subscription(subscription_id)?;

    // ── list recordings, like the RecordingDescriptor utilities ──────────
    println!("\nrecordings in the archive:");
    let mut count = 0i32;
    archive.list_recordings_fn(&mut count, 0, 100, |descriptor: AeronArchiveRecordingDescriptor| {
        println!(
            "  recording {}: stream {} session {} [{} .. {}] ({} bytes) {}",
            descriptor.recording_id,
            descriptor.stream_id,
            descriptor.session_id,
            descriptor.start_position,
            descriptor.stop_position,
            descriptor.stop_position - descriptor.start_position,
            descriptor.stripped_channel(),
        );
    })?;
    assert!(count >= 1, "expected at least our recording to be listed");
    assert_eq!(recording_id, 0, "first recording in a fresh archive");
    println!("({count} recording(s))");

    drop(publication);
    Ok(())
}
