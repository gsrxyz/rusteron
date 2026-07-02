//! # Recording replication (archive → archive)
//!
//! Port of Aeron's `RecordingReplicator` sample: a **destination** archive pulls a recording
//! from a **source** archive over the source's control channel (`archive.replicate(...)`).
//! This is the standard pattern for archive redundancy / promoting a backup.
//!
//! Flow: record + stop a stream on the source archive → ask the destination archive to
//! replicate it → wait until the destination's copy reaches the source's stop position.
//!
//! Requires `java` on PATH (two embedded Java Archives are started for you).
//!
//! ```bash
//! cargo run --release --features "static precompile" --example recording_replication
//! ```

use rusteron_archive::testing::EmbeddedArchiveMediaDriverProcess;
use rusteron_archive::*;
use std::thread::sleep;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 1060;
const MESSAGES: u64 = 10_000;

fn find_unused_udp_port(start: u16) -> Option<u16> {
    (start..65535).find(|p| std::net::UdpSocket::bind(("127.0.0.1", *p)).is_ok())
}

struct ArchiveInstance {
    aeron: Aeron,
    archive: AeronArchive,
    control_request_channel: String,
    _process: EmbeddedArchiveMediaDriverProcess,
    _contexts: AeronContext,
}

fn start_archive(name: &str, first_port: u16) -> Result<ArchiveInstance, Box<dyn std::error::Error>> {
    let id = Aeron::nano_clock();
    let aeron_dir = format!("target/aeron/{id}_{name}/shm");
    let archive_dir = format!("target/aeron/{id}_{name}/archive");
    let req_port = find_unused_udp_port(first_port).expect("no free port");
    let resp_port = find_unused_udp_port(req_port + 1).expect("no free port");
    let events_port = find_unused_udp_port(resp_port + 1).expect("no free port");
    let control_request_channel = format!("aeron:udp?endpoint=localhost:{req_port}");
    let process = EmbeddedArchiveMediaDriverProcess::build_and_start(
        &aeron_dir,
        &archive_dir,
        &control_request_channel,
        &format!("aeron:udp?endpoint=localhost:{resp_port}"),
        &format!("aeron:udp?endpoint=localhost:{events_port}"),
    )?;

    let aeron_context = AeronContext::new()?;
    aeron_context.set_dir(&aeron_dir.clone().into_c_string())?;
    aeron_context.set_error_handler(Some(move |code: i32, msg: &str| {
        eprintln!("[client error] {code}: {msg}")
    }))?;
    let aeron = Aeron::new(&aeron_context)?;
    aeron.start()?;

    let archive = AeronArchive::connect(
        &aeron,
        &control_request_channel,
        &format!("aeron:udp?endpoint=localhost:{resp_port}"),
        Some(&format!("aeron:udp?endpoint=localhost:{events_port}")),
        Duration::from_secs(20),
    )?;
    println!("[{name}] archive up (control {control_request_channel})");
    Ok(ArchiveInstance {
        aeron,
        archive,
        control_request_channel,
        _process: process,
        _contexts: aeron_context,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

    let src = start_archive("src", 9600)?;
    let dst = start_archive("dst", 9700)?;

    // ── 1. Record a finished stream on the SOURCE archive ────────────────
    let subscription_id = src
        .archive
        .start_recording(AERON_IPC_STREAM, STREAM_ID, SOURCE_LOCATION_LOCAL, false)?;
    let publication = src
        .aeron
        .async_add_exclusive_publication(AERON_IPC_STREAM, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    for i in 0..MESSAGES {
        let message = format!("replicated-{i}");
        loop {
            match publication.offer_simple(message.as_bytes()) {
                Ok(_) => break,
                Err(e) if e.is_retryable() => sleep(Duration::from_millis(1)),
                Err(e) => return Err(format!("offer failed: {e}").into()),
            }
        }
    }
    let session_id = publication.get_constants()?.session_id;
    let counters = src.aeron.counters_reader();
    let counter_id = RecordingPos::find_counter_id_by_session(&counters, session_id);
    let src_recording_id = RecordingPos::get_recording_id_block(&counters, counter_id, Duration::from_secs(5))?;
    let stop_position = publication.position();
    let deadline = Instant::now() + Duration::from_secs(10);
    while counters.get_counter_value(counter_id) < stop_position && Instant::now() < deadline {
        sleep(Duration::from_millis(5));
    }
    drop(publication);
    src.archive.stop_recording_subscription(subscription_id)?;
    println!("[src] recording {src_recording_id} complete at position {stop_position}");

    // ── 2. Ask the DESTINATION archive to replicate it from the source ───
    // Builder defaults: replicate into a NEW recording, no live merge, the context's
    // default replication channel, no credentials.
    let params = AeronArchiveReplicationParams::builder().build()?;
    let replication_id = dst.archive.replicate(
        src_recording_id,
        &src.control_request_channel.clone().into_c_string(),
        dst.archive.get_archive_context().get_control_request_stream_id(), // archives share the default control stream id
        &params,
    )?;
    println!("[dst] replication {replication_id} started");

    // ── 3. Wait for the destination copy to reach the source stop position ──
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut replicated: Option<AeronArchiveRecordingDescriptor> = None;
    while Instant::now() < deadline {
        dst.archive.poll_for_recording_signals()?;
        if let Some(err) = dst.archive.poll_for_error()? {
            return Err(format!("destination archive error: {err}").into());
        }
        let mut count = 0i32;
        let mut found = None;
        dst.archive.list_recordings_once(&mut count, 0, 100, |descriptor| {
            if descriptor.stop_position == stop_position {
                found = Some(descriptor.clone_struct());
            }
        })?;
        if let Some(descriptor) = found {
            replicated = Some(descriptor);
            break;
        }
        sleep(Duration::from_millis(50));
    }
    let replicated = replicated.ok_or("replication did not complete in time")?;
    println!(
        "[dst] recording {} replicated: [{} .. {}] ({} bytes), source session {}",
        replicated.recording_id,
        replicated.start_position,
        replicated.stop_position,
        replicated.stop_position - replicated.start_position,
        replicated.session_id,
    );
    assert_eq!(replicated.stop_position, stop_position);
    assert_eq!(
        replicated.session_id, session_id,
        "replication keeps the source session id"
    );
    println!("replication complete and verified");
    Ok(())
}
