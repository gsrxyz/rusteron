//! # Record and Replay example
//!
//! Ports Aeron's `RecordedBasicPublisher` + `ReplayedBasicSubscriber`: record a stream to the
//! Aeron Archive, then replay it from the start. Focuses on **retryable vs unrecoverable error
//! handling** — the split Aeron expects callers to make (back off + retry transient errors;
//! abort on fatal ones).
//!
//! Requires a running Aeron **Java** Archive. This example builds and starts one for you via the
//! `rusteron_archive::testing` harness, so `java` must be on `PATH`.
//!
//! ```bash
//! cargo run --release --features "static precompile" --example record_and_replay
//! ```
//!
//! See the archive README and the upstream sample
//! <https://github.com/aeron-io/aeron/tree/main/aeron-samples/src/main/java/io/aeron/samples/archive>.

use rusteron_archive::testing::{find_unused_udp_port, EmbeddedArchiveMediaDriverProcess};
use rusteron_archive::*;
use std::thread::sleep;
use std::time::{Duration, Instant};

/// Retry `op` while it fails with a **retryable** (transient) error — back off between attempts.
/// Abort immediately on an **unrecoverable** error, or when `deadline` passes. This is the
/// retryable/unrecoverable split callers should apply to archive/client operations:
/// back-pressure, admin action, buffer-full and polling timeouts are retried; a closed
/// publication, an exhausted position, or a driver timeout is fatal.
fn retry_transient<T, F>(deadline: Instant, mut op: F) -> Result<T, AeronCError>
where
    F: FnMut() -> Result<T, AeronCError>,
{
    loop {
        match op() {
            Ok(v) => return Ok(v),
            Err(e) if e.is_retryable() && Instant::now() < deadline => {
                sleep(Duration::from_millis(10));
            }
            Err(e) => return Err(e), // unrecoverable, or timed out retrying
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

    // 1. Start the Java Aeron Archive (builds the jars on first run). Three UDP control ports.
    let id = Aeron::nano_clock();
    let aeron_dir = format!("target/aeron/{id}_replay/shm");
    let archive_dir = format!("target/aeron/{id}_replay/archive");
    let req_port = find_unused_udp_port(9000).expect("no free port");
    let resp_port = find_unused_udp_port(req_port + 1).expect("no free port");
    let events_port = find_unused_udp_port(resp_port + 1).expect("no free port");
    let _process = EmbeddedArchiveMediaDriverProcess::build_and_start(
        &aeron_dir,
        &archive_dir,
        &format!("aeron:udp?endpoint=localhost:{req_port}"),
        &format!("aeron:udp?endpoint=localhost:{resp_port}"),
        &format!("aeron:udp?endpoint=localhost:{events_port}"),
    )?;

    // 2. Connect a client + archive context. The error logger surfaces async client errors.
    // `cformat!` = format + CString in one named step (the one heap allocation stays visible).
    let aeron_context = AeronContext::new()?;
    aeron_context.set_dir(&cformat!("{aeron_dir}"))?;
    let aeron = Aeron::new(&aeron_context)?;
    aeron.start()?;
    let archive_context = AeronArchiveContext::new()?;
    archive_context.set_aeron(&aeron)?;
    archive_context.set_control_request_channel(&cformat!("aeron:udp?endpoint=localhost:{req_port}"))?;
    archive_context.set_control_response_channel(&cformat!("aeron:udp?endpoint=localhost:{resp_port}"))?;
    archive_context.set_recording_events_channel(&cformat!("aeron:udp?endpoint=localhost:{events_port}"))?;
    let archive = retry_transient(Instant::now() + Duration::from_secs(20), || {
        AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(Duration::from_secs(5))
    })?;
    println!("connected to archive");

    // 3. Start recording an IPC stream and publish a small batch.
    let channel = "aeron:ipc";
    let stream_id = 4001;
    archive.start_recording(&cformat!("{channel}"), stream_id, SOURCE_LOCATION_LOCAL, true)?;
    let publication = retry_transient(Instant::now() + Duration::from_secs(10), || {
        aeron
            .async_add_publication(&cformat!("{channel}"), stream_id)?
            .poll_blocking(Duration::from_secs(2))
    })?;

    const MSG_COUNT: usize = 50;
    for i in 0..MSG_COUNT {
        let msg = format!("message-{i}");
        let deadline = Instant::now() + Duration::from_secs(5);
        // offer() returns Ok(position) or a typed error: retry the retryable ones
        // (back-pressure, not-connected, admin action); abort on fatal ones.
        loop {
            if Instant::now() > deadline {
                return Err("timed out offering a message".into());
            }
            match publication.offer(msg.as_bytes()) {
                Ok(_) => break,
                Err(e) if e.is_retryable() => sleep(Duration::from_millis(1)),
                Err(e) => return Err(format!("publication gone: {e}").into()),
            }
        }
    }
    println!("recorded {MSG_COUNT} messages");

    // 4. Resolve the recording id (wait for the recorder to flush what we published).
    let session_id = publication.session_id();
    let counters = aeron.counters_reader();
    let counter_id = RecordingPos::find_counter_id_by_session(&counters, session_id);
    let recording_id = RecordingPos::get_recording_id_block(&counters, counter_id, Duration::from_secs(5))?;
    let published_position = publication.position();
    let deadline = Instant::now() + Duration::from_secs(5);
    while counters.get_counter_value(counter_id) < published_position && Instant::now() < deadline {
        sleep(Duration::from_millis(10));
    }

    // 5. Replay the recording from the start onto a scratch stream and consume it.
    let replay_stream_id = 4002;
    let replay_params = AeronArchiveReplayParams::builder().position(0).follow_live().build()?;
    let replay_session_id =
        archive.start_replay(recording_id, &cformat!("{channel}"), replay_stream_id, &replay_params)?;
    let replay_channel = ChannelUri::add_session_id(channel, replay_session_id as i32).into_c_string();
    let replay_sub = retry_transient(Instant::now() + Duration::from_secs(10), || {
        aeron
            .async_add_subscription(&replay_channel, replay_stream_id, Handlers::NONE, Handlers::NONE)?
            .poll_blocking(Duration::from_secs(2))
    })?;

    let mut received = 0usize;
    let deadline = Instant::now() + Duration::from_secs(20);
    while received < MSG_COUNT && Instant::now() < deadline {
        // poll returns the fragment count; 0 means nothing to do this cycle.
        let n = replay_sub.poll_fn(
            |buf, _hdr| {
                if let Ok(s) = std::str::from_utf8(buf) {
                    println!("replayed: {s}");
                }
            },
            100,
        )?;
        if n == 0 {
            sleep(Duration::from_millis(1));
        } else {
            received += n as usize;
        }
    }
    println!("replayed {received}/{MSG_COUNT} messages");

    if received < MSG_COUNT {
        return Err(format!("only received {received} of {MSG_COUNT} replayed messages").into());
    }
    Ok(())
}
