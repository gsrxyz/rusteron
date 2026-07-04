//! # Persistent Subscription — failure & recovery
//!
//! The failure-mode companion to `persistent_subscription.rs`. A persistent subscription
//! rides through the loss of the live stream:
//!
//! 1. replay + join live (`on_live_joined`);
//! 2. the live publication dies → the subscription **falls back to replay**
//!    (`on_live_left`, `is_replaying()`), losing nothing — the archive keeps recording
//!    history it already has;
//! 3. the live stream comes back → it **rejoins live** (`on_live_joined` again).
//!
//! Requires `java` on PATH (an embedded Java Archive is started for you).
//!
//! ```bash
//! cargo run --release --features "static precompile" --example persistent_subscription_failover
//! ```

use rusteron_archive::testing::{find_unused_udp_port, EmbeddedArchiveMediaDriverProcess};
use rusteron_archive::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

    let id = Aeron::nano_clock();
    let aeron_dir = format!("target/aeron/{id}_failover/shm");
    let archive_dir = format!("target/aeron/{id}_failover/archive");
    let req_port = find_unused_udp_port(9300).expect("no free port");
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

    // Record the live stream and seed history. An *exclusive* publication so we can
    // tear it down (simulating the upstream service dying) and re-add it cleanly.
    let live_channel = "aeron:ipc";
    let stream_id = 3201;
    archive.start_recording(&cformat!("{live_channel}"), stream_id, SOURCE_LOCATION_LOCAL, true)?;
    let mut publication = aeron
        .async_add_exclusive_publication(&cformat!("{live_channel}"), stream_id)?
        .poll_blocking(Duration::from_secs(5))?;
    let start = Instant::now();
    while !publication.is_connected() && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(10));
    }
    for i in 0..10 {
        let m = format!("Seed-{i}");
        while publication.offer(m.as_bytes()).is_err() {
            sleep(Duration::from_millis(1));
        }
    }
    let session_id = publication.get_constants()?.session_id;
    let counters_reader = aeron.counters_reader();
    let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
    let recording_id = RecordingPos::get_recording_id_block(&counters_reader, counter_id, Duration::from_secs(5))?;
    println!("recording id {recording_id}, seeded 10 messages");

    struct FailoverListener {
        joined: Arc<AtomicUsize>,
        left: Arc<AtomicUsize>,
        errors: Arc<Mutex<Vec<(i32, String)>>>,
    }
    impl PersistentSubscriptionListener for FailoverListener {
        fn on_live_joined(&self) {
            self.joined.fetch_add(1, Ordering::SeqCst);
            println!(
                "[listener] on_live_joined (total {})",
                self.joined.load(Ordering::SeqCst)
            );
        }
        fn on_live_left(&self) {
            self.left.fetch_add(1, Ordering::SeqCst);
            println!("[listener] on_live_left — falling back to replay");
        }
        fn on_error(&self, code: i32, msg: &str) {
            eprintln!("[listener] error {code}: {msg}");
            self.errors.lock().unwrap().push((code, msg.into()));
        }
    }
    let joined = Arc::new(AtomicUsize::new(0));
    let left = Arc::new(AtomicUsize::new(0));
    let errors: Arc<Mutex<Vec<(i32, String)>>> = Arc::new(Mutex::new(Vec::new()));

    let ps = persistent_subscription_builder()?
        .aeron(&aeron)?
        .archive_context(&archive_context)?
        .live_channel(live_channel)?
        .live_stream_id(stream_id)?
        .replay_channel("aeron:udp?endpoint=localhost:0")?
        .replay_stream_id(stream_id + 1)?
        .start_from_beginning()?
        .recording_id(recording_id)?
        .listener(FailoverListener {
            joined: joined.clone(),
            left: left.clone(),
            errors: errors.clone(),
        })?
        .build()?;

    // ── Phase 1: replay history, then join live ─────────────────────────
    println!("phase 1: replaying history, waiting to join live...");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ps.is_live() && Instant::now() < deadline {
        if ps.has_failed() {
            let (code, msg) = ps.get_failure_reason().unwrap_or((-1, "unknown".into()));
            return Err(format!("persistent subscription failed ({code}): {msg}").into());
        }
        let _ = publication.offer(b"live-beat");
        ps.poll_once(|_buf, _hdr| {}, 100)?;
        sleep(Duration::from_millis(1));
    }
    assert!(joined.load(Ordering::SeqCst) >= 1, "never joined live");

    // ── Phase 2: the live stream dies ────────────────────────────────────
    println!("phase 2: dropping the live publication (upstream service dies)...");
    drop(publication);
    let deadline = Instant::now() + Duration::from_secs(30);
    while ps.is_live() && Instant::now() < deadline {
        if ps.has_failed() {
            let (code, msg) = ps.get_failure_reason().unwrap_or((-1, "unknown".into()));
            return Err(format!("persistent subscription failed during outage ({code}): {msg}").into());
        }
        ps.poll_once(|_buf, _hdr| {}, 100)?;
        sleep(Duration::from_millis(10));
    }
    assert!(left.load(Ordering::SeqCst) >= 1, "never detected loss of live stream");
    println!("now replaying from the archive (is_replaying = {})", ps.is_replaying());

    // ── Phase 3: the live stream comes back — rejoin ─────────────────────
    println!("phase 3: restoring the live publication...");
    publication = aeron
        .async_add_exclusive_publication(&cformat!("{live_channel}"), stream_id)?
        .poll_blocking(Duration::from_secs(5))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while joined.load(Ordering::SeqCst) < 2 && Instant::now() < deadline {
        if ps.has_failed() {
            let (code, msg) = ps.get_failure_reason().unwrap_or((-1, "unknown".into()));
            return Err(format!("persistent subscription failed on rejoin ({code}): {msg}").into());
        }
        let _ = publication.offer(b"live-beat");
        ps.poll_once(|_buf, _hdr| {}, 100)?;
        sleep(Duration::from_millis(1));
    }

    let joined_count = joined.load(Ordering::SeqCst);
    ps.close()?;
    assert!(
        joined_count >= 2,
        "did not rejoin live (joined {joined_count}); errors: {:?}",
        errors.lock().unwrap().clone()
    );
    println!(
        "failover complete: joined live {joined_count} time(s), fell back {} time(s)",
        left.load(Ordering::SeqCst)
    );
    Ok(())
}
