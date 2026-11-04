//! # Persistent Subscription failure & recovery
//!
//! The failure-mode companion to `persistent_subscription.rs`. A persistent subscription
//! rides through the loss of the live stream:
//!
//! 1. replay + join live (`on_live_joined`);
//! 2. the live publication dies → `on_live_left`. The subscription re-reads the recording,
//!    replays anything recorded past its position (nothing here, as the stream stopped)
//!    and waits for live again;
//! 3. the live stream comes back, resumed where it stopped → it **rejoins live**
//!    (`on_live_joined` again). Aeron refuses a live stream that restarts behind what the
//!    subscription has seen.
//!
//! Requires `java` on PATH (an embedded Java Archive is started for you).
//!
//! ```bash
//! cargo run --release --features "static precompile" --example persistent_subscription_failover
//! ```

use rusteron_archive::testing::{EmbeddedArchiveMediaDriverProcess, find_unused_udp_port};
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
    let seed_deadline = Instant::now() + Duration::from_secs(5);
    for i in 0..10 {
        let m = format!("Seed-{i}");
        loop {
            match publication.offer(m.as_bytes()) {
                Ok(_) => break,
                Err(e) if e.is_retryable() && Instant::now() < seed_deadline => sleep(Duration::from_millis(1)),
                Err(e) => return Err(e.into()),
            }
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
            println!("[listener] on_live_left — live stream lost");
        }
        fn on_error(&self, code: i32, msg: &str) {
            eprintln!("[listener] error {code}: {msg}");
            self.errors.lock().unwrap().push((code, msg.into()));
        }
    }
    let joined = Arc::new(AtomicUsize::new(0));
    let left = Arc::new(AtomicUsize::new(0));
    let errors: Arc<Mutex<Vec<(i32, String)>>> = Arc::new(Mutex::new(Vec::new()));

    // one client for the subscription and its archive context
    let ps = PersistentSubscriptionBuilder::new_with_aeron(&archive_context, &aeron)?
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
        ps.poll_fn(|_buf, _hdr| {}, 100)?;
        sleep(Duration::from_millis(1));
    }
    assert!(joined.load(Ordering::SeqCst) >= 1, "never joined live");

    // ── Phase 2: the live stream dies ────────────────────────────────────
    println!("phase 2: dropping the live publication (upstream service dies)...");
    // where the stream stopped, so the restarted publisher can resume it there
    let constants = publication.get_constants()?;
    let stopped_at = publication.position();
    drop(publication);
    let deadline = Instant::now() + Duration::from_secs(30);
    while ps.is_live() && Instant::now() < deadline {
        if ps.has_failed() {
            let (code, msg) = ps.get_failure_reason().unwrap_or((-1, "unknown".into()));
            return Err(format!("persistent subscription failed during outage ({code}): {msg}").into());
        }
        ps.poll_fn(|_buf, _hdr| {}, 100)?;
        sleep(Duration::from_millis(10));
    }
    assert!(left.load(Ordering::SeqCst) >= 1, "never detected loss of live stream");
    println!(
        "live stream lost; waiting to rejoin (is_replaying = {})",
        ps.is_replaying()
    );

    // ── Phase 3: the live stream comes back and rejoins ─────────────────────
    // Aeron refuses a live stream behind what the subscription has already seen, so a
    // restarted publisher resumes the same session where it stopped.
    println!("phase 3: resuming the live publication where it stopped...");
    let resumed = live_channel.parse::<AeronUriStringBuilder>()?;
    resumed.session_id(&constants.session_id().to_string())?;
    resumed.set_initial_position(
        stopped_at,
        constants.initial_term_id(),
        constants.term_buffer_length() as i32,
    )?;
    let resumed = resumed.build(bindings::AERON_URI_MAX_LENGTH as usize)?;
    publication = aeron
        .async_add_exclusive_publication(&cformat!("{resumed}"), stream_id)?
        .poll_blocking(Duration::from_secs(5))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while joined.load(Ordering::SeqCst) < 2 && Instant::now() < deadline {
        if ps.has_failed() {
            let (code, msg) = ps.get_failure_reason().unwrap_or((-1, "unknown".into()));
            return Err(format!("persistent subscription failed on rejoin ({code}): {msg}").into());
        }
        let _ = publication.offer(b"live-beat");
        ps.poll_fn(|_buf, _hdr| {}, 100)?;
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
        "failover complete: joined live {joined_count} time(s), lost it {} time(s)",
        left.load(Ordering::SeqCst)
    );
    Ok(())
}
