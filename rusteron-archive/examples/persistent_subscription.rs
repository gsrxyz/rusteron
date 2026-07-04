//! # Persistent Subscription example
//!
//! Demonstrates Aeron Archive **persistent subscriptions**: replay a recording from the start,
//! then seamlessly join the live stream (`on_live_joined` fires at the handover).
//!
//! Requires a running Aeron **Java** Archive. This example builds and starts one for you via the
//! `rusteron_archive::testing` harness, so `java` must be on `PATH`.
//!
//! ```bash
//! cargo run --release --features "static precompile" --example persistent_subscription
//! ```
//!
//! See the archive README's "Persistent Subscriptions" section and the upstream docs:
//! <https://github.com/aeron-io/aeron/wiki/Persistent-Subscriptions>

use rusteron_archive::testing::{find_unused_udp_port, EmbeddedArchiveMediaDriverProcess};
use rusteron_archive::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

    // Unique dirs + three distinct UDP control ports for the Archive.
    let id = Aeron::nano_clock();
    let aeron_dir = format!("target/aeron/{id}_example/shm");
    let archive_dir = format!("target/aeron/{id}_example/archive");
    let req_port = find_unused_udp_port(9000).expect("no free port");
    let resp_port = find_unused_udp_port(req_port + 1).expect("no free port");
    let events_port = find_unused_udp_port(resp_port + 1).expect("no free port");

    // Keep `_process` in scope so the Java Archive lives for the whole example.
    let _process = EmbeddedArchiveMediaDriverProcess::build_and_start(
        &aeron_dir,
        &archive_dir,
        &format!("aeron:udp?endpoint=localhost:{req_port}"),
        &format!("aeron:udp?endpoint=localhost:{resp_port}"),
        &format!("aeron:udp?endpoint=localhost:{events_port}"),
    )?;

    // Aeron client + archive context. The archive context is reused by the
    // persistent subscription below, so we build it explicitly rather than via
    // `archive_connect()` (which hides it).
    let aeron_context = AeronContext::new()?;
    aeron_context.set_dir(&cformat!("{aeron_dir}"))?;
    aeron_context.set_client_name(&cformat!("ps-example-{id}"))?;
    let aeron = Aeron::new(&aeron_context)?;
    aeron.start()?;

    let archive_context = AeronArchiveContext::new()?;
    archive_context.set_aeron(&aeron)?;
    archive_context.set_control_request_channel(&cformat!("aeron:udp?endpoint=localhost:{req_port}"))?;
    archive_context.set_control_response_channel(&cformat!("aeron:udp?endpoint=localhost:{resp_port}"))?;
    archive_context.set_recording_events_channel(&cformat!("aeron:udp?endpoint=localhost:{events_port}"))?;

    let archive =
        AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(Duration::from_secs(20))?;
    println!("connected to archive");

    // 1. Record a live IPC stream and seed it with a little history.
    let live_channel = c"aeron:ipc";
    let stream_id = 1001;
    archive.start_recording(live_channel, stream_id, SOURCE_LOCATION_LOCAL, true)?;

    let publication = aeron
        .async_add_publication(live_channel, stream_id)?
        .poll_blocking(Duration::from_secs(5))?;
    while !publication.is_connected() {
        sleep(Duration::from_millis(10));
    }
    for i in 0..10 {
        let msg = format!("History-{i}");
        while publication.offer(msg.as_bytes()).is_err() {
            sleep(Duration::from_millis(1));
        }
    }
    println!("seeded 10 historical messages");

    // 2. Resolve the recording id (wait for the recorder to flush what we published).
    let session_id = publication.get_constants()?.session_id;
    let counters = aeron.counters_reader();
    let counter_id = RecordingPos::find_counter_id_by_session(&counters, session_id);
    let recording_id = RecordingPos::get_recording_id_block(&counters, counter_id, Duration::from_secs(5))?;
    let published_position = publication.position();
    let deadline = Instant::now() + Duration::from_secs(5);
    while counters.get_counter_value(counter_id) < published_position && Instant::now() < deadline {
        sleep(Duration::from_millis(10));
    }

    // 3. Listener records the replay -> live transition. State is shared via Arc so
    //    the instance handed to the builder and the one this thread reads stay in sync
    //    (the builder takes ownership and boxes the listener).
    struct Listener {
        joined: Arc<AtomicUsize>,
        errors: Arc<Mutex<Vec<(i32, String)>>>,
    }
    impl PersistentSubscriptionListener for Listener {
        fn on_live_joined(&self) {
            self.joined.fetch_add(1, Ordering::SeqCst);
            println!("on_live_joined: replay merged into the live stream");
        }
        fn on_live_left(&self) {
            println!("on_live_left: fell back to replay");
        }
        fn on_error(&self, code: i32, msg: &str) {
            self.errors.lock().unwrap().push((code, msg.into()));
        }
    }
    let joined = Arc::new(AtomicUsize::new(0));
    let errors: Arc<Mutex<Vec<(i32, String)>>> = Arc::new(Mutex::new(Vec::new()));

    // 4. Build the persistent subscription. start_from_beginning replays from the
    //    start; use start_from_live() to skip straight to the live stream.
    let ps = persistent_subscription_builder()?
        .aeron(&aeron)?
        .archive_context(&archive_context)?
        .live_channel(live_channel.to_str()?)?
        .live_stream_id(stream_id)?
        .replay_channel("aeron:udp?endpoint=localhost:0")? // ephemeral scratch channel
        .replay_stream_id(stream_id + 1)?
        .start_from_beginning()?
        .recording_id(recording_id)?
        .listener(Listener {
            joined: joined.clone(),
            errors: errors.clone(),
        })?
        .build()?;
    println!("persistent subscription created, replaying then joining live...");

    // 5. Drive it: keep publishing so the live image stays active; poll until live.
    //    `ps.poll_fn()` runs the PS state machine (and drives the archive async client)
    //    internally, so — unlike a manual replay loop — there is no need to call
    //    `archive.poll_for_recording_signals()` here. Abort if the PS fails terminally.
    //
    //    Each live message carries a send timestamp in the frame's reserved-value field:
    //    Aeron invokes the supplier synchronously inside `offer`, handing it the whole
    //    frame (header + payload) as a mutable slice; the returned i64 is stored in the
    //    header, where subscribers can read it back via `header.reserved_value()`.
    let send_timestamp_supplier = Handler::new(|_frame: &mut [u8]| Aeron::nano_clock());
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut live_sent = 0;
    while !ps.is_live() && Instant::now() < deadline {
        if ps.has_failed() {
            let (code, msg) = ps.get_failure_reason().unwrap_or((-1, "unknown".into()));
            return Err(format!("persistent subscription failed ({code}): {msg}").into());
        }
        live_sent += 1;
        let msg = format!("Live-{live_sent}");
        let _ = publication.offer_with_reserved_value(msg.as_bytes(), Some(&send_timestamp_supplier));
        let fragments = ps.poll_fn(|buf, _hdr| println!("  fragment ({} bytes)", buf.len()), 100)?;
        if fragments == 0 {
            sleep(Duration::from_millis(1));
        }
    }

    let joined_count = joined.load(Ordering::SeqCst);
    println!("joined live {joined_count} time(s); sent {live_sent} live messages; done");

    assert!(joined_count >= 1, "did not join live; errors: {:?}", errors);
    Ok(())
}
