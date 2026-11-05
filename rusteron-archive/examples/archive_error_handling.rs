//! # Archive error handling
//!
//! The patterns every archive application needs, in one place:
//!
//! 1. **Error handlers on both contexts**. Without one, the client's default handler
//!    prints and exits the process, and the archive context drops errors that arrive
//!    during other calls.
//! 2. **Recording signal consumer**. The archive's signals about each recording (START,
//!    STOP, EXTEND, REPLICATE, MERGE, SYNC, DELETE, REPLICATE_END) are delivered from
//!    `archive.do_work()`.
//! 3. **Typed control-session errors**. A blocking call returns the archive's refusal
//!    as an `AeronArchiveError` (match on `e.code`); errors for requests no longer
//!    awaited arrive later on the control channel, so drain them each cycle with
//!    `archive.do_work()` (to the context's error handler) or `archive.poll_for_error()`.
//! 4. **Archive down**. A request fails with a `Generic` code: `offer failed` once the
//!    control request publication sees the archive gone, or a response timeout
//!    (`AERON_ARCHIVE_MESSAGE_TIMEOUT`, 10 s by default) before that. A lost archive shows
//!    as `archive.get_control_response_subscription().is_connected()` turning false; then
//!    reconnect with bounded retries. Here the media driver stops with the archive, so the
//!    example reconnects with a new client too.
//!
//! Requires `java` on PATH (an embedded Java Archive is started for you).
//!
//! ```bash
//! cargo run --release --features "static precompile" --example archive_error_handling
//! ```

use rusteron_archive::testing::{EmbeddedArchiveMediaDriverProcess, find_unused_udp_port};
use rusteron_archive::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

    let id = Aeron::nano_clock();
    let aeron_dir = format!("target/aeron/{id}_err/shm");
    let archive_dir = format!("target/aeron/{id}_err/archive");
    let req_port = find_unused_udp_port(9100).expect("no free port");
    let resp_port = find_unused_udp_port(req_port + 1).expect("no free port");
    let events_port = find_unused_udp_port(resp_port + 1).expect("no free port");
    let request_channel = format!("aeron:udp?endpoint=localhost:{req_port}");
    let response_channel = format!("aeron:udp?endpoint=localhost:{resp_port}");
    let events_channel = format!("aeron:udp?endpoint=localhost:{events_port}");
    let process = EmbeddedArchiveMediaDriverProcess::build_and_start(
        &aeron_dir,
        &archive_dir,
        &request_channel,
        &response_channel,
        &events_channel,
    )?;

    // 1. Error handlers on BOTH contexts (closures are accepted directly)
    let client_errors = Arc::new(AtomicUsize::new(0));
    let aeron_context = AeronContext::new()?;
    aeron_context.set_dir(&cformat!("{aeron_dir}"))?;
    let seen = client_errors.clone();
    aeron_context.set_error_handler(Some(move |code: i32, msg: &str| {
        seen.fetch_add(1, Ordering::SeqCst);
        eprintln!("[client error] {code}: {msg}");
    }))?;
    let aeron = Aeron::new(&aeron_context)?;
    aeron.start()?;

    let archive_context = process.archive_context(&aeron)?;
    archive_context.set_error_handler(Some(|code: i32, msg: &str| {
        eprintln!("[archive error] {code}: {msg}");
    }))?;

    // 2. Recording signals: the archive announces recording lifecycle events
    struct SignalLogger(Arc<AtomicUsize>);
    impl AeronArchiveRecordingSignalConsumerFuncCallback for SignalLogger {
        fn handle_aeron_archive_recording_signal_consumer_func(&mut self, signal: &AeronArchiveRecordingSignal) {
            self.0.fetch_add(1, Ordering::SeqCst);
            println!("[recording signal] {:?}", signal.signal());
        }
    }
    let signals = Arc::new(AtomicUsize::new(0));
    archive_context.set_recording_signal_consumer(Some(SignalLogger(signals.clone())))?;

    let archive =
        AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(Duration::from_secs(10))?;
    println!(
        "connected to archive (control session {})",
        archive.control_session_id()
    );

    // Record something so the archive has signals to send. Signals reach the consumer
    // from do_work, once a cycle.
    archive.start_recording(c"aeron:ipc", 5000, SOURCE_LOCATION_LOCAL, true)?;
    let publication = aeron
        .async_add_publication(c"aeron:ipc", 5000)?
        .poll_blocking(Duration::from_secs(5))?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while signals.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        archive.do_work()?;
        sleep(Duration::from_millis(1));
    }
    assert!(signals.load(Ordering::SeqCst) >= 1, "no recording signal");
    // the recording stops with its publication; the STOP signal arrives on a later do_work
    drop(publication);

    // 3. Control-session errors
    // Ask the archive to replay a recording that does not exist. The blocking call
    // returns the refusal, typed
    let bogus_recording_id = 424242;
    let params = AeronArchiveReplayParams::builder().position(0).length(100).build()?;
    let replay_port = find_unused_udp_port(events_port + 1).expect("no free port");
    let Err(e) = archive.start_replay(
        bogus_recording_id,
        &cformat!("aeron:udp?endpoint=localhost:{replay_port}"),
        9999,
        &params,
    ) else {
        return Err("replaying a missing recording succeeded".into());
    };
    println!("[expected] replay failed with {:?}: {}", e.code, e.message);
    assert_eq!(e.code, AeronArchiveErrorCode::UnknownRecording);

    // A healthy control loop polls for error responses (and recording signals)
    // even when nothing seems wrong. `do_work` is the once-a-cycle call: it runs the
    // client's conductor when it uses the agent invoker, then hands one recording
    // signal to the consumer or one archive error to the error handler.
    assert!(archive.poll_for_error()?.is_none(), "unexpected archive error");
    archive.do_work()?;

    // 4. Archive down: a request fails, then reconnect with bounded retries
    println!("stopping the archive process to simulate an outage...");
    drop(process); // kills the Java archive + media driver

    let bad = archive.start_recording(c"aeron:ipc", 5001, SOURCE_LOCATION_LOCAL, true);
    println!("[expected] request while archive is down -> {:?}", bad.err());

    // Reconnect pattern: bounded retries with back-off; each attempt has its own timeout.
    println!("restarting archive...");
    let process2 = EmbeddedArchiveMediaDriverProcess::build_and_start(
        &format!("target/aeron/{id}_err2/shm"),
        &format!("target/aeron/{id}_err2/archive"),
        &request_channel,
        &response_channel,
        &events_channel,
    )?;
    let aeron_context2 = AeronContext::new()?;
    aeron_context2.set_dir(&cformat!("target/aeron/{id}_err2/shm"))?;
    aeron_context2.set_error_handler(Some(|code: i32, msg: &str| eprintln!("[client error] {code}: {msg}")))?;
    let aeron2 = Aeron::new(&aeron_context2)?;
    aeron2.start()?;
    let archive_context2 = process2.archive_context(&aeron2)?;
    archive_context2.set_error_handler(Some(|code: i32, msg: &str| eprintln!("[archive error] {code}: {msg}")))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let archive2 = loop {
        match AeronArchiveAsyncConnect::new_with_aeron(&archive_context2, &aeron2)
            .and_then(|c| c.poll_blocking(Duration::from_secs(5)))
        {
            Ok(a) => break a,
            Err(e) if Instant::now() < deadline => {
                eprintln!("reconnect attempt failed ({e:?}); retrying...");
                sleep(Duration::from_millis(500));
            }
            Err(e) => return Err(format!("could not reconnect to archive: {e:?}").into()),
        }
    };
    println!("reconnected (control session {})", archive2.control_session_id());

    println!(
        "done — client errors observed: {}",
        client_errors.load(Ordering::SeqCst)
    );
    Ok(())
}
