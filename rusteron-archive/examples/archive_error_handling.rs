//! # Archive error handling
//!
//! The patterns every archive application needs, in one place:
//!
//! 1. **Error handlers on both contexts** — async client errors otherwise vanish.
//! 2. **Recording signal consumer** — the archive's own lifecycle events (START/STOP/EXTEND).
//! 3. **`poll_for_error_response`** — archive control-session errors (e.g. replaying a
//!    recording that does not exist) arrive asynchronously on the control channel; poll for
//!    them after requests, and periodically.
//! 4. **Archive down** — control requests fail with a timeout-class error; detect it,
//!    then reconnect with bounded retries.
//!
//! Requires `java` on PATH (an embedded Java Archive is started for you).
//!
//! ```bash
//! cargo run --release --features "static precompile" --example archive_error_handling
//! ```

use rusteron_archive::testing::{find_unused_udp_port, EmbeddedArchiveMediaDriverProcess};
use rusteron_archive::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

    // ── 1. Error handlers on BOTH contexts (closures are accepted directly) ──
    let client_errors = Arc::new(AtomicUsize::new(0));
    let aeron_context = AeronContext::new()?;
    aeron_context.set_dir(&aeron_dir.clone().into_c_string())?;
    let seen = client_errors.clone();
    aeron_context.set_error_handler(Some(move |code: i32, msg: &str| {
        seen.fetch_add(1, Ordering::SeqCst);
        eprintln!("[client error] {code}: {msg}");
    }))?;
    let aeron = Aeron::new(&aeron_context)?;
    aeron.start()?;

    let archive_context = AeronArchiveContext::new()?;
    archive_context.set_aeron(&aeron)?;
    archive_context.set_control_request_channel(&request_channel.clone().into_c_string())?;
    archive_context.set_control_response_channel(&response_channel.clone().into_c_string())?;
    archive_context.set_recording_events_channel(&events_channel.clone().into_c_string())?;
    archive_context.set_error_handler(Some(|code: i32, msg: &str| {
        eprintln!("[archive error] {code}: {msg}");
    }))?;

    // ── 2. Recording signals: the archive announces recording lifecycle events ──
    struct SignalLogger;
    impl AeronArchiveRecordingSignalConsumerFuncCallback for SignalLogger {
        fn handle_aeron_archive_recording_signal_consumer_func(&mut self, signal: AeronArchiveRecordingSignal) {
            println!("[recording signal] {:?}", signal.signal());
        }
    }
    archive_context.set_recording_signal_consumer(Some(SignalLogger))?;

    let archive =
        AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(Duration::from_secs(10))?;
    println!(
        "connected to archive (control session {})",
        archive.control_session_id()
    );

    // ── 3. Control-session errors arrive asynchronously: poll for them ──
    // Ask the archive to replay a recording that does not exist. The call itself
    // fails (or the error response arrives on the control channel shortly after) —
    // poll_for_error_response is how you drain those without killing the session.
    let bogus_recording_id = 424242;
    let params = AeronArchiveReplayParams::builder().position(0).length(100).build()?;
    let replay = archive.start_replay(
        bogus_recording_id,
        &format!(
            "aeron:udp?endpoint=localhost:{}",
            find_unused_udp_port(events_port + 1).unwrap()
        )
        .into_c_string(),
        9999,
        &params,
    );
    match replay {
        Err(e) => {
            // the archive's error code travels inside the message text; parse it out
            let typed = AeronArchiveError::parse(&format!("{:?}", e));
            println!("[expected] replay failed with {:?}", typed.code);
            assert_eq!(typed.code, AeronArchiveErrorCode::UnknownRecording);
        }
        Ok(_) => {
            // some archive versions report via the control channel instead
            if let Some(err) = archive.poll_for_error()? {
                println!("[expected] archive error response: {:?} — {}", err.code, err.message);
            }
        }
    }

    // A healthy control loop polls for error responses (and recording signals)
    // even when nothing seems wrong:
    assert!(archive.poll_for_error()?.is_none(), "unexpected archive error");
    archive.poll_for_recording_signals()?;

    // ── 4. Archive down: detect, then reconnect with bounded retries ──
    println!("stopping the archive process to simulate an outage...");
    drop(process); // kills the Java archive + media driver

    let bad = archive.start_recording(&"aeron:ipc".into_c_string(), 5001, SOURCE_LOCATION_LOCAL, true);
    println!("[expected] request while archive is down -> {:?}", bad.err());

    // Reconnect pattern: bounded retries with back-off; each attempt has its own timeout.
    println!("restarting archive...");
    let _process = EmbeddedArchiveMediaDriverProcess::build_and_start(
        &format!("target/aeron/{id}_err2/shm"),
        &format!("target/aeron/{id}_err2/archive"),
        &request_channel,
        &response_channel,
        &events_channel,
    )?;
    let aeron_context2 = AeronContext::new()?;
    aeron_context2.set_dir(&format!("target/aeron/{id}_err2/shm").into_c_string())?;
    let aeron2 = Aeron::new(&aeron_context2)?;
    aeron2.start()?;
    let archive_context2 = AeronArchiveContext::new()?;
    archive_context2.set_aeron(&aeron2)?;
    archive_context2.set_control_request_channel(&request_channel.clone().into_c_string())?;
    archive_context2.set_control_response_channel(&response_channel.clone().into_c_string())?;
    archive_context2.set_recording_events_channel(&events_channel.clone().into_c_string())?;
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
