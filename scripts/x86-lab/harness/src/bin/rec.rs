//! Recording through an external archive (a Java `ArchivingMediaDriver` on `AERON_DIR`,
//! control requests on `ARCHIVE_CONTROL`).
//!
//! rec tput <messages> <length> <cpu|->
//!   records an IPC stream while a thread pinned to <cpu> offers <messages> of <length>
//!   bytes as fast as the publication allows, then waits until the recording has caught up.
//!   prints: rec,<label>,ipc,<length>,<publish msgs/s>,<recorded msgs/s>,<recorded MB/s>
//! rec start <stream>
//!   starts recording `aeron:ipc` <stream>, left running when this exits, so that the rtt
//!   run that follows is recorded.

use rusteron_archive::*;
use std::error::Error;
use std::ffi::CString;
use std::time::{Duration, Instant};
use x86lab::{aeron_dir, cpu_arg, label, pin};

const STREAM_ID: i32 = 1501;

fn connect() -> Result<(Aeron, AeronArchive), Box<dyn Error>> {
    let context = AeronContext::new()?;
    context.set_dir(&aeron_dir())?;
    let aeron = Aeron::new(&context)?;
    aeron.start()?;
    let control = std::env::var("ARCHIVE_CONTROL").expect("ARCHIVE_CONTROL: the archive's control channel");
    let archive_context = AeronArchiveContext::new()?;
    archive_context.set_aeron(&aeron)?;
    archive_context.set_control_request_channel(&CString::new(control)?)?;
    archive_context.set_control_response_channel(c"aeron:udp?endpoint=localhost:0")?;
    let archive =
        AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?.poll_blocking(Duration::from_secs(20))?;
    Ok((aeron, archive))
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().expect("tput|start");
    let number = |a: Option<String>| -> usize { a.expect("a number").parse().unwrap() };
    let (aeron, archive) = connect()?;
    if mode == "start" {
        let stream = number(args.next()) as i32;
        archive.start_recording(AERON_IPC_STREAM, stream, SOURCE_LOCATION_LOCAL, false)?;
        return Ok(());
    }
    assert_eq!(mode, "tput", "unknown mode {mode}");
    let (messages, length, cpu) = (number(args.next()), number(args.next()), cpu_arg(args.next()));

    let subscription_id = archive.start_recording(AERON_IPC_STREAM, STREAM_ID, SOURCE_LOCATION_LOCAL, true)?;
    let publication = aeron
        .async_add_exclusive_publication(AERON_IPC_STREAM, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    x86lab::wait_until("the recording", 10, || publication.is_connected());
    let counters = aeron.counters_reader();
    let session_id = publication.get_constants()?.session_id();
    let mut counter_id = -1;
    x86lab::wait_until("the recording counter", 10, || {
        counter_id = RecordingPos::find_counter_id_by_session(&counters, session_id);
        counter_id >= 0
    });
    let recording_id = RecordingPos::get_recording_id(&counters, counter_id)?;

    pin(cpu);
    let message = vec![7u8; length];
    let start = Instant::now();
    let mut sent = 0;
    let mut idle = BusySpinIdleStrategy::new();
    while sent < messages {
        match publication.offer(&message) {
            Ok(_) => sent += 1,
            Err(e) if e.is_retryable() => idle.idle(0),
            Err(e) => return Err(format!("offer failed after {sent} messages: {e}").into()),
        }
    }
    let published = start.elapsed();
    let target = publication.position();
    while counters.get_counter_value(counter_id)? < target {
        assert!(
            RecordingPos::is_active(&counters, counter_id, recording_id)?,
            "the recording stopped"
        );
        assert!(
            start.elapsed() < Duration::from_secs(120),
            "the recording did not catch up"
        );
        idle.idle(0);
    }
    let recorded = start.elapsed();
    let rate = |d: Duration| messages as f64 / d.as_secs_f64();
    println!(
        "rec,{},ipc,{length},{:.0},{:.0},{:.1}",
        label(),
        rate(published),
        rate(recorded),
        (messages * length) as f64 / recorded.as_secs_f64() / 1e6
    );
    archive.stop_recording_subscription(subscription_id)?;
    Ok(())
}
