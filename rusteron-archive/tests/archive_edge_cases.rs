//! Archive requests at the edges of what the Archive accepts: unknown ids, positions outside a
//! recording, listing limits and a lost Archive process. Each test starts its own Java Archive
use rusteron_archive::testing::{EmbeddedArchiveMediaDriverProcess, find_unused_udp_port};
use rusteron_archive::*;
use serial_test::serial;
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

const NULL_POSITION: i64 = -1;
const TIMEOUT: Duration = Duration::from_secs(10);

/// Fields drop in order, so the clients close before the Archive process stops
struct Running {
    archive: AeronArchive,
    aeron: Aeron,
    process: EmbeddedArchiveMediaDriverProcess,
}

fn start_archive_process() -> EmbeddedArchiveMediaDriverProcess {
    let id = Aeron::nano_clock();
    let request = find_unused_udp_port(17400).unwrap();
    let response = find_unused_udp_port(request + 1).unwrap();
    let events = find_unused_udp_port(response + 1).unwrap();
    EmbeddedArchiveMediaDriverProcess::build_and_start(
        &format!("target/aeron/{id}/shm"),
        &format!("target/aeron/{id}/archive"),
        &format!("aeron:udp?endpoint=localhost:{request}"),
        &format!("aeron:udp?endpoint=localhost:{response}"),
        &format!("aeron:udp?endpoint=localhost:{events}"),
    )
    .unwrap()
}

fn start_archive() -> Running {
    let process = start_archive_process();
    let (archive, aeron) = process.archive_connect().unwrap();
    Running {
        archive,
        aeron,
        process,
    }
}

/// Repeats `attempt` until it returns a value, and fails the test once `TIMEOUT` has passed
fn eventually<T>(mut attempt: impl FnMut() -> Option<T>) -> T {
    let started = Instant::now();
    loop {
        if let Some(value) = attempt() {
            return value;
        }
        assert!(started.elapsed() < TIMEOUT, "no result after {TIMEOUT:?}");
        std::thread::park_timeout(Duration::from_micros(200));
    }
}

/// Stops a recording and waits until the Archive has both dropped its live position and
/// written `end` to the catalogue as the stop position
fn stop(archive: &AeronArchive, subscription_id: i64, recording_id: i64, end: i64) {
    archive.stop_recording_subscription(subscription_id).unwrap();
    eventually(|| {
        let live = archive.get_recording_position(recording_id).unwrap();
        let stopped = archive.get_stop_position(recording_id).unwrap();
        (live == NULL_POSITION && stopped == end).then_some(())
    });
}

/// A recording of some messages, left open when `keep_recording` is set
struct Recorded {
    recording_id: i64,
    subscription_id: i64,
    end: i64,
    // Kept so an open recording has a live source
    _publication: AeronExclusivePublication,
}

fn record(archive: &AeronArchive, aeron: &Aeron, messages: &[&[u8]], keep_recording: bool) -> Recorded {
    // A fresh stream for each recording, so an earlier one cannot match it
    static NEXT_STREAM: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(7311);
    let stream_id = NEXT_STREAM.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let channel = c"aeron:ipc?term-length=64k";
    let subscription_id = archive
        .start_recording(channel, stream_id, SOURCE_LOCATION_LOCAL, true)
        .unwrap();
    let publication = aeron.add_exclusive_publication(channel, stream_id, TIMEOUT).unwrap();
    for message in messages {
        eventually(|| publication.offer(message).ok());
    }
    let reader = aeron.counters_reader();
    let counter = testing::find_counter_id_by_session_in_archive_blocking(
        &reader,
        publication.get_constants().unwrap().session_id(),
        archive.get_archive_id(),
        TIMEOUT,
    )
    .unwrap();
    let recording_id = RecordingPos::get_recording_id(&reader, counter).unwrap();
    let end = publication.position();
    eventually(|| (reader.get_counter_value(counter).unwrap() >= end).then_some(()));
    if !keep_recording {
        stop(archive, subscription_id, recording_id, end);
    }
    Recorded {
        recording_id,
        subscription_id,
        end,
        _publication: publication,
    }
}

fn replay_from(position: i64) -> AeronArchiveReplayParams {
    AeronArchiveReplayParams::builder()
        .position(position)
        .length(-1)
        .build()
        .unwrap()
}

/// The count and recording ids from a listing that starts at `from` and takes at most `limit`
fn listed(archive: &AeronArchive, from: i64, limit: i32) -> (i32, Vec<i64>) {
    let ids = Rc::new(Cell::new(Vec::new()));
    let sink = Rc::clone(&ids);
    let consumer = Handler::new(move |descriptor: &AeronArchiveRecordingDescriptor| {
        let mut seen = sink.take();
        seen.push(descriptor.recording_id());
        sink.set(seen);
    });
    let count = archive.list_recordings(from, limit, &consumer).unwrap();
    (count, ids.take())
}

#[test]
#[serial]
fn requests_for_a_recording_that_was_never_made_report_it_as_unknown() {
    let running = start_archive();
    let archive = &running.archive;
    let missing = 4_242;
    let results = [
        ("start position", archive.get_start_position(missing)),
        ("stop position", archive.get_stop_position(missing)),
        ("truncate", archive.truncate_recording(missing, 0)),
        ("purge", archive.purge_recording(missing)),
        (
            "replay",
            archive.start_replay(missing, c"aeron:ipc", 1, &replay_from(0)),
        ),
    ];
    for (request, result) in results {
        let error = result.unwrap_err();
        assert!(
            matches!(error.code, AeronArchiveErrorCode::UnknownRecording),
            "{request}: {error}"
        );
    }
    // A refused request leaves the control session usable
    assert_eq!(listed(archive, 0, 10).0, 0);
}

#[test]
#[serial]
fn listings_stop_at_the_limit_and_at_the_end_of_the_catalogue() {
    let Running {
        archive,
        aeron,
        process: _process,
    } = start_archive();
    let first = record(&archive, &aeron, &[b"first"], false).recording_id;
    let second = record(&archive, &aeron, &[b"second"], false).recording_id;

    // A limit of 0 is left out: the Archive sends no reply to it, so the C and Java clients
    // both wait for the full message timeout and then fail
    assert_eq!(listed(&archive, 0, 1), (1, vec![first]));
    assert_eq!(listed(&archive, 0, i32::MAX), (2, vec![first, second]));
    assert_eq!(listed(&archive, second, 10), (1, vec![second]));
    assert_eq!(listed(&archive, second + 1, 10), (0, vec![]));
}

#[test]
#[serial]
fn the_live_position_is_reported_only_while_the_recording_runs() {
    let Running {
        archive,
        aeron,
        process: _process,
    } = start_archive();
    let recorded = record(&archive, &aeron, &[b"live"], true);
    let id = recorded.recording_id;
    assert!(archive.get_recording_position(id).unwrap() >= recorded.end);
    assert_eq!(archive.get_stop_position(id).unwrap(), NULL_POSITION);

    stop(&archive, recorded.subscription_id, id, recorded.end);
    assert_eq!(archive.get_start_position(id).unwrap(), 0);
}

#[test]
#[serial]
fn positions_outside_a_stopped_recording_are_refused() {
    let Running {
        archive,
        aeron,
        process: _process,
    } = start_archive();
    let recorded = record(&archive, &aeron, &[b"one", b"two", b"three"], false);
    let id = recorded.recording_id;

    // Replays must start on a frame boundary inside what was recorded
    for start in [recorded.end + 1024, 3] {
        let result = archive.start_replay(id, c"aeron:ipc", 1, &replay_from(start));
        assert!(result.is_err(), "replay from {start}");
    }
    // Truncation cannot extend a recording
    assert!(archive.truncate_recording(id, recorded.end + 1024).is_err());
    assert_eq!(archive.get_stop_position(id).unwrap(), recorded.end);
}

#[test]
#[serial]
fn an_open_recording_cannot_be_truncated_or_purged() {
    let Running {
        archive,
        aeron,
        process: _process,
    } = start_archive();
    let recorded = record(&archive, &aeron, &[b"still writing"], true);
    let id = recorded.recording_id;
    for (request, result) in [
        ("truncate", archive.truncate_recording(id, 0)),
        ("purge", archive.purge_recording(id)),
    ] {
        let error = result.unwrap_err();
        assert!(
            matches!(error.code, AeronArchiveErrorCode::ActiveRecording),
            "{request}: {error}"
        );
    }
    assert!(archive.get_recording_position(id).unwrap() >= recorded.end);
}

#[test]
#[serial]
fn a_lost_archive_process_turns_requests_into_errors_within_the_message_timeout() {
    let process = start_archive_process();
    // Aeron C's default error handler exits the process when the driver goes, so this client
    // needs its own handler to see the failure as an error
    let context = AeronContext::new().unwrap();
    context.set_dir(&process.aeron_dir).unwrap();
    context.set_error_handler(Some(|_: i32, _: &str| {})).unwrap();
    // Aeron::new waits for the driver. Connect once, so an earlier subscription cannot
    // still hold the response endpoint in CLOSING state.
    let aeron = Aeron::new(&context).unwrap();
    aeron.start().unwrap();
    let archive = AeronArchiveAsyncConnect::new_with_aeron(&process.archive_context(&aeron).unwrap(), &aeron)
        .unwrap()
        .poll_blocking(TIMEOUT)
        .unwrap();
    let id = record(&archive, &aeron, &[b"before"], false).recording_id;

    drop(process);
    let started = Instant::now();
    assert!(archive.get_stop_position(id).is_err());
    // The Archive client waits at most its message timeout, 10 seconds by default
    assert!(started.elapsed() < 2 * TIMEOUT, "took {:?}", started.elapsed());
}
