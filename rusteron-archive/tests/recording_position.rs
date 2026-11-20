use rusteron_archive::*;
use std::time::{Duration, Instant};

#[test]
fn recording_id_lookup_validates_the_counter_and_reads_its_key() {
    let directory = std::env::temp_dir().join(format!("rusteron-recording-id-{}", Aeron::nano_clock()));
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(|context| {
        context
            .set_dir(&directory.to_string_lossy().into_c_string())
            .map(|_| ())
    })
    .unwrap();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let reader = client.counters_reader();
    let timeout = Duration::from_secs(5);
    let mut counters = Vec::new();
    // Keep unrelated counters allocated before and between the recording counters.
    for recording_id in [0i64, 1234, i64::MAX] {
        let other = client.add_counter(42, &[], "unrelated", timeout).unwrap();
        let other_id = other.get_constants().unwrap().counter_id();
        assert_eq!(RecordingPos::get_recording_id(&reader, other_id).unwrap_err().code, -1);
        counters.push(other);
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&recording_id.to_ne_bytes());
        key[8..12].copy_from_slice(&17i32.to_ne_bytes());
        let counter = client.add_counter(100, &key, "recording position", timeout).unwrap();
        let id = counter.get_constants().unwrap().counter_id();
        assert_eq!(RecordingPos::get_recording_id(&reader, id).unwrap(), recording_id);
        // A freed counter must no longer identify a recording.
        counter.close().unwrap();
        let deadline = Instant::now() + timeout;
        while reader.counter_record_state(id).unwrap() == AeronCounterState::Allocated {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(RecordingPos::get_recording_id(&reader, id).unwrap_err().code, -1);
    }
    for id in [-1, i32::MAX] {
        assert_eq!(RecordingPos::get_recording_id(&reader, id).unwrap_err().code, -1);
    }
}

#[test]
fn source_identity_is_bounded_by_the_caller_buffer() {
    let directory = std::env::temp_dir().join(format!("rusteron-source-identity-{}", Aeron::nano_clock()));
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(|context| {
        context
            .set_dir(&directory.to_string_lossy().into_c_string())
            .map(|_| ())
    })
    .unwrap();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let reader = client.counters_reader();
    let timeout = Duration::from_secs(5);
    let identity = b"127.0.0.1:40123";
    // Key layout: recording id, session id, source identity length, then the identity
    let mut key = vec![0u8; 16];
    key[..8].copy_from_slice(&7i64.to_ne_bytes());
    key[12..16].copy_from_slice(&(identity.len() as i32).to_ne_bytes());
    key.extend_from_slice(identity);
    let counter = client
        .add_counter(
            AeronCounterTypeId::ArchiveRecordingPosition.code(),
            &key,
            "recording position",
            timeout,
        )
        .unwrap();
    let id = counter.get_constants().unwrap().counter_id();

    let mut buffer = [0xAA; 64];
    let length = reader
        .aeron_archive_recording_pos_get_source_identity(id, &mut buffer)
        .unwrap();
    assert_eq!(&buffer[..length], identity);
    assert_eq!(buffer[length], 0);

    // A short buffer keeps the last byte for the NUL and leaves the rest untouched
    let mut short = [0xAA; 8];
    let length = reader
        .aeron_archive_recording_pos_get_source_identity(id, &mut short[..5])
        .unwrap();
    assert_eq!(length, 4);
    assert_eq!(&short[..5], b"127.\0");
    assert!(short[5..].iter().all(|&byte| byte == 0xAA));
    assert_eq!(
        reader
            .aeron_archive_recording_pos_get_source_identity(id, &mut [])
            .unwrap(),
        0
    );

    // Another counter type reads as an empty identity, an invalid id is an error
    let other = client.add_counter(42, &[], "unrelated", timeout).unwrap();
    let other_id = other.get_constants().unwrap().counter_id();
    assert_eq!(
        reader
            .aeron_archive_recording_pos_get_source_identity(other_id, &mut buffer)
            .unwrap(),
        0
    );
    assert!(
        reader
            .aeron_archive_recording_pos_get_source_identity(-1, &mut buffer)
            .is_err()
    );
}
