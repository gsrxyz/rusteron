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
        while reader.counter_state(id).unwrap() == 1 {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(RecordingPos::get_recording_id(&reader, id).unwrap_err().code, -1);
    }
    for id in [-1, i32::MAX] {
        assert_eq!(RecordingPos::get_recording_id(&reader, id).unwrap_err().code, -1);
    }
}
