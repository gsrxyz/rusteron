//! Consumer tests for generated value APIs and stateless native calls.
use rusteron_client::*;

#[test]
fn initialized_headers_can_be_wrapped_and_copied_without_unsafe() {
    let native = bindings::aeron_frame_header_t {
        frame_length: 128,
        version: 1,
        flags: 0xc0,
        type_: 7,
    };
    let copy = {
        let header = AeronFrameHeader::from_native(native);
        header.clone_struct()
    };
    assert_eq!(copy.frame_length(), 128);
    assert_eq!(copy.version(), 1);
    assert_eq!(copy.flags(), 0xc0);
    assert_eq!(copy.type_(), 7);
    let zero = AeronFrameHeader::new_zeroed_on_stack();
    assert_eq!(zero.frame_length(), 0);
}

#[test]
fn stateless_idle_calls_accept_unused_pointers() {
    let idle: [fn(*mut std::ffi::c_void, i32); 3] = [
        AeronIdleStrategy::yielding_idle,
        AeronIdleStrategy::busy_spinning_idle,
        AeronIdleStrategy::noop_idle,
    ];
    for state in [std::ptr::null_mut(), std::ptr::without_provenance_mut(1)] {
        for work in [0, 1] {
            for idle in idle {
                idle(state, work);
            }
        }
    }
}

#[test]
fn temporary_directory_text_borrows_the_caller_buffer() {
    fn temporary_directory(buffer: &mut [u8]) -> &str {
        Aeron::tmpdir(buffer)
    }
    let mut buffer = [0u8; 4096];
    assert!(!temporary_directory(&mut buffer).is_empty());
    // Windows reports failure for an empty buffer. POSIX returns its constant path.
    let _ = temporary_directory(&mut []);
}
