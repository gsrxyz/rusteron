//! Compare guard finalization against the linked Aeron C implementation.
use super::*;

fn frame() -> aeron_data_header_t {
    aeron_data_header_t {
        frame_header: aeron_frame_header_t {
            frame_length: -64,
            version: 0,
            flags: 0xc0,
            type_: 1,
        },
        session_id: 17,
        stream_id: 29,
        term_id: 41,
        reserved_value: 0x123456789,
        ..Default::default()
    }
}

fn claim<'a>(header: &'a mut aeron_data_header_t, payload: &'a mut [u8]) -> AeronClaim<'a> {
    AeronClaim {
        frame_header: std::ptr::NonNull::from(header).cast(),
        data: std::ptr::NonNull::new(payload.as_mut_ptr()).unwrap(),
        length: payload.len(),
        position: 1234,
        _publication: std::marker::PhantomData,
    }
}

#[test]
fn guard_commit_abort_and_drop_match_native_frame_updates() {
    assert!(std::mem::align_of::<aeron_data_header_t>() >= std::mem::align_of::<std::sync::atomic::AtomicI32>());
    for length in [0, 1, 32, 1024] {
        for operation in 0..3 {
            let mut native_frame = frame();
            let mut guard_frame = frame();
            let mut payload = vec![7; length];
            let mut native_claim = aeron_buffer_claim_t {
                frame_header: (&mut native_frame as *mut aeron_data_header_t).cast(),
                data: payload.as_mut_ptr(),
                length,
            };
            // SAFETY: each native claim points to a live, aligned frame and payload.
            let native_result = unsafe {
                if operation == 0 {
                    aeron_buffer_claim_commit(&mut native_claim)
                } else {
                    aeron_buffer_claim_abort(&mut native_claim)
                }
            };
            assert_eq!(native_result, 0);
            let guard = claim(&mut guard_frame, &mut payload);
            match operation {
                0 => assert_eq!(guard.commit().unwrap(), 1234),
                1 => guard.abort().unwrap(),
                _ => drop(guard),
            }
            assert_eq!(guard_frame, native_frame, "length={length}, operation={operation}");
        }
    }
}

#[test]
fn unwinding_aborts_the_claim_like_native_abort() {
    let mut native_frame = frame();
    let mut guard_frame = frame();
    let mut payload = [0; 32];
    let mut native_claim = aeron_buffer_claim_t {
        frame_header: (&mut native_frame as *mut aeron_data_header_t).cast(),
        data: payload.as_mut_ptr(),
        length: payload.len(),
    };
    // SAFETY: the claim points to a live, aligned frame and payload.
    assert_eq!(unsafe { aeron_buffer_claim_abort(&mut native_claim) }, 0);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = claim(&mut guard_frame, &mut payload);
        panic!("abort during unwind");
    }));
    assert!(result.is_err());
    assert_eq!(guard_frame, native_frame);
}
