use super::*;

#[test]
fn debug_preserves_plain_fields() {
    let header = AeronFrameHeader::new(4096, 7, 0xc0, 3).unwrap();
    let debug = format!("{header:?}");
    assert!(debug.contains("AeronFrameHeader"));
    assert!(debug.contains("resource"));
    assert!(debug.contains("frame_length: 4096"));
}

#[test]
fn runner_debug_preserves_role_name_and_running_without_allocating() {
    use std::fmt::Write;

    struct Output {
        bytes: [u8; 2048],
        len: usize,
    }
    impl Write for Output {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let end = self.len.checked_add(text.len()).ok_or(std::fmt::Error)?;
            let output = self.bytes.get_mut(self.len..end).ok_or(std::fmt::Error)?;
            output.copy_from_slice(text.as_bytes());
            self.len = end;
            Ok(())
        }
    }

    let native = aeron_agent_runner_t {
        role_name: c"test-runner".as_ptr(),
        running: true,
        ..Default::default()
    };
    // SAFETY: the role name is static and this runner never starts a native thread.
    let runner = unsafe { AeronAgentRunner::from_native(native) };
    for running in [true, false] {
        // SAFETY: this test has exclusive access to the unstarted runner.
        unsafe { (*runner.get_inner()).running = running };
        let mut output = Output {
            bytes: [0; 2048],
            len: 0,
        };
        crate::test_alloc::assert_no_heap_allocation(|| {
            write!(&mut output, "{runner:?}").unwrap();
        });
        let output = std::str::from_utf8(&output.bytes[..output.len]).unwrap();
        assert!(output.contains("role_name: \"test-runner\""), "{output}");
        assert!(
            output.contains(if running { "running: true" } else { "running: false" }),
            "{output}"
        );
    }
    // The safe native stop method must publish the same value that running() reads.
    unsafe { (*runner.get_inner()).running = true };
    assert!(runner.running());
    runner.aeron_agent_stop().unwrap();
    assert!(!runner.running());
}

#[test]
fn debug_does_not_query_uninitialized_callback_headers() {
    // SAFETY: this storage is never passed to a native header method. Debug must
    // remain usable before a native callback supplies an initialized header.
    let header = unsafe { AeronHeader::new_zeroed_on_stack() };
    assert!(format!("{header:?}").contains("AeronHeader"));
}

#[test]
fn debug_handles_completed_async_storage() {
    // SAFETY: the opaque test allocation is never used by a native method.
    let native = unsafe { std::mem::zeroed::<aeron_async_add_publication_t>() };
    let registration = AeronAsyncAddPublication::from_resource(CResource::owned_value(native));
    let owner = registration.resource().as_owned().unwrap();
    // SAFETY: owned_value created this box. Model the native poll freeing it,
    // then mark it released so Rust cleanup cannot free it again.
    unsafe { drop(Box::from_raw(owner.get())) };
    owner.mark_resource_released();
    assert!(format!("{registration:?}").contains("inner: \"null\""));
}

#[test]
fn counter_callback_checks_native_label_utf8() {
    fn invoke<F: FnMut(i64, i32, i32, &[u8], &str)>(label: &[u8], mut callback: F) {
        // SAFETY: all buffers and the callback stay alive until this synchronous
        // native callback returns. Aeron counter labels are length-delimited bytes.
        unsafe {
            aeron_counters_reader_foreach_counter_func_t_callback_for_once_closure_catch::<F>(
                42,
                7,
                3,
                std::ptr::null(),
                0,
                label.as_ptr().cast(),
                label.len(),
                &mut callback as *mut _ as *mut std::ffi::c_void,
            );
        }
        resume_caught_panic();
    }
    for (bytes, expected) in [(b"counter".as_slice(), "counter"), (&[0xff, 0xfe][..], "")] {
        let mut calls = 0;
        invoke(bytes, |value, id, type_id, key, label| {
            calls += 1;
            assert_eq!((value, id, type_id), (42, 7, 3));
            assert!(key.is_empty());
            assert_eq!(label, expected);
        });
        assert_eq!(calls, 1);
    }
}

#[test]
fn clone_struct_copies_fields_into_independent_stable_storage() {
    let native = aeron_frame_header_t {
        frame_length: 4096,
        version: 7,
        flags: 0xc0,
        type_: 3,
    };
    let source = AeronFrameHeader::from_native(native);
    let copy = source.clone_struct();
    assert_ne!(source.get_inner(), copy.get_inner());
    // SAFETY: no reference to the source header is live, and the copy is separate.
    unsafe { source.get_inner_mut().frame_length = 8192 };
    assert_eq!(copy.frame_length(), 4096);
    drop(source);

    let address = copy.get_inner();
    let alias = copy.clone();
    drop(copy);
    assert_eq!(alias.get_inner(), address);
    let copied: aeron_frame_header_t = alias.into();
    assert_eq!(copied, native);
}

#[test]
fn clone_struct_preserves_large_plain_structs() {
    let mut native = aeron_counter_metadata_descriptor_t::default();
    native.state = 1;
    native.type_id = 86;
    native.free_for_reuse_deadline_ms = 123_456;
    native.key.fill(0xab);
    native.label_length = native.label.len() as i32;
    native.label.fill(0x5a);
    let source = AeronCounterMetadataDescriptor::from_native(native);
    let copy = source.clone_struct();
    drop(source);
    let copied: aeron_counter_metadata_descriptor_t = copy.into();
    assert_eq!(copied, native);
}
