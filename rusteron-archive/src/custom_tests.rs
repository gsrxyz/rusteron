//! Tests that use the native library or an embedded driver, without a Java Archive.
use super::*;

#[test]
#[cfg_attr(feature = "backtrace", ignore = "backtrace allocates on each AeronCError")]
fn source_identity_errors_do_not_allocate_on_the_rust_heap() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch()?;
    let client = Aeron::connect_dir(driver.dir())?;
    let reader = client.counters_reader();
    let mut destination = [0xAA; 16];
    // Warm Aeron's per-thread error state before counting Rust allocations.
    assert!(
        reader
            .aeron_archive_recording_pos_get_source_identity(-1, &mut destination)
            .is_err()
    );
    crate::test_alloc::assert_no_heap_allocation(|| {
        for counter_id in [-1, i32::MAX] {
            let error = reader
                .aeron_archive_recording_pos_get_source_identity(counter_id, &mut destination)
                .unwrap_err();
            assert_eq!(error.code, -1);
            assert_eq!(error.message(), None);
            error.with_last_err_message(|message| assert!(message.contains("failed to read metadata for counter id")));
            assert_eq!(destination, [0xAA; 16]);
        }
    });
    Ok(())
}

#[test]
fn replay_queries_and_copy_use_native_fields_without_allocating() {
    let expected = aeron_archive_replay_params_t {
        bounding_limit_counter_id: 17,
        file_io_max_length: 4096,
        position: 64,
        length: 1024,
        replay_token: 555,
        subscription_registration_id: 777,
    };
    let source = AeronArchiveReplayParams::from_native(expected);
    let destination = AeronArchiveReplayParams::from_native(aeron_archive_replay_params_t::default());
    let check_fields = |params: &AeronArchiveReplayParams<Inline<aeron_archive_replay_params_t>>| {
        assert_eq!(params.bounding_limit_counter_id(), expected.bounding_limit_counter_id);
        assert_eq!(params.file_io_max_length(), expected.file_io_max_length);
        assert_eq!(params.position(), expected.position);
        assert_eq!(params.length(), expected.length);
        assert_eq!(params.replay_token(), expected.replay_token);
        assert_eq!(
            params.subscription_registration_id(),
            expected.subscription_registration_id
        );
    };
    crate::test_alloc::assert_no_heap_allocation(|| {
        destination.init().unwrap();
        assert!(!destination.is_bounded());
        assert_eq!(destination.bounding_limit_counter_id(), AERON_NULL_VALUE);
        assert!(source.is_bounded());
        destination.copy_from(&source.as_ref()).unwrap();
        assert!(destination.is_bounded());
        check_fields(&destination);
        check_fields(&source);
        destination.copy_from(&destination.as_ref()).unwrap();
        check_fields(&destination);
        destination.init().unwrap();
        assert!(!destination.is_bounded());
        assert!(source.is_bounded());
    });
}

#[test]
fn archive_protocol_versions_match_the_native_configuration() {
    crate::test_alloc::assert_no_heap_allocation(|| {
        let version = AeronArchive::semantic_version();
        assert_eq!(
            Aeron::semantic_version_major(version) as u32,
            AERON_ARCHIVE_MAJOR_VERSION
        );
        assert_eq!(
            Aeron::semantic_version_minor(version) as u32,
            AERON_ARCHIVE_MINOR_VERSION
        );
        assert_eq!(
            Aeron::semantic_version_patch(version) as u32,
            AERON_ARCHIVE_PATCH_VERSION
        );
        let supported = AeronArchive::protocol_version_with_archive_id();
        assert!(supported > 0 && supported <= version);
        assert_eq!(
            AeronArchiveSourceLocation::Local,
            aeron_archive_source_location_t::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL
        );
        assert_eq!(
            AeronArchiveClientRecordingSignal::Start,
            aeron_archive_client_recording_signal_t::AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_START
        );
    });
}

#[test]
fn credentials_text_uses_its_length_without_allocating_or_reading_a_terminator() {
    for bytes in [
        b"abc".as_slice(),
        b"a\0b".as_slice(),
        b"\xff".as_slice(),
        b"".as_slice(),
    ] {
        let native = aeron_archive_encoded_credentials_t {
            data: bytes.as_ptr().cast(),
            length: bytes.len() as u32,
        };
        // SAFETY: bytes stays alive and unchanged until credentials is dropped.
        let credentials = unsafe { AeronArchiveEncodedCredentials::from_native(native) };
        crate::test_alloc::assert_no_heap_allocation(|| {
            let text: &str = credentials.data();
            assert_eq!(text, std::str::from_utf8(bytes).unwrap_or(""));
            if !bytes.is_empty() && bytes[0] != 0xff {
                assert_eq!(text.as_ptr(), bytes.as_ptr());
            }
        });
    }
}

#[test]
fn context_channel_borrows_do_not_allocate_or_copy() {
    let context = AeronArchiveContext::new().unwrap();
    context.set_control_response_channel(c"aeron:ipc").unwrap();
    context
        .set_control_request_channel(c"aeron:ipc?term-length=64k")
        .unwrap();
    crate::test_alloc::assert_no_heap_allocation(|| {
        for _ in 0..100 {
            // SAFETY: this test keeps the context alive and does not change it
            // or start an Archive connection while these borrows are in use.
            unsafe {
                let response: &str = context.get_control_response_channel();
                let request: &str = context.get_control_request_channel();
                assert_eq!(response, "aeron:ipc");
                assert_eq!(request, "aeron:ipc?term-length=64k");
                assert_eq!(
                    response.as_ptr(),
                    aeron_archive_context_get_control_response_channel(context.get_inner()).cast::<u8>()
                );
                assert_eq!(context.control_response_channel().as_ptr(), response.as_ptr());
            }
        }
    });
}

#[test]
fn recording_descriptor_text_borrows_callback_storage_without_allocating() {
    let text = c"aeron:ipc";
    let mut native = aeron_archive_recording_descriptor_t::default();
    native.stripped_channel = text.as_ptr().cast_mut();
    native.original_channel = text.as_ptr().cast_mut();
    native.source_identity = text.as_ptr().cast_mut();
    native.stripped_channel_length = text.to_bytes().len();
    native.original_channel_length = text.to_bytes().len();
    native.source_identity_length = text.to_bytes().len();
    // SAFETY: all text points to a static C string and each length is valid.
    let descriptor = unsafe { AeronArchiveRecordingDescriptor::from_native(native) };
    crate::test_alloc::assert_no_heap_allocation(|| {
        let stripped: &str = descriptor.stripped_channel();
        let original: &str = descriptor.original_channel();
        let source: &str = descriptor.source_identity();
        assert_eq!((stripped, original, source), ("aeron:ipc", "aeron:ipc", "aeron:ipc"));
        assert_eq!(original.as_ptr(), text.as_ptr().cast::<u8>());
    });
}

#[test]
fn archive_context_rejects_native_client_ownership() -> Result<(), AeronCError> {
    let context = AeronArchiveContext::new()?;
    assert!(!context.get_owns_aeron_client());
    assert!(context.set_owns_aeron_client(true).is_err());
    assert!(!context.get_owns_aeron_client());
    context.set_owns_aeron_client(false)?;
    assert!(!context.get_owns_aeron_client());
    Ok(())
}

#[test]
fn credentials_supplier_without_callbacks_returns_none() -> Result<(), AeronCError> {
    let context = AeronArchiveContext::new()?;
    let supplier = context.credentials_supplier();
    let challenge = AeronArchiveEncodedCredentials::new_zeroed_on_heap();
    assert!(supplier.encoded_credentials().is_none());
    assert!(supplier.on_challenge(&challenge).is_none());
    Ok(())
}

#[test]
fn persistent_subscription_copy_does_not_inherit_native_ownership() -> Result<(), Box<dyn std::error::Error>> {
    use rusteron_media_driver::testing::EmbeddedDriver;

    let driver = EmbeddedDriver::launch()?;
    let source = AeronArchiveContext::new()?;
    source.set_aeron_directory_name(&driver.dir().into_c_string())?;
    source.set_control_request_channel(c"aeron:ipc")?;
    source.set_control_response_channel(c"aeron:ipc")?;
    source.conclude()?;
    let source_owner = ResourceRef::downgrade(source.inner.as_owned().unwrap());
    // Native conclusion creates both a client and a backoff idle strategy for this context.
    assert!(source.get_owns_aeron_client());
    assert!(unsafe { (*source.get_inner()).owns_idle_strategy });

    let subscription_context = AeronArchivePersistentSubscriptionContext::new()?;
    subscription_context.set_archive_context(&source)?;
    let copy = subscription_context
        .inner
        .get_handle_dependency::<AeronArchiveContext>()
        .unwrap();
    assert!(!copy.get_owns_aeron_client());
    let native_copy = unsafe { &*copy.get_inner() };
    assert!(native_copy.aeron.is_null());
    assert!(native_copy.idle_strategy_func.is_none());
    assert!(native_copy.idle_strategy_state.is_null());

    drop(source);
    assert!(source_owner.upgrade().is_some());
    drop(copy);
    drop(subscription_context);
    assert!(source_owner.upgrade().is_none());
    Ok(())
}

#[test]
fn archive_resource_errors_only_classify_storage_and_session_limits() {
    for (code, expected) in [
        (200, false),
        (203, false),
        (207, true),
        (208, true),
        (205, false),
        (211, true),
        (216, false),
        (999, false),
    ] {
        assert_eq!(AeronArchiveErrorCode::from_code(code).is_resource_exhausted(), expected);
    }
}

#[test]
fn replay_builder_defaults_match_native_defaults() -> Result<(), AeronCError> {
    let mut native = aeron_archive_replay_params_t::default();
    // SAFETY: native init writes only to this live params struct.
    assert_eq!(unsafe { aeron_archive_replay_params_init(&mut native) }, 0);
    let params = AeronArchiveReplayParams::builder().build()?;
    // SAFETY: params owns this initialized scalar-only struct.
    assert_eq!(unsafe { *params.get_inner_ref() }, native);
    Ok(())
}

#[test]
fn replay_builder_sets_every_field_and_follows_live() -> Result<(), AeronCError> {
    let params = AeronArchiveReplayParams::builder()
        .position(128)
        .length(4096)
        .bounded_by(3)
        .file_io_max_length(8192)
        .replay_token(7)
        .subscription_registration_id(11)
        .build()?;
    assert_eq!(params.position(), 128);
    assert_eq!(params.length(), 4096);
    assert_eq!(params.bounding_limit_counter_id(), 3);
    assert_eq!(params.file_io_max_length(), 8192);
    assert_eq!(params.replay_token(), 7);
    assert_eq!(params.subscription_registration_id(), 11);
    assert_eq!(
        AeronArchiveReplayParams::builder()
            .length(1)
            .follow_live()
            .build()?
            .length(),
        i64::MAX
    );
    assert_eq!(
        AeronArchiveReplayParams::builder()
            .follow_live()
            .length(1)
            .build()?
            .length(),
        1
    );
    Ok(())
}

#[test]
fn replication_builder_defaults_match_native_defaults() -> Result<(), AeronCError> {
    let mut native = aeron_archive_replication_params_t::default();
    // SAFETY: native init writes only to this live params struct.
    assert_eq!(unsafe { aeron_archive_replication_params_init(&mut native) }, 0);
    let params = AeronArchiveReplicationParams::builder().build()?;
    assert_eq!(params.stop_position(), native.stop_position);
    assert_eq!(params.dst_recording_id(), native.dst_recording_id);
    assert_eq!(params.channel_tag_id(), native.channel_tag_id);
    assert_eq!(params.subscription_tag_id(), native.subscription_tag_id);
    assert_eq!(params.file_io_max_length(), native.file_io_max_length);
    assert_eq!(params.replication_session_id(), native.replication_session_id);
    assert_eq!(params.live_destination(), "");
    assert_eq!(params.replication_channel(), "");
    assert_eq!(params.src_response_channel(), "");
    let credentials = params.encoded_credentials();
    assert_eq!(credentials.length(), 0);
    Ok(())
}

#[test]
fn replication_params_clone_retains_strings_and_credentials_after_builder_owner_drops() -> Result<(), AeronCError> {
    let owned = AeronArchiveReplicationParams::builder()
        .stop_position(4096)
        .extend_recording(17)
        .live_destination(&String::from("aeron:udp?endpoint=127.0.0.1:4040"))
        .replication_channel(&String::from("aeron:ipc"))
        .src_response_channel(&String::from("aeron:udp?control-mode=response"))
        .channel_tag_id(31)
        .subscription_tag_id(32)
        .file_io_max_length(65536)
        .replication_session_id(9)
        .encoded_credentials(&String::from("credentials"))
        .build()?;
    let params = AeronArchiveReplicationParams::clone(&owned);
    let owner = params.inner.as_owned().unwrap();
    assert_eq!(
        owner.dependency_len(),
        1,
        "the params owner retains the input allocations"
    );
    drop(owned);
    assert_eq!(params.stop_position(), 4096);
    assert_eq!(params.dst_recording_id(), 17);
    assert_eq!(params.channel_tag_id(), 31);
    assert_eq!(params.subscription_tag_id(), 32);
    assert_eq!(params.file_io_max_length(), 65536);
    assert_eq!(params.replication_session_id(), 9);
    assert_eq!(params.live_destination(), "aeron:udp?endpoint=127.0.0.1:4040");
    assert_eq!(params.replication_channel(), "aeron:ipc");
    assert_eq!(params.src_response_channel(), "aeron:udp?control-mode=response");
    let credentials = params.encoded_credentials();
    assert_eq!(credentials.length(), 11);
    assert_eq!(credentials.data(), "credentials");
    Ok(())
}

#[test]
fn replication_builder_rejects_embedded_nuls_in_each_string() {
    for builder in [
        AeronArchiveReplicationParams::builder().live_destination("bad\0value"),
        AeronArchiveReplicationParams::builder().replication_channel("bad\0value"),
        AeronArchiveReplicationParams::builder().src_response_channel("bad\0value"),
        AeronArchiveReplicationParams::builder().encoded_credentials("bad\0value"),
    ] {
        assert!(builder.build().is_err());
    }
}

#[test]
fn recording_signal_maps_known_codes_and_rejects_unknown_codes() {
    use aeron_archive_client_recording_signal_en::*;
    let signal = AeronArchiveRecordingSignal::default();
    for (code, expected) in [
        (0, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_START),
        (1, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_STOP),
        (2, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EXTEND),
        (3, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE),
        (4, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_MERGE),
        (5, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_SYNC),
        (6, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_DELETE),
        (7, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE_END),
    ] {
        // SAFETY: this test has exclusive access to a scalar-only owned struct.
        unsafe { signal.get_inner_mut().recording_signal_code = code };
        assert_eq!(signal.signal(), Some(expected));
    }
    for code in [-1, 8, i32::MIN, i32::MAX] {
        unsafe { signal.get_inner_mut().recording_signal_code = code };
        assert_eq!(signal.signal(), None);
    }
}

#[test]
fn native_archive_enums_convert_known_values_and_preserve_unknown_values() {
    use aeron_archive_client_recording_signal_en::*;
    for (code, expected) in [
        (0, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_START),
        (1, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_STOP),
        (2, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EXTEND),
        (3, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE),
        (4, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_MERGE),
        (5, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_SYNC),
        (6, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_DELETE),
        (7, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE_END),
        (i32::MIN, AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_NULL_VALUE),
    ] {
        assert_eq!(aeron_archive_client_recording_signal_t::try_from(code), Ok(expected));
        assert_eq!(i32::from(expected), code);
    }
    for code in [-1, 8, i32::MAX] {
        assert_eq!(aeron_archive_client_recording_signal_t::try_from(code), Err(code));
    }
    for (code, expected) in [
        (0, aeron_archive_source_location_en::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL),
        (
            1,
            aeron_archive_source_location_en::AERON_ARCHIVE_SOURCE_LOCATION_REMOTE,
        ),
    ] {
        assert_eq!(aeron_archive_source_location_t::try_from(code), Ok(expected));
        assert_eq!(u32::from(expected), code);
    }
    for code in [2, u32::MAX] {
        assert_eq!(aeron_archive_source_location_t::try_from(code), Err(code));
    }
}

// Reset writes fields without reading the context or subscription. Zeroed test
// storage has no native close callback. These tests never poll after installing a handler.
macro_rules! descriptor_poller {
    ($test:ident, $poller:ident, $consumer:ident, $descriptor:ident, $field:ident, $clientd:ident, $count:ident) => {
        #[test]
        fn $test() -> Result<(), AeronCError> {
            use std::{cell::Cell, rc::Rc};
            let calls = Rc::new(Cell::new(0));
            let lifetime = Rc::downgrade(&calls);
            let handler = Handler::new({
                let calls = calls.clone();
                move |_: &$descriptor| calls.set(calls.get() + 1)
            });
            // SAFETY: reset only writes this struct. Its pointer fields stay unused.
            let mut poller = $poller::from_owned(CResource::owned_value(unsafe { std::mem::zeroed() }));
            assert!(poller.poll().is_err());
            assert!(poller.reset::<$consumer>(1, 2, None).is_err());
            assert_eq!(unsafe { (*poller.get_inner()).correlation_id }, 0);
            poller.reset(23, 7, Some(&handler))?;
            assert_eq!(unsafe { (*poller.get_inner()).$count }, 7);
            // SAFETY: the owner remains live. Both methods must reject borrowed storage
            // before reading native fields or calling C.
            let mut borrowed = unsafe { $poller::from_ptr(poller.get_inner()) };
            assert!(borrowed.reset(99, 99, Some(&handler)).is_err());
            assert!(borrowed.poll().is_err());
            assert_eq!(unsafe { (*poller.get_inner()).correlation_id }, 23);
            drop(handler);
            drop(calls);
            assert!(lifetime.upgrade().is_some());
            // SAFETY: the callback does not read this descriptor's null pointer fields.
            let descriptor = unsafe { $descriptor::new_zeroed_on_heap() };
            // SAFETY: the poller retains the handler and this initialized descriptor lives through the callback.
            unsafe {
                let native = &*poller.get_inner();
                native.$field.unwrap()(descriptor.get_inner(), native.$clientd);
            }
            assert_eq!(lifetime.upgrade().unwrap().get(), 1);
            drop(borrowed);
            drop(poller);
            assert!(lifetime.upgrade().is_none());
            Ok(())
        }
    };
}

descriptor_poller!(
    recording_descriptor_poller_retains_consumer_and_rejects_borrowed_calls,
    AeronArchiveRecordingDescriptorPoller,
    AeronArchiveRecordingDescriptorConsumerFuncLogger,
    AeronArchiveRecordingDescriptor,
    recording_descriptor_consumer,
    recording_descriptor_consumer_clientd,
    remaining_record_count
);
descriptor_poller!(
    subscription_descriptor_poller_retains_consumer_and_rejects_borrowed_calls,
    AeronArchiveRecordingSubscriptionDescriptorPoller,
    AeronArchiveRecordingSubscriptionDescriptorConsumerFuncLogger,
    AeronArchiveRecordingSubscriptionDescriptor,
    recording_subscription_descriptor_consumer,
    recording_subscription_descriptor_consumer_clientd,
    remaining_subscription_count
);
