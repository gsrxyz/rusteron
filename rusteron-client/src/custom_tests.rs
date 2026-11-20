//! Native tests for custom helpers and generated URI setters.
use super::*;

#[test]
fn publication_error_copy_keeps_the_full_frame_after_the_source_drops() {
    #[repr(C)]
    struct Frame {
        prefix: aeron_publication_error_values_t,
        trailing: [u8; 20],
    }

    for (message, text) in [
        (b"".as_slice(), ""),
        (b"publication rejected".as_slice(), "publication rejected"),
        (b"rejected\0\xff".as_slice(), "<invalid UTF-8>"),
    ] {
        let owned = {
            let mut frame = Frame {
                prefix: aeron_publication_error_values_t {
                    registration_id: 42,
                    error_code: 3,
                    error_message_length: message.len() as i32,
                    ..Default::default()
                },
                trailing: [0; 20],
            };
            let ptr = std::ptr::from_mut(&mut frame).cast::<aeron_publication_error_values_t>();
            // The frame has enough storage for the message and Aeron's full copy.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    message.as_ptr(),
                    std::ptr::addr_of_mut!((*ptr).error_message).cast::<u8>(),
                    message.len(),
                );
                AeronPublicationErrorValues::from_ptr(ptr).try_to_owned().unwrap()
            }
        };
        std::thread::spawn(move || {
            assert_eq!(owned.registration_id(), 42);
            assert_eq!(owned.error_code(), 3);
            assert_eq!(owned.error_message_bytes(), message);
            assert_eq!(owned.error_message(), text);
            let display = owned.to_string();
            if text.is_empty() {
                assert_eq!(display, "publication error 3 (registration 42)");
            } else {
                assert_eq!(display, format!("publication error 3 (registration 42): {text}"));
            }
            assert!(format!("{owned:?}").contains(&format!("error_message: {text:?}")));
        })
        .join()
        .unwrap();
    }
}

#[test]
fn rust_enum_names_use_the_native_types_without_conversion_or_allocation() {
    fn native_action(action: aeron_controlled_fragment_handler_action_t) -> u32 {
        action as u32
    }
    crate::test_alloc::assert_no_heap_allocation(|| {
        use AeronControlledFragmentHandlerAction as Action;
        for (action, native) in [
            (
                Action::Abort,
                aeron_controlled_fragment_handler_action_en::AERON_ACTION_ABORT,
            ),
            (
                Action::Break,
                aeron_controlled_fragment_handler_action_en::AERON_ACTION_BREAK,
            ),
            (
                Action::Commit,
                aeron_controlled_fragment_handler_action_en::AERON_ACTION_COMMIT,
            ),
            (
                Action::Continue,
                aeron_controlled_fragment_handler_action_en::AERON_ACTION_CONTINUE,
            ),
        ] {
            assert_eq!(action, native);
            assert_eq!(native_action(action), native as u32);
            assert_eq!(Action::try_from(native as u32), Ok(action));
        }
        assert_eq!(Action::try_from(0), Err(0));
        assert_eq!(AeronUriType::Udp, aeron_uri_type_t::AERON_URI_UDP);
        assert_eq!(AeronUriType::Ipc, aeron_uri_type_t::AERON_URI_IPC);
        assert_eq!(
            AeronUriAtsStatus::Enabled,
            aeron_uri_ats_status_t::AERON_URI_ATS_STATUS_ENABLED
        );
        assert_eq!(
            AeronCncLoadResult::Success,
            aeron_cnc_load_result_t::AERON_CNC_LOAD_SUCCESS
        );
    });
}

#[test]
fn version_text_and_publication_constants_borrow_without_allocating() {
    let native = aeron_publication_constants_t {
        channel: c"aeron:ipc".as_ptr(),
        ..Default::default()
    };
    // SAFETY: the channel points to static, immutable storage.
    let constants = unsafe { AeronPublicationConstants::from_native(native) };
    crate::test_alloc::assert_no_heap_allocation(|| {
        let version: &'static str = Aeron::version_text();
        let full: &'static str = Aeron::version_full();
        let sha: &'static str = Aeron::version_gitsha();
        assert!(!version.is_empty());
        assert!(!full.is_empty());
        assert!(!sha.is_empty());
        let channel: &str = constants.channel();
        assert_eq!(channel, "aeron:ipc");
        assert_eq!(channel.as_ptr(), c"aeron:ipc".as_ptr().cast::<u8>());
    });
}

#[test]
fn error_formatting_and_reentrant_message_reads_do_not_allocate() {
    use std::fmt::Write;
    struct Sink<'a> {
        expected: &'a str,
        saw_message: bool,
    }
    impl std::fmt::Write for Sink<'_> {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            // SAFETY: this changes only the current thread's native error buffer.
            unsafe { aeron_err_clear() };
            self.saw_message |= text == self.expected;
            Ok(())
        }
    }
    let error = AeronCError::from_code(-1);
    // Set a nonempty native message with an invalid URI, without touching the filesystem.
    let mut uri = aeron_uri_t::default();
    // SAFETY: the input is a live C string and uri is writable native storage.
    assert!(unsafe { aeron_uri_parse(c"invalid-uri".to_bytes().len(), c"invalid-uri".as_ptr(), &mut uri) } < 0);
    // SAFETY: close releases any storage created by the failed parse.
    unsafe { aeron_uri_close(&mut uri) };
    crate::test_alloc::assert_no_heap_allocation(|| {
        Aeron::with_errmsg(|before| {
            assert!(!before.is_empty());
            assert_ne!(before, "no error");
            let first = before.as_bytes()[0];
            let mut sink = Sink {
                expected: before,
                saw_message: false,
            };
            write!(&mut sink, "{error}").unwrap();
            assert!(sink.saw_message);
            write!(&mut sink, "{error:?}").unwrap();
            assert_eq!(before.as_bytes()[0], first);
            Aeron::with_errmsg(|after| assert_eq!(after, "no error"));
        });
    });
}

#[test]
#[cfg_attr(feature = "backtrace", ignore = "backtrace allocates on each AeronCError")]
fn literal_error_messages_and_their_clones_do_not_allocate() {
    crate::test_alloc::assert_no_heap_allocation(|| {
        let error = AeronCError::with_message(-1, "invalid counter id");
        let copy = error.clone();
        assert_eq!(error.message(), Some("invalid counter id"));
        assert_eq!(copy.message().unwrap().as_ptr(), error.message().unwrap().as_ptr());
    });
}

#[test]
#[cfg_attr(feature = "backtrace", ignore = "backtrace allocates on each AeronCError")]
fn owned_error_messages_keep_their_original_allocation() {
    let message = String::from("saved native error");
    let pointer = message.as_ptr();
    crate::test_alloc::assert_no_heap_allocation(|| {
        let error = AeronCError::with_message(-1, message);
        assert_eq!(error.message().unwrap().as_ptr(), pointer);
        assert_eq!(error.message(), Some("saved native error"));
    });
}

#[test]
fn last_error_message_prefers_the_captured_text_over_the_native_buffer() {
    let mut uri = aeron_uri_t::default();
    // SAFETY: the input is a live C string and uri is writable native storage.
    assert!(unsafe { aeron_uri_parse(c"invalid-uri".to_bytes().len(), c"invalid-uri".as_ptr(), &mut uri) } < 0);
    // SAFETY: close releases any storage created by the failed parse.
    unsafe { aeron_uri_close(&mut uri) };
    let native = Aeron::errmsg_owned();
    assert!(!native.is_empty());

    let captured = AeronCError::with_message(-1, "captured");
    assert_eq!(captured.get_last_err_message_owned(), "captured");
    // SAFETY: no native call runs while the borrowed text is in use.
    assert_eq!(unsafe { captured.get_last_err_message() }, "captured");

    let uncaptured = AeronCError::from_code(-1);
    assert_eq!(uncaptured.get_last_err_message_owned(), native);
    // SAFETY: as above.
    assert_eq!(unsafe { uncaptured.get_last_err_message() }, native);
    uncaptured.with_last_err_message(|text| assert_eq!(text, native));
}

#[test]
fn native_enum_conversions_reject_invalid_values_without_changing_the_integer() {
    use aeron_controlled_fragment_handler_action_en::*;
    for (code, expected) in [
        (1, AERON_ACTION_ABORT),
        (2, AERON_ACTION_BREAK),
        (3, AERON_ACTION_COMMIT),
        (4, AERON_ACTION_CONTINUE),
    ] {
        // The typedef and the original enum share the generated implementation.
        assert_eq!(aeron_controlled_fragment_handler_action_t::try_from(code), Ok(expected));
        assert_eq!(u32::from(expected), code);
    }
    for code in [0, 5, u32::MAX] {
        assert_eq!(aeron_controlled_fragment_handler_action_t::try_from(code), Err(code));
    }
    use aeron_cnc_load_result_stct::*;
    for (code, expected) in [
        (-1, AERON_CNC_LOAD_FAILED),
        (0, AERON_CNC_LOAD_SUCCESS),
        (1, AERON_CNC_LOAD_AWAIT_FILE),
        (2, AERON_CNC_LOAD_AWAIT_MMAP),
        (3, AERON_CNC_LOAD_AWAIT_VERSION),
        (4, AERON_CNC_LOAD_AWAIT_CNC_DATA),
    ] {
        assert_eq!(aeron_cnc_load_result_t::try_from(code), Ok(expected));
        assert_eq!(i32::from(expected), code);
    }
    for code in [i32::MIN, -2, 5, i32::MAX] {
        assert_eq!(aeron_cnc_load_result_t::try_from(code), Err(code));
    }
    for (code, expected) in [
        (0, aeron_uri_type_enum::AERON_URI_UDP),
        (1, aeron_uri_type_enum::AERON_URI_IPC),
        (2, aeron_uri_type_enum::AERON_URI_UNKNOWN),
    ] {
        assert_eq!(aeron_uri_type_t::try_from(code), Ok(expected));
        assert_eq!(u32::from(expected), code);
    }
    for (code, expected) in [
        (0, aeron_uri_ats_status_en::AERON_URI_ATS_STATUS_DEFAULT),
        (1, aeron_uri_ats_status_en::AERON_URI_ATS_STATUS_ENABLED),
        (2, aeron_uri_ats_status_en::AERON_URI_ATS_STATUS_DISABLED),
    ] {
        assert_eq!(aeron_uri_ats_status_t::try_from(code), Ok(expected));
        assert_eq!(u32::from(expected), code);
    }
    for code in [3, u32::MAX] {
        assert_eq!(aeron_uri_type_t::try_from(code), Err(code));
        assert_eq!(aeron_uri_ats_status_t::try_from(code), Err(code));
    }
}

// Read numeric defines independently of the bindings used by the generator.
fn native_defines(header: &str, prefix: &str, suffix: &str) -> Vec<i32> {
    header
        .lines()
        .filter_map(|line| {
            let mut fields = line.trim().strip_prefix("#define ")?.split_whitespace();
            let name = fields.next()?;
            if !name.starts_with(prefix) || !name.ends_with(suffix) {
                return None;
            }
            let number = fields
                .next()
                .expect("missing native constant value")
                .trim_matches(['(', ')', ';']);
            Some(
                number
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid native constant {name}: {number}")),
            )
        })
        .collect()
}

#[test]
fn counter_type_ids_match_native_definitions_and_keep_application_ids() {
    let header = include_str!("../aeron/aeron-client/src/main/c/aeron_counters.h");
    let mut known = std::collections::HashSet::new();
    let codes = native_defines(header, "AERON_COUNTER_", "_TYPE_ID");
    assert!(!codes.is_empty());
    for code in codes {
        let kind = AeronCounterTypeId::from_code(code);
        assert!(!matches!(kind, AeronCounterTypeId::Unknown(_)), "{code}");
        assert_eq!(kind.code(), code);
        assert!(known.insert(kind), "duplicate type ID {code}");
    }
    assert_eq!(AeronCounterTypeId::from_code(0), AeronCounterTypeId::SystemCounter);
    assert_eq!(AeronCounterTypeId::from_code(1), AeronCounterTypeId::PublisherLimit);
    assert_eq!(
        AeronCounterTypeId::from_code(100),
        AeronCounterTypeId::ArchiveRecordingPosition
    );
    assert_eq!(AeronCounterTypeId::from_code(201), AeronCounterTypeId::ClusterNodeRole);
    for code in [i32::MIN, -1, 1_234_567, i32::MAX] {
        let kind = AeronCounterTypeId::from(code);
        assert_eq!(kind, AeronCounterTypeId::Unknown(code));
        assert_eq!(i32::from(kind), code);
    }
}

#[test]
fn counter_record_states_preserve_signed_values() {
    const RECLAIMED: AeronCounterState = AeronCounterState::from_code(-1);
    assert_eq!(RECLAIMED, AeronCounterState::Reclaimed);
    for (code, expected) in [
        (0, AeronCounterState::Unused),
        (1, AeronCounterState::Allocated),
        (-1, RECLAIMED),
    ] {
        assert_eq!(AeronCounterState::from(code), expected);
        assert_eq!(i32::from(expected), code);
    }
    let header = include_str!("../aeron/aeron-client/src/main/c/aeronc.h");
    for code in native_defines(header, "AERON_COUNTER_RECORD_", "") {
        assert!(!matches!(
            AeronCounterState::from_code(code),
            AeronCounterState::Unknown(_)
        ));
    }
    for code in [i32::MIN, -2, 2, i32::MAX] {
        assert_eq!(AeronCounterState::from_code(code), AeronCounterState::Unknown(code));
        assert_eq!(AeronCounterState::from_code(code).code(), code);
    }
}

#[test]
fn driver_error_codes_match_wire_values_without_normalizing_their_sign() {
    let header = include_str!("../aeron/aeron-client/src/main/c/aeron_client_error.h");
    let codes = native_defines(header, "AERON_ERROR_CODE_", "");
    assert!(!codes.is_empty());
    for code in codes {
        let kind = AeronDriverErrorCode::from(code);
        assert!(!matches!(kind, AeronDriverErrorCode::Unknown(_)), "{code}");
        assert_eq!(i32::from(kind), code);
    }
    assert_eq!(
        AeronDriverErrorCode::from_code(-1),
        AeronDriverErrorCode::UnknownCodeValue
    );
    assert_eq!(AeronDriverErrorCode::from_code(1), AeronDriverErrorCode::InvalidChannel);
    assert_eq!(
        AeronDriverErrorCode::from_code(14),
        AeronDriverErrorCode::PublicationRevoked
    );
    for code in [i32::MIN, -14, -2, 1_234_567, i32::MAX] {
        assert_eq!(
            AeronDriverErrorCode::from_code(code),
            AeronDriverErrorCode::Unknown(code)
        );
        assert_eq!(AeronDriverErrorCode::from_code(code).code(), code);
    }
    assert_eq!(AeronErrorType::from_code(-1), AeronErrorType::GenericError);
    assert_eq!(AeronOfferError::from_position(-1), Err(AeronOfferError::NotConnected));
}

#[test]
fn system_counter_ids_follow_native_constants_and_reject_unknown_values() {
    for id in 0..=45 {
        assert_eq!(AeronSystemCounterType::try_from(id).unwrap() as u32, id as u32);
    }
    for (variant, native) in [
        (
            AeronSystemCounterType::PublicationsRevoked,
            AERON_SYSTEM_COUNTER_ID_PUBLICATIONS_REVOKED,
        ),
        (
            AeronSystemCounterType::PublicationImagesRevoked,
            AERON_SYSTEM_COUNTER_ID_PUBLICATION_IMAGES_REVOKED,
        ),
        (
            AeronSystemCounterType::ImagesRejected,
            AERON_SYSTEM_COUNTER_ID_IMAGES_REJECTED,
        ),
        (
            AeronSystemCounterType::ControlProtocolVersion,
            AERON_SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION,
        ),
        (
            AeronSystemCounterType::StatusMessagesRejected,
            AERON_SYSTEM_COUNTER_ID_STATUS_MESSAGES_REJECTED,
        ),
        (
            AeronSystemCounterType::NativeResourceAgentProxyFails,
            AERON_SYSTEM_COUNTER_ID_NATIVE_RESOURCE_AGENT_PROXY_FAILS,
        ),
    ] {
        assert_eq!(variant as u32, native);
        assert_eq!(AeronSystemCounterType::try_from(native as i32).unwrap(), variant);
    }
    for id in [46, 47, i32::MAX] {
        assert!(AeronSystemCounterType::try_from(id).is_err());
    }
    for id in [-1, i32::MIN] {
        assert_eq!(AeronSystemCounterType::try_from(id).unwrap_err().code, id);
    }
}

// aeron_image.h defines this type; public bindings keep it opaque.
#[repr(C)]
struct NativeHeader {
    frame: *mut aeron_data_header_t,
    initial_term_id: i32,
    position_bits_to_shift: usize,
    fragmented_frame_length: i32,
    context: *mut std::ffi::c_void,
}

impl NativeHeader {
    fn new(frame: *mut aeron_data_header_t) -> Self {
        Self {
            frame,
            initial_term_id: 0,
            position_bits_to_shift: 16,
            fragmented_frame_length: 0,
            context: std::ptr::null_mut(),
        }
    }
}

#[test]
fn fragment_header_accessors_read_the_frame_and_handle_a_missing_frame() {
    let mut frame = aeron_data_header_t::default();
    frame.session_id = 71;
    frame.stream_id = 72;
    frame.reserved_value = i64::MIN;
    frame.term_id = 73;
    frame.term_offset = 128;
    let mut native = NativeHeader::new(&mut frame);
    // SAFETY: the C header layout matches aeron_image.h. Both allocations stay live
    // and unchanged throughout the reads, and the null context represents no image.
    let header = unsafe { AeronHeader::from_ptr(std::ptr::from_mut(&mut native).cast()) };
    assert_eq!(header.session_id(), Some(71));
    assert_eq!(header.stream_id(), Some(72));
    assert_eq!(header.reserved_value(), Some(i64::MIN));
    assert_eq!(header.term_id(), Some(73));
    assert_eq!(header.term_offset(), Some(128));
    assert!(header.image().is_none());
    drop(header);
    native.frame = std::ptr::null_mut();
    // SAFETY: the live C header now has no frame. Accessors must return None.
    let header = unsafe { AeronHeader::from_ptr(std::ptr::from_mut(&mut native).cast()) };
    assert_eq!(header.session_id(), None);
    assert_eq!(header.stream_id(), None);
    assert_eq!(header.reserved_value(), None);
    assert_eq!(header.term_id(), None);
    assert_eq!(header.term_offset(), None);
}

#[test]
fn idle_strategy_strings_match_the_c_loader_names() {
    for (kind, name, arguments) in [
        (AeronIdleStrategyKind::Sleeping, "sleeping", "1000000"),
        (AeronIdleStrategyKind::Yielding, "yield", ""),
        (AeronIdleStrategyKind::BusySpin, "spin", ""),
        (AeronIdleStrategyKind::NoOp, "noop", ""),
        (AeronIdleStrategyKind::Backoff, "backoff", "10-20-1000-1000000"),
    ] {
        assert_eq!(kind.name(), name);
        assert_eq!(kind.name_c().to_bytes(), name.as_bytes());
        assert_eq!(kind.default_init_args(), arguments);
        assert_eq!(kind.default_init_args_c().to_bytes(), arguments.as_bytes());
    }
}

#[test]
fn uri_builder_reinitialization_removal_and_output_reuse() -> Result<(), AeronCError> {
    let builder = AeronUriStringBuilder::ipc()?;
    builder.alias("before")?;
    builder.init_on_string(c"aeron:udp?endpoint=127.0.0.1:40123|alias=after")?;
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"alias") }, "after");
    builder.remove(c"alias")?;
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"alias") }, "");
    builder.put_str(c"alias", "restored")?;
    builder.remove_str("alias")?;
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"alias") }, "");
    assert!(builder.remove_str("bad\0key").is_err());
    let mut output = String::with_capacity(1024);
    output.push_str("replace this text");
    builder.build_into(&mut output)?;
    assert_eq!(output, "aeron:udp?endpoint=127.0.0.1:40123");
    for (mode, expected) in [
        (ControlMode::Manual, "manual"),
        (ControlMode::Dynamic, "dynamic"),
        (ControlMode::Response, "response"),
    ] {
        let builder = AeronUriStringBuilder::udp_control("127.0.0.1:40124", mode)?;
        // SAFETY: the comparison ends before the next builder mutation.
        assert_eq!(unsafe { builder.get(c"control") }, "127.0.0.1:40124");
        // SAFETY: the comparison ends before the next builder mutation.
        assert_eq!(unsafe { builder.get(c"control-mode") }, expected);
    }
    Ok(())
}

macro_rules! uri_parameter {
    ($test:ident, $method:ident, $value:expr, $key:expr, $expected:expr) => {
        #[test]
        fn $test() -> Result<(), AeronCError> {
            let builder = AeronUriStringBuilder::ipc()?;
            assert!(std::ptr::eq(builder.$method($value)?, &builder));
            // SAFETY: the comparison ends before the next builder mutation.
            assert_eq!(unsafe { builder.get($key) }, $expected);
            Ok(())
        }
    };
}

uri_parameter!(uri_media, media, Media::Udp, c"__media", "udp");
uri_parameter!(
    uri_control_mode,
    control_mode,
    ControlMode::Response,
    c"control-mode",
    "response"
);
uri_parameter!(uri_prefix, prefix, "test-value", c"__prefix", "test-value");
uri_parameter!(
    uri_initial_term_id,
    initial_term_id,
    i32::MIN,
    c"init-term-id",
    "-2147483648"
);
uri_parameter!(uri_term_id, term_id, i32::MIN, c"term-id", "-2147483648");
uri_parameter!(uri_term_offset, term_offset, i32::MIN, c"term-offset", "-2147483648");
uri_parameter!(uri_alias, alias, "test-value", c"alias", "test-value");
uri_parameter!(uri_term_length, term_length, "test-value", c"term-length", "test-value");
uri_parameter!(
    uri_linger_timeout,
    linger_timeout,
    i64::MAX,
    c"linger",
    "9223372036854775807"
);
uri_parameter!(uri_mtu_length, mtu_length, i32::MIN, c"mtu", "-2147483648");
uri_parameter!(uri_ttl, ttl, i32::MIN, c"ttl", "-2147483648");
uri_parameter!(uri_sparse_term, sparse_term, false, c"sparse", "false");
uri_parameter!(uri_reliable, reliable, false, c"reliable", "false");
uri_parameter!(uri_eos, eos, false, c"eos", "false");
uri_parameter!(uri_tether, tether, false, c"tether", "false");
uri_parameter!(uri_tags, tags, "test-value", c"tags", "test-value");
uri_parameter!(uri_endpoint, endpoint, "test-value", c"endpoint", "test-value");
uri_parameter!(uri_interface, interface, "test-value", c"interface", "test-value");
uri_parameter!(uri_control, control, "test-value", c"control", "test-value");
uri_parameter!(uri_session_id, session_id, "test-value", c"session-id", "test-value");
uri_parameter!(uri_group, group, false, c"group", "false");
uri_parameter!(uri_rejoin, rejoin, false, c"rejoin", "false");
uri_parameter!(uri_fc, fc, "test-value", c"fc", "test-value");
uri_parameter!(uri_gtag, gtag, "test-value", c"gtag", "test-value");
uri_parameter!(uri_cc, cc, "test-value", c"cc", "test-value");
uri_parameter!(
    uri_spies_simulate_connection,
    spies_simulate_connection,
    false,
    c"ssc",
    "false"
);
uri_parameter!(uri_ats, ats, false, c"ats", "false");
uri_parameter!(uri_socket_sndbuf, socket_sndbuf, i32::MIN, c"so-sndbuf", "-2147483648");
uri_parameter!(uri_socket_rcvbuf, socket_rcvbuf, i32::MIN, c"so-rcvbuf", "-2147483648");
uri_parameter!(uri_socket_tos, socket_tos, u8::MAX, c"so-tos", "255");
uri_parameter!(
    uri_receiver_window,
    receiver_window,
    i32::MIN,
    c"rcv-wnd",
    "-2147483648"
);
uri_parameter!(
    uri_media_rcv_timestamp_offset,
    media_rcv_timestamp_offset,
    "test-value",
    c"media-rcv-ts-offset",
    "test-value"
);
uri_parameter!(
    uri_channel_rcv_timestamp_offset,
    channel_rcv_timestamp_offset,
    "test-value",
    c"channel-rcv-ts-offset",
    "test-value"
);
uri_parameter!(
    uri_channel_snd_timestamp_offset,
    channel_snd_timestamp_offset,
    "test-value",
    c"channel-snd-ts-offset",
    "test-value"
);
uri_parameter!(
    uri_response_correlation_id,
    response_correlation_id,
    i64::MAX,
    c"response-correlation-id",
    "9223372036854775807"
);
uri_parameter!(uri_nak_delay, nak_delay, i64::MAX, c"nak-delay", "9223372036854775807");
uri_parameter!(
    uri_untethered_window_limit_timeout,
    untethered_window_limit_timeout,
    i64::MAX,
    c"untethered-window-limit-timeout",
    "9223372036854775807"
);
uri_parameter!(
    uri_untethered_resting_timeout,
    untethered_resting_timeout,
    i64::MAX,
    c"untethered-resting-timeout",
    "9223372036854775807"
);
uri_parameter!(
    uri_untethered_linger_timeout,
    untethered_linger_timeout,
    i64::MAX,
    c"untethered-linger-timeout",
    "9223372036854775807"
);
uri_parameter!(uri_max_resend, max_resend, i32::MIN, c"max-resend", "-2147483648");
uri_parameter!(uri_stream_id, stream_id, i32::MIN, c"stream-id", "-2147483648");
uri_parameter!(
    uri_publication_window,
    publication_window,
    i32::MIN,
    c"pub-wnd",
    "-2147483648"
);

#[test]
fn uri_string_setters_reject_nuls_without_replacing_the_value() -> Result<(), AeronCError> {
    let builder = AeronUriStringBuilder::ipc()?;
    builder.prefix("old")?;
    assert!(builder.prefix("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"__prefix") }, "old");
    builder.alias("old")?;
    assert!(builder.alias("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"alias") }, "old");
    builder.term_length("old")?;
    assert!(builder.term_length("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"term-length") }, "old");
    builder.tags("old")?;
    assert!(builder.tags("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"tags") }, "old");
    builder.endpoint("old")?;
    assert!(builder.endpoint("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"endpoint") }, "old");
    builder.interface("old")?;
    assert!(builder.interface("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"interface") }, "old");
    builder.control("old")?;
    assert!(builder.control("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"control") }, "old");
    builder.session_id("old")?;
    assert!(builder.session_id("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"session-id") }, "old");
    builder.fc("old")?;
    assert!(builder.fc("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"fc") }, "old");
    builder.gtag("old")?;
    assert!(builder.gtag("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"gtag") }, "old");
    builder.cc("old")?;
    assert!(builder.cc("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"cc") }, "old");
    builder.media_rcv_timestamp_offset("old")?;
    assert!(builder.media_rcv_timestamp_offset("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"media-rcv-ts-offset") }, "old");
    builder.channel_rcv_timestamp_offset("old")?;
    assert!(builder.channel_rcv_timestamp_offset("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"channel-rcv-ts-offset") }, "old");
    builder.channel_snd_timestamp_offset("old")?;
    assert!(builder.channel_snd_timestamp_offset("bad\0value").is_err());
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"channel-snd-ts-offset") }, "old");
    Ok(())
}

#[test]
fn uri_timestamp_offsets_accept_reserved_as_a_value() -> Result<(), AeronCError> {
    let builder = AeronUriStringBuilder::udp("127.0.0.1:40123")?;
    builder.media_rcv_timestamp_offset("reserved")?;
    builder.channel_rcv_timestamp_offset("reserved")?;
    builder.channel_snd_timestamp_offset("reserved")?;
    for key in [
        c"media-rcv-ts-offset",
        c"channel-rcv-ts-offset",
        c"channel-snd-ts-offset",
    ] {
        // SAFETY: the comparison ends before the next builder mutation.
        assert_eq!(unsafe { builder.get(key) }, "reserved");
    }
    // SAFETY: the comparison ends before the next builder mutation.
    assert_eq!(unsafe { builder.get(c"reserved") }, "");
    let channel = builder.build(1024)?;
    assert!(channel.contains("media-rcv-ts-offset=reserved"));
    assert!(channel.contains("channel-rcv-ts-offset=reserved"));
    assert!(channel.contains("channel-snd-ts-offset=reserved"));
    Ok(())
}
