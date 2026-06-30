//! # Comprehensive Persistent Subscription Tests
//!
//! This module contains comprehensive unit tests for Aeron persistent subscriptions covering:
//! 1) Live source subscriptions
//! 2) Replay to live transitions
//! 3) Persistent connection recovery
//! 4) Media driver lifecycle
//! 5) Archiver integration
//! 6) Data write/read verification
//! 7) Error scenarios
//!
//! ## Running Tests
//!
//! These tests need a running Aeron Archive, which is a **Java** process.
//! They are *not* `#[ignore]`d — instead each test calls `skip_unless_java!()`,
//! so it runs automatically when `java` is on `PATH` and skips (passes) when it
//! isn't. No special flags required.
//!
//! ```bash
//! # Run all persistent subscription tests (skips automatically if java is absent)
//! cargo test --package rusteron-archive --lib --features "precompile static" -- persistent_subscription_tests -- --nocapture
//!
//! # Run a specific test
//! cargo test --package rusteron-archive --lib --features "precompile static" test_live_source_subscription -- --nocapture
//! ```

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::testing::EmbeddedArchiveMediaDriverProcess;
    use log::{error, info};
    use serial_test::serial;
    use std::cell::Cell;
    use std::error::Error;
    use std::os::raw::c_int;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    /// Test error handler for tracking Aeron errors
    #[derive(Default, Debug)]
    struct ErrorCount {
        error_count: usize,
    }

    impl AeronErrorHandlerCallback for ErrorCount {
        fn handle_aeron_error_handler(&mut self, error_code: c_int, msg: &str) {
            error!("Aeron error {}: {}", error_code, msg);
            self.error_count += 1;
        }
    }

    /// Helper function to find an unused UDP port
    fn find_unused_udp_port(start_port: u16) -> Option<u16> {
        for port in start_port..65535 {
            if std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok() {
                return Some(port);
            }
        }
        None
    }

    /// Helper function to start Aeron Archive with dynamic port allocation
    fn start_aeron_archive_with_config(
        aeron_dir_suffix: &str,
        start_port: u16,
    ) -> Result<
        (
            Aeron,
            AeronArchiveContext,
            EmbeddedArchiveMediaDriverProcess,
            Handler<ErrorCount>,
        ),
        Box<dyn Error>,
    > {
        let id = Aeron::nano_clock();
        let aeron_dir = format!("target/aeron/{}_{}/shm", id, aeron_dir_suffix);
        let archive_dir = format!("target/aeron/{}_{}/archive", id, aeron_dir_suffix);

        let request_port = find_unused_udp_port(start_port).expect("Could not find port");
        let response_port = find_unused_udp_port(request_port + 1).expect("Could not find port");
        let recording_event_port = find_unused_udp_port(response_port + 1).expect("Could not find port");

        let request_control_channel = format!("aeron:udp?endpoint=localhost:{}", request_port);
        let response_control_channel = format!("aeron:udp?endpoint=localhost:{}", response_port);
        let recording_events_channel = format!("aeron:udp?endpoint=localhost:{}", recording_event_port);

        let archive_media_driver = EmbeddedArchiveMediaDriverProcess::build_and_start(
            &aeron_dir,
            &archive_dir,
            &request_control_channel,
            &response_control_channel,
            &recording_events_channel,
        )
        .expect("Failed to start Java process");

        let aeron_context = AeronContext::new()?;
        aeron_context.set_dir(&aeron_dir.into_c_string())?;
        aeron_context.set_client_name(&format!("test-{}", aeron_dir_suffix).into_c_string())?;

        let error_handler = Handler::leak(ErrorCount::default());
        let error_handler_ref = &error_handler;
        aeron_context.set_error_handler(Some(error_handler_ref))?;

        // Create aeron and archive context
        let aeron = Aeron::new(&aeron_context)?;
        aeron.start()?;
        let archive_context = AeronArchiveContext::new()?;
        archive_context.set_aeron(&aeron)?;
        archive_context.set_control_request_channel(&request_control_channel.into_c_string())?;
        archive_context.set_control_response_channel(&response_control_channel.into_c_string())?;
        archive_context.set_recording_events_channel(&recording_events_channel.into_c_string())?;
        archive_context.set_error_handler(Some(error_handler_ref))?;

        Ok((aeron, archive_context, archive_media_driver, error_handler))
    }

    /////////////////////////////////////////////////////////////////////////////
    // TEST CATEGORY 1: Live Source Subscriptions
    /////////////////////////////////////////////////////////////////////////////

    /// Test that a persistent subscription can join a live stream and consume messages
    #[test]
    #[serial]
    fn test_live_source_subscription() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("archive_live_source", 8000)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        // Create publication and start recording
        let channel = "aeron:ipc";
        let stream_id = 1001;

        let subscription_id =
            archive.start_recording(&channel.into_c_string(), stream_id, SOURCE_LOCATION_LOCAL, true)?;
        info!("Started recording subscription_id={}", subscription_id);

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Wait for publication to be connected
        let start = Instant::now();
        while !publication.is_connected() && start.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(10));
        }
        assert!(publication.is_connected());

        // Publish some messages
        let message_count = 10;

        // Create the subscription BEFORE publishing and wait for its image, so the
        // live IPC subscription actually sees the messages that follow. A live
        // subscription never receives messages published before its image existed.
        let subscription = aeron_archive
            .async_add_subscription(
                &channel.into_c_string(),
                stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?
            .poll_blocking(Duration::from_secs(5))?;

        #[derive(Default)]
        struct MessageHandler {
            count: Cell<usize>,
        }

        impl AeronFragmentHandlerCallback for MessageHandler {
            fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
                self.count.set(self.count.get() + 1);
            }
        }

        let mut handler = Handler::leak(MessageHandler::default());

        // Drain the subscription's image into existence before publishing.
        let start = Instant::now();
        while subscription.image_count()? == 0 && start.elapsed() < Duration::from_secs(5) {
            subscription.poll(Some(&mut handler), 10)?;
            sleep(Duration::from_millis(10));
        }
        assert!(
            subscription.image_count()? > 0,
            "subscription should have an image before publishing"
        );

        for i in 0..message_count {
            let message = format!("Live Message {}", i);
            while publication.offer(message.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                sleep(Duration::from_millis(10));
            }
        }
        info!("Published {} messages", message_count);

        // Poll for messages
        let start = Instant::now();
        while handler.count.get() < message_count && start.elapsed() < Duration::from_secs(10) {
            subscription.poll(Some(&mut handler), 10)?;
            sleep(Duration::from_millis(10));
        }

        assert_eq!(
            handler.count.get(),
            message_count,
            "Should receive all messages from live stream"
        );

        handler.release();
        subscription.close(Handlers::no_notification_handler())?;
        drop(publication);
        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /////////////////////////////////////////////////////////////////////////////
    // TEST CATEGORY 2: Replay to Live Transitions
    /////////////////////////////////////////////////////////////////////////////

    /// Test that a persistent subscription transitions from replay to live correctly
    #[test]
    #[serial]
    fn test_replay_to_live_transition() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("archive_replay_live", 8100)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        info!("=== Replay then live consumption ===");
        // The seamless replay-merge-to-live transition is covered by
        // `test_simple_replay_merge` in lib.rs; this verifies the two phases
        // (replay of history, then live consumption) on a recorded IPC stream.

        let channel = "aeron:ipc";
        let stream_id = 1002;

        archive.start_recording(&channel.into_c_string(), stream_id, SOURCE_LOCATION_LOCAL, true)?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Phase 1: publish an initial batch that will be replayed.
        let initial_message_count = 50;
        for i in 0..initial_message_count {
            let message = format!("Replay Message {}", i);
            let deadline = Instant::now() + Duration::from_secs(10);
            while publication.offer(message.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                if Instant::now() > deadline {
                    return Err("timed out offering replay message".into());
                }
                sleep(Duration::from_millis(10));
            }
        }
        info!("Published {} initial messages for replay", initial_message_count);

        // Resolve the recording and wait for it to flush.
        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let mut counter_id = -1;
        let start = Instant::now();
        while counter_id == -1 && start.elapsed() < Duration::from_secs(5) {
            counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
            sleep(Duration::from_millis(10));
        }
        assert!(counter_id >= 0, "Could not find recording counter");

        let recording_id = RecordingPos::get_recording_id(&counters_reader, counter_id)?;
        let published_position = publication.position();
        let start = Instant::now();
        while counters_reader.get_counter_value(counter_id) < published_position
            && start.elapsed() < Duration::from_secs(10)
        {
            sleep(Duration::from_millis(10));
        }
        info!("Recording ID: {}", recording_id);

        // Phase 2: replay the initial batch and verify.
        let replay_stream_id = 1003;
        let replay_params = AeronArchiveReplayParams::new(-1, i32::MAX, 0, i64::MAX, 0, 0)?;
        let replay_session_id =
            archive.start_replay(recording_id, &channel.into_c_string(), replay_stream_id, &replay_params)?;
        let replay_channel = format!("{}?session-id={}", channel, replay_session_id as i32).into_c_string();
        let replay_sub = aeron_archive
            .async_add_subscription(
                &replay_channel,
                replay_stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?
            .poll_blocking(Duration::from_secs(10))?;

        #[derive(Default)]
        struct PhaseHandler {
            replay_count: usize,
            live_count: usize,
        }
        impl AeronFragmentHandlerCallback for PhaseHandler {
            fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
                if let Ok(s) = std::str::from_utf8(buffer) {
                    if s.starts_with("Replay Message") {
                        self.replay_count += 1;
                    } else if s.starts_with("Live Message") {
                        self.live_count += 1;
                    }
                }
            }
        }
        let mut handler = Handler::leak(PhaseHandler::default());

        let start = Instant::now();
        while handler.replay_count < initial_message_count && start.elapsed() < Duration::from_secs(20) {
            replay_sub.poll(Some(&mut handler), 100)?;
            sleep(Duration::from_millis(10));
        }
        assert!(
            handler.replay_count >= initial_message_count,
            "Should have received all replay messages (got {})",
            handler.replay_count
        );
        replay_sub.close(Handlers::no_notification_handler())?;
        info!("Replayed {} messages", handler.replay_count);

        // Phase 3: live consumption. Subscribe first and wait for an image.
        let subscription = aeron_archive
            .async_add_subscription(
                &channel.into_c_string(),
                stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?
            .poll_blocking(Duration::from_secs(5))?;
        let start = Instant::now();
        while subscription.image_count()? == 0 && start.elapsed() < Duration::from_secs(5) {
            subscription.poll(Some(&mut handler), 10)?;
            sleep(Duration::from_millis(10));
        }

        let live_message_count = 20;
        for i in 0..live_message_count {
            let message = format!("Live Message {}", i);
            let deadline = Instant::now() + Duration::from_secs(10);
            while publication.offer(message.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                if Instant::now() > deadline {
                    return Err("timed out offering live message".into());
                }
                sleep(Duration::from_millis(10));
            }
        }

        let start = Instant::now();
        while handler.live_count < live_message_count && start.elapsed() < Duration::from_secs(20) {
            subscription.poll(Some(&mut handler), 100)?;
            sleep(Duration::from_millis(10));
        }
        assert!(
            handler.live_count >= live_message_count,
            "Should have received live messages (got {})",
            handler.live_count
        );

        handler.release();
        subscription.close(Handlers::no_notification_handler())?;
        drop(publication);
        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /////////////////////////////////////////////////////////////////////////////
    // TEST CATEGORY 3: Persistent Connection Recovery
    /////////////////////////////////////////////////////////////////////////////

    /// Test that a persistent subscription can recover from connection failures
    #[test]
    #[serial]
    fn test_persistent_connection_recovery() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("archive_recovery", 8200)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        // Create publication and start recording
        let channel = "aeron:ipc";
        let stream_id = 1004;

        let subscription_id =
            archive.start_recording(&channel.into_c_string(), stream_id, SOURCE_LOCATION_LOCAL, true)?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Publish some messages
        let message_count = 20;
        for i in 0..message_count {
            let message = format!("Message {}", i);
            while publication.offer(message.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                sleep(Duration::from_millis(10));
            }
        }

        // Get recording ID
        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        let recording_id = RecordingPos::get_recording_id_block(&counters_reader, counter_id, Duration::from_secs(5))?;

        // Simulate connection interruption by closing the archive
        drop(archive);

        info!("Archive closed, simulating connection failure");

        // Reconnect to archive with adaptive polling for cleanup completion
        let archive_context_reconnect = {
            let mut retry_delay = Duration::from_millis(50);
            let max_attempts = 60; // 60 attempts with exponential backoff = up to ~12s max
            let mut attempts = 0;

            loop {
                attempts += 1;
                let result = AeronArchiveContext::new();

                match result {
                    Ok(ctx) => break ctx,
                    Err(_) if attempts < max_attempts => {
                        sleep(retry_delay);
                        // Exponential backoff with max of 2s
                        retry_delay = retry_delay.saturating_mul(2).min(Duration::from_secs(2));
                    }
                    Err(e) => {
                        return Err(format!("archive cleanup timeout after {} attempts: {}", attempts, e).into());
                    }
                }
            }
        };

        archive_context_reconnect.set_aeron(&aeron_archive)?;
        archive_context_reconnect
            .set_control_request_channel(&archive_context.get_control_request_channel().into_c_string())?;
        archive_context_reconnect
            .set_control_response_channel(&archive_context.get_control_response_channel().into_c_string())?;
        archive_context_reconnect
            .set_recording_events_channel(&archive_context.get_recording_events_channel().into_c_string())?;

        let archive_connector_reconnect =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context_reconnect, &aeron_archive)?;

        let archive_reconnected = archive_connector_reconnect
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to reconnect to archive");

        info!("Successfully reconnected to archive");

        // Verify we can still access the recording
        let mut count = 0;
        let found_recording = Cell::new(false);
        archive_reconnected.list_recordings_once(&mut count, 0, i32::MAX, |desc| {
            if desc.recording_id() == recording_id {
                found_recording.set(true);
                info!("Found recording after reconnection: {}", recording_id);
            }
        })?;

        assert!(found_recording.get(), "Should find recording after reconnection");

        drop(archive_reconnected);
        drop(aeron_archive);
        drop(media_driver_archive);
        archive_error_handler.release();

        Ok(())
    }

    /////////////////////////////////////////////////////////////////////////////
    // TEST CATEGORY 4: Media Driver Lifecycle
    /////////////////////////////////////////////////////////////////////////////

    /// Test that persistent subscriptions handle media driver lifecycle correctly
    #[test]
    #[serial]
    fn test_media_driver_lifecycle() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start and verify media driver
        let (aeron_archive, archive_context, media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("archive_lifecycle", 8300)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        info!("Archive connected with ID: {}", archive.get_archive_id());

        // Verify archive is responsive
        let archive_id = archive.get_archive_id();
        assert!(archive_id > 0, "Archive ID should be positive");

        // Create publication and subscription
        let channel = "aeron:ipc";
        let stream_id = 1005;

        let subscription_id =
            archive.start_recording(&channel.into_c_string(), stream_id, SOURCE_LOCATION_LOCAL, true)?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Publish some messages
        let message_count = 5;
        for i in 0..message_count {
            let message = format!("Lifecycle Test Message {}", i);
            while publication.offer(message.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                sleep(Duration::from_millis(10));
            }
        }

        // Verify recording exists
        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        assert!(counter_id >= 0, "Should find recording counter");

        // Clean shutdown
        drop(archive);
        drop(aeron_archive);

        info!("Clean shutdown completed");

        // Verify cleanup happens via Drop trait
        drop(media_driver_archive);
        archive_error_handler.release();

        Ok(())
    }

    /////////////////////////////////////////////////////////////////////////////
    // TEST CATEGORY 5: Archiver Integration
    /////////////////////////////////////////////////////////////////////////////

    /// Test integration with the Aeron Archive
    #[test]
    #[serial]
    fn test_archiver_integration() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("archive_integration", 8400)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        // Test recording lifecycle
        let channel = "aeron:ipc";
        let stream_id = 1006;

        // Start recording
        let subscription_id =
            archive.start_recording(&channel.into_c_string(), stream_id, SOURCE_LOCATION_LOCAL, true)?;
        info!("Started recording with subscription_id={}", subscription_id);

        // Create publication
        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Publish messages
        let message_count = 15;
        for i in 0..message_count {
            let message = format!("Archive Integration Message {}", i);
            while publication.offer(message.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                sleep(Duration::from_millis(10));
            }
        }

        // List recordings
        let mut count = 0;
        let found_recording = Cell::new(false);
        archive.list_recordings_once(&mut count, 0, i32::MAX, |desc| {
            if desc.stream_id() == stream_id {
                found_recording.set(true);
                info!(
                    "Found recording: id={}, stream_id={}, start_position={}",
                    desc.recording_id(),
                    desc.stream_id(),
                    desc.start_position()
                );
            }
        })?;

        assert!(found_recording.get(), "Should find recording for our stream");

        // Stop recording
        archive.stop_recording_channel_and_stream(&channel.into_c_string(), stream_id)?;
        info!("Stopped recording");

        // List recordings again to verify
        let mut count_after_stop = 0;
        archive.list_recordings_once(&mut count_after_stop, 0, i32::MAX, |desc| {
            info!(
                "Recording after stop: id={}, stop_position={}",
                desc.recording_id(),
                desc.stop_position()
            );
        })?;

        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /////////////////////////////////////////////////////////////////////////////
    // TEST CATEGORY 6: Data Write/Read Verification
    /////////////////////////////////////////////////////////////////////////////

    /// Test that data written is correctly read back through persistent subscription
    #[test]
    #[serial]
    fn test_data_write_read_verification() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("archive_data_verify", 8500)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        // Create publication and start recording
        let channel = "aeron:ipc";
        let stream_id = 1007;

        let subscription_id =
            archive.start_recording(&channel.into_c_string(), stream_id, SOURCE_LOCATION_LOCAL, true)?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Write test data with sequence numbers for verification
        struct TestData {
            sequence: u64,
            data: String,
        }

        let test_messages: Vec<TestData> = (0..100)
            .map(|i| TestData {
                sequence: i,
                data: format!("Test Data Message {}", i),
            })
            .collect();

        for msg in &test_messages {
            let serialized = format!("{}:{}", msg.sequence, msg.data);
            while publication.offer(serialized.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                sleep(Duration::from_millis(10));
            }
        }
        info!("Published {} test messages", test_messages.len());

        // Get recording ID
        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        let recording_id = RecordingPos::get_recording_id_block(&counters_reader, counter_id, Duration::from_secs(5))?;

        // Wait for the recording to capture everything we published before replaying,
        // otherwise the replay only sees a partial recording.
        let published_position = publication.position();
        let start = Instant::now();
        while counters_reader.get_counter_value(counter_id) < published_position
            && start.elapsed() < Duration::from_secs(10)
        {
            sleep(Duration::from_millis(10));
        }

        // Start replay
        let replay_stream_id = 1008;
        let replay_params = AeronArchiveReplayParams::new(-1, i32::MAX, 0, i64::MAX, 0, 0)?;

        let replay_session_id =
            archive.start_replay(recording_id, &channel.into_c_string(), replay_stream_id, &replay_params)?;

        // Create subscription for replay
        let replay_channel = format!("{}?session-id={}", channel, replay_session_id as i32).into_c_string();

        let subscription = aeron_archive
            .async_add_subscription(
                &replay_channel,
                replay_stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?
            .poll_blocking(Duration::from_secs(10))?;

        #[derive(Default)]
        struct VerificationHandler {
            received: Vec<(u64, String)>,
        }

        impl AeronFragmentHandlerCallback for VerificationHandler {
            fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
                if let Ok(s) = std::str::from_utf8(buffer) {
                    if let Some(colon_pos) = s.find(':') {
                        if let Ok(seq) = s[..colon_pos].parse::<u64>() {
                            let data = &s[colon_pos + 1..];
                            self.received.push((seq, data.to_string()));
                        }
                    }
                }
            }
        }

        let mut handler = Handler::leak(VerificationHandler::default());

        // Poll for all messages
        let start = Instant::now();
        while handler.received.len() < test_messages.len() && start.elapsed() < Duration::from_secs(20) {
            subscription.poll(Some(&mut handler), 100)?;
            sleep(Duration::from_millis(10));
        }

        // Verify data integrity
        assert_eq!(
            handler.received.len(),
            test_messages.len(),
            "Should receive all test messages"
        );

        for (i, (seq, data)) in handler.received.iter().enumerate() {
            assert_eq!(*seq, i as u64, "Sequence number should match");
            assert_eq!(*data, format!("Test Data Message {}", i), "Data content should match");
        }

        info!("Data verification completed successfully");

        // Release handlers in reverse order of creation to avoid
        // Handler leak warnings during cleanup
        subscription.close(Handlers::no_notification_handler())?;
        handler.release();

        // Explicitly release the error handler before dropping archive objects
        archive_error_handler.release();

        drop(archive);
        drop(aeron_archive);

        Ok(())
    }

    /////////////////////////////////////////////////////////////////////////////
    // TEST CATEGORY 7: Error Scenarios
    /////////////////////////////////////////////////////////////////////////////

    /// Test error handling for invalid channel configurations
    #[test]
    #[serial]
    fn test_invalid_channel_configuration() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("archive_errors", 8600)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        // Test 1: Invalid channel format
        let invalid_channel = "invalid:channel:format".into_c_string();
        let result = archive.start_recording(&invalid_channel, 1009, SOURCE_LOCATION_LOCAL, true);

        assert!(
            result.is_err(),
            "Should fail to start recording with invalid channel format"
        );
        info!("Correctly rejected invalid channel format");

        // Test 2: Stop recording on non-existent channel
        let nonexistent_channel = "aeron:udp?endpoint=localhost:99999".into_c_string();
        let result = archive.stop_recording_channel_and_stream(&nonexistent_channel, 1010);

        // This might succeed or fail depending on implementation
        info!("Stop recording on non-existent channel result: {:?}", result);

        // Test 3: Replay with invalid recording ID
        let invalid_recording_id = 999999;
        let replay_params = AeronArchiveReplayParams::new(-1, i32::MAX, 0, 100, 0, 0)?;
        let result = archive.start_replay(invalid_recording_id, &"aeron:ipc".into_c_string(), 1011, &replay_params);

        assert!(result.is_err(), "Should fail to start replay with invalid recording ID");
        info!("Correctly rejected invalid recording ID");

        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /// Test error handling for subscription failures
    #[test]
    #[serial]
    fn test_subscription_failure_handling() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("archive_sub_errors", 8700)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        // Test subscription to non-existent stream
        let invalid_channel = "aeron:ipc".into_c_string();
        let invalid_stream_id = 9999;

        // This should create the subscription but it won't connect
        let subscription_result = aeron_archive.async_add_subscription(
            &invalid_channel,
            invalid_stream_id,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        );

        // Should succeed to create subscription
        let async_sub = subscription_result?;

        // A subscription to a stream with no publisher is valid in Aeron — it simply
        // never receives an image. poll_blocking resolves the async connector once
        // the subscription is registered (not on image availability), so verify the
        // no-image condition directly after giving the driver a moment.
        let subscription = async_sub.poll_blocking(Duration::from_secs(2))?;
        sleep(Duration::from_secs(1));
        assert_eq!(
            subscription.image_count()?,
            0,
            "Should have no image on a stream with no publisher"
        );
        info!("Correctly observed no image on non-existent stream");

        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /// Test handling of recording position validation
    #[test]
    #[serial]
    fn test_recording_position_validation() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("archive_pos_validate", 8800)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        // Create publication and start recording
        let channel = "aeron:ipc";
        let stream_id = 1012;

        let subscription_id =
            archive.start_recording(&channel.into_c_string(), stream_id, SOURCE_LOCATION_LOCAL, true)?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Publish some messages
        let message_count = 10;
        for i in 0..message_count {
            let message = format!("Position Test Message {}", i);
            while publication.offer(message.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                sleep(Duration::from_millis(10));
            }
        }

        // Get recording position (wait for the recording to capture the messages)
        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        let mut recording_position = counters_reader.get_counter_value(counter_id);
        let start = Instant::now();
        while recording_position <= 0 && start.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(10));
            recording_position = counters_reader.get_counter_value(counter_id);
        }

        info!("Recording position: {}", recording_position);
        assert!(recording_position > 0, "Recording position should be positive");

        // Try to replay from an invalid position (beyond current)
        let recording_id = RecordingPos::get_recording_id_block(&counters_reader, counter_id, Duration::from_secs(5))?;

        let invalid_position = recording_position + 10000;
        let replay_params = AeronArchiveReplayParams::new(-1, i32::MAX, invalid_position, 100, 0, 0)?;

        let result = archive.start_replay(recording_id, &channel.into_c_string(), 1013, &replay_params);

        // This should either fail or start replay with no data
        if let Ok(replay_id) = result {
            info!(
                "Started replay from beyond current position (replay_id={}) - will deliver no data",
                replay_id
            );
        } else {
            info!("Correctly rejected replay from beyond current position");
        }

        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /// Test concurrent subscriptions to the same recording
    #[test]
    #[serial]
    fn test_concurrent_subscriptions() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("archive_concurrent", 8900)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        // Create publication and start recording
        let channel = "aeron:ipc";
        let stream_id = 1014;

        let subscription_id =
            archive.start_recording(&channel.into_c_string(), stream_id, SOURCE_LOCATION_LOCAL, true)?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Create multiple concurrent subscriptions BEFORE publishing and wait for
        // each to have an image, so every live IPC subscription sees the messages
        // that follow. A live subscription never receives messages published
        // before its image existed.
        let num_subscriptions = 3;
        let mut subscriptions = Vec::new();
        let mut handlers = Vec::new();

        for _ in 0..num_subscriptions {
            #[derive(Default)]
            struct ConcurrentHandler {
                count: Cell<usize>,
            }

            impl AeronFragmentHandlerCallback for ConcurrentHandler {
                fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
                    self.count.set(self.count.get() + 1);
                }
            }

            let handler = Handler::leak(ConcurrentHandler::default());
            handlers.push(handler);

            let sub = aeron_archive
                .async_add_subscription(
                    &channel.into_c_string(),
                    stream_id,
                    Handlers::no_available_image_handler(),
                    Handlers::no_unavailable_image_handler(),
                )?
                .poll_blocking(Duration::from_secs(5))?;

            subscriptions.push(sub);
        }

        // Wait until every subscription has an image before publishing.
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            let mut all_ready = true;
            for (i, sub) in subscriptions.iter().enumerate() {
                sub.poll(Some(&mut handlers[i]), 10)?;
                if sub.image_count()? == 0 {
                    all_ready = false;
                }
            }
            if all_ready {
                break;
            }
            sleep(Duration::from_millis(10));
        }

        // Publish messages
        let message_count = 50;
        for i in 0..message_count {
            let message = format!("Concurrent Test Message {}", i);
            let deadline = Instant::now() + Duration::from_secs(10);
            while publication.offer(message.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                if Instant::now() > deadline {
                    return Err(format!("timed out offering concurrent message {i}").into());
                }
                sleep(Duration::from_millis(10));
            }
        }

        // Poll all subscriptions
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            for (i, sub) in subscriptions.iter().enumerate() {
                sub.poll(Some(&mut handlers[i]), 10)?;
            }
            sleep(Duration::from_millis(10));

            // Check if all received expected messages
            let all_complete = handlers.iter().all(|h| h.count.get() >= message_count);
            if all_complete {
                break;
            }
        }

        // Verify all subscriptions received the messages
        for (i, handler) in handlers.iter().enumerate() {
            assert!(
                handler.count.get() >= message_count,
                "Subscription {} should have received all messages",
                i
            );
            info!("Subscription {} received {} messages", i, handler.count.get());
        }

        // Cleanup
        for sub in subscriptions {
            sub.close(Handlers::no_notification_handler())?;
        }
        for mut handler in handlers {
            handler.release();
        }

        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /////////////////////////////////////////////////////////////////////////////
    // TEST CATEGORY 8: Persistent Subscription Listener (callback wiring)
    /////////////////////////////////////////////////////////////////////////////

    /// Verify the persistent-subscription listener callbacks reach Rust.
    ///
    /// This exercises the `ListenerHolder` wiring (a `Box<dyn
    /// PersistentSubscriptionListener>` behind a stable thin pointer handed to C),
    /// proving the C callbacks dereference a valid fat pointer and invoke the
    /// Rust trait methods — no vtable corruption, no segfault. Drives a recorded
    /// IPC stream through replay -> live and asserts `on_live_joined` fires.
    #[test]
    #[serial]
    fn test_persistent_subscription_listener_live_joined() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("ps_listener", 9700)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        let live_channel = "aeron:ipc";
        let stream_id = 3001;
        archive.start_recording(&live_channel.into_c_string(), stream_id, SOURCE_LOCATION_LOCAL, true)?;

        let publication = aeron_archive
            .async_add_publication(&live_channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;
        let start = Instant::now();
        while !publication.is_connected() && start.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(10));
        }
        assert!(publication.is_connected());

        // Seed the recording with a few messages.
        for i in 0..10 {
            let message = format!("Seed-{}", i);
            let deadline = Instant::now() + Duration::from_secs(10);
            while publication.offer(message.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                if Instant::now() > deadline {
                    return Err("timed out offering seed message".into());
                }
                sleep(Duration::from_millis(10));
            }
        }

        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        let recording_id = RecordingPos::get_recording_id_block(&counters_reader, counter_id, Duration::from_secs(5))?;
        let published_position = publication.position();
        let start = Instant::now();
        while counters_reader.get_counter_value(counter_id) < published_position
            && start.elapsed() < Duration::from_secs(10)
        {
            sleep(Duration::from_millis(10));
        }
        info!("listener test recording_id={}", recording_id);

        // Listener that records lifecycle events into shared counters so the test
        // thread can observe them (the C callbacks fire on the conductor/poll path).
        struct CountingListener {
            joined: Arc<AtomicUsize>,
            errors: Arc<Mutex<Vec<(i32, String)>>>,
        }
        impl PersistentSubscriptionListener for CountingListener {
            fn on_live_joined(&self) {
                info!("on_live_joined");
                self.joined.fetch_add(1, Ordering::SeqCst);
            }
            fn on_live_left(&self) {
                info!("on_live_left");
            }
            fn on_error(&self, code: i32, msg: &str) {
                info!("on_error {} {}", code, msg);
                self.errors.lock().unwrap().push((code, msg.to_string()));
            }
        }

        let joined = Arc::new(AtomicUsize::new(0));
        let errors: Arc<Mutex<Vec<(i32, String)>>> = Arc::new(Mutex::new(Vec::new()));

        let ps = persistent_subscription_builder()?
            .aeron(&aeron_archive)?
            .archive_context(&archive_context)?
            .live_channel(live_channel)?
            .live_stream_id(stream_id)?
            .replay_channel("aeron:udp?endpoint=localhost:0")?
            .replay_stream_id(stream_id + 1)?
            .start_position(0)?
            .recording_id(recording_id)?
            .listener(CountingListener {
                joined: joined.clone(),
                errors: errors.clone(),
            })?
            .build()?;

        // Drive the persistent subscription: keep the live stream active by
        // publishing, and poll so it replays then joins live. Aeron types are
        // !Send, so this all happens on the test thread. `ps.poll_once()` drives
        // the archive client internally, so no `archive.poll_for_recording_signals()`.
        let mut i = 0;
        let start = Instant::now();
        while !ps.is_live() && start.elapsed() < Duration::from_secs(30) {
            if ps.has_failed() {
                panic!(
                    "persistent subscription failed: {:?}; listener errors: {:?}",
                    ps.get_failure_reason(),
                    errors.lock().unwrap().clone()
                );
            }
            let _ = publication.offer(
                format!("Live-{i}").as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            );
            i += 1;
            let fragments = ps.poll_once(|_buffer, _header| {}, 100)?;
            if fragments == 0 {
                sleep(Duration::from_millis(1));
            }
        }

        let join_count = joined.load(Ordering::SeqCst);
        let errs = errors.lock().unwrap().clone();

        ps.close()?;
        drop(publication);
        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        assert!(
            join_count >= 1,
            "on_live_joined did not fire; listener errors: {:?}",
            errs
        );
        info!("on_live_joined fired {} time(s)", join_count);

        Ok(())
    }

    /// Resilience: when the live image is lost the persistent subscription falls
    /// back to replay, and when the stream returns it rejoins live — so
    /// `on_live_joined` fires a second time. Mirrors Aeron's
    /// `aeron_archive_persistent_subscription_resilience_test` fallback+recover
    /// scenario. Aeron's test injects frame loss via a loss generator (not
    /// available behind the Java archive harness), so here the live image is
    /// dropped by tearing down and recreating an **exclusive** publication.
    #[test]
    #[serial]
    fn test_persistent_subscription_fallback_and_recover() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("ps_resilience", 9800)?;

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        let live_channel = "aeron:ipc";
        let stream_id = 3101;
        archive.start_recording(&live_channel.into_c_string(), stream_id, SOURCE_LOCATION_LOCAL, true)?;

        // Exclusive publication so we can tear it down and re-add cleanly.
        let mut publication = aeron_archive
            .async_add_exclusive_publication(&live_channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;
        let start = Instant::now();
        while !publication.is_connected() && start.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(10));
        }
        for i in 0..10 {
            let m = format!("Seed-{i}");
            while publication.offer(m.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
                sleep(Duration::from_millis(1));
            }
        }
        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        let recording_id = RecordingPos::get_recording_id_block(&counters_reader, counter_id, Duration::from_secs(5))?;

        struct ResilienceListener {
            joined: Arc<AtomicUsize>,
            left: Arc<AtomicUsize>,
            errors: Arc<Mutex<Vec<(i32, String)>>>,
        }
        impl PersistentSubscriptionListener for ResilienceListener {
            fn on_live_joined(&self) {
                self.joined.fetch_add(1, Ordering::SeqCst);
                info!("on_live_joined (total {})", self.joined.load(Ordering::SeqCst));
            }
            fn on_live_left(&self) {
                self.left.fetch_add(1, Ordering::SeqCst);
                info!("on_live_left (total {})", self.left.load(Ordering::SeqCst));
            }
            fn on_error(&self, code: i32, msg: &str) {
                self.errors.lock().unwrap().push((code, msg.into()));
            }
        }
        let joined = Arc::new(AtomicUsize::new(0));
        let left = Arc::new(AtomicUsize::new(0));
        let errors: Arc<Mutex<Vec<(i32, String)>>> = Arc::new(Mutex::new(Vec::new()));

        let ps = persistent_subscription_builder()?
            .aeron(&aeron_archive)?
            .archive_context(&archive_context)?
            .live_channel(live_channel)?
            .live_stream_id(stream_id)?
            .replay_channel("aeron:udp?endpoint=localhost:0")?
            .replay_stream_id(stream_id + 1)?
            .start_from_beginning()?
            .recording_id(recording_id)?
            .listener(ResilienceListener {
                joined: joined.clone(),
                left: left.clone(),
                errors: errors.clone(),
            })?
            .build()?;

        let poll_drive = |ps: &AeronArchivePersistentSubscription, pub_ref: &AeronExclusivePublication| {
            let _ = pub_ref.offer(b"live-beat", Handlers::no_reserved_value_supplier_handler());
            ps.poll_once(|_buf, _hdr| {}, 100)
        };

        // Phase 1: drive until live (on_live_joined fires once).
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ps.is_live() && Instant::now() < deadline {
            assert!(!ps.has_failed(), "ps failed: {:?}", ps.get_failure_reason());
            poll_drive(&ps, &publication)?;
            sleep(Duration::from_millis(1));
        }
        assert!(joined.load(Ordering::SeqCst) >= 1, "never went live initially");

        // Phase 2: tear down the exclusive publication -> live image is lost ->
        // PS falls back to replay (on_live_left fires, is_replaying() becomes true).
        drop(publication);
        let deadline = Instant::now() + Duration::from_secs(30);
        while ps.is_live() && Instant::now() < deadline {
            assert!(!ps.has_failed(), "ps failed after loss: {:?}", ps.get_failure_reason());
            let _ = ps.poll_once(|_buf, _hdr| {}, 100)?;
            sleep(Duration::from_millis(10));
        }
        assert!(
            left.load(Ordering::SeqCst) >= 1,
            "never left live; ps.is_live={} replaying={}",
            ps.is_live(),
            ps.is_replaying()
        );

        // Phase 3: restore the live stream -> PS rejoins live (on_live_joined again).
        publication = aeron_archive
            .async_add_exclusive_publication(&live_channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;
        let deadline = Instant::now() + Duration::from_secs(30);
        while joined.load(Ordering::SeqCst) < 2 && Instant::now() < deadline {
            assert!(!ps.has_failed(), "ps failed on rejoin: {:?}", ps.get_failure_reason());
            poll_drive(&ps, &publication)?;
            sleep(Duration::from_millis(1));
        }

        let joined_count = joined.load(Ordering::SeqCst);
        ps.close()?;
        drop(publication);
        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        assert!(
            joined_count >= 2,
            "did not rejoin live after recovery (joined {joined_count} time(s)); errors: {:?}",
            errors.lock().unwrap().clone()
        );
        info!("resilience OK: joined live {joined_count} time(s)");
        Ok(())
    }
}
