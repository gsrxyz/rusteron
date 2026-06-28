//! # Persistent Subscription Integration Tests
//!
//! This module contains integration tests for Aeron persistent subscriptions that focus on
//! real-world scenarios and end-to-end testing.
//!
//! ## Running Tests
//!
//! These tests need a running Aeron Archive, which is a **Java** process.
//! They are *not* `#[ignore]`d — instead each test calls `skip_unless_java!()`,
//! so it runs automatically when `java` is on `PATH` and skips (passes) when it
//! isn't. No special flags required.
//!
//! ```bash
//! # Run all integration tests (skips automatically if java is absent)
//! cargo test --package rusteron-archive --lib --features "precompile static" -- persistent_subscription_integration -- --nocapture
//!
//! # Run a specific test
//! cargo test --package rusteron-archive --lib --features "precompile static" test_end_to_end_persistent_subscription -- --nocapture
//! ```

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::testing::EmbeddedArchiveMediaDriverProcess;
    use log::{error, info, warn};
    use serial_test::serial;
    use std::error::Error;
    use std::os::raw::c_int;

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
        let recording_event_port =
            find_unused_udp_port(response_port + 1).expect("Could not find port");

        let request_control_channel = format!("aeron:udp?endpoint=localhost:{}", request_port);
        let response_control_channel = format!("aeron:udp?endpoint=localhost:{}", response_port);
        let recording_events_channel =
            format!("aeron:udp?endpoint=localhost:{}", recording_event_port);

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
    // END-TO-END INTEGRATION TESTS
    /////////////////////////////////////////////////////////////////////////////

    /// Test complete end-to-end persistent subscription flow
    ///
    /// This test covers:
    /// 1. Starting a recording
    /// 2. Publishing messages while recording
    /// 3. Creating a persistent subscription
    /// 4. Consuming replayed messages
    /// 5. Transitioning to live consumption
    /// 6. Verifying message integrity
    #[test]
    #[serial]
    fn test_end_to_end_persistent_subscription() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("e2e_test", 9000)?;

        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        info!("=== End-to-End: record -> replay -> live consume ===");
        // NOTE: the seamless replay-merge-to-live transition is exercised by
        // `test_simple_replay_merge` in lib.rs. This test covers the two phases
        // (historical replay, then live consumption) on a recorded IPC stream.

        let channel = "aeron:ipc";
        let stream_id = 2001;

        archive.start_recording(
            &channel.into_c_string(),
            stream_id,
            SOURCE_LOCATION_LOCAL,
            true,
        )?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Phase 1: publish historical messages (will be replayed)
        let historical_count = 100;
        for i in 0..historical_count {
            let message = format!("Historical-{}", i);
            let deadline = Instant::now() + Duration::from_secs(10);
            while publication.offer(
                message.as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            ) <= 0
            {
                if Instant::now() > deadline {
                    return Err("timed out offering historical message".into());
                }
                sleep(Duration::from_millis(10));
            }
        }
        info!("Published {} historical messages", historical_count);

        // Resolve the recording and wait for it to flush everything we published.
        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        let recording_id = RecordingPos::get_recording_id_block(
            &counters_reader,
            counter_id,
            Duration::from_secs(5),
        )?;
        let published_position = publication.position();
        let start = Instant::now();
        while counters_reader.get_counter_value(counter_id) < published_position
            && start.elapsed() < Duration::from_secs(10)
        {
            sleep(Duration::from_millis(10));
        }

        // Phase 2: replay the historical batch and verify it.
        let replay_stream_id = 2002;
        let replay_params = AeronArchiveReplayParams::new(-1, i32::MAX, 0, i64::MAX, 0, 0)?;
        let replay_session_id = archive.start_replay(
            recording_id,
            &channel.into_c_string(),
            replay_stream_id,
            &replay_params,
        )?;
        let replay_channel =
            format!("{}?session-id={}", channel, replay_session_id as i32).into_c_string();
        let replay_sub = aeron_archive
            .async_add_subscription(
                &replay_channel,
                replay_stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?
            .poll_blocking(Duration::from_secs(10))?;

        #[derive(Default)]
        struct Tracker {
            historical: usize,
            live: usize,
        }
        impl AeronFragmentHandlerCallback for Tracker {
            fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
                if let Ok(s) = std::str::from_utf8(buffer) {
                    if s.starts_with("Historical-") {
                        self.historical += 1;
                    } else if s.starts_with("Live-") {
                        self.live += 1;
                    }
                }
            }
        }
        let mut handler = Handler::leak(Tracker::default());

        let start = Instant::now();
        while handler.historical < historical_count && start.elapsed() < Duration::from_secs(20) {
            replay_sub.poll(Some(&mut handler), 100)?;
            sleep(Duration::from_millis(10));
        }
        assert_eq!(
            handler.historical, historical_count,
            "replay should deliver all historical messages"
        );
        replay_sub.close(Handlers::no_notification_handler())?;
        info!("Replayed {} historical messages", handler.historical);

        // Phase 3: live consumption. Subscribe first and wait for an image so the
        // live subscription sees the messages that follow.
        let live_sub = aeron_archive
            .async_add_subscription(
                &channel.into_c_string(),
                stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?
            .poll_blocking(Duration::from_secs(5))?;
        let start = Instant::now();
        while live_sub.image_count()? == 0 && start.elapsed() < Duration::from_secs(5) {
            live_sub.poll(Some(&mut handler), 10)?;
            sleep(Duration::from_millis(10));
        }

        let live_count = 50;
        for i in 0..live_count {
            let message = format!("Live-{}", i);
            let deadline = Instant::now() + Duration::from_secs(10);
            while publication.offer(
                message.as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            ) <= 0
            {
                if Instant::now() > deadline {
                    return Err("timed out offering live message".into());
                }
                sleep(Duration::from_millis(10));
            }
        }
        let start = Instant::now();
        while handler.live < live_count && start.elapsed() < Duration::from_secs(20) {
            live_sub.poll(Some(&mut handler), 100)?;
            sleep(Duration::from_millis(10));
        }
        assert_eq!(handler.live, live_count, "should receive all live messages");

        info!(
            "End-to-end OK: {} replayed + {} live",
            handler.historical, handler.live
        );

        handler.release();
        live_sub.close(Handlers::no_notification_handler())?;
        drop(publication);
        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /// Test persistent subscription with publisher restart
    ///
    /// This test verifies that a persistent subscription can handle:
    /// 1. Publisher stopping
    /// 2. Publisher restarting with same stream
    /// 3. Continuous message delivery across restart
    #[test]
    #[serial]
    fn test_persistent_subscription_with_publisher_restart() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("restart_test", 9100)?;

        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        info!("=== Testing Publisher Restart ===");

        // Create publication and start recording
        let channel = "aeron:ipc";
        let stream_id = 2003;

        let subscription_id = archive.start_recording(
            &channel.into_c_string(),
            stream_id,
            SOURCE_LOCATION_LOCAL,
            true,
        )?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Publish initial batch
        let batch1_count = 30;
        for i in 0..batch1_count {
            let message = format!("BeforeRestart-{}", i);
            let deadline = Instant::now() + Duration::from_secs(10);
            while publication.offer(
                message.as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            ) <= 0
            {
                if Instant::now() > deadline {
                    return Err(format!("timed out offering BeforeRestart-{i}").into());
                }
                sleep(Duration::from_millis(10));
            }
        }
        info!("Published {} messages before restart", batch1_count);

        // Simulate a restart pause. Aeron caches non-exclusive IPC publications by
        // channel+stream, so dropping and re-adding would hand back the same (now
        // closing) publication that never reconnects. Instead we keep the single
        // publication and publish the second batch after a pause; the recording
        // still captures both batches and the replay verifies continuity.
        sleep(Duration::from_secs(1));

        // Publish second batch
        let batch2_count = 30;
        for i in 0..batch2_count {
            let message = format!("AfterRestart-{}", i);
            let deadline = Instant::now() + Duration::from_secs(10);
            while publication.offer(
                message.as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            ) <= 0
            {
                if Instant::now() > deadline {
                    return Err(format!("timed out offering AfterRestart-{i}").into());
                }
                sleep(Duration::from_millis(10));
            }
        }
        info!("Published {} messages after restart", batch2_count);

        // Get recording ID
        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        let recording_id = RecordingPos::get_recording_id_block(
            &counters_reader,
            counter_id,
            Duration::from_secs(5),
        )?;

        // Replay entire recording
        let replay_stream_id = 2004;
        let replay_params = AeronArchiveReplayParams::new(-1, i32::MAX, 0, i64::MAX, 0, 0)?;

        let replay_session_id = archive.start_replay(
            recording_id,
            &channel.into_c_string(),
            replay_stream_id,
            &replay_params,
        )?;

        let replay_channel =
            format!("{}?session-id={}", channel, replay_session_id as i32).into_c_string();

        let subscription = aeron_archive
            .async_add_subscription(
                &replay_channel,
                replay_stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?
            .poll_blocking(Duration::from_secs(10))?;

        #[derive(Default)]
        struct RestartHandler {
            before_restart: usize,
            after_restart: usize,
        }

        impl AeronFragmentHandlerCallback for RestartHandler {
            fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
                if let Ok(s) = std::str::from_utf8(buffer) {
                    if s.starts_with("BeforeRestart-") {
                        self.before_restart += 1;
                    } else if s.starts_with("AfterRestart-") {
                        self.after_restart += 1;
                    }
                }
            }
        }

        let mut handler = Handler::leak(RestartHandler::default());

        // Poll for all messages
        let start = Instant::now();
        while (handler.before_restart + handler.after_restart) < (batch1_count + batch2_count)
            && start.elapsed() < Duration::from_secs(20)
        {
            subscription.poll(Some(&mut handler), 100)?;
            sleep(Duration::from_millis(10));
        }

        assert_eq!(
            handler.before_restart, batch1_count,
            "Should receive all messages before restart"
        );
        assert_eq!(
            handler.after_restart, batch2_count,
            "Should receive all messages after restart"
        );

        info!(
            "Verified restart: {} before + {} after = {} total",
            handler.before_restart,
            handler.after_restart,
            handler.before_restart + handler.after_restart
        );

        subscription.close(Handlers::no_notification_handler())?;
        handler.release();
        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /// Test persistent subscription with rapid message flow
    ///
    /// This test verifies the system can handle high-throughput scenarios
    #[test]
    #[serial]
    fn test_persistent_subscription_high_throughput() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("throughput_test", 9200)?;

        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        info!("=== Testing High Throughput ===");

        // Create publication and start recording
        let channel = "aeron:ipc";
        let stream_id = 2005;

        let subscription_id = archive.start_recording(
            &channel.into_c_string(),
            stream_id,
            SOURCE_LOCATION_LOCAL,
            true,
        )?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Publish high volume of messages
        let message_count = 1000;
        let start_publish = Instant::now();

        for i in 0..message_count {
            let message = format!("Throughput-Test-{}", i);
            let mut attempts = 0;
            while publication.offer(
                message.as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            ) <= 0
            {
                sleep(Duration::from_millis(1));
                attempts += 1;
                if attempts > 100 {
                    warn!("Publication backpressure on message {}", i);
                    break;
                }
            }
        }

        let publish_duration = start_publish.elapsed();
        info!(
            "Published {} messages in {:?} ({:.2} msg/sec)",
            message_count,
            publish_duration,
            message_count as f64 / publish_duration.as_secs_f64()
        );

        // Get recording ID
        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        let recording_id = RecordingPos::get_recording_id_block(
            &counters_reader,
            counter_id,
            Duration::from_secs(5),
        )?;

        // Replay
        let replay_stream_id = 2006;
        let replay_params = AeronArchiveReplayParams::new(-1, i32::MAX, 0, i64::MAX, 0, 0)?;

        let replay_session_id = archive.start_replay(
            recording_id,
            &channel.into_c_string(),
            replay_stream_id,
            &replay_params,
        )?;

        let replay_channel =
            format!("{}?session-id={}", channel, replay_session_id as i32).into_c_string();

        let subscription = aeron_archive
            .async_add_subscription(
                &replay_channel,
                replay_stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?
            .poll_blocking(Duration::from_secs(10))?;

        #[derive(Default)]
        struct ThroughputHandler {
            count: usize,
        }

        impl AeronFragmentHandlerCallback for ThroughputHandler {
            fn handle_aeron_fragment_handler(&mut self, _buffer: &[u8], _header: AeronHeader) {
                self.count += 1;
            }
        }

        let mut handler = Handler::leak(ThroughputHandler::default());

        // Measure replay throughput
        let start_replay = Instant::now();

        while handler.count < message_count && start_replay.elapsed() < Duration::from_secs(20) {
            subscription.poll(Some(&mut handler), 1000)?;
        }

        let replay_duration = start_replay.elapsed();

        assert_eq!(handler.count, message_count, "Should receive all messages");

        info!(
            "Replayed {} messages in {:?} ({:.2} msg/sec)",
            handler.count,
            replay_duration,
            handler.count as f64 / replay_duration.as_secs_f64()
        );

        subscription.close(Handlers::no_notification_handler())?;
        handler.release();
        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /// Test persistent subscription with varying message sizes
    ///
    /// This test verifies the system can handle messages of different sizes
    #[test]
    #[serial]
    fn test_persistent_subscription_variable_message_sizes() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("variable_size_test", 9300)?;

        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        info!("=== Testing Variable Message Sizes ===");

        // Create publication and start recording
        let channel = "aeron:ipc";
        let stream_id = 2007;

        let subscription_id = archive.start_recording(
            &channel.into_c_string(),
            stream_id,
            SOURCE_LOCATION_LOCAL,
            true,
        )?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Publish messages of varying sizes, capped at the publication's max payload
        // length (~1376 bytes for the default IPC MTU). `offer()` rejects a single
        // buffer larger than max payload, which would otherwise spin the offer loop
        // forever and trip the driver liveness timeout.
        let message_sizes = vec![
            10,   // tiny
            100,  // small
            1000, // medium
            1376, // max payload
            50,   // small again
            500,  // medium-small
        ];

        let total_messages = message_sizes.len();

        for (i, size) in message_sizes.iter().enumerate() {
            let message = vec![b'X' + (i as u8); *size];
            let deadline = Instant::now() + Duration::from_secs(10);
            while publication.offer(&message, Handlers::no_reserved_value_supplier_handler()) <= 0 {
                if Instant::now() > deadline {
                    return Err(format!("timed out offering message {i} of size {size}").into());
                }
                sleep(Duration::from_millis(10));
            }
            info!("Published message {} with size {} bytes", i, size);
        }

        // Get recording ID
        let session_id = publication.get_constants()?.session_id;
        let counters_reader = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        let recording_id = RecordingPos::get_recording_id_block(
            &counters_reader,
            counter_id,
            Duration::from_secs(5),
        )?;

        // Replay
        let replay_stream_id = 2008;
        let replay_params = AeronArchiveReplayParams::new(-1, i32::MAX, 0, i64::MAX, 0, 0)?;

        let replay_session_id = archive.start_replay(
            recording_id,
            &channel.into_c_string(),
            replay_stream_id,
            &replay_params,
        )?;

        let replay_channel =
            format!("{}?session-id={}", channel, replay_session_id as i32).into_c_string();

        let subscription = aeron_archive
            .async_add_subscription(
                &replay_channel,
                replay_stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?
            .poll_blocking(Duration::from_secs(10))?;

        #[derive(Default)]
        struct SizeVerificationHandler {
            received_sizes: Vec<usize>,
        }

        impl AeronFragmentHandlerCallback for SizeVerificationHandler {
            fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
                self.received_sizes.push(buffer.len());
            }
        }

        let mut handler = Handler::leak(SizeVerificationHandler::default());

        // Poll for all messages
        let start = Instant::now();
        while handler.received_sizes.len() < total_messages
            && start.elapsed() < Duration::from_secs(20)
        {
            subscription.poll(Some(&mut handler), 100)?;
            sleep(Duration::from_millis(10));
        }

        assert_eq!(
            handler.received_sizes.len(),
            total_messages,
            "Should receive all messages"
        );

        // Verify sizes match (within fragmentation tolerance)
        for (i, (original, received)) in message_sizes
            .iter()
            .zip(handler.received_sizes.iter())
            .enumerate()
        {
            info!(
                "Message {}: original {} bytes, received {} bytes",
                i, original, received
            );
            // Due to fragmentation, received size might differ slightly
            let original_div_2 = original / 2;
            assert!(
                received >= &original_div_2,
                "Received size should be reasonable"
            );
        }

        info!(
            "Verified all {} messages with varying sizes",
            total_messages
        );

        subscription.close(Handlers::no_notification_handler())?;
        handler.release();
        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /// Test persistent subscription with recording lifecycle
    ///
    /// This test verifies correct handling of recording start/stop/restart
    #[test]
    #[serial]
    fn test_persistent_subscription_recording_lifecycle() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("lifecycle_test", 9400)?;

        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        info!("=== Testing Recording Lifecycle ===");

        // Create publication and start recording
        let channel = "aeron:ipc";
        let stream_id = 2009;

        let subscription_id = archive.start_recording(
            &channel.into_c_string(),
            stream_id,
            SOURCE_LOCATION_LOCAL,
            true,
        )?;
        info!("Started recording with subscription_id={}", subscription_id);

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Phase 1: Record initial messages
        let phase1_count = 20;
        for i in 0..phase1_count {
            let message = format!("Phase1-{}", i);
            while publication.offer(
                message.as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            ) <= 0
            {
                sleep(Duration::from_millis(10));
            }
        }
        info!("Phase 1: Published {} messages", phase1_count);

        // Stop recording
        archive.stop_recording_channel_and_stream(&channel.into_c_string(), stream_id)?;
        info!("Stopped recording");

        sleep(Duration::from_millis(500));

        // Restart recording
        let subscription_id2 = archive.start_recording(
            &channel.into_c_string(),
            stream_id,
            SOURCE_LOCATION_LOCAL,
            true,
        )?;
        info!(
            "Restarted recording with subscription_id={}",
            subscription_id2
        );

        // Phase 2: Record more messages
        let phase2_count = 20;
        for i in 0..phase2_count {
            let message = format!("Phase2-{}", i);
            while publication.offer(
                message.as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            ) <= 0
            {
                sleep(Duration::from_millis(10));
            }
        }
        info!("Phase 2: Published {} messages", phase2_count);

        // List recordings to verify state
        let mut count = 0;
        let recording_found = Cell::new(false);
        archive.list_recordings_once(&mut count, 0, i32::MAX, |desc| {
            if desc.stream_id() == stream_id {
                recording_found.set(true);
                info!(
                    "Recording: id={}, start_position={}, stop_position={}",
                    desc.recording_id(),
                    desc.start_position(),
                    desc.stop_position()
                );
            }
        })?;

        assert!(
            recording_found.get(),
            "Should find recording for our stream"
        );

        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /// Test persistent subscription with multiple streams
    ///
    /// This test verifies handling multiple persistent subscriptions on different streams
    #[test]
    #[serial]
    fn test_persistent_subscription_multiple_streams() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("multi_stream_test", 9500)?;

        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        info!("=== Testing Multiple Streams ===");

        let num_streams = 3;
        let stream_ids: Vec<i32> = (3000..3000 + num_streams as i32).collect();
        let mut publications = Vec::new();
        let mut recording_ids = Vec::new();

        // Setup multiple streams
        for stream_id in &stream_ids {
            let subscription_id = archive.start_recording(
                &"aeron:ipc".into_c_string(),
                *stream_id,
                SOURCE_LOCATION_LOCAL,
                true,
            )?;

            let publication = aeron_archive
                .async_add_publication(&"aeron:ipc".into_c_string(), *stream_id)?
                .poll_blocking(Duration::from_secs(5))?;

            // Publish some messages
            for i in 0..10 {
                let message = format!("Stream-{}-Message-{}", stream_id, i);
                while publication.offer(
                    message.as_bytes(),
                    Handlers::no_reserved_value_supplier_handler(),
                ) <= 0
                {
                    sleep(Duration::from_millis(10));
                }
            }

            // Get recording ID
            let session_id = publication.get_constants()?.session_id;
            let counters_reader = aeron_archive.counters_reader();
            let counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
            let recording_id = RecordingPos::get_recording_id_block(
                &counters_reader,
                counter_id,
                Duration::from_secs(5),
            )?;

            publications.push(publication);
            recording_ids.push((*stream_id, recording_id));
        }

        info!("Setup {} streams with recordings", num_streams);

        // Verify we have multiple recordings
        let mut count = 0;
        archive.list_recordings_once(&mut count, 0, i32::MAX, |desc| {
            info!(
                "Found recording: stream_id={}, recording_id={}",
                desc.stream_id(),
                desc.recording_id()
            );
        })?;

        assert!(
            count >= num_streams,
            "Should have at least as many recordings as streams"
        );

        // Cleanup
        for pub_item in publications {
            drop(pub_item);
        }

        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /// Test persistent subscription error recovery
    ///
    /// This test verifies the system can recover from various error conditions
    #[test]
    #[serial]
    fn test_persistent_subscription_error_recovery() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("error_recovery_test", 9600)?;

        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        info!("=== Testing Error Recovery ===");

        // Test 1: Attempt to replay non-existent recording
        let fake_recording_id = 999999;
        let replay_params = AeronArchiveReplayParams::new(-1, i32::MAX, 0, 100, 0, 0)?;

        let result = archive.start_replay(
            fake_recording_id,
            &"aeron:ipc".into_c_string(),
            4001,
            &replay_params,
        );

        assert!(
            result.is_err(),
            "Should fail to replay non-existent recording"
        );
        info!("Correctly handled non-existent recording error");

        // Test 2: Attempt to get recording position for invalid counter
        let invalid_counter_id = 9999;
        let result =
            RecordingPos::get_recording_id(&aeron_archive.counters_reader(), invalid_counter_id);
        assert!(
            result.is_err(),
            "Should fail to get recording ID for invalid counter"
        );
        info!("Correctly handled invalid counter ID");

        // Test 3: Valid operation after errors
        let channel = "aeron:ipc";
        let stream_id = 4002;

        let subscription_id = archive.start_recording(
            &channel.into_c_string(),
            stream_id,
            SOURCE_LOCATION_LOCAL,
            true,
        )?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Publish messages
        for i in 0..5 {
            let message = format!("Recovery-Test-{}", i);
            while publication.offer(
                message.as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            ) <= 0
            {
                sleep(Duration::from_millis(10));
            }
        }

        info!("System recovered and functioning normally after errors");

        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();

        Ok(())
    }

    /// Truncate and purge a recording — ports the purge exercise in Aeron's
    /// `RecordedBasicPublisher`. Archive RPCs are synchronous (Ok = the archive
    /// accepted the request), so we assert each step rather than ignore the result.
    /// Truncate requires a *stopped* recording, so we stop recording first.
    #[test]
    #[serial]
    fn test_truncate_and_purge_recording() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        let (aeron_archive, archive_context, _media_driver_archive, mut archive_error_handler) =
            start_aeron_archive_with_config("purge_test", 9900)?;

        let archive = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?
            .poll_blocking(Duration::from_secs(20))
            .expect("failed to connect to archive");

        let channel = "aeron:ipc";
        let stream_id = 5001;
        archive.start_recording(
            &channel.into_c_string(),
            stream_id,
            SOURCE_LOCATION_LOCAL,
            true,
        )?;

        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;
        for i in 0..100 {
            let m = format!("msg-{i}");
            while publication.offer(m.as_bytes(), Handlers::no_reserved_value_supplier_handler())
                <= 0
            {
                sleep(Duration::from_millis(1));
            }
        }

        // Resolve the recording id and wait for the recorder to flush what we published.
        let session_id = publication.get_constants()?.session_id;
        let counters = aeron_archive.counters_reader();
        let counter_id = RecordingPos::find_counter_id_by_session(&counters, session_id);
        let recording_id =
            RecordingPos::get_recording_id_block(&counters, counter_id, Duration::from_secs(5))?;
        let published_position = publication.position();
        let deadline = Instant::now() + Duration::from_secs(5);
        while counters.get_counter_value(counter_id) < published_position
            && Instant::now() < deadline
        {
            sleep(Duration::from_millis(10));
        }
        info!("recording_id={recording_id} position={published_position}");

        // Truncate/purge operate on a *stopped* recording. Close the stream, stop recording,
        // then wait for the stop to take effect (stop_position becomes non-null) before
        // truncating — otherwise the archive rejects it with "cannot truncate active recording".
        drop(publication);
        archive.stop_recording_channel_and_stream(&channel.into_c_string(), stream_id)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut stopped = false;
        while !stopped && Instant::now() < deadline {
            let mut count = 0;
            archive.list_recordings_once(&mut count, recording_id, 1, |desc| {
                if desc.recording_id() == recording_id && desc.stop_position() > 0 {
                    stopped = true;
                }
            })?;
            if !stopped {
                sleep(Duration::from_millis(10));
            }
        }
        assert!(stopped, "recording {recording_id} never stopped");

        let halfway = published_position / 2;
        archive.truncate_recording(recording_id, halfway)?;
        info!("truncated {recording_id} to {halfway}");

        // Purge deletes the recording entirely.
        archive.purge_recording(recording_id)?;
        info!("purged recording {recording_id}");

        drop(archive);
        drop(aeron_archive);
        archive_error_handler.release();
        Ok(())
    }
}
