#[cfg(test)]
mod tests {
    //! Slow Consumer Replay Tests
    //!
    //! These tests are marked with `#[ignore]` and will not run during normal `cargo test`.
    //!
    //! To run these tests explicitly:
    //! ```bash
    //! # Run all ignored tests
    //! cargo test --package rusteron-archive --lib --features "precompile static" -- --ignored
    //!
    //! # Run a specific slow consumer test
    //! cargo test --package rusteron-archive --lib --features "precompile static" test_slow_consumer_replay_unicast_local_max -- --ignored
    //!
    //! # Run all tests including ignored ones
    //! cargo test --package rusteron-archive --lib --features "precompile static" -- --include-ignored
    //! ```

    use super::super::*;
    use crate::testing::EmbeddedArchiveMediaDriverProcess;
    use log::{error, info};
    use serial_test::serial;
    use std::error::Error;
    use std::os::raw::c_int;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread::{self, sleep};
    use std::time::{Duration, Instant};

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

    fn start_aeron_archive_with_config(
        aeron_dir_suffix: &str,
        start_port: u16,
    ) -> Result<
        (
            Aeron,
            AeronArchiveContext,
            EmbeddedArchiveMediaDriverProcess,
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
        aeron_context.set_error_handler(Some(&error_handler))?;

        let aeron = Aeron::new(&aeron_context)?;
        aeron.start()?;

        let archive_context = AeronArchiveContext::new_with_no_credentials_supplier(
            &aeron,
            &request_control_channel,
            &response_control_channel,
            &recording_events_channel,
        )?;
        archive_context.set_error_handler(Some(&error_handler))?;

        Ok((aeron, archive_context, archive_media_driver))
    }

    fn find_unused_udp_port(start_port: u16) -> Option<u16> {
        for port in start_port..65535 {
            if std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok() {
                return Some(port);
            }
        }
        None
    }

    fn run_slow_consumer_test(
        recording_channel_params: &str,
        source_location: SourceLocation,
        replay_length: i64,
    ) -> Result<(), Box<dyn Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();

        // 1. Start Archive/Publisher Driver
        let (aeron_archive, archive_context, _media_driver_archive) =
            start_aeron_archive_with_config("archive", 8000)?;

        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron_archive)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        // 2. Start Subscriber Driver
        let (aeron_subscriber, _subscriber_archive_context, _media_driver_subscriber) =
            start_aeron_archive_with_config("subscriber", 9000)?;

        let recording_port = find_unused_udp_port(20121).unwrap();
        let control_port = find_unused_udp_port(recording_port + 1).unwrap();

        let channel = if recording_channel_params.contains("control-mode=dynamic") {
            format!(
                "aeron:udp?endpoint=localhost:{}|control=localhost:{}|term-length=65536{}",
                recording_port, control_port, recording_channel_params
            )
        } else {
            format!(
                "aeron:udp?endpoint=localhost:{}|term-length=65536{}",
                recording_port, recording_channel_params
            )
        };
        let stream_id = 1001;

        // Start recording on Archive Driver
        let subscription_id = archive.start_recording(
            &channel.clone().into_c_string(),
            stream_id,
            source_location,
            true,
        )?;
        info!("Started recording subscription_id={}", subscription_id);

        // Create publication on Archive Driver
        let publication = aeron_archive
            .async_add_publication(&channel.into_c_string(), stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        // Wait for publication to be connected (recording started)
        let start = Instant::now();
        while !publication.is_connected() && start.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(10));
        }
        assert!(publication.is_connected());

        // Find recording id
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
        info!("Recording ID: {}", recording_id);

        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let publication_clone = publication.clone();

        // Publisher thread
        let publisher_thread = thread::spawn(move || {
            let mut seq = 0u64;
            while running_clone.load(Ordering::Acquire) {
                let message = seq.to_le_bytes();
                while publication_clone
                    .offer(&message, Handlers::no_reserved_value_supplier_handler())
                    <= 0
                {
                    if !running_clone.load(Ordering::Acquire) {
                        break;
                    }
                    thread::yield_now();
                }
                seq += 1;

                if seq % 1_000_000 == 0 {
                    info!("Published {} messages", seq);
                }
                // No sleep to flood the buffer
            }
            seq
        });

        // Wait a bit for some data to be recorded
        sleep(Duration::from_secs(2));

        // Start Replay with length = -1 (follow mode)
        // Use localhost:0 to let the OS pick a port, then resolve it
        let replay_stream_id = 1002;
        let replay_params = AeronArchiveReplayParams::new(0, i32::MAX, 0, replay_length, 0, 0)?;

        // 3. Create Subscription on Subscriber Driver with localhost:0
        let subscription_channel_template = "aeron:udp?endpoint=localhost:0";
        let subscription = aeron_subscriber
            .async_add_subscription(
                &subscription_channel_template.into_c_string(),
                replay_stream_id,
                Some(&Handler::leak(AeronAvailableImageLogger)),
                Some(&Handler::leak(AeronUnavailableImageLogger)),
            )?
            .poll_blocking(Duration::from_secs(5))?;

        // 4. Resolve the port
        let mut buffer = [0u8; 4096];
        let len = subscription
            .try_resolve_channel_endpoint_port(buffer.as_mut_ptr() as *mut i8, buffer.len())?;
        let resolved_channel = String::from_utf8_lossy(&buffer[..len as usize]).to_string();
        info!("Resolved subscription channel: {}", resolved_channel);

        // 5. Start Replay on Archive Driver pointing to the resolved Subscriber port
        let replay_session_id = archive.start_replay(
            recording_id,
            &resolved_channel.into_c_string(),
            replay_stream_id,
            &replay_params,
        )?;
        info!("Started replay session_id={}", replay_session_id);

        // Consumer loop
        let expected_seq = 0u64;
        let start_check = Instant::now();
        let test_duration = Duration::from_secs(60);

        struct FragmentHandler {
            expected_seq: u64,
            gaps: u64,
        }

        impl AeronFragmentHandlerCallback for FragmentHandler {
            fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
                let received_seq = u64::from_le_bytes(buffer.try_into().unwrap());
                if received_seq != self.expected_seq {
                    error!(
                        "Gap detected! Expected {} but got {}",
                        self.expected_seq, received_seq
                    );
                    self.gaps += 1;
                    self.expected_seq = received_seq + 1; // Reset expectation to next
                } else {
                    self.expected_seq += 1;
                }

                // Simulate slow processing
                sleep(Duration::from_millis(500));
            }
        }

        let handler = FragmentHandler {
            expected_seq: 0,
            gaps: 0,
        };
        let handler_box = Handler::leak(handler); // Leak to pass to C callback safely

        info!("Starting slow consumer loop");
        while start_check.elapsed() < test_duration {
            let _fragments = subscription.poll(Some(&handler_box), 1)?;

            if handler_box.gaps > 0 {
                break;
            }
        }

        running.store(false, Ordering::Release);
        let published_count = publisher_thread.join().unwrap();

        if handler_box.gaps > 0 {
            panic!("Test failed: Sequence gaps detected!");
        }

        assert!(
            published_count > 1_000_000,
            "Published count {} is not > 1,000,000",
            published_count
        );
        assert!(handler_box.expected_seq > 0, "No messages received");

        info!(
            "Test passed: No sequence gaps detected. Published: {}, Received: {}",
            published_count, handler_box.expected_seq
        );
        Ok(())
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_slow_consumer_replay_unicast_local_max() -> Result<(), Box<dyn Error>> {
        run_slow_consumer_test("", SOURCE_LOCATION_LOCAL, i64::MAX)
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_slow_consumer_replay_unicast_remote_neg1() -> Result<(), Box<dyn Error>> {
        run_slow_consumer_test("", SOURCE_LOCATION_REMOTE, -1)
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_slow_consumer_replay_gtag_local_max() -> Result<(), Box<dyn Error>> {
        run_slow_consumer_test(
            "|control-mode=dynamic|fc=tagged,g:123",
            SOURCE_LOCATION_LOCAL,
            i64::MAX,
        )
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_slow_consumer_replay_gtag_remote_neg1() -> Result<(), Box<dyn Error>> {
        run_slow_consumer_test(
            "|control-mode=dynamic|fc=tagged,g:123",
            SOURCE_LOCATION_REMOTE,
            -1,
        )
    }
}
