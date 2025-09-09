#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![doc = include_str!("../README.md")]
//! # Features
//!
//! - **`static`**: When enabled, this feature statically links the Aeron C code.
//!   By default, the library uses dynamic linking to the Aeron C libraries.
//! - **`backtrace`** - When enabled will log a backtrace for each AeronCError
//! - **`extra-logging`** - When enabled will log when resource is created and destroyed. useful if your seeing a segfault due to a resource being closed
//! - **`precompile`** - When enabled will use precompiled c code instead of requiring cmake and java to me installed

pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use bindings::*;

include!(concat!(env!("OUT_DIR"), "/aeron.rs"));
include!(concat!(env!("OUT_DIR"), "/aeron_custom.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_alloc::current_allocs;
    use log::{error, info};
    use rusteron_media_driver::AeronDriverContext;
    use serial_test::serial;
    use std::error;
    use std::error::Error;
    use std::io::Write;
    use std::os::raw::c_int;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::{sleep, JoinHandle};
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

    #[test]
    #[serial]
    fn version_check() -> Result<(), Box<dyn error::Error>> {
        unsafe {
            aeron_randomised_int32();
        }
        let alloc_count = current_allocs();

        {
            let major = unsafe { crate::aeron_version_major() };
            let minor = unsafe { crate::aeron_version_minor() };
            let patch = unsafe { crate::aeron_version_patch() };

            let cargo_version = "1.48.6";
            let aeron_version = format!("{}.{}.{}", major, minor, patch);
            assert_eq!(aeron_version, cargo_version);

            let ctx = AeronContext::new()?;
            let error_count = 1;
            let mut handler = Handler::leak(ErrorCount::default());
            ctx.set_error_handler(Some(&handler))?;

            assert!(Aeron::epoch_clock() > 0);
            drop(ctx);
            assert!(handler.should_drop);
            handler.release();
            assert!(!handler.should_drop);
            drop(handler);
        }

        assert!(
            current_allocs() <= alloc_count,
            "allocations {} > {alloc_count}",
            current_allocs()
        );

        Ok(())
    }

    #[test]
    #[serial]
    pub fn simple_large_send() -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();
        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        assert_eq!(media_driver_ctx.get_dir(), ctx.get_dir());
        let error_count = 1;
        ctx.set_error_handler(Some(&Handler::leak(ErrorCount::default())))?;
        ctx.set_on_new_publication(Some(&Handler::leak(AeronNewPublicationLogger)))?;
        ctx.set_on_available_counter(Some(&Handler::leak(AeronAvailableCounterLogger)))?;
        ctx.set_on_close_client(Some(&Handler::leak(AeronCloseClientLogger)))?;
        ctx.set_on_new_subscription(Some(&Handler::leak(AeronNewSubscriptionLogger)))?;
        ctx.set_on_unavailable_counter(Some(&Handler::leak(AeronUnavailableCounterLogger)))?;
        ctx.set_on_available_counter(Some(&Handler::leak(AeronAvailableCounterLogger)))?;
        ctx.set_on_new_exclusive_publication(Some(&Handler::leak(AeronNewPublicationLogger)))?;

        info!("creating client [simple_large_send test]");
        let aeron = Aeron::new(&ctx)?;
        info!("starting client");

        aeron.start()?;
        info!("client started");
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
        info!("created publisher");

        assert!(AeronCncMetadata::load_from_file(ctx.get_dir())?.pid > 0);
        let cstr = std::ffi::CString::new(ctx.get_dir()).unwrap();
        AeronCncMetadata::read_from_file(&cstr, |cnc| {
            assert!(cnc.pid > 0);
        })?;
        assert!(AeronCnc::new_on_heap(ctx.get_dir())?.get_to_driver_heartbeat_ms()? > 0);
        let cstr = std::ffi::CString::new(ctx.get_dir()).unwrap();
        for _ in 0..50 {
            AeronCnc::read_on_partial_stack(&cstr, |cnc| {
                assert!(cnc.get_to_driver_heartbeat_ms().unwrap() > 0);
            })?;
        }

        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;
        info!("created subscription");

        subscription
            .poll_once(|msg, header| println!("foo"), 1024)
            .unwrap();

        // pick a large enough size to confirm fragement assembler is working
        let string_len = media_driver_ctx.ipc_mtu_length * 100;
        info!("string length: {}", string_len);

        let publisher_handler = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let binding = "1".repeat(string_len);
                let large_msg = binding.as_bytes();
                loop {
                    if stop.load(Ordering::Acquire) || publisher.is_closed() {
                        break;
                    }
                    let result =
                        publisher.offer(large_msg, Handlers::no_reserved_value_supplier_handler());

                    assert_eq!(123, publisher.get_constants().unwrap().stream_id);

                    if result < large_msg.len() as i64 {
                        let error = AeronCError::from_code(result as i32);
                        match error.kind() {
                            AeronErrorType::PublicationBackPressured
                            | AeronErrorType::PublicationAdminAction => {
                                // ignore
                            }
                            _ => {
                                error!(
                                    "ERROR: failed to send message {:?}",
                                    AeronCError::from_code(result as i32)
                                );
                            }
                        }
                        sleep(Duration::from_millis(500));
                    }
                }
                info!("stopping publisher thread");
            })
        };

        let mut assembler = AeronFragmentClosureAssembler::new()?;

        struct Context {
            count: Arc<AtomicUsize>,
            stop: Arc<AtomicBool>,
            string_len: usize,
        }

        let count = Arc::new(AtomicUsize::new(0usize));
        let mut context = Context {
            count: count.clone(),
            stop: stop.clone(),
            string_len,
        };

        // Start the timer
        let start_time = Instant::now();

        loop {
            if start_time.elapsed() > Duration::from_secs(30) {
                info!("Failed: exceeded 30-second timeout");
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Timeout exceeded",
                )));
            }
            let c = count.load(Ordering::SeqCst);
            if c > 100 {
                break;
            }

            fn process_msg(ctx: &mut Context, buffer: &[u8], header: AeronHeader) {
                ctx.count.fetch_add(1, Ordering::SeqCst);

                let values = header.get_values().unwrap();
                assert_ne!(values.frame.session_id, 0);

                if buffer.len() != ctx.string_len {
                    ctx.stop.store(true, Ordering::SeqCst);
                    error!(
                        "ERROR: message was {} but was expecting {} [header={:?}]",
                        buffer.len(),
                        ctx.string_len,
                        header
                    );
                    sleep(Duration::from_secs(1));
                }

                assert_eq!(buffer.len(), ctx.string_len);
                assert_eq!(buffer, "1".repeat(ctx.string_len).as_bytes());
            }

            subscription.poll(assembler.process(&mut context, process_msg), 128)?;
            assert_eq!(123, subscription.get_constants().unwrap().stream_id);
        }

        subscription.close(Handlers::no_notification_handler())?;

        info!("stopping client");
        stop.store(true, Ordering::SeqCst);

        let _ = publisher_handler.join().unwrap();
        let _ = driver_handle.join().unwrap();

        let cnc = AeronCnc::new_on_heap(ctx.get_dir())?;
        cnc.counters_reader().foreach_counter_once(
            |value: i64, id: i32, type_id: i32, key: &[u8], label: &str| {
                println!("counter reader id={id}, type_id={type_id}, key={key:?}, label={label}, value={value} [type={:?}]",
                AeronSystemCounterType::try_from(type_id));
            },
        );
        cnc.error_log_read_once(| observation_count: i32,
                                     first_observation_timestamp: i64,
                                     last_observation_timestamp: i64,
                                     error: &str| {
            println!("error: {error} observationCount={observation_count}, first_observation_timestamp={first_observation_timestamp}, last_observation_timestamp={last_observation_timestamp}");
        }, 0);
        cnc.loss_reporter_read_once(|    observation_count: i64,
                                    total_bytes_lost: i64,
                                    first_observation_timestamp: i64,
                                    last_observation_timestamp: i64,
                                    session_id: i32,
                                    stream_id: i32,
                                    channel: &str,
                                    source: &str,| {
            println!("loss reporter observationCount={observation_count}, totalBytesLost={total_bytes_lost}, first_observed={first_observation_timestamp}, last_observed={last_observation_timestamp}, session_id={session_id}, stream_id={stream_id}, channel={channel} source={source}");
        })?;

        Ok(())
    }

    #[test]
    #[serial]
    pub fn try_claim() -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();
        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        assert_eq!(media_driver_ctx.get_dir(), ctx.get_dir());
        ctx.set_error_handler(Some(&Handler::leak(ErrorCount::default())))?;

        info!("creating client [try_claim test]");
        let aeron = Aeron::new(&ctx)?;
        info!("starting client");

        aeron.start()?;
        info!("client started");
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
        info!("created publisher");

        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;
        info!("created subscription");

        // pick a large enough size to confirm fragement assembler is working
        let string_len = 156;
        info!("string length: {}", string_len);

        let publisher_handler = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let binding = "1".repeat(string_len);
                let msg = binding.as_bytes();
                let buffer = AeronBufferClaim::default();
                loop {
                    if stop.load(Ordering::Acquire) || publisher.is_closed() {
                        break;
                    }

                    let result = publisher.try_claim(string_len, &buffer);

                    if result < msg.len() as i64 {
                        error!(
                            "ERROR: failed to send message {:?}",
                            AeronCError::from_code(result as i32)
                        );
                    } else {
                        buffer.data().write_all(&msg).unwrap();
                        buffer.commit().unwrap();
                    }
                }
                info!("stopping publisher thread");
            })
        };

        let count = Arc::new(AtomicUsize::new(0usize));
        let count_copy = Arc::clone(&count);
        let stop2 = stop.clone();

        struct FragmentHandler {
            count_copy: Arc<AtomicUsize>,
            stop2: Arc<AtomicBool>,
            string_len: usize,
        }

        impl AeronFragmentHandlerCallback for FragmentHandler {
            fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
                self.count_copy.fetch_add(1, Ordering::SeqCst);

                if buffer.len() != self.string_len {
                    self.stop2.store(true, Ordering::SeqCst);
                    error!(
                        "ERROR: message was {} but was expecting {} [header={:?}]",
                        buffer.len(),
                        self.string_len,
                        header
                    );
                    sleep(Duration::from_secs(1));
                }

                assert_eq!(buffer.len(), self.string_len);
                assert_eq!(buffer, "1".repeat(self.string_len).as_bytes());
            }
        }

        let (closure, _inner) = Handler::leak_with_fragment_assembler(FragmentHandler {
            count_copy,
            stop2,
            string_len,
        })?;
        let start_time = Instant::now();

        loop {
            if start_time.elapsed() > Duration::from_secs(30) {
                info!("Failed: exceeded 30-second timeout");
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Timeout exceeded",
                )));
            }
            let c = count.load(Ordering::SeqCst);
            if c > 100 {
                break;
            }
            subscription.poll(Some(&closure), 128)?;
        }

        info!("stopping client");

        stop.store(true, Ordering::SeqCst);

        let _ = publisher_handler.join().unwrap();
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    pub fn counters() -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();
        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        assert_eq!(media_driver_ctx.get_dir(), ctx.get_dir());
        ctx.set_error_handler(Some(&Handler::leak(ErrorCount::default())))?;
        ctx.set_on_unavailable_counter(Some(&Handler::leak(AeronUnavailableCounterLogger)))?;

        struct AvailableCounterHandler {
            found_counter: bool,
        }

        impl AeronAvailableCounterCallback for AvailableCounterHandler {
            fn handle_aeron_on_available_counter(
                &mut self,
                counters_reader: AeronCountersReader,
                registration_id: i64,
                counter_id: i32,
            ) -> () {
                info!(
            "on counter key={:?}, label={:?} registration_id={registration_id}, counter_id={counter_id}, value={}, {counters_reader:?}",
            String::from_utf8(counters_reader.get_counter_key(counter_id).unwrap()),
            counters_reader.get_counter_label(counter_id, 1000),
            counters_reader.addr(counter_id)
        );

                assert_eq!(
                    counters_reader.counter_registration_id(counter_id).unwrap(),
                    registration_id
                );

                if let Ok(label) = counters_reader.get_counter_label(counter_id, 1000) {
                    if label == "label_buffer" {
                        self.found_counter = true;
                        assert_eq!(
                            &counters_reader.get_counter_key(counter_id).unwrap(),
                            "key".as_bytes()
                        );
                    }
                }
            }
        }

        let handler = &Handler::leak(AvailableCounterHandler {
            found_counter: false,
        });
        ctx.set_on_available_counter(Some(handler))?;

        info!("creating client");
        let aeron = Aeron::new(&ctx)?;
        info!("starting client");

        aeron.start()?;
        info!("client started [counters test]");

        let counter = aeron.add_counter(
            123,
            "key".as_bytes(),
            "label_buffer",
            Duration::from_secs(5),
        )?;
        let constants = counter.get_constants()?;
        let counter_id = constants.counter_id;

        let publisher_handler = {
            let stop = stop.clone();
            let counter = counter.clone();
            std::thread::spawn(move || {
                for _ in 0..150 {
                    if stop.load(Ordering::Acquire) || counter.is_closed() {
                        break;
                    }
                    counter.addr_atomic().fetch_add(1, Ordering::SeqCst);
                }
                info!("stopping publisher thread");
            })
        };

        let now = Instant::now();
        while counter.addr_atomic().load(Ordering::SeqCst) < 100
            && now.elapsed() < Duration::from_secs(10)
        {
            sleep(Duration::from_micros(10));
        }

        assert!(now.elapsed() < Duration::from_secs(10));

        info!(
            "counter is {}",
            counter.addr_atomic().load(Ordering::SeqCst)
        );

        info!("stopping client");

        #[cfg(not(target_os = "windows"))] // not sure why windows version doesn't fire event
        assert!(handler.found_counter);

        stop.store(true, Ordering::SeqCst);

        let reader = aeron.counters_reader();
        assert_eq!(reader.get_counter_label(counter_id, 256)?, "label_buffer");
        assert_eq!(reader.get_counter_key(counter_id)?, "key".as_bytes());
        let buffers = AeronCountersReaderBuffers::default();
        reader.get_buffers(&buffers)?;

        let _ = publisher_handler.join().unwrap();
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    /// A simple error counter for testing error callback invocation.
    #[derive(Default, Debug)]
    struct TestErrorCount {
        pub error_count: usize,
    }

    impl Drop for TestErrorCount {
        fn drop(&mut self) {
            info!("TestErrorCount dropped with {} errors", self.error_count);
        }
    }

    impl AeronErrorHandlerCallback for TestErrorCount {
        fn handle_aeron_error_handler(&mut self, error_code: c_int, msg: &str) {
            error!("Aeron error {}: {}", error_code, msg);
            self.error_count += 1;
        }
    }

    #[test]
    #[serial]
    pub fn backpressure_recovery_test() -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        ctx.set_error_handler(Some(&Handler::leak(TestErrorCount::default())))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;

        let count = Arc::new(AtomicUsize::new(0));
        let start_time = Instant::now();

        // Spawn a publisher thread that repeatedly sends "test" messages.
        let publisher_thread = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    let msg = b"test";
                    let result =
                        publisher.offer(msg, Handlers::no_reserved_value_supplier_handler());
                    // If backpressure is encountered, sleep a bit.
                    if result == AeronErrorType::PublicationBackPressured.code() as i64 {
                        sleep(Duration::from_millis(50));
                    }
                }
            })
        };

        // Poll using the inline closure via poll_once until we receive at least 50 messages.
        while count.load(Ordering::SeqCst) < 50 && start_time.elapsed() < Duration::from_secs(10) {
            let _ = subscription.poll_once(
                |_msg, _header| {
                    count.fetch_add(1, Ordering::SeqCst);
                },
                128,
            )?;
        }

        stop.store(true, Ordering::SeqCst);
        publisher_thread.join().unwrap();
        let _ = driver_handle.join().unwrap();

        assert!(
            count.load(Ordering::SeqCst) >= 50,
            "Expected at least 50 messages received"
        );
        Ok(())
    }

    #[test]
    #[serial]
    pub fn multi_subscription_test() -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        ctx.set_error_handler(Some(&Handler::leak(TestErrorCount::default())))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;

        // Create two subscriptions on the same channel.
        let subscription1 = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;
        let subscription2 = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;

        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        // Publish a single message.
        let msg = b"hello multi-subscription";
        let result = publisher.offer(msg, Handlers::no_reserved_value_supplier_handler());
        assert!(
            result >= msg.len() as i64,
            "Message should be sent successfully"
        );

        let start_time = Instant::now();
        // Poll both subscriptions with inline closures until each has received at least one message.
        while (count1.load(Ordering::SeqCst) < 1 || count2.load(Ordering::SeqCst) < 1)
            && start_time.elapsed() < Duration::from_secs(5)
        {
            let _ = subscription1.poll_once(
                |_msg, _header| {
                    count1.fetch_add(1, Ordering::SeqCst);
                },
                128,
            )?;
            let _ = subscription2.poll_once(
                |_msg, _header| {
                    count2.fetch_add(1, Ordering::SeqCst);
                },
                128,
            )?;
        }

        assert!(
            count1.load(Ordering::SeqCst) >= 1,
            "Subscription 1 did not receive the message"
        );
        assert!(
            count2.load(Ordering::SeqCst) >= 1,
            "Subscription 2 did not receive the message"
        );

        _stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    pub fn should_be_able_to_drop_after_close_manually_being_closed(
    ) -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        ctx.set_error_handler(Some(&Handler::leak(AeronErrorHandlerLogger)))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        {
            let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
            info!("created publication [sessionId={}]", publisher.session_id());
            publisher.close_with_no_args()?;
            drop(publisher);
        }

        {
            let publisher = aeron.add_publication(AERON_IPC_STREAM, 124, Duration::from_secs(5))?;
            info!("created publication [sessionId={}]", publisher.session_id());
            publisher.close(Handlers::no_notification_handler())?;
            drop(publisher);
        }

        {
            let publisher = aeron.add_publication(AERON_IPC_STREAM, 125, Duration::from_secs(5))?;
            publisher.close_once(|| println!("on close"))?;
            info!("created publication [sessionId={}]", publisher.session_id());
            drop(publisher);
        }

        Ok(())
    }

    #[test]
    #[serial]
    pub fn offer_on_closed_publication_error_test() -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        ctx.set_error_handler(Some(&Handler::leak(TestErrorCount::default())))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;

        // Close the publication immediately.
        publisher.close(Handlers::no_notification_handler())?;

        // Attempt to send a message after the publication is closed.
        let result = publisher.offer(
            b"should fail",
            Handlers::no_reserved_value_supplier_handler(),
        );
        assert!(
            result < 0,
            "Offering on a closed publication should return a negative error code"
        );

        _stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    /// Test sending and receiving an empty (zero-length) message using inline closures with poll_once.
    #[test]
    #[serial]
    pub fn empty_message_test() -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        ctx.set_error_handler(Some(&Handler::leak(TestErrorCount::default())))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;

        let empty_received = Arc::new(AtomicBool::new(false));
        let start_time = Instant::now();

        let result = publisher.offer(b"", Handlers::no_reserved_value_supplier_handler());
        assert!(result > 0);

        while !empty_received.load(Ordering::SeqCst)
            && start_time.elapsed() < Duration::from_secs(5)
        {
            let _ = subscription.poll_once(
                |msg, _header| {
                    if msg.is_empty() {
                        empty_received.store(true, Ordering::SeqCst);
                    }
                },
                128,
            )?;
        }

        assert!(
            empty_received.load(Ordering::SeqCst),
            "Empty message was not received"
        );
        _stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    #[ignore] // need to work to get tags working properly, its more of testing issue then tag issue
    pub fn tags() -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Debug)
            .try_init();

        let (md_ctx, stop, md) = start_media_driver(1)?;

        let (_a_ctx2, aeron_sub) = create_client(&md_ctx)?;

        info!("creating suscriber 1");
        let sub = aeron_sub
            .add_subscription(
                &"aeron:udp?tags=100".into_c_string(),
                123,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
                Duration::from_secs(50),
            )
            .map_err(|e| {
                error!("aeron error={}", Aeron::errmsg());
                e
            })?;

        let ctx = AeronContext::new()?;
        ctx.set_dir(&aeron_sub.context().get_dir().into_c_string())?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        info!("creating suscriber 2");
        let sub2 = aeron_sub.add_subscription(
            &"aeron:udp?tags=100".into_c_string(),
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(50),
        )?;

        let (_a_ctx1, aeron_pub) = create_client(&md_ctx)?;
        info!("creating publisher");
        assert!(!aeron_pub.is_closed());
        let publisher = aeron_pub
            .add_publication(
                &"aeron:udp?endpoint=localhost:4040|alias=test|tags=100".into_c_string(),
                123,
                Duration::from_secs(5),
            )
            .map_err(|e| {
                error!("aeron error={}", Aeron::errmsg());
                e
            })?;

        info!("publishing msg");

        loop {
            let result = publisher.offer(
                "213".as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            );
            if result < 0 {
                error!(
                    "failed to publish {:?}",
                    AeronCError::from_code(result as i32)
                );
            } else {
                break;
            }
        }

        sub.poll_once(
            |msg, _header| {
                println!("Received message: {:?}", msg);
            },
            128,
        )?;
        sub2.poll_once(
            |msg, _header| {
                println!("Received message: {:?}", msg);
            },
            128,
        )?;

        stop.store(true, Ordering::SeqCst);

        Ok(())
    }

    fn create_client(
        media_driver_ctx: &AeronDriverContext,
    ) -> Result<(AeronContext, Aeron), Box<dyn Error>> {
        let dir = media_driver_ctx.get_dir();
        info!("creating aeron client [dir={}]", dir);
        let ctx = AeronContext::new()?;
        ctx.set_dir(&dir.into_c_string())?;
        ctx.set_error_handler(Some(&Handler::leak(TestErrorCount::default())))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        Ok((ctx, aeron))
    }

    fn start_media_driver(
        instance: u64,
    ) -> Result<
        (
            AeronDriverContext,
            Arc<AtomicBool>,
            JoinHandle<Result<(), rusteron_media_driver::AeronCError>>,
        ),
        Box<dyn Error>,
    > {
        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!(
                "{}{}-{}",
                media_driver_ctx.get_dir(),
                Aeron::epoch_clock(),
                instance
            )
            .into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);
        Ok((media_driver_ctx, stop, driver_handle))
    }

    #[doc = include_str!("../../README.md")]
    mod readme_tests {}
}
