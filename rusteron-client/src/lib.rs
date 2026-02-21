#![allow(improper_ctypes_definitions)]
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
//! - **`backtrace`**: When enabled will log a backtrace for each AeronCError
//! - **`extra-logging`**: When enabled will log when resource is created and destroyed. Useful if you're seeing a segfault due to a resource being closed
//! - **`log-c-bindings`**: When enabled will log every C binding call with arguments and return values. Useful for debugging FFI interactions
//! - **`precompile`**: When enabled will use precompiled C code instead of requiring cmake and java to be installed

#[allow(improper_ctypes_definitions)]
#[allow(unpredictable_function_pointer_comparisons)]
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
    use hdrhistogram::Histogram;
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

            let cargo_version = "1.48.10";
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
    fn async_publication_invalid_interface_poll_then_drop() -> Result<(), Box<dyn error::Error>> {
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
        ctx.set_error_handler(Some(&Handler::leak(ErrorCount::default())))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:udp?endpoint=203.0.113.1:54321");

        // Create async publication and subscription pollers on the same invalid channel and
        // attempt to resolve them. If both are created, try a small send/receive cycle and then exit.
        let pub_poller = aeron.async_add_publication(&channel.clone().into_c_string(), 4321)?;
        let sub_poller = aeron.async_add_subscription(
            &channel.into_c_string(),
            4321,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?;

        let mut publication: Option<AeronPublication> = None;
        let mut subscription: Option<AeronSubscription> = None;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if publication.is_none() {
                match pub_poller.poll() {
                    Ok(Some(p)) => publication = Some(p),
                    Ok(None) | Err(_) => {}
                }
            }
            if subscription.is_none() {
                match sub_poller.poll() {
                    Ok(Some(s)) => subscription = Some(s),
                    Ok(None) | Err(_) => {}
                }
            }
            if publication.is_some() && subscription.is_some() {
                break;
            }
            #[cfg(debug_assertions)]
            std::thread::sleep(Duration::from_millis(10));
        }

        info!("publication: {:?}", publication);
        info!("subscription: {:?}", subscription);

        if let (Some(publisher), Some(subscription)) = (publication, subscription) {
            let payload = b"hello-aeron";
            let send_start = Instant::now();
            let mut sent = false;
            while send_start.elapsed() < Duration::from_millis(500) {
                let res = publisher.offer(payload, Handlers::no_reserved_value_supplier_handler());
                if res >= payload.len() as i64 {
                    sent = true;
                    info!("sent {:?}", payload);
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }

            if sent {
                let mut got = false;
                let read_start = Instant::now();
                while read_start.elapsed() < Duration::from_millis(500) {
                    let _ = subscription.poll_once(
                        |msg, _hdr| {
                            if msg == payload {
                                got = true;
                                info!("received {:?}", payload);
                            }
                        },
                        1024,
                    );
                    if got {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                // We don't assert on got, just exercise the path.
            }
        }

        // Shutdown
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    fn async_pub_sub_invalid_endpoint_create_drop_stress() -> Result<(), Box<dyn error::Error>> {
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
        ctx.set_error_handler(Some(&Handler::leak(ErrorCount::default())))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        // Stress: repeatedly create and drop pub/sub async pollers on an invalid endpoint.
        for i in 0..60u16 {
            let port = 55000u16 + i;
            let channel = format!("aeron:udp?endpoint=203.0.113.1:{}", port);
            let pub_poller =
                aeron.async_add_publication(&channel.clone().into_c_string(), 4500 + i as i32)?;
            let sub_poller = aeron.async_add_subscription(
                &channel.into_c_string(),
                4500 + i as i32,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?;

            let start = Instant::now();
            while start.elapsed() < Duration::from_millis(50) {
                let _ = pub_poller.poll();
                let _ = sub_poller.poll();
            }

            if i % 2 == 0 {
                drop(sub_poller);
                drop(pub_poller);
            } else {
                drop(pub_poller);
                drop(sub_poller);
            }
        }

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    // // #[ignore] // TODO FIXME broken test
    fn async_subscription_invalid_interface_poll_then_drop() -> Result<(), Box<dyn error::Error>> {
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
        ctx.set_error_handler(Some(&Handler::leak(ErrorCount::default())))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        // Invalid remote endpoint only (no interface)
        let channel = String::from("aeron:udp?endpoint=203.0.113.1:54323");

        let poller = aeron.async_add_subscription(
            &channel.into_c_string(),
            4323,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?;

        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(250) {
            let _ = poller.poll();
            #[cfg(debug_assertions)]
            std::thread::sleep(Duration::from_millis(10));
        }

        drop(poller);

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    fn blocking_add_subscription_invalid_interface_timeout() -> Result<(), Box<dyn error::Error>> {
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
        ctx.set_error_handler(Some(&Handler::leak(ErrorCount::default())))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:udp?endpoint=203.0.113.1:54324");

        let result = aeron.add_subscription(
            &channel.into_c_string(),
            4324,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_millis(300),
        );

        assert!(result.is_err(), "expected error for invalid interface");

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    fn async_publication_invalid_bind_poll_then_drop() -> Result<(), Box<dyn error::Error>> {
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
        ctx.set_error_handler(Some(&Handler::leak(ErrorCount::default())))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        // Use an invalid bind on publication (bind is not valid for publication, and the IP is unowned).
        let channel = format!("aeron:udp?endpoint=127.0.0.1:54330|bind=203.0.113.1:60000");

        let poller = aeron.async_add_publication(&channel.into_c_string(), 4330)?;
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(250) {
            let _ = poller.poll();
            #[cfg(debug_assertions)]
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(poller);

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
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
        const STREAM_ID: i32 = 123;
        let publisher =
            aeron.add_publication(AERON_IPC_STREAM, STREAM_ID, Duration::from_secs(5))?;
        info!("created publisher");

        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            STREAM_ID,
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
                assert_eq!(STREAM_ID, header.get_values().unwrap().frame.stream_id);
                let header = header.get_values().unwrap();
                let frame = header.frame();
                let stream_id = frame.stream_id();
                assert_eq!(STREAM_ID, stream_id);

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

    #[derive(Default, Debug)]
    struct MdcTotals {
        gap_events: u64,
        missing_messages: u64,
        received_messages: u64,
    }

    #[derive(Debug)]
    struct MdcWindowStats {
        expected_seq: Option<u64>,
        gap_events: u64,
        missing_messages: u64,
        received_messages: u64,
        histogram: Histogram<u64>,
    }

    impl MdcWindowStats {
        fn new() -> Result<Self, Box<dyn error::Error>> {
            Ok(Self {
                expected_seq: None,
                gap_events: 0,
                missing_messages: 0,
                received_messages: 0,
                histogram: Histogram::new(3)?,
            })
        }

        fn observe(&mut self, seq: u64, sent_ts_ns: u64) {
            self.received_messages += 1;

            match self.expected_seq {
                None => self.expected_seq = Some(seq.saturating_add(1)),
                Some(expected) if seq > expected => {
                    self.gap_events += 1;
                    self.missing_messages += seq - expected;
                    self.expected_seq = Some(seq.saturating_add(1));
                }
                Some(expected) if seq == expected => {
                    self.expected_seq = Some(expected.saturating_add(1));
                }
                Some(_) => {
                    // Ignore out-of-order/late packets for gap counting in this window.
                }
            }

            let now_ns = Aeron::nano_clock().max(0) as u64;
            let latency_ns = now_ns.saturating_sub(sent_ts_ns);
            let _ = self.histogram.record(latency_ns);
        }

        fn print_and_reset(
            &mut self,
            window_number: usize,
            interval: Duration,
            totals: &mut MdcTotals,
        ) {
            totals.gap_events += self.gap_events;
            totals.missing_messages += self.missing_messages;
            totals.received_messages += self.received_messages;

            if self.histogram.len() > 0 {
                let min_us = self.histogram.min() / 1_000;
                let p50_us = self.histogram.value_at_quantile(0.50) / 1_000;
                let p99_us = self.histogram.value_at_quantile(0.99) / 1_000;
                let max_us = self.histogram.max() / 1_000;
                println!(
                    "[mdc-window-{window_number}] interval={interval:?} received={} gaps={} missing={} latency_us[min={}, p50={}, p99={}, max={}]",
                    self.received_messages, self.gap_events, self.missing_messages, min_us, p50_us, p99_us, max_us,
                );
            } else {
                println!(
                    "[mdc-window-{window_number}] interval={interval:?} received=0 gaps=0 missing=0 latency_us[min=n/a, p50=n/a, p99=n/a, max=n/a]"
                );
            }

            self.expected_seq = None;
            self.gap_events = 0;
            self.missing_messages = 0;
            self.received_messages = 0;
            self.histogram.reset();
        }
    }

    /// Run with loss profile on macOS:
    /// `just mdc-loss-run 120 10 0.10`
    ///
    /// The recipe handles PF setup and cleanup automatically.
    ///
    /// Manual cleanup (if needed):
    /// `sudo pfctl -a com.apple/rusteron-mdc-loss -F all`
    /// `sudo dnctl -q flush`
    /// `sudo pfctl -f /etc/pf.conf`
    #[test]
    #[serial]
    #[ignore] // Long-running diagnostics test for manual MDC with rolling latency/gap reports.
    pub fn mdc_unreliable_gap_latency_histogram_report() -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();

        const STREAM_ID: i32 = 32931;
        const CONTROL_PORT: u16 = 32929;
        const SUBSCRIBER_PORT: u16 = 32930;
        const MESSAGE_LEN: usize = 130;

        let report_interval = Duration::from_secs(
            std::env::var("RUSTERON_MDC_REPORT_INTERVAL_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(10),
        );
        let test_duration = Duration::from_secs(
            std::env::var("RUSTERON_MDC_TEST_DURATION_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(300),
        );
        let mdc_host = std::env::var("RUSTERON_MDC_HOST").unwrap_or("127.0.0.1".to_string());

        // Low-latency loss tuning for a single subscriber:
        // - group=false: do not apply MDC receiver-group semantics.
        // - rejoin=false: do not rejoin stream after falling behind.
        // - nak-delay=100us: shorten unreliable-stream gap-fill decision latency.
        // - short untethered timeouts: reduce latency spikes when gaps appear.
        let publication_channel = format!(
            "aeron:udp?control-mode=manual|control={}:{CONTROL_PORT}",
            mdc_host
        );
        let subscription_channel = format!(
            "aeron:udp?endpoint={}:{SUBSCRIBER_PORT}|reliable=false|tether=false|group=false|nak-delay=500us",
            mdc_host
        );
        let destination_uri = format!("aeron:udp?endpoint={}:{SUBSCRIBER_PORT}", mdc_host);

        let (media_driver_ctx, stop_driver, driver_handle) = start_media_driver(32930)?;
        let aeron_dir = media_driver_ctx.get_dir().to_string();

        println!(
            "[mdc-start] publication={} subscription={} duration={:?} report_interval={:?}",
            publication_channel, subscription_channel, test_duration, report_interval
        );

        let running = Arc::new(AtomicBool::new(true));

        let subscriber_dir = aeron_dir.clone();
        let subscriber_channel = subscription_channel.clone();
        let subscriber_running = Arc::clone(&running);
        let subscriber_thread = std::thread::spawn(move || -> MdcTotals {
            let (_ctx, aeron) = create_client_for_dir(&subscriber_dir)
                .expect("failed to create subscriber aeron client");

            let subscription = aeron
                .add_subscription(
                    &subscriber_channel.into_c_string(),
                    STREAM_ID,
                    Handlers::no_available_image_handler(),
                    Handlers::no_unavailable_image_handler(),
                    Duration::from_secs(5),
                )
                .expect("failed to create subscriber");

            let mut totals = MdcTotals::default();
            let mut window_stats = MdcWindowStats::new().expect("failed to create histogram");
            let test_start = Instant::now();
            let mut window_start = test_start;
            let mut window_number = 1usize;

            while test_start.elapsed() < test_duration {
                let _ = subscription
                    .poll_once(
                        |msg, _header| {
                            if msg.len() < 16 {
                                return;
                            }

                            let seq = u64::from_le_bytes(msg[0..8].try_into().unwrap());
                            let sent_ts_ns = u64::from_le_bytes(msg[8..16].try_into().unwrap());
                            window_stats.observe(seq, sent_ts_ns);
                        },
                        10_000,
                    )
                    .expect("subscriber poll failed");

                if window_start.elapsed() >= report_interval {
                    window_stats.print_and_reset(window_number, report_interval, &mut totals);
                    window_number += 1;
                    window_start = Instant::now();
                }
            }

            window_stats.print_and_reset(window_number, report_interval, &mut totals);
            subscriber_running.store(false, Ordering::SeqCst);
            totals
        });

        // Ensure subscriber has started before publisher setup.
        sleep(Duration::from_millis(250));

        let publisher_dir = aeron_dir;
        let publisher_channel = publication_channel.clone();
        let publisher_destination = destination_uri.clone();
        let publisher_running = Arc::clone(&running);
        let publisher_thread = std::thread::spawn(move || -> u64 {
            let (_ctx, aeron) = create_client_for_dir(&publisher_dir)
                .expect("failed to create publisher aeron client");

            let publication = aeron
                .add_exclusive_publication(
                    &publisher_channel.into_c_string(),
                    STREAM_ID,
                    Duration::from_secs(5),
                )
                .expect("failed to create publication");

            let add_destination =
                AeronAsyncDestination::aeron_exclusive_publication_async_add_destination(
                    &aeron,
                    &publication,
                    &publisher_destination.into_c_string(),
                )
                .expect("failed to add manual MDC destination");

            let add_destination_start = Instant::now();
            while add_destination
                .aeron_exclusive_publication_async_destination_poll()
                .expect("destination add poll failed")
                == 0
            {
                assert!(
                    add_destination_start.elapsed() <= Duration::from_secs(5),
                    "Timed out adding manual MDC destination"
                );
                sleep(Duration::from_millis(10));
            }

            let connect_start = Instant::now();
            while !publication.is_connected() && connect_start.elapsed() < Duration::from_secs(5) {
                sleep(Duration::from_millis(10));
            }
            assert!(
                publication.is_connected(),
                "manual MDC publication did not connect to subscriber destination"
            );

            let mut seq: u64 = 0;
            let mut payload = [0u8; MESSAGE_LEN];
            while publisher_running.load(Ordering::Acquire) {
                payload[0..8].copy_from_slice(&seq.to_le_bytes());
                let ts_ns = Aeron::nano_clock().max(0) as u64;
                payload[8..16].copy_from_slice(&ts_ns.to_le_bytes());

                let result =
                    publication.offer(&payload, Handlers::no_reserved_value_supplier_handler());
                if result > 0 {
                    seq = seq.wrapping_add(1);
                }
                sleep(Duration::from_millis(1));
            }
            seq
        });

        let totals = subscriber_thread.join().unwrap();
        running.store(false, Ordering::SeqCst);
        let sent_messages = publisher_thread.join().unwrap();
        stop_driver.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();

        println!(
            "[mdc-summary] sent={} received={} total_gaps={} total_missing={}",
            sent_messages, totals.received_messages, totals.gap_events, totals.missing_messages
        );

        assert!(sent_messages > 0, "publisher failed to send any messages");
        assert!(
            totals.received_messages > 0,
            "subscriber did not receive any messages"
        );
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

    fn create_client_for_dir(dir: &str) -> Result<(AeronContext, Aeron), Box<dyn Error>> {
        info!("creating aeron client [dir={}]", dir);
        let ctx = AeronContext::new()?;
        ctx.set_dir(&dir.into_c_string())?;
        ctx.set_error_handler(Some(&Handler::leak(TestErrorCount::default())))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        Ok((ctx, aeron))
    }

    fn create_client(
        media_driver_ctx: &AeronDriverContext,
    ) -> Result<(AeronContext, Aeron), Box<dyn Error>> {
        let dir = media_driver_ctx.get_dir().to_string();
        create_client_for_dir(&dir)
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
