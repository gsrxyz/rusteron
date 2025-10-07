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

pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use bindings::*;
use log::info;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

include!(concat!(env!("OUT_DIR"), "/aeron.rs"));
include!(concat!(env!("OUT_DIR"), "/aeron_custom.rs"));

unsafe impl Sync for AeronDriverContext {}
unsafe impl Send for AeronDriverContext {}
unsafe impl Sync for AeronDriver {}
unsafe impl Send for AeronDriver {}

impl AeronDriver {
    pub fn launch_embedded(
        aeron_context: AeronDriverContext,
        register_sigint: bool,
    ) -> (Arc<AtomicBool>, JoinHandle<Result<(), AeronCError>>) {
        AeronDriver::wait_for_previous_media_driver_to_timeout(&aeron_context);

        let stop = Arc::new(AtomicBool::new(false));
        let stop_copy = stop.clone();
        // Register signal handler for SIGINT (Ctrl+C)
        if register_sigint {
            let stop_copy2 = stop.clone();
            ctrlc::set_handler(move || {
                stop_copy2.store(true, Ordering::SeqCst);
            })
            .expect("Error setting Ctrl-C handler");
        }

        let started = Arc::new(AtomicBool::new(false));
        let started2 = started.clone();

        let dir = aeron_context.get_dir().to_string();
        info!("Starting media driver [dir={}]", dir);
        let handle = std::thread::spawn(move || {
            let aeron_context = aeron_context.clone();
            let aeron_driver = AeronDriver::new(&aeron_context)?;
            aeron_driver.start(true)?;

            info!(
                "Aeron driver started [dir={}]",
                aeron_driver.context().get_dir()
            );

            started2.store(true, Ordering::SeqCst);

            // Poll for work until Ctrl+C is pressed
            while !stop.load(Ordering::Acquire) {
                while aeron_driver.main_do_work()? > 0 {
                    // busy spin
                }
            }

            info!("stopping media driver");

            Ok::<_, AeronCError>(())
        });

        while !started.load(Ordering::SeqCst) && !handle.is_finished() {
            sleep(Duration::from_millis(100));
        }

        if handle.is_finished() {
            panic!("failed to start media driver {:?}", handle.join())
        }
        info!("started media driver [dir={}]", dir);

        (stop_copy, handle)
    }

    /// if you have existing shm files and its before the driver timeout it will try to reuse it and fail
    /// this makes sure that if that is the case it will wait else it proceeds
    pub fn wait_for_previous_media_driver_to_timeout(aeron_context: &AeronDriverContext) {
        if !aeron_context.get_dir_delete_on_start() {
            let cnc_file = Path::new(aeron_context.get_dir()).join("cnc.dat");

            if cnc_file.exists() {
                let timeout = Duration::from_millis(aeron_context.get_driver_timeout_ms() * 2)
                    .as_nanos() as i64;

                let mut duration = timeout;

                if let Ok(md) = cnc_file.metadata() {
                    if let Ok(modified_time) = md.modified() {
                        if let Ok(took) = modified_time.elapsed() {
                            duration = took.as_nanos() as i64;
                        }
                    }
                }

                let delay = timeout - duration;

                if delay > 0 {
                    let sleep_duration = Duration::from_nanos((delay + 1_000_000) as u64);
                    info!("cnc file already exists, will need to wait {sleep_duration:?} for timeout [file={cnc_file:?}]");
                    sleep(sleep_duration);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::error;
    use std::os::raw::c_int;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[test]
    fn version_check() {
        let major = unsafe { crate::aeron_version_major() };
        let minor = unsafe { crate::aeron_version_minor() };
        let patch = unsafe { crate::aeron_version_patch() };

        let aeron_version = format!("{}.{}.{}", major, minor, patch);
        let cargo_version = "1.48.6";
        assert_eq!(aeron_version, cargo_version);
    }

    #[test]
    fn send_message() -> Result<(), AeronCError> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();
        let topic = AERON_IPC_STREAM;
        let stream_id = 32;

        let aeron_context = AeronDriverContext::new()?;
        aeron_context.set_dir_delete_on_shutdown(true)?;
        aeron_context.set_dir_delete_on_start(true)?;

        let (stop, _driver_handle) = AeronDriver::launch_embedded(aeron_context.clone(), false);

        // aeron_driver
        //     .conductor()
        //     .context()
        //     .print_configuration();
        // aeron_driver.main_do_work()?;
        info!("aeron dir: {:?}", aeron_context.get_dir());

        let dir = aeron_context.get_dir().to_string();
        let ctx = AeronContext::new()?;
        ctx.set_dir(&dir.into_c_string())?;

        let client = Aeron::new(&ctx)?;

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

        let error_handler = Some(Handler::leak(ErrorCount::default()));
        ctx.set_error_handler(error_handler.as_ref())?;

        struct Test {}
        impl AeronAvailableCounterCallback for Test {
            fn handle_aeron_on_available_counter(
                &mut self,
                counters_reader: AeronCountersReader,
                registration_id: i64,
                counter_id: i32,
            ) -> () {
                info!("new counter counters_reader={counters_reader:?} registration_id={registration_id} counter_id={counter_id}");
            }
        }

        impl AeronNewPublicationCallback for Test {
            fn handle_aeron_on_new_publication(
                &mut self,
                async_: AeronAsyncAddPublication,
                channel: &str,
                stream_id: i32,
                session_id: i32,
                correlation_id: i64,
            ) -> () {
                info!("on new publication {async_:?} {channel} {stream_id} {session_id} {correlation_id}")
            }
        }
        let handler = Some(Handler::leak(Test {}));
        ctx.set_on_available_counter(handler.as_ref())?;
        ctx.set_on_new_publication(handler.as_ref())?;

        client.start()?;
        info!("aeron driver started");
        assert!(Aeron::epoch_clock() > 0);
        assert!(Aeron::nano_clock() > 0);

        let counter_async =
            AeronAsyncAddCounter::new(&client, 2543543, "12312312".as_bytes(), "abcd")?;

        let counter = counter_async.poll_blocking(Duration::from_secs(15))?;
        unsafe {
            *counter.addr() += 1;
        }

        let result = AeronAsyncAddPublication::new(&client, topic, stream_id)?;

        let publication = result.poll_blocking(std::time::Duration::from_secs(15))?;

        info!("publication channel: {:?}", publication.channel());
        info!("publication stream_id: {:?}", publication.stream_id());
        info!("publication status: {:?}", publication.channel_status());

        // client.main_do_work();
        // let claim = AeronBufferClaim::default();
        // assert!(publication.try_claim(100, &claim) > 0, "publication claim is empty");

        stop.store(true, Ordering::SeqCst);

        Ok(())
    }

    #[test]
    pub fn test_debug() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = AeronDriverContext::new()?;

        println!("{:#?}", ctx);

        struct AgentStartHandler {
            ctx: AeronDriverContext,
        }

        impl AeronAgentStartFuncCallback for AgentStartHandler {
            fn handle_aeron_agent_on_start_func(&mut self, role: &str) -> () {
                unsafe {
                    aeron_set_thread_affinity_on_start(
                        self.ctx.get_inner() as *mut _,
                        std::ffi::CString::new(role).unwrap().into_raw(),
                    );
                }
            }
        }

        ctx.set_agent_on_start_function(Some(&Handler::leak(AgentStartHandler {
            ctx: ctx.clone(),
        })))?;

        println!("{:#?}", ctx);

        Ok(())
    }
}
