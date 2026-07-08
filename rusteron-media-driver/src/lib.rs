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

/// RAII guard for an embedded media driver launched via
/// [`AeronDriver::launch_embedded_guard`]. Signals stop on drop so the driver
/// thread always joins even on panic / early return.
pub struct EmbeddedMediaDriver {
    stop: Option<Arc<AtomicBool>>,
    handle: Option<JoinHandle<Result<(), AeronCError>>>,
}

impl EmbeddedMediaDriver {
    /// Signal the driver thread to stop (idempotent; the thread exits on its next
    /// idle cycle). Blocks until it joins when consumed by [`Self::join`].
    pub fn stop(&self) {
        if let Some(stop) = &self.stop {
            stop.store(true, Ordering::SeqCst);
        }
    }

    /// Whether the driver thread has finished.
    pub fn is_finished(&self) -> bool {
        self.handle.as_ref().map_or(true, |h| h.is_finished())
    }

    /// Signal stop and block until the driver thread joins, returning its result.
    pub fn join(mut self) -> Result<(), AeronCError> {
        self.stop();
        if let Some(h) = self.handle.take() {
            // Flatten JoinHandle<Result<..>>: a panic becomes an AeronCError,
            // the inner AeronCError is propagated.
            return h.join().map_err(|_| AeronCError::from_code(-1)).and_then(|r| r);
        }
        Ok(())
    }
}

impl Drop for EmbeddedMediaDriver {
    fn drop(&mut self) {
        if let Some(stop) = &self.stop {
            stop.store(true, Ordering::SeqCst);
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

pub mod testing;

impl AeronDriverContext {
    /// Typed variant of [`Self::set_conductor_idle_strategy`].
    pub fn set_conductor_idle_strategy_kind(&self, kind: AeronIdleStrategyKind) -> Result<i32, AeronCError> {
        self.set_conductor_idle_strategy(kind.name_c())
    }

    /// Typed variant of [`Self::set_sender_idle_strategy`].
    pub fn set_sender_idle_strategy_kind(&self, kind: AeronIdleStrategyKind) -> Result<i32, AeronCError> {
        self.set_sender_idle_strategy(kind.name_c())
    }

    /// Typed variant of [`Self::set_receiver_idle_strategy`].
    pub fn set_receiver_idle_strategy_kind(&self, kind: AeronIdleStrategyKind) -> Result<i32, AeronCError> {
        self.set_receiver_idle_strategy(kind.name_c())
    }

    /// Typed variant of [`Self::set_sharednetwork_idle_strategy`].
    pub fn set_sharednetwork_idle_strategy_kind(&self, kind: AeronIdleStrategyKind) -> Result<i32, AeronCError> {
        self.set_sharednetwork_idle_strategy(kind.name_c())
    }

    /// Typed variant of [`Self::set_shared_idle_strategy`].
    pub fn set_shared_idle_strategy_kind(&self, kind: AeronIdleStrategyKind) -> Result<i32, AeronCError> {
        self.set_shared_idle_strategy(kind.name_c())
    }
}

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

            info!("Aeron driver started [dir={}]", aeron_driver.context().get_dir());

            started2.store(true, Ordering::SeqCst);

            // Poll for work until Ctrl+C is pressed
            while !stop.load(Ordering::Acquire) {
                aeron_driver.main_idle_strategy(aeron_driver.main_do_work()?);
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

    /// Launch an embedded media driver, returning a RAII guard that stops the
    /// driver on drop (so a panic or early return can't leak a driver process).
    ///
    /// Prefer this over [`Self::launch_embedded`], which returns a raw
    /// `(Arc<AtomicBool>, JoinHandle)` tuple the caller must remember to drive.
    /// `register_sigint` is `true` to mirror the standalone binary.
    pub fn launch_embedded_guard(aeron_context: AeronDriverContext, register_sigint: bool) -> EmbeddedMediaDriver {
        let (stop, handle) = Self::launch_embedded(aeron_context, register_sigint);
        EmbeddedMediaDriver {
            stop: Some(stop),
            handle: Some(handle),
        }
    }

    /// if you have existing shm files and its before the driver timeout it will try to reuse it and fail
    /// this makes sure that if that is the case it will wait else it proceeds
    pub fn wait_for_previous_media_driver_to_timeout(aeron_context: &AeronDriverContext) {
        if !aeron_context.get_dir_delete_on_start() {
            let cnc_file = Path::new(aeron_context.get_dir()).join("cnc.dat");

            if cnc_file.exists() {
                let timeout = Duration::from_millis(aeron_context.get_driver_timeout_ms() * 2).as_nanos() as i64;

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
                    info!(
                        "cnc file already exists, will need to wait {sleep_duration:?} for timeout [file={cnc_file:?}]"
                    );
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
    fn driver_idle_strategy_kinds_round_trip() {
        let ctx = AeronDriverContext::new().unwrap();
        for (kind, name) in [
            (AeronIdleStrategyKind::Sleeping, "sleeping"),
            (AeronIdleStrategyKind::Yielding, "yield"),
            (AeronIdleStrategyKind::BusySpin, "spin"),
            (AeronIdleStrategyKind::NoOp, "noop"),
            (AeronIdleStrategyKind::Backoff, "backoff"),
        ] {
            ctx.set_conductor_idle_strategy_kind(kind).unwrap();
            assert_eq!(name, ctx.get_conductor_idle_strategy(), "conductor {kind:?}");
            ctx.set_sender_idle_strategy_kind(kind).unwrap();
            ctx.set_receiver_idle_strategy_kind(kind).unwrap();
            ctx.set_sharednetwork_idle_strategy_kind(kind).unwrap();
            ctx.set_shared_idle_strategy_kind(kind).unwrap();
        }
    }

    #[test]
    fn version_check() {
        let major = unsafe { crate::aeron_version_major() };
        let minor = unsafe { crate::aeron_version_minor() };
        let patch = unsafe { crate::aeron_version_patch() };

        let aeron_version = format!("{}.{}.{}", major, minor, patch);
        let cargo_version = "1.51.0";
        assert_eq!(aeron_version, cargo_version);
    }

    #[test]
    fn send_message() -> Result<(), AeronCError> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
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

        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;

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
                channel: &str,
                stream_id: i32,
                session_id: i32,
                correlation_id: i64,
            ) -> () {
                info!("on new publication {channel} {stream_id} {session_id} {correlation_id}")
            }
        }
        let handler = Handler::new(Test {});
        ctx.set_on_available_counter(Some(handler.clone()))?;
        ctx.set_on_new_publication(Some(handler.clone()))?;

        client.start()?;
        info!("aeron driver started");
        assert!(Aeron::epoch_clock() > 0);
        assert!(Aeron::nano_clock() > 0);

        let counter_async = AeronAsyncAddCounter::new(&client, 2543543, "12312312".as_bytes(), "abcd")?;

        let counter = counter_async.poll_blocking(Duration::from_secs(15))?;
        unsafe {
            *counter.addr() += 1;
        }

        let result = AeronAsyncAddPublication::new(&client, topic, stream_id)?;

        let publication = result.poll_blocking(std::time::Duration::from_secs(15))?;

        info!("publication channel: {:?}", publication.channel());
        info!("publication stream_id: {:?}", publication.stream_id());
        info!("publication status: {:?}", publication.channel_status());

        drop(publication);
        drop(counter);
        drop(client);
        stop.store(true, Ordering::SeqCst);

        Ok(())
    }

    #[test]
    pub fn test_debug() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = AeronDriverContext::new()?;

        println!("{:#?}", ctx);

        // Capture the raw context pointer, NOT a context clone: the handler is
        // owned by `ctx` (set_agent_on_start_function stores it as a dependency),
        // so a clone would form a strong-reference cycle (ctx -> handler -> ctx)
        // that close_resource_deferred_if_shared can never drain, leaking the C
        // context. The raw pointer is sound because the handler cannot outlive
        // the context that owns it.
        struct AgentStartHandler {
            ctx_ptr: *mut aeron_driver_context_t,
        }
        // SAFETY: see comment above — handler is owned by the context and only
        // dereferenced on the driver agent thread while the context is alive.
        unsafe impl Send for AgentStartHandler {}

        impl AeronAgentStartFuncCallback for AgentStartHandler {
            fn handle_aeron_agent_on_start_func(&mut self, role: &str) -> () {
                unsafe {
                    aeron_set_thread_affinity_on_start(
                        self.ctx_ptr as *mut _,
                        std::ffi::CString::new(role).unwrap().into_raw(),
                    );
                }
            }
        }

        let agent_handler = Handler::new(AgentStartHandler {
            ctx_ptr: ctx.get_inner(),
        });
        ctx.set_agent_on_start_function(Some(agent_handler.clone()))?;

        println!("{:#?}", ctx);

        Ok(())
    }
}
