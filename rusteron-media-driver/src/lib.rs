#![allow(improper_ctypes_definitions)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![allow(unsafe_op_in_unsafe_fn)]
#![doc = include_str!("../README.md")]
//! # Features
//!
//! - **`static`**: Statically links the Aeron C code.
//!   By default, the library links the Aeron C libraries dynamically
//! - **`backtrace`**: Logs a backtrace for each `AeronCError`
//! - **`extra-logging`**: Logs when a resource is created and destroyed. Useful if you're seeing a segfault due to a resource being closed
//! - **`log-c-bindings`**: Logs every C binding call with arguments and return values. Useful for debugging FFI interactions
//! - **`precompile`**: Uses precompiled C code instead of requiring cmake and java to be installed

#[allow(improper_ctypes_definitions)]
#[allow(unpredictable_function_pointer_comparisons)]
// Preserve upstream Doxygen markup and bracketed IPv6 examples in the raw C comments.
#[allow(rustdoc::broken_intra_doc_links, rustdoc::invalid_html_tags)]
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use bindings::*;
use log::info;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{JoinHandle, sleep};

include!(concat!(env!("OUT_DIR"), "/aeron.rs"));
include!(concat!(env!("OUT_DIR"), "/aeron_custom.rs"));

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
            // A panic becomes an AeronCError; the inner AeronCError passes through
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

    /// Validates and applies the driver's affinity configuration, as `aeronmd` does.
    /// On Linux, configured affinities are CPU IDs unless cpuset offsets are enabled
    /// (`AERON_THREAD_AFFINITY_USE_CPUSET_OFFSETS` or
    /// [`Self::set_thread_affinity_use_cpuset_offsets`]).
    /// Call once, before the driver starts.
    ///
    /// # Errors
    ///
    /// On Linux, affinity validation failed or the cpuset could not be read.
    /// On other platforms this is a no-op, matching Aeron's standalone driver.
    pub fn apply_cgroup_cpuset_affinity(&self) -> Result<(), AeronCError> {
        self.aeron_driver_validate_and_apply_affinity_configuration()
            .map(|_| ())
            .map_err(AeronCError::capture_errmsg)
    }

    /// Pins each driver agent thread, as it starts, to the CPU set for its role
    /// (`AERON_CONDUCTOR_CPU_AFFINITY` and the like, or [`Self::set_conductor_cpu_affinity`]
    /// and its siblings), as `aeronmd` does. An on-start function already set still runs
    /// after it, so set any other one first. Call before [`AeronDriver::new`] or
    /// [`AeronDriver::launch_embedded`], which hand the on-start function to the agents.
    pub fn set_thread_affinity_on_start(&self) {
        self.inner
            .check_configurable()
            .expect("context cannot be changed after use");
        let context = self.get_inner();
        // SAFETY: the context is live and its agents have not started, so nothing else reads
        // these fields; Aeron's affinity function takes the driver context as its state.
        unsafe {
            // installing it again would make it its own delegate, which calls itself forever
            if (*context).agent_on_start_func.map(|f| f as *const ())
                == Some(aeron_set_thread_affinity_on_start as *const ())
            {
                return;
            }
            (*context).agent_on_start_func_delegate = (*context).agent_on_start_func;
            (*context).agent_on_start_state_delegate = (*context).agent_on_start_state;
            aeron_driver_context_set_agent_on_start_function(
                context,
                Some(aeron_set_thread_affinity_on_start),
                context.cast(),
            );
        }
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

        let dir = aeron_context.get_dir_owned();
        info!("Starting media driver [dir={}]", dir);
        let driver = Self::new(&aeron_context)
            .map_err(|error| error.capture_errmsg())
            .expect("failed to initialize embedded media driver");
        let driver_owner = driver.inner.as_owned().expect("driver requires owned storage").clone();
        drop(driver);
        let handle = std::thread::spawn(move || {
            // The native driver is initialized but not started. Move only its
            // checked owner; the public driver handle remains local to this worker.
            let aeron_driver = AeronDriver::from_resource(CResource::new(driver_owner));
            aeron_driver.start(true)?;

            let context = aeron_driver.context();
            // SAFETY: driver initialization froze the context configuration
            info!("Aeron driver started [dir={}]", unsafe { context.get_dir() });

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

    /// If existing shm files are still within the driver timeout, a new driver tries to reuse them and fails.
    /// This waits in that case and otherwise proceeds
    pub fn wait_for_previous_media_driver_to_timeout(aeron_context: &AeronDriverContext) {
        if !aeron_context.get_dir_delete_on_start() {
            // SAFETY: the text is used at once, before any setter can change it
            let cnc_file = Path::new(&unsafe { aeron_context.get_dir() }).join("cnc.dat");

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
    fn rust_threading_mode_names_work_with_native_context_methods() {
        let context = AeronDriverContext::new().unwrap();
        for mode in [
            AeronThreadingMode::Dedicated,
            AeronThreadingMode::SharedNetwork,
            AeronThreadingMode::Shared,
            AeronThreadingMode::Invoker,
        ] {
            context.set_threading_mode(mode).unwrap();
            assert_eq!(context.get_threading_mode(), mode);
        }
        assert_eq!(AeronThreadingMode::try_from(u32::MAX), Err(u32::MAX));
    }

    #[test]
    fn ipv4_prefix_comparison_uses_both_addresses() {
        // SAFETY: in_addr contains one initialized integer and owns no external storage.
        let first = unsafe { InAddr::new(u32::from_ne_bytes([192, 168, 1, 10])).unwrap() };
        let same_subnet = unsafe { InAddr::new(u32::from_ne_bytes([192, 168, 1, 11])).unwrap() };
        let other_subnet = unsafe { InAddr::new(u32::from_ne_bytes([192, 168, 2, 10])).unwrap() };
        assert!(first.aeron_ipv4_does_prefix_match(&same_subnet, 24).unwrap());
        assert!(!first.aeron_ipv4_does_prefix_match(&same_subnet, 32).unwrap());
        assert!(!first.aeron_ipv4_does_prefix_match(&other_subnet, 24).unwrap());
        assert!(first.aeron_ipv4_does_prefix_match(&other_subnet, 16).unwrap());
        assert!(first.aeron_ipv4_does_prefix_match(&other_subnet, 0).unwrap());
        assert_eq!(Aeron::ipv4_netmask_from_prefixlen(0).unwrap(), 0);
        assert_eq!(Aeron::ipv4_netmask_from_prefixlen(32).unwrap(), u32::MAX);
    }

    #[test]
    fn invalid_ip_prefixes_are_rejected_before_calling_native_code() {
        // SAFETY: these initialized address values have no pointers or external storage.
        let ipv4 = unsafe { InAddr::new(0).unwrap() };
        let ipv6 = unsafe { In6Addr::new_zeroed_on_heap() };
        for prefix in [33, usize::MAX] {
            assert!(ipv4.aeron_ipv4_does_prefix_match(&ipv4, prefix).is_err());
            assert!(Aeron::ipv4_netmask_from_prefixlen(prefix).is_err());
        }
        assert!(ipv6.aeron_ipv6_does_prefix_match(&ipv6, 0).unwrap());
        assert!(ipv6.aeron_ipv6_does_prefix_match(&ipv6, 128).unwrap());
        for prefix in [129, usize::MAX] {
            assert!(ipv6.aeron_ipv6_does_prefix_match(&ipv6, prefix).is_err());
        }
    }
    #[test]
    fn native_enum_conversions_keep_signed_values_and_reject_unknown_commands() {
        use aeron_driver_conductor_command_state_en::*;
        for (code, expected) in [
            (-1, AERON_DRIVER_CONDUCTOR_COMMAND_STATE_ERROR),
            (0, AERON_DRIVER_CONDUCTOR_COMMAND_STATE_RUNNING),
            (1, AERON_DRIVER_CONDUCTOR_COMMAND_STATE_DONE),
        ] {
            assert_eq!(aeron_driver_conductor_command_state_t::try_from(code), Ok(expected));
            assert_eq!(i32::from(expected), code);
        }
        for code in [i32::MIN, -2, 2, i32::MAX] {
            assert_eq!(aeron_driver_conductor_command_state_t::try_from(code), Err(code));
        }
        use aeron_driver_native_resource_agent_command_type_enum::*;
        for (code, expected) in [
            (1, AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_TYPE_RESOLVE_ADDRESS),
            (2, AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_TYPE_PARSE_CHANNEL),
            (3, AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_TYPE_FREE_LOG_BUFFER),
            (4, AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_TYPE_MAP_LOG_BUFFER),
        ] {
            assert_eq!(
                aeron_driver_native_resource_agent_command_type_t::try_from(code),
                Ok(expected)
            );
            assert_eq!(u32::from(expected), code);
        }
        for code in [0, 5, u32::MAX] {
            assert_eq!(
                aeron_driver_native_resource_agent_command_type_t::try_from(code),
                Err(code)
            );
        }
    }

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
            // SAFETY: this context is local and no call changes it during the comparison
            assert_eq!(name, unsafe { ctx.get_conductor_idle_strategy() }, "conductor {kind:?}");
            ctx.set_sender_idle_strategy_kind(kind).unwrap();
            ctx.set_receiver_idle_strategy_kind(kind).unwrap();
            ctx.set_sharednetwork_idle_strategy_kind(kind).unwrap();
            ctx.set_shared_idle_strategy_kind(kind).unwrap();
        }
    }

    #[test]
    fn driver_rejects_a_send_batch_larger_than_its_vector_capacity() -> Result<(), AeronCError> {
        let context = AeronDriverContext::new()?;
        let dir = std::env::temp_dir().join(format!("rusteron-invalid-send-batch-{}", Aeron::nano_clock()));
        context.set_dir(&dir.to_string_lossy().into_c_string())?;
        context.set_dir_delete_on_shutdown(true)?;
        context.set_sender_io_vector_capacity(2)?;
        context.set_network_publication_max_messages_per_send(5)?;
        let error = AeronDriver::new(&context).unwrap_err().capture_errmsg();
        assert!(
            error.to_string().contains(
                "network_publication_max_messages_per_send may not exceed sender_io_vector_capacity (2): value=5"
            ),
            "{error}"
        );
        Ok(())
    }

    #[test]
    fn version_check() {
        let major = unsafe { crate::aeron_version_major() };
        let minor = unsafe { crate::aeron_version_minor() };
        let patch = unsafe { crate::aeron_version_patch() };

        let aeron_version = format!("{}.{}.{}", major, minor, patch);
        let cargo_version = include_str!("../aeron/version.txt").trim().split('-').next().unwrap();
        assert_eq!(aeron_version, cargo_version);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn socket_bindings_match_the_native_library() {
        // SAFETY: the next native call fills this output before it is read.
        let address = unsafe { SockaddrStorage::new_zeroed_on_heap() };
        address.aeron_set_ipv4_wildcard_host_and_port();
        // Linux writes a 16-bit AF_INET at offset zero. Darwin bindings read
        // an 8-bit family at offset one and return zero.
        assert_eq!(address.ss_family(), 2);
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

        info!("aeron dir: {:?}", aeron_context.get_dir_owned());

        let dir = aeron_context.get_dir_owned();
        let ctx = AeronContext::new()?;
        ctx.set_dir(&dir.into_c_string())?;

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

        let error_handler = SharedHandler::new_shared(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;

        struct Test {}
        impl AeronAvailableCounterCallback for Test {
            fn handle_aeron_on_available_counter(
                &mut self,
                counters_reader: &AeronCountersReaderRef,
                registration_id: i64,
                counter_id: i32,
            ) -> () {
                info!(
                    "new counter counters_reader={counters_reader:?} registration_id={registration_id} counter_id={counter_id}"
                );
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
        let handler = SharedHandler::new_shared(Test {});
        ctx.set_on_available_counter(Some(handler.clone()))?;
        ctx.set_on_new_publication(Some(handler.clone()))?;

        let client = Aeron::new(&ctx)?;
        client.start()?;
        info!("aeron driver started");
        assert!(Aeron::epoch_clock() > 0);
        assert!(Aeron::nano_clock() > 0);

        let counter_async = AeronAsyncAddCounter::new(&client, 2543543, "12312312".as_bytes(), "abcd")?;

        let counter = counter_async.poll_blocking(Duration::from_secs(15))?;
        unsafe {
            counter.addr_atomic().fetch_add(1, Ordering::Relaxed);
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
        // SAFETY: see comment above. The handler is owned by the context and only
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

        let agent_handler = SharedHandler::new_shared(AgentStartHandler {
            ctx_ptr: ctx.get_inner(),
        });
        ctx.set_agent_on_start_function(Some(agent_handler.clone()))?;

        println!("{:#?}", ctx);

        Ok(())
    }

    /// Cpuset affinity is off by default, so applying it changes nothing.
    #[test]
    fn cgroup_cpuset_affinity_is_a_no_op_when_off() -> Result<(), AeronCError> {
        AeronDriverContext::new()?.apply_cgroup_cpuset_affinity()
    }

    #[test]
    fn affinity_methods_reject_a_frozen_context() -> Result<(), AeronCError> {
        let context = AeronDriverContext::new()?;
        context.inner.as_owned().unwrap().freeze()?;
        let generated = context
            .aeron_driver_validate_and_apply_affinity_configuration()
            .unwrap_err();
        let helper = context.apply_cgroup_cpuset_affinity().unwrap_err();
        assert_eq!(generated.message(), Some("context cannot be changed after use"));
        assert_eq!(helper.message(), generated.message());
        Ok(())
    }

    #[test]
    fn new_driver_configuration_round_trips() -> Result<(), AeronCError> {
        let context = AeronDriverContext::new()?;
        for tos in [-1, 0, 128, 255] {
            context.set_socket_tos(tos)?;
            assert_eq!(context.get_socket_tos(), tos);
        }
        for invalid in [-2, 256] {
            assert!(context.set_socket_tos(invalid).is_err());
            assert_eq!(context.get_socket_tos(), 255);
        }
        context.set_receiver_poll_to_slow_work_ratio(11)?;
        assert_eq!(context.get_receiver_poll_to_slow_work_ratio(), 11);
        context.set_udp_channel_transport_poller_iteration_threshold(6)?;
        assert_eq!(context.get_udp_channel_transport_poller_iteration_threshold(), 6);
        context.set_udp_channel_transport_poller_socket_poll_limit(8)?;
        assert_eq!(context.get_udp_channel_transport_poller_socket_poll_limit(), 8);
        context.set_thread_affinity_use_cpuset_offsets(true)?;
        assert!(context.get_thread_affinity_use_cpuset_offsets());
        context.set_thread_affinity_fail_on_validation_errors(true)?;
        assert!(context.get_thread_affinity_fail_on_validation_errors());
        Ok(())
    }

    /// The native validators accept an i32 length. Check nonempty and empty slices.
    #[test]
    fn termination_validator_defaults_take_i32_length_slices() {
        let mut token = *b"token";
        // SAFETY: Both native defaults ignore the state and token arguments.
        unsafe {
            assert!(AeronDriver::termination_validator_default_allow(
                std::ptr::null_mut(),
                &mut token
            ));
            assert!(!AeronDriver::termination_validator_default_deny(
                std::ptr::null_mut(),
                &mut token
            ));
            assert!(AeronDriver::termination_validator_default_allow(
                std::ptr::null_mut(),
                &mut []
            ));
        }
    }

    /// Affinity configuration is a no-op off Linux, including cpuset offsets.
    #[test]
    #[cfg(not(target_os = "linux"))]
    fn cgroup_cpuset_affinity_is_a_no_op_off_linux() -> Result<(), AeronCError> {
        let context = AeronDriverContext::new()?;
        context.set_thread_affinity_use_cpuset_offsets(true)?;
        context.apply_cgroup_cpuset_affinity()?;
        Ok(())
    }

    /// Installing the affinity function again keeps the earlier on-start function as its
    /// delegate, rather than making the affinity function call itself.
    #[test]
    fn thread_affinity_on_start_installs_once() -> Result<(), AeronCError> {
        let context = AeronDriverContext::new()?;
        context.set_thread_affinity_on_start();
        context.set_thread_affinity_on_start();
        // SAFETY: the context is live and no driver reads it.
        let delegate = unsafe { (*context.get_inner()).agent_on_start_func_delegate }.map(|f| f as *const ());
        assert_ne!(delegate, Some(aeron_set_thread_affinity_on_start as *const ()));
        Ok(())
    }

    /// Aeron's affinity function runs as each agent starts, and still calls the on-start
    /// function set before it.
    #[test]
    fn thread_affinity_on_start_keeps_the_previous_on_start_function() -> Result<(), Box<dyn std::error::Error>> {
        struct Roles(Arc<std::sync::Mutex<Vec<String>>>);
        impl AeronAgentStartFuncCallback for Roles {
            fn handle_aeron_agent_on_start_func(&mut self, role: &str) {
                self.0.lock().unwrap().push(role.to_owned());
            }
        }

        let context = AeronDriverContext::new()?;
        // SAFETY: the text is copied before set_dir changes this local context
        context.set_dir(&format!("{}{}", unsafe { context.get_dir() }, Aeron::nano_clock()).into_c_string())?;
        context.set_dir_delete_on_start(true)?;
        context.set_dir_delete_on_shutdown(true)?;
        context.set_threading_mode(aeron_threading_mode_enum::AERON_THREADING_MODE_DEDICATED)?;
        context.set_conductor_cpu_affinity(0)?;
        let roles = Arc::new(std::sync::Mutex::new(Vec::new()));
        context.set_agent_on_start_function(Some(Roles(Arc::clone(&roles))))?;
        context.set_thread_affinity_on_start();
        let installed = context.get_agent_on_start_function().map(|f| f as *const ());
        assert_eq!(installed, Some(aeron_set_thread_affinity_on_start as *const ()));

        let driver = AeronDriver::launch_embedded_guard(context.clone(), false);
        let start = std::time::Instant::now();
        while roles.lock().unwrap().len() < 4 && start.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(10));
        }
        driver.join()?;
        let mut seen = roles.lock().unwrap().clone();
        seen.sort();
        assert_eq!(seen, ["aeron-md-nra", "conductor", "receiver", "sender"]);
        Ok(())
    }
}
