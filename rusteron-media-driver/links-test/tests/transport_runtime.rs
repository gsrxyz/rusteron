//! Compile and run a C transport with an owned runtime against a real driver.

use rusteron_media_driver::extensions::IdleStrategy;
use rusteron_media_driver::{bindings::*, extensions::*, testing::EmbeddedDriver, *};
use rusteron_media_driver_links_test as _;
use std::{
    ffi::c_void,
    path::PathBuf,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

// This unpublished fixture crate tests the example shipped in its parent package.
#[allow(dead_code)]
#[path = "../../examples/extensions/native_transport_owned.rs"]
mod native_transport_owned;
use native_transport_owned::NativeRuntime;

unsafe extern "C" {
    fn links_test_runtime_open() -> *mut c_void;
    fn links_test_runtime_bindings(runtime: *mut c_void) -> *const aeron_udp_channel_transport_bindings_t;
    fn links_test_runtime_snapshot(runtime: *mut c_void, values: *mut u64);
    fn links_test_runtime_close(runtime: *mut c_void);
    fn links_test_runtime_close_count() -> u64;
}

struct CheckedRuntime {
    runtime: NativeRuntime,
    directory: PathBuf,
    started: Arc<AtomicBool>,
    joined: Arc<AtomicBool>,
}
impl Drop for CheckedRuntime {
    fn drop(&mut self) {
        if !self.started.load(Ordering::Acquire) || std::thread::panicking() {
            return;
        }
        assert!(
            self.joined.load(Ordering::Acquire),
            "runtime dropped before driver join returned"
        );
        // Only native context close deletes this directory. Driver close has
        // already joined its agents, but leaves the retained context intact.
        assert!(!self.directory.exists(), "runtime dropped before native context close");
        let mut counts = [0; 6];
        // SAFETY: the runtime guard is still alive, and every agent has joined.
        unsafe { links_test_runtime_snapshot(self.runtime.as_ptr(), counts.as_mut_ptr()) };
        assert_eq!(&counts[..4], &[0, 0, 2, 2]);
        assert!(counts[4] > 0, "custom C send callback was not used");
        assert!(counts[5] > 0, "custom C receive callback was not used");
        // NativeRuntime's field destructor now calls the C close function once.
    }
}

struct ResolverDrop(Arc<AtomicUsize>);
impl Drop for ResolverDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct Observer(Arc<AtomicUsize>);
impl InterceptorStateFactory for Observer {
    type State = Arc<AtomicUsize>;
    fn new(state: &Self::State, _: StrategyContext, _: TransportAffinity) -> Result<Self, AeronCError> {
        Ok(Self(state.clone()))
    }
}
impl OutgoingInterceptor for Observer {
    fn inspect(&mut self, _: &OutgoingPacket<'_>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
impl IncomingInterceptor for Observer {
    fn inspect(&mut self, _: &IncomingPacket<'_>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

static FLOW_MESSAGES: AtomicUsize = AtomicUsize::new(0);
struct Spin;
impl IdleStrategy for Spin {
    const BUSY_SPIN: bool = true;
    fn idle(&mut self, work: i32) {
        if work <= 0 {
            std::hint::spin_loop();
        }
    }
}

struct ObservedFlow(MaxFlowControl);
impl FlowControl for ObservedFlow {
    type Shared = ();
    fn new(args: &FlowControlArgs<'_>) -> Result<(Self, ()), AeronCError> {
        Ok((Self(MaxFlowControl::new(args)?.0), ()))
    }
    fn on_status_message(
        &mut self,
        shared: &(),
        message: StatusMessage<'_>,
        source: SockAddrRef<'_>,
        limit: i64,
        initial_term: i32,
        shift: usize,
        now: i64,
        metrics: StrategyMetrics<'_>,
    ) -> Position {
        FLOW_MESSAGES.fetch_add(1, Ordering::Relaxed);
        self.0
            .on_status_message(shared, message, source, limit, initial_term, shift, now, metrics)
    }
}

fn eventually(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "transport runtime test timed out");
        std::thread::yield_now();
    }
}

#[test]
fn c_runtime_composes_with_resolver_and_interceptors_in_both_installation_orders() {
    for transport_first in [true, false] {
        // SAFETY: this C function returns a new runtime. The guard owns the sole
        // close right, including failed setup.
        let runtime = unsafe {
            NativeRuntime::from_raw(
                NonNull::new(links_test_runtime_open()).expect("native runtime allocation"),
                links_test_runtime_close,
            )
        };
        let outgoing = Arc::new(AtomicUsize::new(0));
        let incoming = Arc::new(AtomicUsize::new(0));
        let resolutions = Arc::new(AtomicUsize::new(0));
        let resolver_drops = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicBool::new(false));
        let joined = Arc::new(AtomicBool::new(false));
        let closed_before = unsafe { links_test_runtime_close_count() };
        let flow_before = FLOW_MESSAGES.load(Ordering::Relaxed);
        let driver = EmbeddedDriver::launch_with(|context| {
            context.set_threading_mode(AeronThreadingMode::Dedicated)?;
            context.set_idle_strategy(IdleStrategyRole::Sender, Spin)?;
            context.set_idle_strategy(IdleStrategyRole::Receiver, Spin)?;
            let handle = runtime.as_ptr();
            let guard = CheckedRuntime {
                runtime,
                directory: context.get_dir_owned().into(),
                started: started.clone(),
                joined: joined.clone(),
            };
            // SAFETY: copy this runtime's C table. Its metadata and callbacks
            // use handle until native shutdown, and guard retains it.
            let bindings =
                unsafe { NativeTransportBindings::from_callbacks(*links_test_runtime_bindings(handle), guard) }?;
            let add_interceptors_and_resolver = || {
                context.add_outgoing_interceptor_with_state::<Observer>(outgoing.clone())?;
                context.add_incoming_interceptor_with_state::<Observer>(incoming.clone())?;
                let resolutions = resolutions.clone();
                let resolver_drop = ResolverDrop(resolver_drops.clone());
                context.set_name_resolver_fn(move |_, _, _| {
                    let _ = &resolver_drop;
                    resolutions.fetch_add(1, Ordering::Relaxed);
                    Ok(std::net::Ipv4Addr::LOCALHOST.into())
                })?;
                context.set_unicast_flow_control::<ObservedFlow>()
            };
            if transport_first {
                context.set_native_transport_bindings(TransportRole::Data, bindings)?;
                add_interceptors_and_resolver()?;
            } else {
                add_interceptors_and_resolver()?;
                context.set_native_transport_bindings(TransportRole::Data, bindings)?;
            }
            Ok(())
        })
        .unwrap();
        started.store(true, Ordering::Release);
        let retained_context = driver.context().clone();
        let directory = PathBuf::from(driver.dir());
        let client_context = AeronContext::new().unwrap();
        client_context.set_dir(&driver.dir().into_c_string()).unwrap();
        // A panic in this C callback would abort the test binary, so count errors instead
        let client_errors = Arc::new(AtomicUsize::new(0));
        let errors = client_errors.clone();
        client_context
            .set_error_handler(Some(move |code: i32, message: &str| {
                eprintln!("client error {code}: {message}");
                errors.fetch_add(1, Ordering::Relaxed);
            }))
            .unwrap();
        let client = Aeron::new(&client_context).unwrap();
        client.start().unwrap();
        // Port 0 lets the kernel pick a free port. The publication reuses the host name
        // with the bound port, so both sides go through the Rust resolver
        let timeout = Duration::from_secs(10);
        let subscription = client
            .add_subscription(
                c"aeron:udp?endpoint=runtime-test.invalid:0",
                78,
                Handlers::NONE,
                Handlers::NONE,
                timeout,
            )
            .unwrap();
        let mut resolved = String::new();
        eventually(|| {
            resolved = subscription.try_resolve_channel_endpoint_port_as_string(256).unwrap();
            !resolved.is_empty()
        });
        let port = resolved.rsplit_once(':').unwrap().1;
        let channel = format!("aeron:udp?endpoint=runtime-test.invalid:{port}").into_c_string();
        let publication = client.add_publication(&channel, 78, timeout).unwrap();
        eventually(|| publication.is_connected());
        let (mut offered, mut received, mut in_order) = (0u64, 0u64, true);
        eventually(|| {
            if offered < 128 {
                match publication.offer(&offered.to_le_bytes()) {
                    Ok(_) => offered += 1,
                    Err(error) => assert!(error.is_retryable()),
                }
            }
            subscription
                .poll_fn(
                    // Asserting here would panic inside a C callback and abort
                    |bytes, _| {
                        in_order &= bytes == received.to_le_bytes();
                        received += 1;
                    },
                    10,
                )
                .unwrap();
            received == 128
        });
        assert!(in_order, "messages arrived out of order or corrupted");
        assert_eq!(client_errors.load(Ordering::Relaxed), 0, "client reported errors");
        assert!(outgoing.load(Ordering::Relaxed) > 0);
        assert!(incoming.load(Ordering::Relaxed) > 0);
        assert!(resolutions.load(Ordering::Relaxed) > 0);
        assert!(FLOW_MESSAGES.load(Ordering::Relaxed) > flow_before);
        assert_eq!(resolver_drops.load(Ordering::Relaxed), 0);
        assert_eq!(unsafe { links_test_runtime_close_count() }, closed_before);
        drop(publication);
        drop(subscription);
        drop(client);
        drop(driver);
        joined.store(true, Ordering::Release);
        assert_eq!(resolver_drops.load(Ordering::Relaxed), 1);
        assert!(directory.exists(), "the retained context must keep its directory");
        assert_eq!(unsafe { links_test_runtime_close_count() }, closed_before);
        let last_context = retained_context.clone();
        drop(retained_context);
        assert_eq!(unsafe { links_test_runtime_close_count() }, closed_before);
        drop(last_context);
        assert_eq!(unsafe { links_test_runtime_close_count() }, closed_before + 1);
        assert_eq!(resolver_drops.load(Ordering::Relaxed), 1);
    }

    // Both contexts must retrieve their own runtime while their agents coexist.
    let launch = || {
        let runtime = unsafe {
            NativeRuntime::from_raw(
                NonNull::new(links_test_runtime_open()).expect("native runtime allocation"),
                links_test_runtime_close,
            )
        };
        let handle = runtime.as_ptr();
        let driver = EmbeddedDriver::launch_with(|context| {
            context.set_threading_mode(AeronThreadingMode::Dedicated)?;
            context.set_idle_strategy(IdleStrategyRole::Sender, Spin)?;
            context.set_idle_strategy(IdleStrategyRole::Receiver, Spin)?;
            // SAFETY: the copied table's metadata is retained by runtime.
            let bindings =
                unsafe { NativeTransportBindings::from_callbacks(*links_test_runtime_bindings(handle), runtime) }?;
            context.set_native_transport_bindings(TransportRole::Data, bindings)
        })
        .unwrap();
        (driver, handle)
    };
    let closed_before = unsafe { links_test_runtime_close_count() };
    let (first, first_handle) = launch();
    let (second, second_handle) = launch();
    for handle in [first_handle, second_handle] {
        let mut counts = [0; 6];
        // SAFETY: both drivers and their runtime guards are alive.
        unsafe { links_test_runtime_snapshot(handle, counts.as_mut_ptr()) };
        assert_eq!(&counts[..4], &[0, 2, 0, 0]);
    }
    drop(first);
    assert_eq!(unsafe { links_test_runtime_close_count() }, closed_before + 1);
    let mut counts = [0; 6];
    // SAFETY: the second driver's runtime guard is still alive.
    unsafe { links_test_runtime_snapshot(second_handle, counts.as_mut_ptr()) };
    assert_eq!(&counts[..4], &[0, 2, 0, 0]);
    drop(second);
    assert_eq!(unsafe { links_test_runtime_close_count() }, closed_before + 2);
}
