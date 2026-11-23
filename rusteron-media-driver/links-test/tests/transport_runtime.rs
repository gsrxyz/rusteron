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
    fn links_test_runtime_install(context: *mut aeron_driver_context_t, runtime: *mut c_void) -> i32;
    fn links_test_runtime_bindings() -> *const aeron_udp_channel_transport_bindings_t;
    fn links_test_runtime_snapshot(runtime: *mut c_void, values: *mut u64);
    fn links_test_runtime_close(runtime: *mut c_void);
    fn links_test_runtime_close_count() -> u64;
}

struct CheckedRuntime {
    runtime: NativeRuntime,
    directory: PathBuf,
    joined: Arc<AtomicBool>,
}
impl Drop for CheckedRuntime {
    fn drop(&mut self) {
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
fn c_runtime_outlives_agent_join_and_native_context_close_and_composes_in_both_installation_orders() {
    for transport_first in [true, false] {
        // SAFETY: these C functions return a new runtime and an immutable static
        // table. The guard owns the sole close right, including failed setup.
        let runtime = unsafe {
            NativeRuntime::from_raw(
                NonNull::new(links_test_runtime_open()).expect("native runtime allocation"),
                links_test_runtime_close,
            )
        };
        let outgoing = Arc::new(AtomicUsize::new(0));
        let incoming = Arc::new(AtomicUsize::new(0));
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
                joined: joined.clone(),
            };
            // SAFETY: copy the C library's static table. Its callbacks keep
            // using handle until native shutdown, and guard retains it.
            let bindings = unsafe { NativeTransportBindings::from_callbacks(*links_test_runtime_bindings(), guard) }?;
            let add_interceptors = || {
                context.add_outgoing_interceptor_with_state::<Observer>(outgoing.clone())?;
                context.add_incoming_interceptor_with_state::<Observer>(incoming.clone())?;
                context.set_unicast_flow_control::<ObservedFlow>()
            };
            if transport_first {
                context.set_native_transport_bindings(TransportRole::Data, bindings)?;
                add_interceptors()?;
            } else {
                add_interceptors()?;
                context.set_native_transport_bindings(TransportRole::Data, bindings)?;
            }
            // SAFETY: configure this local context before driver initialization.
            // The C library retains the pointer in its named context entry.
            assert_eq!(unsafe { links_test_runtime_install(context.get_inner(), handle) }, 0);
            Ok(())
        })
        .unwrap();
        let retained_context = driver.context().clone();
        let directory = PathBuf::from(driver.dir());
        let client_context = AeronContext::new().unwrap();
        client_context.set_dir(&driver.dir().into_c_string()).unwrap();
        client_context
            .set_error_handler(Some(|code: i32, message: &str| panic!("{code}: {message}")))
            .unwrap();
        let client = Aeron::new(&client_context).unwrap();
        client.start().unwrap();
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let channel = format!("aeron:udp?endpoint={}", socket.local_addr().unwrap()).into_c_string();
        drop(socket);
        let timeout = Duration::from_secs(10);
        let subscription = client
            .add_subscription(&channel, 78, Handlers::NONE, Handlers::NONE, timeout)
            .unwrap();
        let publication = client.add_publication(&channel, 78, timeout).unwrap();
        eventually(|| publication.is_connected());
        let (mut offered, mut received) = (0u64, 0u64);
        eventually(|| {
            if offered < 128 {
                match publication.offer(&offered.to_le_bytes()) {
                    Ok(_) => offered += 1,
                    Err(error) => assert!(error.is_retryable()),
                }
            }
            subscription
                .poll_fn(
                    |bytes, _| {
                        assert_eq!(bytes, received.to_le_bytes());
                        received += 1;
                    },
                    10,
                )
                .unwrap();
            received == 128
        });
        assert!(outgoing.load(Ordering::Relaxed) > 0);
        assert!(incoming.load(Ordering::Relaxed) > 0);
        assert!(FLOW_MESSAGES.load(Ordering::Relaxed) > flow_before);
        assert_eq!(unsafe { links_test_runtime_close_count() }, closed_before);
        drop(publication);
        drop(subscription);
        drop(client);
        drop(driver);
        joined.store(true, Ordering::Release);
        assert!(directory.exists(), "the retained context must keep its directory");
        assert_eq!(unsafe { links_test_runtime_close_count() }, closed_before);
        let last_context = retained_context.clone();
        drop(retained_context);
        assert_eq!(unsafe { links_test_runtime_close_count() }, closed_before);
        drop(last_context);
        assert_eq!(unsafe { links_test_runtime_close_count() }, closed_before + 1);
    }
}
