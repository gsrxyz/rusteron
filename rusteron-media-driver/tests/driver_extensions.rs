use rusteron_media_driver::extensions::IdleStrategy as _;
use rusteron_media_driver::*;
use rusteron_media_driver::{bindings as native, extensions::*, testing::EmbeddedDriver};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

const TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn manual_work_errors_leave_message_capture_to_the_caller() {
    unsafe extern "C" fn fail_work(_: *mut std::ffi::c_void) -> i32 {
        -7
    }

    let context = AeronDriverContext::new().unwrap();
    context
        .set_dir(&format!("/tmp/rusteron-work-error-{}", std::process::id()).into_c_string())
        .unwrap();
    context.set_dir_delete_on_start(true).unwrap();
    context.set_dir_delete_on_shutdown(true).unwrap();
    context.set_threading_mode(AeronThreadingMode::Invoker).unwrap();
    let driver = AeronDriver::new(&context).unwrap();
    let raw = driver.get_inner();
    let mut driver = driver.start_manual().unwrap();

    // SAFETY: INVOKER has no background runner. Replace the work callback on
    // this thread for one call, then restore it before assertions or shutdown.
    let error = unsafe {
        native::aeron_err_set(
            22,
            c"test".as_ptr(),
            c"fail_work".as_ptr(),
            0,
            c"work failed 100%%".as_ptr(),
        );
        let original = (*raw).runners[0].do_work;
        (*raw).runners[0].do_work = Some(fail_work);
        let result = driver.do_work();
        (*raw).runners[0].do_work = original;
        result.unwrap_err()
    };
    assert_eq!(error.code, -7);
    assert_eq!(error.message(), None);
    let captured = error.capture_errmsg();
    unsafe { native::aeron_err_clear() };
    assert!(captured.message().unwrap().contains("work failed 100%"));
}

// Compile and exercise examples from this published package.
#[allow(dead_code)]
#[path = "../examples/extensions/bounded_flow_control.rs"]
mod bounded_flow_control;
#[allow(dead_code)]
#[path = "../examples/extensions/congestion_control.rs"]
mod congestion_control;
#[allow(dead_code)]
#[path = "../examples/extensions/duty_cycle_tracker.rs"]
mod duty_cycle_tracker;
#[allow(dead_code)]
#[path = "../examples/extensions/flow_control.rs"]
mod flow_control;
#[allow(dead_code)]
#[path = "../examples/extensions/idle_strategy.rs"]
mod idle_strategy;
#[allow(dead_code)]
#[path = "../examples/extensions/incoming_interceptor.rs"]
mod incoming_interceptor;
#[allow(dead_code)]
#[path = "../examples/extensions/name_resolver.rs"]
mod name_resolver;
#[allow(dead_code)]
#[path = "../examples/extensions/native_transport.rs"]
mod native_transport;
#[allow(dead_code)]
#[path = "../examples/extensions/outgoing_interceptor.rs"]
mod outgoing_interceptor;
#[allow(dead_code)]
#[path = "../examples/extensions/packet_loss.rs"]
mod packet_loss;
#[allow(dead_code)]
#[path = "../examples/extensions/port_manager.rs"]
mod port_manager;
use self::{
    congestion_control::REBUILDS,
    duty_cycle_tracker::MAX_CYCLE_NS,
    flow_control::FLOW_MESSAGES,
    idle_strategy::IDLE_CALLS,
    incoming_interceptor::INCOMING_PACKETS,
    name_resolver::RESOLUTIONS,
    native_transport::TRANSPORT_SENDS,
    outgoing_interceptor::OUTGOING_BATCHES,
    port_manager::{PORT_RELEASES, PORT_REQUESTS},
};

type Configure = fn(&AeronDriverContext) -> Result<(), AeronCError>;
const EXAMPLES: [(&str, Configure, &std::sync::atomic::AtomicU64); 9] = [
    ("flow_control", flow_control::configure, &FLOW_MESSAGES),
    ("congestion_control", congestion_control::configure, &REBUILDS),
    (
        "outgoing_interceptor",
        outgoing_interceptor::configure,
        &OUTGOING_BATCHES,
    ),
    (
        "incoming_interceptor",
        incoming_interceptor::configure,
        &INCOMING_PACKETS,
    ),
    ("name_resolver", name_resolver::configure, &RESOLUTIONS),
    ("idle_strategy", idle_strategy::configure, &IDLE_CALLS),
    ("duty_cycle_tracker", duty_cycle_tracker::configure, &MAX_CYCLE_NS),
    ("port_manager", port_manager::configure, &PORT_REQUESTS),
    ("native_transport", native_transport::configure, &TRANSPORT_SENDS),
];
static CREATED: AtomicUsize = AtomicUsize::new(0);
static DROPPED: AtomicUsize = AtomicUsize::new(0);
static IDLE: AtomicUsize = AtomicUsize::new(0);
static STATUS: AtomicUsize = AtomicUsize::new(0);
static OUT_CREATED: AtomicUsize = AtomicUsize::new(0);
static OUT_DROPPED: AtomicUsize = AtomicUsize::new(0);
static IN_CREATED: AtomicUsize = AtomicUsize::new(0);
static IN_DROPPED: AtomicUsize = AtomicUsize::new(0);
static SENT: AtomicUsize = AtomicUsize::new(0);
static NOTIFIED: AtomicUsize = AtomicUsize::new(0);
static RECEIVED: AtomicUsize = AtomicUsize::new(0);

struct CountedFlow(MaxFlowControl);
impl Drop for CountedFlow {
    fn drop(&mut self) {
        DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}
impl FlowControl for CountedFlow {
    type Shared = ();
    fn new(args: &FlowControlArgs<'_>) -> Result<(Self, ()), rusteron_media_driver::AeronCError> {
        CREATED.fetch_add(1, Ordering::Relaxed);
        Ok((Self(MaxFlowControl::new(args)?.0), ()))
    }
    fn on_idle(&mut self, _: &(), _: i64, limit: i64, _: i64, _: bool, _: StrategyMetrics<'_>) -> Position {
        IDLE.fetch_add(1, Ordering::Relaxed);
        Position::new(limit)
    }
    fn on_status_message(
        &mut self,
        shared: &(),
        sm: StatusMessage<'_>,
        source: SockAddrRef<'_>,
        limit: i64,
        term: i32,
        shift: usize,
        now: i64,
        metrics: StrategyMetrics<'_>,
    ) -> Position {
        STATUS.fetch_add(1, Ordering::Relaxed);
        self.0
            .on_status_message(shared, sm, source, limit, term, shift, now, metrics)
    }
}
struct Out;
impl Drop for Out {
    fn drop(&mut self) {
        OUT_DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}
impl InterceptorFactory for Out {
    fn new(_: StrategyContext, _: TransportAffinity) -> Result<Self, rusteron_media_driver::AeronCError> {
        OUT_CREATED.fetch_add(1, Ordering::Relaxed);
        Ok(Self)
    }
}
impl OutgoingInterceptor for Out {
    const NOTIFICATIONS: bool = true;
    fn on_notification(&mut self, notification: InterceptorNotification<'_>) -> i32 {
        assert_ne!(notification.transport.identity(), 0);
        match notification.resource {
            NotificationResource::Transport(channel) => assert!(!channel.uri().is_empty()),
            NotificationResource::Publication { stream_id, .. } | NotificationResource::Image { stream_id, .. } => {
                assert_eq!(stream_id, 78)
            }
        }
        NOTIFIED.fetch_add(1, Ordering::Relaxed);
        0
    }

    fn send<'a>(&mut self, next: OutgoingNext<'a>, packet: OutgoingPacket<'a>) -> i32 {
        assert!(!packet.is_empty());
        assert!(packet.address().to_socket_addr().is_some());
        assert!(packet.datagram(0).is_some());
        assert!(packet.datagram(packet.len()).is_none());
        SENT.fetch_add(1, Ordering::Relaxed);
        next.send(packet)
    }
}
struct Incoming;
impl Drop for Incoming {
    fn drop(&mut self) {
        IN_DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}
impl InterceptorFactory for Incoming {
    fn new(_: StrategyContext, _: TransportAffinity) -> Result<Self, rusteron_media_driver::AeronCError> {
        IN_CREATED.fetch_add(1, Ordering::Relaxed);
        Ok(Self)
    }
}
impl IncomingInterceptor for Incoming {
    const NOTIFICATIONS: bool = true;
    fn on_notification(&mut self, notification: InterceptorNotification<'_>) -> i32 {
        assert_ne!(notification.transport.identity(), 0);
        match notification.resource {
            NotificationResource::Transport(channel) => assert!(!channel.uri().is_empty()),
            NotificationResource::Publication { stream_id, .. } | NotificationResource::Image { stream_id, .. } => {
                assert_eq!(stream_id, 78)
            }
        }
        NOTIFIED.fetch_add(1, Ordering::Relaxed);
        0
    }

    fn on_datagram<'a>(&mut self, next: IncomingNext<'a>, packet: IncomingPacket<'a>) {
        assert!(!packet.bytes().is_empty());
        assert!(packet.address().to_socket_addr().is_some());
        let _ = packet.media_timestamp();
        RECEIVED.fetch_add(1, Ordering::Relaxed);
        next.receive(packet);
    }
}
fn eventually<T>(mut f: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    loop {
        if let Some(result) = f() {
            return result;
        }
        assert!(start.elapsed() < TIMEOUT, "driver test timed out");
        std::thread::yield_now();
    }
}
fn client(driver: &EmbeddedDriver) -> Aeron {
    let context = AeronContext::new().unwrap();
    context.set_dir(&driver.dir().into_c_string()).unwrap();
    let client = Aeron::new(&context).unwrap();
    client.start().unwrap();
    client
}
fn channel() -> std::ffi::CString {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    format!("aeron:udp?endpoint={}", socket.local_addr().unwrap()).into_c_string()
}
fn round_trip(client: &Aeron, channel: &std::ffi::CStr, count: u64) {
    let subscription = client
        .add_subscription(channel, 78, Handlers::NONE, Handlers::NONE, TIMEOUT)
        .unwrap();
    let publication = client.add_publication(channel, 78, TIMEOUT).unwrap();
    eventually(|| publication.is_connected().then_some(()));
    let mut offered = 0u64;
    let mut received = 0u64;
    eventually(|| {
        if offered < count {
            match publication.offer(&offered.to_le_bytes()) {
                Ok(_) => offered += 1,
                Err(error) => assert!(error.is_retryable(), "{error:?}"),
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
        (received == count).then_some(())
    });
}

#[test]
#[serial_test::serial(driver_extensions)]
fn typed_callbacks_deliver_udp_and_drop_once_in_each_threading_mode() {
    use rusteron_media_driver::AeronThreadingMode;
    for mode in [
        AeronThreadingMode::Shared,
        AeronThreadingMode::SharedNetwork,
        AeronThreadingMode::Dedicated,
    ] {
        let created = CREATED.load(Ordering::Relaxed);
        let dropped = DROPPED.load(Ordering::Relaxed);
        let out_created = OUT_CREATED.load(Ordering::Relaxed);
        let out_dropped = OUT_DROPPED.load(Ordering::Relaxed);
        let in_created = IN_CREATED.load(Ordering::Relaxed);
        let in_dropped = IN_DROPPED.load(Ordering::Relaxed);
        let driver = EmbeddedDriver::launch_with(|context| {
            context.set_threading_mode(mode)?;
            context.set_unicast_flow_control::<CountedFlow>()?;
            context.set_congestion_control::<StaticWindow>()?;
            context.add_outgoing_interceptor::<Out>()?;
            context.add_incoming_interceptor::<Incoming>()?;
            Ok(())
        })
        .unwrap();
        let client = client(&driver);
        round_trip(&client, &channel(), 128);
        drop(client);
        drop(driver);
        assert_eq!(CREATED.load(Ordering::Relaxed) - created, 1);
        assert_eq!(DROPPED.load(Ordering::Relaxed) - dropped, 1);
        assert_eq!(
            OUT_CREATED.load(Ordering::Relaxed) - out_created,
            OUT_DROPPED.load(Ordering::Relaxed) - out_dropped
        );
        assert_eq!(
            IN_CREATED.load(Ordering::Relaxed) - in_created,
            IN_DROPPED.load(Ordering::Relaxed) - in_dropped
        );
    }
    assert!(IDLE.load(Ordering::Relaxed) > 0);
    assert!(STATUS.load(Ordering::Relaxed) > 0);
    assert!(SENT.load(Ordering::Relaxed) > 0);
    assert!(RECEIVED.load(Ordering::Relaxed) > 0);
    assert!(NOTIFIED.load(Ordering::Relaxed) > 0);
}

#[derive(Default)]
struct PacketCounts {
    sent: AtomicUsize,
    received: AtomicUsize,
}
struct CountInto(std::sync::Arc<PacketCounts>);
impl InterceptorStateFactory for CountInto {
    type State = std::sync::Arc<PacketCounts>;
    fn new(state: &Self::State, _: StrategyContext, _: TransportAffinity) -> Result<Self, AeronCError> {
        Ok(Self(state.clone()))
    }
}
impl OutgoingInterceptor for CountInto {
    fn inspect(&mut self, packet: &OutgoingPacket<'_>) {
        self.0.sent.fetch_add(packet.len(), Ordering::Relaxed);
    }
}
impl IncomingInterceptor for CountInto {
    fn inspect(&mut self, _: &IncomingPacket<'_>) {
        self.0.received.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
#[serial_test::serial(driver_extensions)]
fn stateful_interceptors_report_to_the_state_of_their_own_driver() {
    let counted = std::sync::Arc::new(PacketCounts::default());
    let idle = std::sync::Arc::new(PacketCounts::default());
    let launch = |counts: &std::sync::Arc<PacketCounts>| {
        let counts = counts.clone();
        EmbeddedDriver::launch_with(move |context| {
            context.add_outgoing_interceptor_with_state::<CountInto>(counts.clone())?;
            context.add_incoming_interceptor_with_state::<CountInto>(counts.clone())?;
            context.set_name_resolver_fn(|_, _, _| Ok(std::net::Ipv4Addr::LOCALHOST.into()))
        })
        .unwrap()
    };
    let driver = launch(&counted);
    let other = launch(&idle);
    let client = client(&driver);
    round_trip(&client, &channel(), 64);
    drop(client);
    drop(driver);
    drop(other);
    assert!(counted.sent.load(Ordering::Relaxed) > 0);
    assert!(counted.received.load(Ordering::Relaxed) > 0);
    assert_eq!(idle.sent.load(Ordering::Relaxed), 0);
    assert_eq!(idle.received.load(Ordering::Relaxed), 0);
}

#[test]
#[serial_test::serial(driver_extensions)]
fn documented_extensions_deliver_udp_in_every_threading_mode() {
    for mode in [
        rusteron_media_driver::AeronThreadingMode::Shared,
        rusteron_media_driver::AeronThreadingMode::SharedNetwork,
        rusteron_media_driver::AeronThreadingMode::Dedicated,
        rusteron_media_driver::AeronThreadingMode::Invoker,
    ] {
        let requests = PORT_REQUESTS.load(Ordering::Relaxed);
        let releases = PORT_RELEASES.load(Ordering::Relaxed);
        let counters = [
            &FLOW_MESSAGES,
            &REBUILDS,
            &OUTGOING_BATCHES,
            &INCOMING_PACKETS,
            &RESOLUTIONS,
            &IDLE_CALLS,
            &TRANSPORT_SENDS,
        ];
        let before = counters.map(|counter| counter.load(Ordering::Relaxed));
        let driver = EmbeddedDriver::launch_with(|context| {
            context.set_threading_mode(mode)?;
            for (_, configure, _) in EXAMPLES {
                configure(context)?;
            }
            Ok(())
        })
        .unwrap();
        let client = client(&driver);
        let named = channel()
            .to_str()
            .unwrap()
            .replace("127.0.0.1", "example.local")
            .into_c_string();
        round_trip(&client, &named, 128);
        drop(client);
        drop(driver);
        for (counter, before) in counters.into_iter().zip(before) {
            assert!(
                counter.load(Ordering::Relaxed) > before,
                "missing example callback in {mode:?}"
            );
        }
        assert!(MAX_CYCLE_NS.load(Ordering::Relaxed) > 0);
        assert!(PORT_REQUESTS.load(Ordering::Relaxed) > requests);
        assert_eq!(
            PORT_REQUESTS.load(Ordering::Relaxed) - requests,
            PORT_RELEASES.load(Ordering::Relaxed) - releases
        );
    }
}

#[test]
#[serial_test::serial(driver_extensions)]
fn each_example_works_without_other_extensions() {
    for (name, configure, counter) in EXAMPLES {
        // A maximum need not exceed a previous driver's maximum.
        if name == "duty_cycle_tracker" {
            MAX_CYCLE_NS.store(0, Ordering::Relaxed);
        }
        let before = counter.load(Ordering::Relaxed);
        let requests = PORT_REQUESTS.load(Ordering::Relaxed);
        let releases = PORT_RELEASES.load(Ordering::Relaxed);
        let driver = EmbeddedDriver::launch_with(configure).unwrap();
        let client = client(&driver);
        let mut uri = channel().to_str().unwrap().to_owned();
        if name == "name_resolver" {
            uri = uri.replace("127.0.0.1", "example.local");
        }
        round_trip(&client, &uri.into_c_string(), 128);
        drop(client);
        drop(driver);
        assert!(counter.load(Ordering::Relaxed) > before, "{name} received no callbacks");
        if name == "port_manager" {
            assert_eq!(
                PORT_REQUESTS.load(Ordering::Relaxed) - requests,
                PORT_RELEASES.load(Ordering::Relaxed) - releases
            );
        }
    }
}

#[derive(Default)]
struct AgentEvents {
    idle: [AtomicUsize; 6],
    updates: [AtomicUsize; 4],
    measurements: [AtomicUsize; 4],
    drops: AtomicUsize,
}
struct AgentProbe {
    events: std::sync::Arc<AgentEvents>,
    index: usize,
}
impl Drop for AgentProbe {
    fn drop(&mut self) {
        self.events.drops.fetch_add(1, Ordering::Relaxed);
    }
}
impl rusteron_media_driver::extensions::IdleStrategy for AgentProbe {
    fn idle(&mut self, work: i32) {
        self.events.idle[self.index].fetch_add(1, Ordering::Relaxed);
        if work <= 0 {
            std::thread::yield_now();
        }
    }
}
impl DutyCycleTracker for AgentProbe {
    fn update(&mut self, _: i64) {
        self.events.updates[self.index].fetch_add(1, Ordering::Relaxed);
    }
    fn measure_and_update(&mut self, _: i64) {
        self.events.measurements[self.index].fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
#[serial_test::serial(driver_extensions)]
fn added_duty_trackers_preserve_native_stall_counters_for_every_role() {
    let context = AeronDriverContext::new().unwrap();
    context
        .set_dir(&format!("/tmp/rusteron-duty-observer-{}", std::process::id()).into_c_string())
        .unwrap();
    context.set_dir_delete_on_start(true).unwrap();
    context.set_dir_delete_on_shutdown(true).unwrap();
    context.set_threading_mode(AeronThreadingMode::Invoker).unwrap();
    context.set_conductor_cycle_threshold_ns(1);
    context.set_sender_cycle_threshold_ns(1);
    context.set_receiver_cycle_threshold_ns(1);
    context.set_name_resolver_threshold_ns(1);

    let observations = std::sync::Arc::new(AgentEvents::default());
    for (index, role) in [
        DutyCycleRole::Conductor,
        DutyCycleRole::Sender,
        DutyCycleRole::Receiver,
        DutyCycleRole::NameResolver,
    ]
    .into_iter()
    .enumerate()
    {
        for _ in 0..2 {
            context
                .add_duty_cycle_tracker(
                    role,
                    AgentProbe {
                        events: observations.clone(),
                        index,
                    },
                )
                .unwrap();
        }
    }

    let driver = AeronDriver::new(&context).unwrap();
    assert!(
        context
            .add_duty_cycle_tracker(DutyCycleRole::Conductor, DutyCycleTimer::new(|_| {}))
            .is_err()
    );
    let _driver = driver.start_manual().unwrap();
    // INVOKER runs no agents in the background. The driver has initialized
    // the real native counters, and no other callbacks run during these calls.
    unsafe {
        let raw = context.get_inner();
        for (index, (table, stall)) in [
            (
                (*raw).conductor_duty_cycle_tracker,
                std::ptr::addr_of_mut!((*raw).conductor_duty_cycle_stall_tracker),
            ),
            (
                (*raw).sender_duty_cycle_tracker,
                std::ptr::addr_of_mut!((*raw).sender_duty_cycle_stall_tracker),
            ),
            (
                (*raw).receiver_duty_cycle_tracker,
                std::ptr::addr_of_mut!((*raw).receiver_duty_cycle_stall_tracker),
            ),
            (
                (*raw).name_resolver_time_tracker,
                std::ptr::addr_of_mut!((*raw).name_resolver_time_stall_tracker),
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let max = std::sync::atomic::AtomicI64::from_ptr((*stall).max_cycle_time_counter);
            let exceeded = std::sync::atomic::AtomicI64::from_ptr((*stall).cycle_time_threshold_exceeded_counter);
            let before = exceeded.load(Ordering::Acquire);
            let measurements = observations.measurements[index].load(Ordering::Relaxed);
            (*table).update.unwrap()((*table).state, 10);
            (*table).measure_and_update.unwrap()((*table).state, 110);
            assert!(max.load(Ordering::Acquire) >= 100);
            assert_eq!(exceeded.load(Ordering::Acquire), before + 1);
            assert_eq!(
                observations.measurements[index].load(Ordering::Relaxed),
                measurements + 2
            );
        }
    }
}

#[test]
#[serial_test::serial(driver_extensions)]
fn native_agents_use_their_own_idle_state_and_trackers_until_shutdown() {
    use rusteron_media_driver::AeronThreadingMode as Mode;
    for (mode, active) in [
        (Mode::Shared, [false, false, false, false, true, false]),
        (Mode::SharedNetwork, [true, false, false, true, false, true]),
        (Mode::Dedicated, [true, true, true, false, false, true]),
        (Mode::Invoker, [false, false, false, false, true, false]),
    ] {
        let events = std::sync::Arc::new(AgentEvents::default());
        let driver = EmbeddedDriver::launch_with(|context| {
            context.set_threading_mode(mode)?;
            for (index, role) in [
                IdleStrategyRole::Conductor,
                IdleStrategyRole::Sender,
                IdleStrategyRole::Receiver,
                IdleStrategyRole::SharedNetwork,
                IdleStrategyRole::Shared,
                IdleStrategyRole::NativeResourceAgent,
            ]
            .into_iter()
            .enumerate()
            {
                context.set_idle_strategy(
                    role,
                    AgentProbe {
                        events: events.clone(),
                        index,
                    },
                )?;
            }
            for (index, role) in [
                DutyCycleRole::Conductor,
                DutyCycleRole::Sender,
                DutyCycleRole::Receiver,
                DutyCycleRole::NameResolver,
            ]
            .into_iter()
            .enumerate()
            {
                context.set_duty_cycle_tracker(
                    role,
                    AgentProbe {
                        events: events.clone(),
                        index,
                    },
                )?;
            }
            Ok(())
        })
        .unwrap();
        let client = client(&driver);
        round_trip(&client, &channel(), 32);
        eventually(|| {
            active
                .iter()
                .enumerate()
                .all(|(index, active)| !active || events.idle[index].load(Ordering::Relaxed) > 0)
                .then_some(())
        });
        assert_eq!(events.drops.load(Ordering::Relaxed), 0);
        drop(client);
        drop(driver);
        assert_eq!(events.drops.load(Ordering::Relaxed), 10);
        for (index, active) in active.into_iter().enumerate() {
            assert_eq!(
                events.idle[index].load(Ordering::Relaxed) > 0,
                active,
                "idle role {index} in {mode:?}"
            );
        }
        for index in 0..4 {
            assert!(
                events.updates[index].load(Ordering::Relaxed) > 0,
                "tracker update {index} in {mode:?}"
            );
            assert!(
                events.measurements[index].load(Ordering::Relaxed) > 0,
                "tracker measurement {index} in {mode:?}"
            );
        }
    }
}

static TRANSPORT_INITS: [AtomicUsize; 3] = [const { AtomicUsize::new(0) }; 3];
static DATA_CLOSES: AtomicUsize = AtomicUsize::new(0);
static RESOLVER_CLOSES: AtomicUsize = AtomicUsize::new(0);
static POLLER_CLOSES: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn counted_transport_init(
    transport: *mut native::aeron_udp_channel_transport_t,
    bind: *mut native::sockaddr_storage,
    multicast: *mut native::sockaddr_storage,
    connect: *mut native::sockaddr_storage,
    params: *mut native::aeron_udp_channel_transport_params_t,
    context: *mut native::aeron_driver_context_t,
    affinity: TransportAffinity,
) -> i32 {
    let result = unsafe {
        native::aeron_udp_channel_transport_init(transport, bind, multicast, connect, params, context, affinity)
    };
    if result >= 0 {
        TRANSPORT_INITS[affinity as usize].fetch_add(1, Ordering::Relaxed);
    }
    result
}
unsafe extern "C" fn data_close(transport: *mut native::aeron_udp_channel_transport_t) -> i32 {
    DATA_CLOSES.fetch_add(1, Ordering::Relaxed);
    unsafe { native::aeron_udp_channel_transport_close(transport) }
}
unsafe extern "C" fn resolver_close(transport: *mut native::aeron_udp_channel_transport_t) -> i32 {
    RESOLVER_CLOSES.fetch_add(1, Ordering::Relaxed);
    unsafe { native::aeron_udp_channel_transport_close(transport) }
}
unsafe extern "C" fn counted_poller_close(poller: *mut native::aeron_udp_transport_poller_t) -> i32 {
    POLLER_CLOSES.fetch_add(1, Ordering::Relaxed);
    unsafe { native::aeron_udp_transport_poller_close(poller) }
}

#[test]
#[serial_test::serial(driver_extensions)]
fn native_transport_tables_cover_data_and_driver_resolver_sockets_and_pollers() {
    let before = TRANSPORT_INITS.each_ref().map(|count| count.load(Ordering::Relaxed));
    let closes = [
        DATA_CLOSES.load(Ordering::Relaxed),
        RESOLVER_CLOSES.load(Ordering::Relaxed),
        POLLER_CLOSES.load(Ordering::Relaxed),
    ];
    let events = std::sync::Arc::new(AgentEvents::default());
    let driver = EmbeddedDriver::launch_with(|context| {
        context.set_resolver_name(c"transport-test")?;
        context.set_resolver_interface(c"127.0.0.1:0")?;
        unsafe {
            context.set_name_resolver_supplier(native::aeron_name_resolver_supplier_load(c"driver".as_ptr()))?;
        }
        for role in [TransportRole::Data, TransportRole::Conductor] {
            let mut callbacks = NativeTransportBindings::udp().callbacks();
            callbacks.init_func = Some(counted_transport_init);
            callbacks.close_func = Some(if role == TransportRole::Data {
                data_close
            } else {
                resolver_close
            });
            callbacks.poller_close_func = Some(counted_poller_close);
            // SAFETY: all replacements delegate to UDP with unchanged arguments.
            let bindings = unsafe {
                NativeTransportBindings::from_callbacks(
                    callbacks,
                    AgentProbe {
                        events: events.clone(),
                        index: 0,
                    },
                )
            }?;
            context.set_native_transport_bindings(role, bindings)?;
        }
        Ok(())
    })
    .unwrap();
    let client = client(&driver);
    round_trip(&client, &channel(), 32);
    assert_eq!(events.drops.load(Ordering::Relaxed), 0);
    drop(client);
    drop(driver);
    let initialized = TRANSPORT_INITS.each_ref().map(|count| count.load(Ordering::Relaxed));
    assert_eq!(
        initialized
            .iter()
            .zip(before)
            .map(|(after, before)| after - before)
            .collect::<Vec<_>>(),
        [1, 1, 1]
    );
    assert_eq!(DATA_CLOSES.load(Ordering::Relaxed) - closes[0], 2);
    assert_eq!(RESOLVER_CLOSES.load(Ordering::Relaxed) - closes[1], 1);
    assert_eq!(POLLER_CLOSES.load(Ordering::Relaxed) - closes[2], 3);
    assert_eq!(events.drops.load(Ordering::Relaxed), 2);
}

struct CountPorts(std::sync::Arc<AgentEvents>);
impl PortManager for CountPorts {
    fn get_managed_port(
        &mut self,
        _: ChannelRef<'_>,
        requested: std::net::SocketAddr,
    ) -> Result<u16, rusteron_media_driver::AeronCError> {
        self.0.updates[0].fetch_add(1, Ordering::Relaxed);
        Ok(requested.port())
    }
    fn free_managed_port(&mut self, _: std::net::SocketAddr) {
        self.0.measurements[0].fetch_add(1, Ordering::Relaxed);
    }
}
unsafe extern "C" fn reject_transport_init(
    _: *mut native::aeron_udp_channel_transport_t,
    _: *mut native::sockaddr_storage,
    _: *mut native::sockaddr_storage,
    _: *mut native::sockaddr_storage,
    _: *mut native::aeron_udp_channel_transport_params_t,
    _: *mut native::aeron_driver_context_t,
    _: TransportAffinity,
) -> i32 {
    unsafe {
        native::aeron_err_set(
            22,
            c"test".as_ptr(),
            c"reject_transport_init".as_ptr(),
            0,
            c"socket setup rejected".as_ptr(),
        );
    }
    -1
}

#[test]
#[serial_test::serial(driver_extensions)]
fn failed_native_socket_setup_releases_the_reserved_port() {
    let events = std::sync::Arc::new(AgentEvents::default());
    let driver = EmbeddedDriver::launch_with(|context| {
        context.set_port_manager(PortManagerRole::Sender, CountPorts(events.clone()))?;
        let mut callbacks = NativeTransportBindings::udp().callbacks();
        callbacks.init_func = Some(reject_transport_init);
        // SAFETY: the callback returns an error without acquiring resources or
        // changing the transport. Native initialization already set fd to -1.
        let bindings = unsafe { NativeTransportBindings::from_callbacks(callbacks, ()) }?;
        context.set_native_transport_bindings(TransportRole::Data, bindings)
    })
    .unwrap();
    let client = client(&driver);
    assert!(client.add_publication(&channel(), 78, TIMEOUT).is_err());
    drop(client);
    drop(driver);
    assert_eq!(events.updates[0].load(Ordering::Relaxed), 1);
    assert_eq!(events.measurements[0].load(Ordering::Relaxed), 1);
}

#[test]
#[serial_test::serial(driver_extensions)]
fn seeded_loss_recovers_all_sequences_and_records_retransmissions() {
    let driver = EmbeddedDriver::launch_with(|context| {
        context.set_threading_mode(rusteron_media_driver::AeronThreadingMode::Dedicated)?;
        packet_loss::configure(context)?;
        context.add_outgoing_interceptor::<Out>()?;
        context.add_incoming_interceptor::<Incoming>()?;
        Ok(())
    })
    .unwrap();
    let client = client(&driver);
    round_trip(&client, &channel(), 1024);
    let reader = client.counters_reader();
    assert!(
        reader
            .system_counter_value(AeronSystemCounterType::NakMessagesSent)
            .unwrap()
            > 0
    );
    assert!(
        reader
            .system_counter_value(AeronSystemCounterType::RetransmitsSent)
            .unwrap()
            > 0
    );
}

#[test]
#[serial_test::serial(driver_extensions)]
fn bounded_archive_wait_releases_a_healthy_non_group_subscriber_and_records_a_gap() {
    let driver = EmbeddedDriver::launch_with(|context| {
        context.set_threading_mode(rusteron_media_driver::AeronThreadingMode::Dedicated)?;
        bounded_flow_control::configure(context)?;
        Ok(())
    })
    .unwrap();
    let client = client(&driver);
    round_trip(&client, &channel(), 128);
    let mut degraded = 0;
    client
        .counters_reader()
        .foreach_counter_fn(|value, _, type_id, _, label| {
            if type_id == 20001 && label.starts_with("bounded tagged flow control degradations") {
                degraded += value;
            }
        });
    assert_eq!(degraded, 1);
}

static ORIGINAL_SUPPLIER: std::sync::Mutex<native::aeron_flow_control_strategy_supplier_func_t> =
    std::sync::Mutex::new(None);

// Fault injection fills the counter manager after the Rust supplier succeeds.
// Aeron's later publication setup must call the returned strategy's finalizer.
unsafe extern "C" fn exhaust_after_supplier(
    out: *mut *mut native::aeron_flow_control_strategy_t,
    context: *mut native::aeron_driver_context_t,
    counters: *mut native::aeron_counters_manager_t,
    channel: *const native::aeron_udp_channel_t,
    stream: i32,
    session: i32,
    registration: i64,
    initial: i32,
    term_length: usize,
) -> i32 {
    use native::*;
    let original = ORIGINAL_SUPPLIER.lock().unwrap().unwrap();
    let result = unsafe {
        original(
            out,
            context,
            counters,
            channel,
            stream,
            session,
            registration,
            initial,
            term_length,
        )
    };
    if result == 0 {
        while unsafe {
            aeron_counters_manager_allocate(counters, 20002, std::ptr::null(), 0, c"fault injection".as_ptr(), 15)
        } >= 0
        {}
    }
    result
}

#[test]
#[serial_test::serial(driver_extensions)]
fn publication_setup_failure_after_supplier_success_finalizes_once() {
    let created = CREATED.load(Ordering::Relaxed);
    let dropped = DROPPED.load(Ordering::Relaxed);
    let driver = EmbeddedDriver::launch_with(|context| {
        context.set_unicast_flow_control::<CountedFlow>()?;
        *ORIGINAL_SUPPLIER.lock().unwrap() = context.get_unicast_flowcontrol_supplier();
        // SAFETY: this test supplier forwards all arguments to the typed supplier,
        // then exhausts the manager while the conductor owns it.
        unsafe {
            context.set_unicast_flowcontrol_supplier(Some(exhaust_after_supplier))?;
        }
        Ok(())
    })
    .unwrap();
    let client = client(&driver);
    let error = client.add_publication(&channel(), 78, TIMEOUT).unwrap_err();
    assert!(!error.to_string().is_empty());
    eventually(|| (DROPPED.load(Ordering::Relaxed) > dropped).then_some(()));
    assert_eq!(CREATED.load(Ordering::Relaxed) - created, 1);
    assert_eq!(DROPPED.load(Ordering::Relaxed) - dropped, 1);
    drop(client);
    drop(driver);
    assert_eq!(DROPPED.load(Ordering::Relaxed) - dropped, 1);
}

#[test]
#[serial_test::serial(driver_extensions)]
fn multicast_publication_uses_the_typed_supplier_without_fc_override() {
    let created = CREATED.load(Ordering::Relaxed);
    let driver = EmbeddedDriver::launch_with(|context| {
        context.set_multicast_flow_control::<CountedFlow>()?;
        Ok(())
    })
    .unwrap();
    let client = client(&driver);
    let port = std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let channel = format!("aeron:udp?endpoint=224.0.1.1:{port}|interface=127.0.0.1").into_c_string();
    round_trip(&client, &channel, 16);
    assert_eq!(CREATED.load(Ordering::Relaxed) - created, 1);
}

static RESOLVERS_CREATED: AtomicUsize = AtomicUsize::new(0);
static RESOLVERS_DROPPED: AtomicUsize = AtomicUsize::new(0);
static RESOLVED: AtomicUsize = AtomicUsize::new(0);
struct LoopbackResolver;
impl Drop for LoopbackResolver {
    fn drop(&mut self) {
        RESOLVERS_DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}
impl NameResolverFactory for LoopbackResolver {
    fn new(args: &std::ffi::CStr) -> Result<Self, rusteron_media_driver::AeronCError> {
        assert_eq!(args, c"loopback");
        RESOLVERS_CREATED.fetch_add(1, Ordering::Relaxed);
        Ok(Self)
    }
}
impl NameResolver for LoopbackResolver {
    fn resolve(
        &mut self,
        name: &std::ffi::CStr,
        _: &std::ffi::CStr,
        _: bool,
    ) -> Result<std::net::IpAddr, rusteron_media_driver::AeronCError> {
        assert_eq!(name, c"typed-test.invalid");
        RESOLVED.fetch_add(1, Ordering::Relaxed);
        Ok(std::net::Ipv4Addr::LOCALHOST.into())
    }
}
#[test]
#[serial_test::serial(driver_extensions)]
fn typed_resolver_connects_a_named_udp_endpoint_and_closes_once() {
    let created = RESOLVERS_CREATED.load(Ordering::Relaxed);
    let dropped = RESOLVERS_DROPPED.load(Ordering::Relaxed);
    let driver = EmbeddedDriver::launch_with(|context| {
        context.set_name_resolver_init_args(c"loopback")?;
        context.set_name_resolver::<LoopbackResolver>()?;
        Ok(())
    })
    .unwrap();
    let client = client(&driver);
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let channel = format!("aeron:udp?endpoint=typed-test.invalid:{port}").into_c_string();
    round_trip(&client, &channel, 128);
    drop(client);
    drop(driver);
    assert_eq!(RESOLVERS_CREATED.load(Ordering::Relaxed) - created, 1);
    assert_eq!(RESOLVERS_DROPPED.load(Ordering::Relaxed) - dropped, 1);
    assert!(RESOLVED.load(Ordering::Relaxed) >= 2);
}

#[test]
#[serial_test::serial(driver_extensions)]
fn captured_closures_deliver_udp_and_release_their_state_in_every_threading_mode() {
    use rusteron_media_driver::AeronThreadingMode as Mode;
    for mode in [Mode::Shared, Mode::SharedNetwork, Mode::Dedicated, Mode::Invoker] {
        let events = std::sync::Arc::new(AgentEvents::default());
        let resolutions = std::sync::Arc::new(AtomicUsize::new(0));
        let hostname = c"closures.invalid".to_owned();
        let driver = EmbeddedDriver::launch_with(|context| {
            context.set_threading_mode(mode)?;
            for (index, role) in [
                IdleStrategyRole::Conductor,
                IdleStrategyRole::Sender,
                IdleStrategyRole::Receiver,
                IdleStrategyRole::SharedNetwork,
                IdleStrategyRole::Shared,
                IdleStrategyRole::NativeResourceAgent,
            ]
            .into_iter()
            .enumerate()
            {
                let mut probe = AgentProbe {
                    events: events.clone(),
                    index,
                };
                context.set_idle_strategy(role, move |work| probe.idle(work))?;
            }
            for (index, role) in [
                DutyCycleRole::Conductor,
                DutyCycleRole::Sender,
                DutyCycleRole::Receiver,
                DutyCycleRole::NameResolver,
            ]
            .into_iter()
            .enumerate()
            {
                let mut probe = AgentProbe {
                    events: events.clone(),
                    index,
                };
                context.set_duty_cycle_tracker(
                    role,
                    DutyCycleTimer::new(move |elapsed| {
                        assert!(elapsed >= 0);
                        probe.measure_and_update(elapsed);
                    }),
                )?;
            }
            for (index, role) in [PortManagerRole::Sender, PortManagerRole::Receiver]
                .into_iter()
                .enumerate()
            {
                let mut probe = AgentProbe {
                    events: events.clone(),
                    index,
                };
                context.set_port_manager(role, move |channel: ChannelRef<'_>, address: std::net::SocketAddr| {
                    assert!(!channel.uri().is_empty());
                    probe.update(i64::from(address.port()));
                    Ok(address.port())
                })?;
            }
            let mut probe = AgentProbe {
                events: events.clone(),
                index: 0,
            };
            let resolutions = resolutions.clone();
            context.set_name_resolver_fn(move |name, parameter, _| {
                assert_eq!(name, hostname.as_c_str());
                assert_eq!(parameter, c"endpoint");
                resolutions.fetch_add(1, Ordering::Relaxed);
                probe.measure_and_update(0);
                Ok(std::net::Ipv4Addr::LOCALHOST.into())
            })?;
            Ok(())
        })
        .unwrap();
        let client = client(&driver);
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let channel = format!("aeron:udp?endpoint=closures.invalid:{port}").into_c_string();
        round_trip(&client, &channel, 128);
        assert!(resolutions.load(Ordering::Relaxed) >= 2);
        assert!(events.idle.iter().any(|count| count.load(Ordering::Relaxed) > 0));
        for count in &events.measurements {
            assert!(count.load(Ordering::Relaxed) > 0);
        }
        assert!(events.updates[0].load(Ordering::Relaxed) > 0);
        assert!(events.updates[1].load(Ordering::Relaxed) > 0);
        assert_eq!(events.drops.load(Ordering::Relaxed), 0);
        drop(client);
        drop(driver);
        assert_eq!(events.drops.load(Ordering::Relaxed), 13, "closure state in {mode:?}");
    }
}
