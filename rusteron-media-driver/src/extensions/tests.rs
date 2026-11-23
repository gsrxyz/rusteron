use super::*;
use super::{bounded_example::BoundedTaggedFlowControl, packet_loss_example::SeededLoss};
use crate::{AeronDriver, AeronThreadingMode, IntoCString};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn native_result_checks_keep_codes_without_capturing_messages() {
    for code in [i32::MIN, -7, -1] {
        let error = check(code).unwrap_err();
        assert_eq!(error.code, code);
        assert_eq!(error.message(), None);
    }
    for result in [0, 1, i32::MAX] {
        assert!(check(result).is_ok());
    }
}

#[test]
fn sender_metrics_count_transitions_and_wrap_without_allocating() {
    use std::sync::atomic::AtomicI64;
    let counter = AtomicI64::new(0);
    let metrics = unsafe { super::flow::test_metrics_at(counter.as_ptr()) };
    let disabled = unsafe { super::flow::test_metrics_at(std::ptr::null_mut()) };
    crate::test_alloc::assert_no_heap_allocation(|| {
        for _ in 0..3 {
            metrics.increment();
        }
        assert_eq!(counter.load(Ordering::Acquire), 3);
        disabled.increment();
        assert_eq!(counter.load(Ordering::Acquire), 3);
        counter.store(i64::MAX, Ordering::Relaxed);
        metrics.increment();
        assert_eq!(counter.load(Ordering::Acquire), i64::MIN);
        metrics.increment();
        assert_eq!(counter.load(Ordering::Acquire), i64::MIN + 1);
    });
}

#[test]
#[cfg(not(feature = "backtrace"))]
fn extension_error_paths_do_not_allocate_on_the_rust_heap() {
    // Initialise Aeron's per-thread error state before measuring Rust costs.
    unsafe { supplier_error(AeronCError::with_message(-1, "initial error")) };
    crate::test_alloc::assert_no_heap_allocation(|| {
        assert!(check(0).is_ok());
        assert_eq!(check(-7).unwrap_err().code, -7);
        unsafe {
            supplier_error(AeronCError::from_code(-8));
            supplier_error(AeronCError::with_message(-9, "%s\0literal"));
        }
    });
    unsafe { aeron_err_clear() };
}

#[test]
fn callback_error_reporting_handles_native_text_nuls_and_long_messages() {
    unsafe {
        supplier_error(AeronCError::with_message(-7, "%s\0literal"));
        let message = CStr::from_ptr(aeron_errmsg()).to_str().unwrap();
        assert!(message.contains("Rust extension error -7: %s\\0literal"));

        // A code-only error reads the same native buffer the adapter overwrites.
        supplier_error(AeronCError::from_code(-8));
        let message = CStr::from_ptr(aeron_errmsg()).to_str().unwrap();
        assert!(message.contains("Rust extension error -8:"));
        assert!(message.contains("%s\\0literal"));

        let long_message = "x".repeat(AERON_ERROR_MAX_TOTAL_LENGTH as usize * 2);
        supplier_error(AeronCError::with_message(-9, long_message));
        let message = CStr::from_ptr(aeron_errmsg()).to_bytes();
        assert!(message.len() < AERON_ERROR_MAX_TOTAL_LENGTH as usize);
        assert!(message.windows(2).any(|pair| pair == b"xx"));
        aeron_err_clear();
    }
}

fn args<'a>(channel: &'a aeron_udp_channel_t, context: &AeronDriverContext) -> FlowControlArgs<'a> {
    FlowControlArgs {
        channel: unsafe { ChannelRef::from_raw(channel) },
        context: unsafe { StrategyContext::from_raw(context.get_inner()) },
        stream_id: 7,
        session_id: 8,
        registration_id: 9,
        initial_term_id: 10,
        term_length: 65536,
    }
}
fn status(receiver: i64, tag: i64, offset: i32) -> [u8; 44] {
    let mut bytes = [0; 44];
    bytes[..4].copy_from_slice(&44i32.to_le_bytes());
    bytes[16..20].copy_from_slice(&10i32.to_le_bytes());
    bytes[20..24].copy_from_slice(&offset.to_le_bytes());
    bytes[24..28].copy_from_slice(&1024i32.to_le_bytes());
    bytes[28..36].copy_from_slice(&receiver.to_le_bytes());
    bytes[36..44].copy_from_slice(&tag.to_le_bytes());
    bytes
}

#[test]
fn views_read_unaligned_fields_and_do_not_trust_frame_length() {
    let mut allocation = [0u8; 46];
    allocation[1..45].copy_from_slice(&status(i64::MIN, 555, 128));
    let view = StatusMessage::from_bytes(&allocation[1..45]).unwrap();
    assert_eq!(view.receiver_id(), i64::MIN);
    assert_eq!(view.group_tag(), Some(555));
    assert_eq!(view.position(9, 16), 65536 + 128);
    assert!(StatusMessage::from_bytes(&allocation[..35]).is_none());
    for frame_length in [0i32, 35, 36, 43, 45, i32::MAX, -1] {
        allocation[1..5].copy_from_slice(&frame_length.to_le_bytes());
        assert_eq!(StatusMessage::from_bytes(&allocation[1..45]).unwrap().group_tag(), None);
    }
    allocation[1..5].copy_from_slice(&44i32.to_le_bytes());
    assert_eq!(StatusMessage::from_bytes(&allocation[1..37]).unwrap().group_tag(), None);
}

#[test]
fn setup_and_error_views_use_their_own_offsets() {
    let mut bytes = [0u8; 48];
    for index in 0..12 {
        bytes[index * 4..index * 4 + 4].copy_from_slice(&(index as i32).to_le_bytes());
    }
    let setup = SetupMessage::from_bytes(&bytes).unwrap();
    assert_eq!(setup.term_offset(), 2);
    assert_eq!(setup.session_id(), 3);
    assert_eq!(setup.stream_id(), 4);
    assert_eq!(setup.initial_term_id(), 5);
    assert_eq!(setup.active_term_id(), 6);
    assert_eq!(setup.term_length(), 7);
    assert_eq!(setup.mtu(), 8);
    bytes[36..40].copy_from_slice(&i32::MAX.to_le_bytes());
    assert_eq!(ErrorMessage::from_bytes(&bytes).unwrap().message(), &bytes[40..]);
    bytes[36..40].copy_from_slice(&(-1i32).to_le_bytes());
    assert!(ErrorMessage::from_bytes(&bytes).unwrap().message().is_empty());
}

#[test]
fn safe_numeric_results_cannot_create_negative_limits_or_windows() {
    for input in [i64::MIN, -1, 0, 1, i64::MAX] {
        assert_eq!(Position::new(input).get(), input.max(0));
    }
    for input in [i32::MIN, -1, 0, 1, i32::MAX] {
        assert_eq!(ReceiverWindow::new(input).get(), input.max(1));
    }
}

#[test]
fn maximum_flow_control_advances_to_the_receiver_window_without_reducing_the_limit() {
    let context = AeronDriverContext::new().unwrap();
    let channel = aeron_udp_channel_t::default();
    let (mut flow, shared) = MaxFlowControl::new(&args(&channel, &context)).unwrap();
    let address = sockaddr_storage::default();
    let mut message = status(1, 555, 128);
    let mut advance = |message: &[u8], limit| {
        flow.on_status_message(
            &shared,
            StatusMessage::from_bytes(message).unwrap(),
            unsafe { SockAddrRef::from_raw(&address) },
            limit,
            9,
            16,
            0,
            unsafe { super::flow::test_metrics_at(std::ptr::null_mut()) },
        )
        .get()
    };
    // Term 10 is one 64 KiB term beyond initial term 9, with a 1024-byte window.
    assert_eq!(advance(&message, 0), 65_536 + 128 + 1024);
    assert_eq!(advance(&message, 100_000), 100_000);
    message[16..20].copy_from_slice(&8i32.to_le_bytes());
    assert_eq!(advance(&message, 0), 0);
}

#[test]
fn address_views_round_trip_both_families() {
    for ip in ["127.0.0.1", "2001:db8::1234"] {
        let mut raw = sockaddr_storage::default();
        unsafe {
            super::resolver::write_ip_for_test(&mut raw, ip.parse().unwrap());
        }
        let addr = unsafe { SockAddrRef::from_raw(&raw) }.to_socket_addr().unwrap();
        assert_eq!(addr.ip(), ip.parse::<std::net::IpAddr>().unwrap());
        assert_eq!(addr.port(), 0);
    }
}

static DROPS: AtomicUsize = AtomicUsize::new(0);
struct CountDrop;
impl Drop for CountDrop {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::Relaxed);
    }
}
impl FlowControl for CountDrop {
    type Shared = ();
    fn new(args: &FlowControlArgs<'_>) -> Result<(Self, ()), AeronCError> {
        assert_eq!(args.stream_id, 7);
        Ok((Self, ()))
    }
    fn on_status_message(
        &mut self,
        _: &(),
        _: StatusMessage<'_>,
        _: SockAddrRef<'_>,
        limit: i64,
        _: i32,
        _: usize,
        _: i64,
        _: StrategyMetrics<'_>,
    ) -> Position {
        Position::new(limit)
    }
    fn max_retransmission_length(&mut self, _: &(), _: usize, _: usize, _: usize, _: usize, _: usize) -> usize {
        usize::MAX
    }
}
#[test]
fn flow_table_calls_every_slot_and_frees_the_box_once() {
    let context = AeronDriverContext::new().unwrap();
    context.set_unicast_flow_control::<CountDrop>().unwrap();
    let channel = aeron_udp_channel_t::default();
    let mut raw = std::ptr::null_mut();
    let before = DROPS.load(Ordering::Relaxed);
    unsafe {
        assert_eq!(
            aeron_driver_context_get_unicast_flowcontrol_supplier(context.get_inner()).unwrap()(
                &mut raw,
                context.get_inner(),
                std::ptr::null_mut(),
                &channel,
                7,
                8,
                9,
                10,
                65536
            ),
            0
        );
        let table = *raw;
        let mut addr = sockaddr_storage::default();
        let sm = status(1, 555, 0);
        assert_eq!(table.on_idle.unwrap()(table.state, 1, -1, 0, false), 0);
        assert_eq!(
            table.on_status_message.unwrap()(table.state, sm.as_ptr(), sm.len(), &mut addr, 55, 10, 16, 1),
            55
        );
        assert_eq!(table.on_setup.unwrap()(table.state, sm.as_ptr(), 40, 1, 55, 16, 0), 55);
        table.on_error.unwrap()(table.state, sm.as_ptr(), 40, &mut addr, 1);
        table.on_trigger_send_setup.unwrap()(table.state, sm.as_ptr(), 44, &mut addr, 1);
        assert!(table.has_required_receivers.unwrap()(raw));
        assert_eq!(
            table.max_retransmission_length.unwrap()(table.state, 65000, 1000, 65536, 1408),
            536
        );
        assert_eq!(
            table.max_retransmission_length.unwrap()(table.state, usize::MAX, 1000, 65536, 1408),
            0
        );
        assert_eq!(table.fini.unwrap()(raw), 0);
    }
    assert_eq!(DROPS.load(Ordering::Relaxed), before + 1);
}

static PARTIAL_DROPS: AtomicUsize = AtomicUsize::new(0);
struct Fails;
impl FlowControl for Fails {
    type Shared = ();
    fn new(_: &FlowControlArgs<'_>) -> Result<(Self, ()), AeronCError> {
        struct Partial;
        impl Drop for Partial {
            fn drop(&mut self) {
                PARTIAL_DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }
        let _partial = Partial;
        Err(AeronCError::with_message(-1, "supplier failed: %s\0literal"))
    }
    fn on_status_message(
        &mut self,
        _: &(),
        _: StatusMessage<'_>,
        _: SockAddrRef<'_>,
        _: i64,
        _: i32,
        _: usize,
        _: i64,
        _: StrategyMetrics<'_>,
    ) -> Position {
        unreachable!()
    }
}
#[test]
fn supplier_failure_leaves_no_pointer_and_drops_partial_state() {
    let context = AeronDriverContext::new().unwrap();
    context.set_unicast_flow_control::<Fails>().unwrap();
    let channel = aeron_udp_channel_t::default();
    let mut raw = std::ptr::dangling_mut();
    unsafe {
        assert_eq!(
            aeron_driver_context_get_unicast_flowcontrol_supplier(context.get_inner()).unwrap()(
                &mut raw,
                context.get_inner(),
                std::ptr::null_mut(),
                &channel,
                7,
                8,
                9,
                10,
                65536
            ),
            -1
        );
        assert!(
            CStr::from_ptr(aeron_errmsg())
                .to_string_lossy()
                .contains("%s\\0literal")
        );
    }
    assert!(raw.is_null());
    assert_eq!(PARTIAL_DROPS.load(Ordering::Relaxed), 1);
}

#[test]
fn bounded_wait_degrades_at_deadline_and_rearms_after_recovery() {
    type Policy = BoundedTaggedFlowControl<100, 2>;
    let context = AeronDriverContext::new().unwrap();
    context.set_flow_control_group_tag(555).unwrap();
    context.set_flow_control_group_min_size(1).unwrap();
    context.set_flow_control_receiver_timeout_ns(1000).unwrap();
    let channel = aeron_udp_channel_t::default();
    let (mut policy, shared) = Policy::new(&args(&channel, &context)).unwrap();
    let addr = sockaddr_storage::default();
    let source = unsafe { SockAddrRef::from_raw(&addr) };
    let sm = status(1, 777, 1024);
    let mut counter = std::sync::atomic::AtomicI64::new(0);
    let metrics =
        |counter: &mut std::sync::atomic::AtomicI64| unsafe { super::flow::test_metrics_at(counter.as_ptr()) };
    assert_eq!(
        policy
            .on_status_message(
                &shared,
                StatusMessage::from_bytes(&sm).unwrap(),
                source,
                0,
                10,
                16,
                0,
                metrics(&mut counter)
            )
            .get(),
        0
    );
    assert!(!Policy::has_required_receivers(&shared));
    assert_eq!(policy.on_idle(&shared, 99, 0, 0, false, metrics(&mut counter)).get(), 0);
    assert_eq!(
        policy.on_idle(&shared, 100, 0, 0, false, metrics(&mut counter)).get(),
        2048
    );
    assert!(Policy::has_required_receivers(&shared));
    policy.on_idle(&shared, 101, 2048, 0, false, metrics(&mut counter));
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    let sm = status(2, 555, 2048);
    policy.on_status_message(
        &shared,
        StatusMessage::from_bytes(&sm).unwrap(),
        source,
        2048,
        10,
        16,
        102,
        metrics(&mut counter),
    );
    policy.on_idle(&shared, 1103, 3072, 0, false, metrics(&mut counter));
    assert!(!Policy::has_required_receivers(&shared));
    policy.on_idle(&shared, 1203, 3072, 0, false, metrics(&mut counter));
    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[test]
fn invalid_bounded_configuration_fails_at_setup() {
    let context = AeronDriverContext::new().unwrap();
    let channel = aeron_udp_channel_t::default();
    let args = args(&channel, &context);
    assert!(BoundedTaggedFlowControl::<-1>::new(&args).is_err());
    assert!(BoundedTaggedFlowControl::<1, 0>::new(&args).is_err());
}

#[test]
fn manual_driver_runs_and_all_strategy_setters_reject_a_frozen_context() {
    let context = AeronDriverContext::new().unwrap();
    context
        .set_dir(&format!("/tmp/rusteron-manual-{}", std::process::id()).into_c_string())
        .unwrap();
    context.set_dir_delete_on_start(true).unwrap();
    context.set_dir_delete_on_shutdown(true).unwrap();
    context.set_threading_mode(AeronThreadingMode::Invoker).unwrap();
    let driver = AeronDriver::new(&context).unwrap();
    assert!(context.set_unicast_flow_control::<MaxFlowControl>().is_err());
    assert!(context.set_multicast_flow_control::<MaxFlowControl>().is_err());
    assert!(context.set_congestion_control::<StaticWindow>().is_err());
    assert!(context.add_outgoing_interceptor::<SeededLoss<0>>().is_err());
    let state = std::sync::Arc::new(AtomicUsize::new(0));
    assert!(
        context
            .add_outgoing_interceptor_with_state::<Stateful>(state.clone())
            .is_err()
    );
    assert_eq!(std::sync::Arc::strong_count(&state), 1);
    let mut driver = driver.start_manual().unwrap();
    for _ in 0..10 {
        let work = driver.do_work().unwrap();
        driver.idle(work);
    }
}

static CC_DROPS: AtomicUsize = AtomicUsize::new(0);
struct ThreadHalf(i32);
impl Drop for ThreadHalf {
    fn drop(&mut self) {
        CC_DROPS.fetch_add(1, Ordering::Relaxed);
    }
}
struct SplitControl;
impl CongestionControl for SplitControl {
    type Receiver = ThreadHalf;
    type Conductor = ThreadHalf;
    type Shared = std::sync::atomic::AtomicI64;
    fn new(_: &CongestionControlArgs<'_>) -> Result<(ThreadHalf, ThreadHalf, Self::Shared), AeronCError> {
        Ok((ThreadHalf(0), ThreadHalf(0), std::sync::atomic::AtomicI64::new(0)))
    }
    fn initial_window_length(_: &mut ThreadHalf, _: &Self::Shared) -> ReceiverWindow {
        ReceiverWindow::new(i32::MAX)
    }
    fn max_window_length(_: &mut ThreadHalf, _: &Self::Shared) -> ReceiverWindow {
        ReceiverWindow::new(-1)
    }
    fn on_track_rebuild(c: &mut ThreadHalf, s: &Self::Shared, _: TrackRebuild, _: &mut bool) -> ReceiverWindow {
        c.0 += 1;
        let _ = s.load(Ordering::Acquire);
        ReceiverWindow::new(999999)
    }
    fn should_measure_rtt(r: &mut ThreadHalf, _: &Self::Shared, _: i64) -> bool {
        r.0 += 1;
        true
    }
    fn on_rttm_sent(r: &mut ThreadHalf, _: &Self::Shared, _: i64) {
        r.0 += 1;
    }
    fn on_rttm(_: &mut ThreadHalf, s: &Self::Shared, _: i64, rtt: i64, _: SockAddrRef<'_>) {
        s.store(rtt, Ordering::Release);
    }
}
#[test]
fn congestion_callbacks_use_disjoint_halves_and_bound_windows() {
    let context = AeronDriverContext::new().unwrap();
    context.set_congestion_control::<SplitControl>().unwrap();
    let mut channel = aeron_udp_channel_t::default();
    let mut raw = std::ptr::null_mut();
    let mut address = sockaddr_storage::default();
    unsafe {
        assert_eq!(
            aeron_driver_context_get_congestioncontrol_supplier(context.get_inner()).unwrap()(
                &mut raw,
                &mut channel,
                7,
                8,
                9,
                65536,
                1408,
                &mut address,
                &mut address,
                context.get_inner(),
                std::ptr::null_mut()
            ),
            0
        );
        let table = *raw;
        assert_eq!(table.initial_window_length.unwrap()(table.state), 32768);
        assert_eq!(table.max_window_length.unwrap()(table.state), 1);
        let state = table.state as usize;
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let barrier = &barrier;
            scope.spawn(move || {
                let mut address = sockaddr_storage::default();
                barrier.wait();
                for _ in 0..10000 {
                    assert!(table.should_measure_rtt.unwrap()(state as *mut c_void, 1));
                    table.on_rttm_sent.unwrap()(state as *mut c_void, 1);
                    table.on_rttm.unwrap()(state as *mut c_void, 1, -99, &mut address);
                }
            });
            barrier.wait();
            for _ in 0..10000 {
                let mut force = true;
                assert_eq!(
                    table.on_track_rebuild.unwrap()(table.state, &mut force, 1, 2, 3, 4, 5, 6, false),
                    32768
                );
                assert!(force);
            }
        });
        assert_eq!(table.fini.unwrap()(raw), 0);
    }
    assert_eq!(CC_DROPS.load(Ordering::Relaxed), 2);
}

#[test]
fn interceptor_installation_preserves_chain_order_and_owns_each_binding() {
    let context = AeronDriverContext::new().unwrap();
    context.add_outgoing_interceptor::<SeededLoss<2>>().unwrap();
    let first = unsafe { aeron_driver_context_get_udp_channel_outgoing_interceptors(context.get_inner()) };
    context.add_outgoing_interceptor::<SeededLoss<3>>().unwrap();
    let second = unsafe { aeron_driver_context_get_udp_channel_outgoing_interceptors(context.get_inner()) };
    unsafe {
        assert_eq!((*second).meta_info.next_interceptor_bindings, first);
        assert_eq!(
            CStr::from_ptr((*second).meta_info.name).to_str().unwrap(),
            std::any::type_name::<SeededLoss<3>>()
        );
        assert!((*first).meta_info.next_interceptor_bindings.is_null());
        assert!((*second).incoming_init_func.is_none());
        assert!((*second).outgoing_close_func.is_some());
        let mut state = std::ptr::null_mut();
        assert_eq!(
            (*second).outgoing_init_func.unwrap()(
                &mut state,
                context.get_inner(),
                TransportAffinity::AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER
            ),
            0
        );
        assert!(!state.is_null());
        assert_eq!((*second).outgoing_close_func.unwrap()(state), 0);
    }
}

struct Stateful(std::sync::Arc<AtomicUsize>);
impl InterceptorStateFactory for Stateful {
    type State = std::sync::Arc<AtomicUsize>;
    fn new(state: &Self::State, _: StrategyContext, _: TransportAffinity) -> Result<Self, AeronCError> {
        state.fetch_add(1, Ordering::Relaxed);
        Ok(Self(state.clone()))
    }
}
impl OutgoingInterceptor for Stateful {}
impl IncomingInterceptor for Stateful {}

struct FailedStateful;
impl InterceptorStateFactory for FailedStateful {
    type State = ();
    fn new(_: &(), _: StrategyContext, _: TransportAffinity) -> Result<Self, AeronCError> {
        Err(AeronCError::with_message(-1, "construction failed"))
    }
}
impl IncomingInterceptor for FailedStateful {}

#[test]
fn failed_stateful_interceptor_factory_publishes_no_pointer() {
    let context = AeronDriverContext::new().unwrap();
    context
        .add_incoming_interceptor_with_state::<FailedStateful>(())
        .unwrap();
    unsafe {
        let ctx = context.get_inner();
        let table = aeron_driver_context_get_udp_channel_incoming_interceptors(ctx);
        let mut state = std::ptr::dangling_mut::<c_void>();
        assert_eq!(
            (*table).incoming_init_func.unwrap()(
                &mut state,
                ctx,
                TransportAffinity::AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER,
            ),
            -1
        );
        assert!(state.is_null());
    }
}

#[test]
fn incoming_interceptors_append_after_existing_bindings() {
    let context = AeronDriverContext::new().unwrap();
    context
        .add_incoming_interceptor_with_state::<FailedStateful>(())
        .unwrap();
    context
        .add_incoming_interceptor_with_state::<Stateful>(Default::default())
        .unwrap();
    unsafe {
        let ctx = context.get_inner();
        let head = aeron_driver_context_get_udp_channel_incoming_interceptors(ctx);
        let tail = (*head).meta_info.next_interceptor_bindings;
        assert!((*tail).meta_info.next_interceptor_bindings.is_null());
        let affinity = TransportAffinity::AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER;
        let mut state = std::ptr::null_mut();
        // The first addition stays at the head, so it sees packets before the second
        assert_eq!((*head).incoming_init_func.unwrap()(&mut state, ctx, affinity), -1);
        assert_eq!((*tail).incoming_init_func.unwrap()(&mut state, ctx, affinity), 0);
        assert_eq!((*tail).incoming_close_func.unwrap()(state), 0);
    }
}

unsafe fn construct(context: &AeronDriverContext, outgoing: bool) -> std::sync::Arc<AtomicUsize> {
    unsafe {
        let ctx = context.get_inner();
        let table = if outgoing {
            &*aeron_driver_context_get_udp_channel_outgoing_interceptors(ctx)
        } else {
            &*aeron_driver_context_get_udp_channel_incoming_interceptors(ctx)
        };
        let (init, close) = if outgoing {
            (table.outgoing_init_func.unwrap(), table.outgoing_close_func.unwrap())
        } else {
            (table.incoming_init_func.unwrap(), table.incoming_close_func.unwrap())
        };
        let mut raw = std::ptr::null_mut();
        let affinity = TransportAffinity::AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER;
        assert_eq!(init(&mut raw, ctx, affinity), 0);
        let state = (*raw.cast::<Stateful>()).0.clone();
        assert_eq!(close(raw), 0);
        state
    }
}

#[test]
fn stateful_interceptors_share_slots_with_a_resolver_and_stay_per_context() {
    let first = std::sync::Arc::new(AtomicUsize::new(0));
    let second = std::sync::Arc::new(AtomicUsize::new(0));
    let incoming = std::sync::Arc::new(AtomicUsize::new(0));
    let context = AeronDriverContext::new().unwrap();
    // The resolver instance takes the only named slot a fresh context has.
    context
        .set_name_resolver_fn(|_, _, _| Ok(std::net::Ipv4Addr::LOCALHOST.into()))
        .unwrap();
    context
        .add_outgoing_interceptor_with_state::<Stateful>(first.clone())
        .unwrap();
    context
        .add_incoming_interceptor_with_state::<Stateful>(incoming.clone())
        .unwrap();
    let other = AeronDriverContext::new().unwrap();
    other
        .add_outgoing_interceptor_with_state::<Stateful>(second.clone())
        .unwrap();
    // Installing interceptors first must leave the resolver's native slot free.
    other
        .set_name_resolver_fn(|_, _, _| Ok(std::net::Ipv4Addr::LOCALHOST.into()))
        .unwrap();

    unsafe {
        assert!(std::sync::Arc::ptr_eq(&construct(&context, true), &first));
        assert!(std::sync::Arc::ptr_eq(&construct(&context, false), &incoming));
        assert!(std::sync::Arc::ptr_eq(&construct(&other, true), &second));
        assert!(
            aeron_driver_context_bindings_clientd_find(
                context.get_inner(),
                c"rusteron.name_resolver.instance".as_ptr()
            ) >= 0
        );
    }
    assert_eq!(
        (first.load(Ordering::Relaxed), incoming.load(Ordering::Relaxed)),
        (1, 1)
    );
    assert_eq!(second.load(Ordering::Relaxed), 1);

    let replacement = std::sync::Arc::new(AtomicUsize::new(0));
    context
        .add_outgoing_interceptor_with_state::<Stateful>(replacement.clone())
        .unwrap();
    let constructed = unsafe { construct(&context, true) };
    assert!(std::sync::Arc::ptr_eq(&constructed, &replacement));
    assert_eq!(first.load(Ordering::Relaxed), 1);
}

struct PanicIdle;
impl FlowControl for PanicIdle {
    type Shared = ();
    fn new(_: &FlowControlArgs<'_>) -> Result<(Self, ()), AeronCError> {
        Ok((Self, ()))
    }
    fn on_idle(&mut self, _: &(), _: i64, _: i64, _: i64, _: bool, _: StrategyMetrics<'_>) -> Position {
        panic!("extension panic marker");
    }
    fn on_status_message(
        &mut self,
        _: &(),
        _: StatusMessage<'_>,
        _: SockAddrRef<'_>,
        _: i64,
        _: i32,
        _: usize,
        _: i64,
        _: StrategyMetrics<'_>,
    ) -> Position {
        unreachable!()
    }
}
#[test]
fn panicking_extension_aborts() {
    if std::env::var_os("RUSTERON_TEST_EXTENSION_PANIC_CHILD").is_some() {
        let context = AeronDriverContext::new().unwrap();
        context.set_unicast_flow_control::<PanicIdle>().unwrap();
        let channel = aeron_udp_channel_t::default();
        let mut strategy = std::ptr::null_mut();
        unsafe {
            aeron_driver_context_get_unicast_flowcontrol_supplier(context.get_inner()).unwrap()(
                &mut strategy,
                context.get_inner(),
                std::ptr::null_mut(),
                &channel,
                7,
                8,
                9,
                10,
                65536,
            );
            ((*strategy).on_idle.unwrap())((*strategy).state, 0, 0, 0, false);
        }
        unreachable!();
    }
    let output = std::process::Command::new("sh")
        .args(["-c", "ulimit -c 0; exec \"$@\"", "extension-panic"])
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "extensions::tests::panicking_extension_aborts",
            "--nocapture",
        ])
        .env("RUSTERON_TEST_EXTENSION_PANIC_CHILD", "1")
        .output()
        .unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(output.status.signal(), Some(6));
    assert!(String::from_utf8_lossy(&output.stderr).contains("extension panic marker"));
}

static RESOLVER_DROPS: AtomicUsize = AtomicUsize::new(0);
struct TestResolver {
    replacement: std::ffi::CString,
}
impl Drop for TestResolver {
    fn drop(&mut self) {
        RESOLVER_DROPS.fetch_add(1, Ordering::Relaxed);
    }
}
impl NameResolverFactory for TestResolver {
    fn new(args: &CStr) -> Result<Self, AeronCError> {
        assert_eq!(args, c"resolver configuration");
        Ok(Self {
            replacement: c"peer:1234".to_owned(),
        })
    }
}
impl NameResolver for TestResolver {
    const LOOKUP_CAPACITY: usize = 64;

    fn lookup(&self, name: &CStr, _: &CStr, _: bool) -> Option<&CStr> {
        (name == c"logical").then_some(&self.replacement)
    }
    fn resolve(&mut self, _: &CStr, _: &CStr, _: bool) -> Result<std::net::IpAddr, AeronCError> {
        self.replacement = c"changed".to_owned();
        Ok("127.0.0.1".parse().unwrap())
    }
    fn on_start(&mut self) -> Result<(), AeronCError> {
        Ok(())
    }
    fn do_work(&mut self, _: i64) -> Result<i32, AeronCError> {
        Ok(7)
    }
}
#[test]
fn resolver_retains_lookup_text_across_mutation_and_closes_once() {
    let context = AeronDriverContext::new().unwrap();
    context.set_name_resolver::<TestResolver>().unwrap();
    let mut resolver = aeron_name_resolver_t::default();
    unsafe {
        let supplier = aeron_driver_context_get_name_resolver_supplier(context.get_inner()).unwrap();
        assert_eq!(
            supplier(&mut resolver, c"resolver configuration".as_ptr(), context.get_inner()),
            0
        );
        assert_eq!(resolver.start_func.unwrap()(&mut resolver), 0);
        assert_eq!(resolver.do_work_func.unwrap()(&mut resolver, 12), 7);
        let mut text = std::ptr::null();
        // The reserved capacity also covers the first lookup.
        crate::test_alloc::assert_no_heap_allocation(|| {
            assert_eq!(
                resolver.lookup_func.unwrap()(
                    &mut resolver,
                    c"logical".as_ptr(),
                    c"endpoint".as_ptr(),
                    false,
                    &mut text
                ),
                1
            );
        });
        let mut addr = sockaddr_storage::default();
        assert_eq!(
            resolver.resolve_func.unwrap()(&mut resolver, c"peer".as_ptr(), c"endpoint".as_ptr(), true, &mut addr),
            0
        );
        assert_eq!(CStr::from_ptr(text), c"peer:1234");
        assert_eq!(
            SockAddrRef::from_raw(&addr).to_socket_addr().unwrap().ip(),
            "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
        );
        assert_eq!(
            resolver.lookup_func.unwrap()(
                &mut resolver,
                c"literal".as_ptr(),
                c"endpoint".as_ptr(),
                false,
                &mut text
            ),
            0
        );
        assert_eq!(CStr::from_ptr(text), c"literal");
        assert_eq!(resolver.close_func.unwrap()(&mut resolver), 0);
        assert!(resolver.state.is_null());
    }
    assert_eq!(RESOLVER_DROPS.load(Ordering::Relaxed), 1);
}

#[test]
fn resolver_reuses_lookup_storage_after_growth_and_literal_fallbacks() {
    #[derive(Default)]
    struct Hosts;
    impl NameResolver for Hosts {
        fn lookup(&self, name: &CStr, _: &CStr, _: bool) -> Option<&CStr> {
            match name.to_bytes() {
                b"short" => Some(c"a:1"),
                b"long" => Some(c"a-much-longer-hostname:12345"),
                b"empty" => Some(c""),
                _ => None,
            }
        }
        fn resolve(&mut self, _: &CStr, _: &CStr, _: bool) -> Result<std::net::IpAddr, AeronCError> {
            Ok(std::net::Ipv4Addr::LOCALHOST.into())
        }
    }
    let context = AeronDriverContext::new().unwrap();
    context.set_name_resolver::<Hosts>().unwrap();
    let mut resolver = aeron_name_resolver_t::default();
    unsafe {
        let supplier = aeron_driver_context_get_name_resolver_supplier(context.get_inner()).unwrap();
        assert_eq!(supplier(&mut resolver, std::ptr::null(), context.get_inner()), 0);
        let lookup = resolver.lookup_func.unwrap();
        let mut text = std::ptr::null();
        for (name, expected) in [(c"short", c"a:1"), (c"long", c"a-much-longer-hostname:12345")] {
            assert_eq!(
                lookup(&mut resolver, name.as_ptr(), c"endpoint".as_ptr(), false, &mut text),
                1
            );
            assert_eq!(CStr::from_ptr(text), expected);
        }
        let allocation = text;
        crate::test_alloc::assert_no_heap_allocation(|| {
            for _ in 0..16 {
                for (name, expected, code) in [
                    (c"short", c"a:1", 1),
                    (c"literal", c"literal", 0),
                    (c"empty", c"", 1),
                    (c"long", c"a-much-longer-hostname:12345", 1),
                ] {
                    assert_eq!(
                        lookup(&mut resolver, name.as_ptr(), c"endpoint".as_ptr(), true, &mut text),
                        code
                    );
                    assert_eq!(CStr::from_ptr(text), expected);
                    assert_eq!(text, if code == 0 { name.as_ptr() } else { allocation });
                }
            }
        });
        assert_eq!(resolver.close_func.unwrap()(&mut resolver), 0);
    }
}

#[test]
fn static_window_matches_native_policy_for_channel_and_context_windows() {
    let context = AeronDriverContext::new().unwrap();
    context.set_rcv_initial_window_length(8192).unwrap();
    context.set_congestion_control::<StaticWindow>().unwrap();
    let mut channel = aeron_udp_channel_t::default();
    let mut address = sockaddr_storage::default();
    for configured in [0, 4096, 32768, 65536] {
        channel.receiver_window_length = configured;
        let mut raw = std::ptr::null_mut();
        unsafe {
            assert_eq!(
                aeron_driver_context_get_congestioncontrol_supplier(context.get_inner()).unwrap()(
                    &mut raw,
                    &mut channel,
                    1,
                    2,
                    3,
                    65536,
                    1408,
                    &mut address,
                    &mut address,
                    context.get_inner(),
                    std::ptr::null_mut()
                ),
                0
            );
            let mut force = true;
            let expected = if configured == 0 { 8192 } else { configured.min(32768) } as i32;
            assert_eq!(((*raw).initial_window_length.unwrap())((*raw).state), expected);
            assert_eq!(((*raw).max_window_length.unwrap())((*raw).state), expected);
            assert_eq!(
                ((*raw).on_track_rebuild.unwrap())((*raw).state, &mut force, 0, 0, 0, 0, 0, 0, true),
                expected
            );
            assert!(!force);
            assert!(!((*raw).should_measure_rtt.unwrap())((*raw).state, 0));
            ((*raw).fini.unwrap())(raw);
        }
    }
}

#[test]
fn bounded_tagged_tracks_capacity_setup_timeout_and_eos() {
    type Policy = BoundedTaggedFlowControl<100, 1>;
    let context = AeronDriverContext::new().unwrap();
    context.set_flow_control_group_tag(555).unwrap();
    context.set_flow_control_group_min_size(0).unwrap();
    context.set_flow_control_receiver_timeout_ns(1000).unwrap();
    let channel = aeron_udp_channel_t::default();
    let (mut policy, shared) = Policy::new(&args(&channel, &context)).unwrap();
    let address = sockaddr_storage::default();
    let source = unsafe { SockAddrRef::from_raw(&address) };
    let count = std::sync::atomic::AtomicI64::new(0);
    let send = |policy: &mut Policy, bytes: &[u8], now, limit| {
        policy.on_status_message(
            &shared,
            StatusMessage::from_bytes(bytes).unwrap(),
            source,
            limit,
            10,
            16,
            now,
            unsafe { super::flow::test_metrics_at(count.as_ptr()) },
        )
    };
    let first = status(1, 555, 0);
    assert_eq!(send(&mut policy, &first, 0, 0).get(), 1024);
    assert!(Policy::has_required_receivers(&shared));
    // A full table keeps the existing receiver's minimum.
    assert_eq!(send(&mut policy, &status(2, 555, 4096), 1, 1024).get(), 1024);
    policy.on_trigger_send_setup(&shared, StatusMessage::from_bytes(&first).unwrap(), source, 1);
    policy.on_setup(&shared, SetupMessage::from_bytes(&[0; 40]).unwrap(), 2, 512, 16, 0);
    assert_eq!(
        policy
            .on_idle(&shared, 3, 1024, 0, false, unsafe {
                super::flow::test_metrics_at(count.as_ptr())
            })
            .get(),
        512
    );
    send(&mut policy, &first, 200, 512);
    assert_eq!(
        policy
            .on_idle(&shared, 1003, 512, 0, false, unsafe {
                super::flow::test_metrics_at(count.as_ptr())
            })
            .get(),
        1024
    );
    let mut eos = first;
    eos[5] = 0x40;
    send(&mut policy, &eos, 1004, 1024);
    policy.on_idle(&shared, 1004, 1024, 0, true, unsafe {
        super::flow::test_metrics_at(count.as_ptr())
    });
    assert_eq!(send(&mut policy, &status(2, 555, 4096), 1005, 1024).get(), 5120);
    let mut error = [0u8; 40];
    error[16..24].copy_from_slice(&2i64.to_le_bytes());
    error[24..32].copy_from_slice(&555i64.to_le_bytes());
    policy.on_error(&shared, ErrorMessage::from_bytes(&error).unwrap(), source, 1006);
    policy.on_idle(&shared, 1006, 5120, 0, false, unsafe {
        super::flow::test_metrics_at(count.as_ptr())
    });
    assert_eq!(send(&mut policy, &status(3, 555, 8192), 1007, 5120).get(), 9216);
    assert_eq!(count.load(Ordering::Relaxed), 0);
}

#[test]
fn bounded_group_recomputes_limits_after_updates_removal_and_setup() {
    type Policy = BoundedTaggedFlowControl<100, 4>;
    let context = AeronDriverContext::new().unwrap();
    context.set_flow_control_group_tag(555).unwrap();
    context.set_flow_control_group_min_size(1).unwrap();
    context.set_flow_control_receiver_timeout_ns(100).unwrap();
    let channel = aeron_udp_channel_t::default();
    let (mut policy, shared) = Policy::new(&args(&channel, &context)).unwrap();
    let address = sockaddr_storage::default();
    let source = unsafe { SockAddrRef::from_raw(&address) };
    let counter = std::sync::atomic::AtomicI64::new(0);
    let metrics = || unsafe { super::flow::test_metrics_at(counter.as_ptr()) };
    let send = |policy: &mut Policy, receiver, offset, now, eos| {
        let mut bytes = status(receiver, 555, offset);
        bytes[5] = if eos { 0x40 } else { 0 };
        policy
            .on_status_message(
                &shared,
                StatusMessage::from_bytes(&bytes).unwrap(),
                source,
                0,
                10,
                16,
                now,
                metrics(),
            )
            .get()
    };
    assert_eq!(send(&mut policy, 1, 0, 0, false), 1024);
    assert_eq!(send(&mut policy, 2, 1024, 1, false), 1024);
    assert_eq!(send(&mut policy, 3, 2048, 2, false), 1024);
    assert_eq!(send(&mut policy, 1, 4096, 3, false), 2048);
    policy.on_trigger_send_setup(
        &shared,
        StatusMessage::from_bytes(&status(1, 555, 0)).unwrap(),
        source,
        4,
    );
    policy.on_setup(&shared, SetupMessage::from_bytes(&[0; 40]).unwrap(), 4, 1000, 16, 0);
    assert_eq!(policy.on_idle(&shared, 4, 0, 0, false, metrics()).get(), 1000);
    // A new receiver clears the setup limit. An existing receiver update does not.
    assert_eq!(send(&mut policy, 4, 3072, 5, false), 2048);
    assert_eq!(send(&mut policy, 2, 1024, 6, true), 2048);
    assert_eq!(policy.on_idle(&shared, 6, 0, 0, false, metrics()).get(), 3072);
    let mut error = [0; 40];
    error[16..24].copy_from_slice(&3i64.to_le_bytes());
    error[24..32].copy_from_slice(&555i64.to_le_bytes());
    policy.on_error(&shared, ErrorMessage::from_bytes(&error).unwrap(), source, 7);
    assert_eq!(policy.on_idle(&shared, 7, 0, 0, false, metrics()).get(), 4096);
    assert_eq!(policy.on_idle(&shared, 104, 0, 0, false, metrics()).get(), 4096);
    assert_eq!(policy.on_idle(&shared, 106, 4096, 0, false, metrics()).get(), 4096);
    assert!(!Policy::has_required_receivers(&shared));
    assert_eq!(send(&mut policy, 5, 8192, 107, false), 9216);
    assert!(Policy::has_required_receivers(&shared));
    assert_eq!(counter.load(Ordering::Relaxed), 0);
}

#[test]
fn bounded_partial_group_keeps_its_limit_after_degradation_and_rearms_when_full() {
    type Policy = BoundedTaggedFlowControl<10, 4>;
    let context = AeronDriverContext::new().unwrap();
    context.set_flow_control_group_tag(555).unwrap();
    context.set_flow_control_group_min_size(2).unwrap();
    context.set_flow_control_receiver_timeout_ns(100).unwrap();
    let channel = aeron_udp_channel_t::default();
    let (mut policy, shared) = Policy::new(&args(&channel, &context)).unwrap();
    let address = sockaddr_storage::default();
    let source = unsafe { SockAddrRef::from_raw(&address) };
    let counter = std::sync::atomic::AtomicI64::new(0);
    let metrics = || unsafe { super::flow::test_metrics_at(counter.as_ptr()) };
    let send = |policy: &mut Policy, receiver, offset, now, limit, eos| {
        let mut bytes = status(receiver, 555, offset);
        bytes[5] = if eos { 0x40 } else { 0 };
        policy
            .on_status_message(
                &shared,
                StatusMessage::from_bytes(&bytes).unwrap(),
                source,
                limit,
                10,
                16,
                now,
                metrics(),
            )
            .get()
    };
    assert_eq!(send(&mut policy, 1, 0, 0, 500, false), 500);
    assert!(!Policy::has_required_receivers(&shared));
    assert_eq!(policy.on_idle(&shared, 9, 500, 0, false, metrics()).get(), 500);
    assert_eq!(policy.on_idle(&shared, 10, 500, 0, false, metrics()).get(), 1024);
    assert!(Policy::has_required_receivers(&shared));
    assert_eq!(send(&mut policy, 1, 1024, 11, 1024, false), 2048);
    assert_eq!(send(&mut policy, 2, 2048, 12, 2048, false), 2048);
    assert_eq!(send(&mut policy, 1, 1024, 13, 2048, true), 2048);
    assert_eq!(policy.on_idle(&shared, 13, 2048, 0, false, metrics()).get(), 2048);
    assert!(!Policy::has_required_receivers(&shared));
    assert_eq!(policy.on_idle(&shared, 22, 2048, 0, false, metrics()).get(), 2048);
    assert_eq!(policy.on_idle(&shared, 23, 2048, 0, false, metrics()).get(), 3072);
    assert!(Policy::has_required_receivers(&shared));
    assert_eq!(send(&mut policy, 2, 4096, 24, 3072, false), 5120);
    assert_eq!(counter.load(Ordering::Relaxed), 2);
}

#[test]
fn threaded_start_consumes_the_driver_and_rejects_invoker_before_starting() {
    for mode in [
        AeronThreadingMode::Shared,
        AeronThreadingMode::SharedNetwork,
        AeronThreadingMode::Dedicated,
        AeronThreadingMode::Invoker,
    ] {
        let context = AeronDriverContext::new().unwrap();
        context
            .set_dir(&format!("/tmp/rusteron-threaded-{}-{mode:?}", std::process::id()).into_c_string())
            .unwrap();
        context.set_dir_delete_on_start(true).unwrap();
        context.set_dir_delete_on_shutdown(true).unwrap();
        context.set_threading_mode(mode).unwrap();
        let started = AeronDriver::new(&context).unwrap().start();
        if mode == AeronThreadingMode::Invoker {
            assert!(started.err().unwrap().message().unwrap().contains("start_manual"));
        } else {
            drop(started.unwrap());
        }
    }
}

#[test]
fn default_retransmission_uses_the_configured_unicast_or_mdc_multiple() {
    struct DefaultRetransmission;
    impl FlowControl for DefaultRetransmission {
        type Shared = ();
        fn new(_: &FlowControlArgs<'_>) -> Result<(Self, ()), AeronCError> {
            Ok((Self, ()))
        }
        fn on_status_message(
            &mut self,
            _: &(),
            _: StatusMessage<'_>,
            _: SockAddrRef<'_>,
            limit: i64,
            _: i32,
            _: usize,
            _: i64,
            _: StrategyMetrics<'_>,
        ) -> Position {
            Position::new(limit)
        }
    }
    let context = AeronDriverContext::new().unwrap();
    context.set_unicast_flow_control::<DefaultRetransmission>().unwrap();
    unsafe {
        (*context.get_inner()).unicast_flow_control_rrwm = 1;
        (*context.get_inner()).multicast_flow_control_rrwm = 2;
    }
    let mut channel = aeron_udp_channel_t::default();
    for (mode, multiple) in [
        (aeron_udp_channel_control_mode::AERON_UDP_CHANNEL_CONTROL_MODE_NONE, 1),
        (aeron_udp_channel_control_mode::AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL, 2),
    ] {
        channel.control_mode = mode;
        let mut raw = std::ptr::null_mut();
        unsafe {
            assert_eq!(
                aeron_driver_context_get_unicast_flowcontrol_supplier(context.get_inner()).unwrap()(
                    &mut raw,
                    context.get_inner(),
                    std::ptr::null_mut(),
                    &channel,
                    7,
                    8,
                    9,
                    10,
                    65536
                ),
                0
            );
            let expected = aeron_flow_control_calculate_retransmission_length(65536, 65536, 0, multiple);
            assert_eq!(
                ((*raw).max_retransmission_length.unwrap())((*raw).state, 0, 65536, 65536, 1408),
                expected
            );
            ((*raw).fini.unwrap())(raw);
        }
    }
}

#[test]
fn unknown_address_family_does_not_read_uninitialized_address_fields() {
    let mut storage = std::mem::MaybeUninit::<sockaddr_storage>::uninit();
    unsafe {
        std::ptr::addr_of_mut!((*storage.as_mut_ptr()).ss_family).write(0);
        assert!(SockAddrRef::from_raw(storage.as_ptr()).to_socket_addr().is_none());
    }
}

/// Aeron's max flow-control policy, with the context's retransmission window multiple.
struct MaxFlowControl {
    retransmit_window_multiple: usize,
}
impl FlowControl for MaxFlowControl {
    type Shared = ();
    fn new(args: &FlowControlArgs<'_>) -> Result<(Self, ()), AeronCError> {
        Ok((
            Self {
                retransmit_window_multiple: args.retransmit_window_multiple(),
            },
            (),
        ))
    }
    #[inline]
    fn on_status_message(
        &mut self,
        _: &(),
        sm: StatusMessage<'_>,
        _: SockAddrRef<'_>,
        limit: i64,
        term: i32,
        shift: usize,
        _: i64,
        _: StrategyMetrics<'_>,
    ) -> Position {
        Position::new(limit.max(sm.position(term, shift).saturating_add(i64::from(sm.receiver_window()))))
    }
    fn max_retransmission_length(
        &mut self,
        _: &(),
        offset: usize,
        length: usize,
        term: usize,
        _: usize,
        _: usize,
    ) -> usize {
        // This helper reads Aeron's default initial window, as the native max policy does.
        unsafe {
            aeron_flow_control_calculate_retransmission_length(length, term, offset, self.retransmit_window_multiple)
        }
    }
}

/// A fixed receiver window with no RTT measurements, as in Aeron's static policy.
struct StaticWindow;
impl CongestionControl for StaticWindow {
    type Receiver = ();
    type Conductor = ReceiverWindow;
    type Shared = ();
    fn new(args: &CongestionControlArgs<'_>) -> Result<((), ReceiverWindow, ()), AeronCError> {
        // SAFETY: the channel is live for the duration of `new`
        let channel_window = unsafe { (*args.channel.raw).receiver_window_length };
        let configured = if channel_window == 0 {
            args.context.initial_receiver_window
        } else {
            channel_window
        };
        Ok((
            (),
            ReceiverWindow::new(configured.min((args.term_length / 2) as usize) as i32),
            (),
        ))
    }
    fn initial_window_length(c: &mut ReceiverWindow, _: &()) -> ReceiverWindow {
        *c
    }
    fn max_window_length(c: &mut ReceiverWindow, _: &()) -> ReceiverWindow {
        *c
    }
    fn on_track_rebuild(c: &mut ReceiverWindow, _: &(), _: TrackRebuild, force: &mut bool) -> ReceiverWindow {
        *force = false;
        *c
    }
}
