use super::*;
use crate::IntoCString;
use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct Observed {
    calls: Mutex<Vec<i64>>,
    updates: Mutex<Vec<i64>>,
    measurements: Mutex<Vec<i64>>,
    drops: AtomicUsize,
}
struct Probe(Arc<Observed>);
impl Drop for Probe {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::Relaxed);
    }
}
impl DutyCycleTracker for Probe {
    fn update(&mut self, now: i64) {
        self.0.calls.lock().unwrap().push(now);
        self.0.updates.lock().unwrap().push(now);
    }
    fn measure_and_update(&mut self, now: i64) {
        self.0.calls.lock().unwrap().push(now);
        self.0.measurements.lock().unwrap().push(now);
    }
}
impl IdleStrategy for Probe {
    fn idle(&mut self, work: i32) {
        self.0.calls.lock().unwrap().push(i64::from(work));
    }
}
impl PortManager for Probe {
    fn get_managed_port(&mut self, channel: ChannelRef<'_>, requested: SocketAddr) -> Result<u16, AeronCError> {
        assert!(channel.uri().is_empty());
        self.0.calls.lock().unwrap().push(i64::from(requested.port()));
        Ok(if requested.port() == 0 { 42001 } else { requested.port() })
    }
    fn free_managed_port(&mut self, address: SocketAddr) {
        self.0.calls.lock().unwrap().push(-i64::from(address.port()));
    }
}

#[test]
fn every_tracker_role_forwards_timestamps_and_retains_replaced_state() {
    let events = Arc::new(Observed::default());
    let context = AeronDriverContext::new().unwrap();
    for role in [
        DutyCycleRole::Conductor,
        DutyCycleRole::Sender,
        DutyCycleRole::Receiver,
        DutyCycleRole::NameResolver,
    ] {
        context.set_duty_cycle_tracker(role, Probe(events.clone())).unwrap();
        let raw = unsafe { &*context.get_inner() };
        let table = unsafe {
            &*match role {
                DutyCycleRole::Conductor => raw.conductor_duty_cycle_tracker,
                DutyCycleRole::Sender => raw.sender_duty_cycle_tracker,
                DutyCycleRole::Receiver => raw.receiver_duty_cycle_tracker,
                DutyCycleRole::NameResolver => raw.name_resolver_time_tracker,
            }
        };
        unsafe {
            table.update.unwrap()(table.state, -17);
            table.measure_and_update.unwrap()(table.state, i64::MAX);
        }
        context.set_duty_cycle_tracker(role, Probe(events.clone())).unwrap();
    }
    assert_eq!(*events.calls.lock().unwrap(), [-17, i64::MAX].repeat(4));
    assert_eq!(*events.updates.lock().unwrap(), [-17; 4]);
    assert_eq!(*events.measurements.lock().unwrap(), [i64::MAX; 4]);
    assert_eq!(events.drops.load(Ordering::Relaxed), 0);
    drop(context);
    assert_eq!(events.drops.load(Ordering::Relaxed), 8);
}

#[test]
fn every_idle_role_forwards_work_and_supports_native_replacement() {
    let events = Arc::new(Observed::default());
    let context = AeronDriverContext::new().unwrap();
    for role in [
        IdleStrategyRole::Conductor,
        IdleStrategyRole::Sender,
        IdleStrategyRole::Receiver,
        IdleStrategyRole::SharedNetwork,
        IdleStrategyRole::Shared,
        IdleStrategyRole::NativeResourceAgent,
    ] {
        context.set_idle_strategy(role, Probe(events.clone())).unwrap();
        context.set_idle_strategy(role, Probe(events.clone())).unwrap();
        let raw = unsafe { &*context.get_inner() };
        let (callback, state, name) = match role {
            IdleStrategyRole::Conductor => (
                raw.conductor_idle_strategy_func,
                raw.conductor_idle_strategy_state,
                raw.conductor_idle_strategy_name,
            ),
            IdleStrategyRole::Sender => (
                raw.sender_idle_strategy_func,
                raw.sender_idle_strategy_state,
                raw.sender_idle_strategy_name,
            ),
            IdleStrategyRole::Receiver => (
                raw.receiver_idle_strategy_func,
                raw.receiver_idle_strategy_state,
                raw.receiver_idle_strategy_name,
            ),
            IdleStrategyRole::SharedNetwork => (
                raw.shared_network_idle_strategy_func,
                raw.shared_network_idle_strategy_state,
                raw.shared_network_idle_strategy_name,
            ),
            IdleStrategyRole::Shared => (
                raw.shared_idle_strategy_func,
                raw.shared_idle_strategy_state,
                raw.shared_idle_strategy_name,
            ),
            IdleStrategyRole::NativeResourceAgent => (
                raw.native_resource_agent_idle_strategy_func,
                raw.native_resource_agent_idle_strategy_state,
                raw.native_resource_agent_idle_strategy_name,
            ),
        };
        unsafe {
            assert_eq!(CStr::from_ptr(name), c"rusteron");
            callback.unwrap()(state, -1);
            callback.unwrap()(state, 0);
            callback.unwrap()(state, i32::MAX);
        }
    }
    context.set_sender_idle_strategy(c"yield").unwrap();
    context.set_conductor_idle_strategy(c"yield").unwrap();
    context.set_receiver_idle_strategy(c"yield").unwrap();
    context.set_sharednetwork_idle_strategy(c"yield").unwrap();
    context.set_shared_idle_strategy(c"yield").unwrap();
    context.set_native_resource_agent_idle_strategy(c"yield").unwrap();
    assert_eq!(*events.calls.lock().unwrap(), [-1, 0, i64::from(i32::MAX)].repeat(6));
    assert_eq!(events.drops.load(Ordering::Relaxed), 0);
    drop(context);
    assert_eq!(events.drops.load(Ordering::Relaxed), 12);
}

fn address(value: SocketAddr) -> sockaddr_storage {
    let mut raw = sockaddr_storage::default();
    unsafe {
        super::resolver::write_ip_for_test(&mut raw, value.ip());
        let bytes = (&mut raw as *mut sockaddr_storage).cast::<u8>();
        bytes.add(2).cast::<u16>().write_unaligned(value.port().to_be());
        if let SocketAddr::V6(value) = value {
            bytes.add(4).cast::<u32>().write_unaligned(value.flowinfo().to_be());
            bytes.add(24).cast::<u32>().write_unaligned(value.scope_id());
        }
    }
    raw
}

#[test]
fn port_managers_preserve_ipv4_ipv6_and_explicit_ports_and_drop_after_close() {
    let events = Arc::new(Observed::default());
    let context = AeronDriverContext::new().unwrap();
    let mut channel = aeron_udp_channel_t::default();
    for role in [PortManagerRole::Sender, PortManagerRole::Receiver] {
        context.set_port_manager(role, Probe(events.clone())).unwrap();
        context.set_port_manager(role, Probe(events.clone())).unwrap();
        let table = unsafe {
            &*match role {
                PortManagerRole::Sender => (*context.get_inner()).sender_port_manager,
                PortManagerRole::Receiver => (*context.get_inner()).receiver_port_manager,
            }
        };
        for value in [
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:42002".parse().unwrap(),
            SocketAddr::V6(std::net::SocketAddrV6::new("fe80::1234".parse().unwrap(), 0, 7, 9)),
        ] {
            let mut requested = address(value);
            let mut output = sockaddr_storage::default();
            unsafe {
                assert_eq!(
                    table.get_managed_port.unwrap()(table.state, &mut output, &mut channel, &mut requested),
                    0
                );
                let mut expected = value;
                expected.set_port(if value.port() == 0 { 42001 } else { value.port() });
                assert_eq!(SockAddrRef::from_raw(&output).to_socket_addr(), Some(expected));
                table.free_managed_port.unwrap()(table.state, &mut output);
            }
        }
    }
    assert_eq!(
        *events.calls.lock().unwrap(),
        [0, -42001, 42002, -42002, 0, -42001].repeat(2)
    );
    drop(context);
    assert_eq!(events.drops.load(Ordering::Relaxed), 4);
}

struct RejectPort;
impl PortManager for RejectPort {
    fn get_managed_port(&mut self, _: ChannelRef<'_>, _: SocketAddr) -> Result<u16, AeronCError> {
        Err(AeronCError::with_message(-1, "port pool 100% used"))
    }
    fn free_managed_port(&mut self, _: SocketAddr) {
        panic!("invalid addresses must not reach user code");
    }
}

#[test]
fn port_errors_leave_output_unchanged_and_invalid_families_skip_user_code() {
    let context = AeronDriverContext::new().unwrap();
    context.set_port_manager(PortManagerRole::Receiver, RejectPort).unwrap();
    let table = unsafe { &*(*context.get_inner()).receiver_port_manager };
    let mut channel = aeron_udp_channel_t::default();
    let mut output = address("127.0.0.1:17".parse().unwrap());
    for mut input in [address("127.0.0.1:0".parse().unwrap()), sockaddr_storage::default()] {
        unsafe {
            assert_eq!(
                table.get_managed_port.unwrap()(table.state, &mut output, &mut channel, &mut input),
                -1
            );
            assert_eq!(SockAddrRef::from_raw(&output).to_socket_addr().unwrap().port(), 17);
            assert!(!CStr::from_ptr(aeron_errmsg()).is_empty());
            aeron_err_clear();
        }
    }
    unsafe {
        table.free_managed_port.unwrap()(table.state, &mut sockaddr_storage::default());
    }
}

#[test]
fn native_transport_validates_required_callbacks_and_retains_resources_for_both_roles() {
    let events = Arc::new(Observed::default());
    let context = AeronDriverContext::new().unwrap();
    let callbacks = NativeTransportBindings::udp().callbacks();
    // Each absent callback is rejected, including callbacks on uncommon paths.
    let mut invalid = Vec::new();
    macro_rules! missing { ($($field:ident),*) => { $( {
        let mut table = callbacks; table.$field = None; invalid.push(table);
    } )* }; }
    missing!(
        init_func,
        reconnect_func,
        close_func,
        recvmmsg_func,
        send_func,
        get_so_rcvbuf_func,
        bind_addr_and_port_func,
        poller_init_func,
        poller_close_func,
        poller_add_func,
        poller_remove_func,
        poller_poll_func
    );
    let mut table = callbacks;
    table.meta_info.name = std::ptr::null();
    invalid.push(table);
    table = callbacks;
    table.meta_info.type_ = std::ptr::null();
    invalid.push(table);
    for table in invalid {
        assert!(unsafe { NativeTransportBindings::from_callbacks(table, ()) }.is_err());
    }
    for role in [TransportRole::Data, TransportRole::Conductor] {
        let bindings = unsafe { NativeTransportBindings::from_callbacks(callbacks, Probe(events.clone())) }.unwrap();
        context.set_native_transport_bindings(role, bindings).unwrap();
        let table = unsafe {
            &*match role {
                TransportRole::Data => (*context.get_inner()).udp_channel_transport_bindings,
                TransportRole::Conductor => (*context.get_inner()).conductor_udp_channel_transport_bindings,
            }
        };
        assert!(std::ptr::fn_addr_eq(
            table.send_func.unwrap(),
            callbacks.send_func.unwrap()
        ));
        context
            .set_native_transport_bindings(role, NativeTransportBindings::udp())
            .unwrap();
    }
    assert_eq!(events.drops.load(Ordering::Relaxed), 0);
    drop(context);
    assert_eq!(events.drops.load(Ordering::Relaxed), 2);
}

#[test]
fn new_extension_setters_reject_frozen_contexts_and_drop_rejected_values() {
    let events = Arc::new(Observed::default());
    let context = AeronDriverContext::new().unwrap();
    context
        .set_dir(&format!("/tmp/rusteron-extensions-{}", std::process::id()).into_c_string())
        .unwrap();
    context.set_dir_delete_on_start(true).unwrap();
    context.set_dir_delete_on_shutdown(true).unwrap();
    let driver = crate::AeronDriver::new(&context).unwrap();
    assert!(
        context
            .set_idle_strategy(IdleStrategyRole::Conductor, Probe(events.clone()))
            .is_err()
    );
    assert!(
        context
            .set_duty_cycle_tracker(DutyCycleRole::Conductor, Probe(events.clone()))
            .is_err()
    );
    assert!(
        context
            .set_port_manager(PortManagerRole::Sender, Probe(events.clone()))
            .is_err()
    );
    let mut probe = Probe(events.clone());
    assert!(
        context
            .set_name_resolver_fn(move |_, _, _| {
                probe.idle(1);
                Ok(std::net::Ipv4Addr::LOCALHOST.into())
            })
            .is_err()
    );
    let bindings = unsafe {
        NativeTransportBindings::from_callbacks(NativeTransportBindings::udp().callbacks(), Probe(events.clone()))
    }
    .unwrap();
    assert!(
        context
            .set_native_transport_bindings(TransportRole::Data, bindings)
            .is_err()
    );
    assert_eq!(events.drops.load(Ordering::Relaxed), 5);
    drop(driver);
}

#[test]
fn idle_closures_keep_mutable_state_and_drop_after_context_close() {
    let context = AeronDriverContext::new().unwrap();
    let events = Arc::new(Observed::default());
    let mut probe = Probe(events.clone());
    let mut total = 0;
    context
        .set_idle_strategy(IdleStrategyRole::Shared, move |work| {
            total += work;
            probe.idle(total);
        })
        .unwrap();
    unsafe {
        let raw = &*context.get_inner();
        let idle = raw.shared_idle_strategy_func.unwrap();
        idle(raw.shared_idle_strategy_state, -1);
        idle(raw.shared_idle_strategy_state, 0);
        idle(raw.shared_idle_strategy_state, 5);
    }
    assert_eq!(*events.calls.lock().unwrap(), [-1, -1, 4]);
    assert_eq!(events.drops.load(Ordering::Relaxed), 0);
    drop(context);
    assert_eq!(events.drops.load(Ordering::Relaxed), 1);
}

#[test]
fn duty_cycle_timer_resets_its_baseline_and_bounds_signed_differences() {
    let events = Arc::new(Observed::default());
    let mut probe = Probe(events.clone());
    let mut timer = DutyCycleTimer::new(move |elapsed| probe.measure_and_update(elapsed));
    timer.measure_and_update(10);
    assert!(events.calls.lock().unwrap().is_empty());
    timer.measure_and_update(25);
    timer.update(100);
    timer.measure_and_update(120);
    timer.measure_and_update(119);
    timer.update(i64::MIN);
    timer.measure_and_update(i64::MAX);
    timer.measure_and_update(i64::MIN);
    timer.measure_and_update(i64::MIN + 7);
    assert_eq!(*events.calls.lock().unwrap(), [15, 20, 0, i64::MAX, 0, 7]);
    drop(timer);
    assert_eq!(events.drops.load(Ordering::Relaxed), 1);
}

#[test]
fn port_closures_receive_channels_and_support_default_release() {
    let context = AeronDriverContext::new().unwrap();
    let events = Arc::new(Observed::default());
    let mut probe = Probe(events.clone());
    context
        .set_port_manager(
            PortManagerRole::Sender,
            move |channel: ChannelRef<'_>, requested: SocketAddr| {
                assert!(channel.uri().is_empty());
                probe.idle(i32::from(requested.port()));
                if requested.port() == 0 {
                    Err(AeronCError::with_message(-1, "explicit ports only"))
                } else {
                    Ok(requested.port())
                }
            },
        )
        .unwrap();
    unsafe {
        let table = &*(*context.get_inner()).sender_port_manager;
        let mut channel = aeron_udp_channel_t::default();
        let mut requested = address("[::1]:42000".parse().unwrap());
        let mut output = sockaddr_storage::default();
        assert_eq!(
            table.get_managed_port.unwrap()(table.state, &mut output, &mut channel, &mut requested),
            0
        );
        assert_eq!(
            SockAddrRef::from_raw(&output).to_socket_addr(),
            Some("[::1]:42000".parse().unwrap())
        );
        table.free_managed_port.unwrap()(table.state, &mut output);
        requested = address("127.0.0.1:0".parse().unwrap());
        assert_eq!(
            table.get_managed_port.unwrap()(table.state, &mut output, &mut channel, &mut requested),
            -1
        );
    }
    assert_eq!(*events.calls.lock().unwrap(), [42000, 0]);
    drop(context);
    assert_eq!(events.drops.load(Ordering::Relaxed), 1);
}

fn install_resolver(context: &AeronDriverContext) -> aeron_name_resolver_t {
    let mut resolver = aeron_name_resolver_t::default();
    unsafe {
        let supplier = aeron_driver_context_get_name_resolver_supplier(context.get_inner()).unwrap();
        assert_eq!(supplier(&mut resolver, std::ptr::null(), context.get_inner()), 0);
    }
    resolver
}

fn resolve(resolver: &mut aeron_name_resolver_t, again: bool) -> std::net::IpAddr {
    let mut output = sockaddr_storage::default();
    unsafe {
        assert_eq!(
            resolver.resolve_func.unwrap()(resolver, c"peer".as_ptr(), c"endpoint".as_ptr(), again, &mut output),
            0
        );
        SockAddrRef::from_raw(&output).to_socket_addr().unwrap().ip()
    }
}

#[test]
fn resolver_closures_keep_contexts_separate_and_transfer_ownership_once() {
    let events = Arc::new(Observed::default());
    let contexts = [AeronDriverContext::new().unwrap(), AeronDriverContext::new().unwrap()];
    for (index, context) in contexts.iter().enumerate() {
        let mut probe = Probe(events.clone());
        // Cell is Send but not Sync. One native agent owns this resolver.
        let calls = std::cell::Cell::new(0);
        context
            .set_name_resolver_fn(move |name, parameter, again| {
                assert_eq!(name, c"peer");
                assert_eq!(parameter, c"endpoint");
                assert_eq!(again, calls.get() > 0);
                calls.set(calls.get() + 1);
                probe.idle(calls.get());
                Ok(std::net::Ipv4Addr::new(127, 0, 0, index as u8 + 1).into())
            })
            .unwrap();
    }
    let mut resolvers = contexts.each_ref().map(install_resolver);
    for (index, resolver) in resolvers.iter_mut().enumerate() {
        let ip = std::net::Ipv4Addr::new(127, 0, 0, index as u8 + 1);
        assert_eq!(resolve(resolver, false), ip);
        assert_eq!(resolve(resolver, true), ip);
        let mut unused = aeron_name_resolver_t::default();
        unsafe {
            let context = contexts[index].get_inner();
            assert_eq!(
                aeron_driver_context_get_name_resolver_supplier(context).unwrap()(
                    &mut unused,
                    std::ptr::null(),
                    context
                ),
                -1
            );
            assert!(unused.state.is_null());
            assert_eq!(resolver.close_func.unwrap()(resolver), 0);
        }
    }
    assert_eq!(*events.calls.lock().unwrap(), [1, 2, 1, 2]);
    assert_eq!(events.drops.load(Ordering::Relaxed), 2);
    drop(contexts);
    assert_eq!(events.drops.load(Ordering::Relaxed), 2);
}

#[test]
fn resolver_replacement_retains_unused_values_and_can_change_concrete_type() {
    let context = AeronDriverContext::new().unwrap();
    let events = Arc::new(Observed::default());
    let mut probe = Probe(events.clone());
    context
        .set_name_resolver_fn(move |_, _, _| {
            probe.idle(1);
            Ok(std::net::Ipv4Addr::LOCALHOST.into())
        })
        .unwrap();
    context
        .set_name_resolver_fn(|_, _, _| Ok(std::net::Ipv6Addr::LOCALHOST.into()))
        .unwrap();
    let mut resolver = install_resolver(&context);
    assert_eq!(resolve(&mut resolver, false), std::net::Ipv6Addr::LOCALHOST);
    unsafe {
        resolver.close_func.unwrap()(&mut resolver);
    }
    assert!(events.calls.lock().unwrap().is_empty());
    assert_eq!(events.drops.load(Ordering::Relaxed), 0);
    drop(context);
    assert_eq!(events.drops.load(Ordering::Relaxed), 1);
}

#[test]
fn resolver_closure_errors_preserve_the_output_address() {
    let context = AeronDriverContext::new().unwrap();
    context
        .set_name_resolver_fn(|_, _, _| Err(AeronCError::with_message(-1, "unknown host")))
        .unwrap();
    let mut resolver = install_resolver(&context);
    let mut output = address("127.0.0.1:42".parse().unwrap());
    unsafe {
        assert_eq!(
            resolver.resolve_func.unwrap()(
                &mut resolver,
                c"peer".as_ptr(),
                c"endpoint".as_ptr(),
                false,
                &mut output
            ),
            -1
        );
        assert_eq!(
            SockAddrRef::from_raw(&output).to_socket_addr(),
            Some("127.0.0.1:42".parse().unwrap())
        );
        resolver.close_func.unwrap()(&mut resolver);
    }
}

#[test]
fn resolver_registration_handles_an_exhausted_native_context() {
    let context = AeronDriverContext::new().unwrap();
    let events = Arc::new(Observed::default());
    unsafe {
        let raw = &mut *context.get_inner();
        for i in 0..raw.num_bindings_clientd_entries {
            let entry = &mut *raw.bindings_clientd_entries.add(i);
            entry.name = c"occupied".as_ptr();
            entry.clientd = std::ptr::dangling_mut::<c_void>();
        }
    }
    let mut probe = Probe(events.clone());
    assert!(
        context
            .set_name_resolver_fn(move |_, _, _| {
                probe.idle(1);
                Ok(std::net::Ipv4Addr::LOCALHOST.into())
            })
            .is_err()
    );
    assert_eq!(events.drops.load(Ordering::Relaxed), 1);
}

#[derive(Default)]
struct DefaultResolver;
impl NameResolver for DefaultResolver {
    fn resolve(&mut self, _: &CStr, _: &CStr, _: bool) -> Result<std::net::IpAddr, AeronCError> {
        Ok(std::net::Ipv4Addr::LOCALHOST.into())
    }
}

#[test]
fn default_resolvers_need_only_a_resolve_implementation() {
    let context = AeronDriverContext::new().unwrap();
    context.set_name_resolver::<DefaultResolver>().unwrap();
    let mut resolver = install_resolver(&context);
    assert_eq!(resolve(&mut resolver, false), std::net::Ipv4Addr::LOCALHOST);
    unsafe {
        resolver.close_func.unwrap()(&mut resolver);
    }
}
