//! Per-callback cost of the Rust extension trampolines, called through the same C tables
//! the driver uses. Flow control compares against Aeron's native unicast max strategy.
//! The interceptor compares against calling the next delegate directly.

use criterion::{Criterion, criterion_group, criterion_main};
use rusteron_media_driver::AeronCError;
use rusteron_media_driver::AeronDriverContext;
use rusteron_media_driver::bindings::*;
use rusteron_media_driver::extensions::*;
use std::hint::black_box;

// A 44-byte status message for term 10, offset 128 and a 1024-byte window
fn status_message() -> [u8; 44] {
    let mut bytes = [0; 44];
    bytes[..4].copy_from_slice(&44i32.to_le_bytes());
    bytes[16..20].copy_from_slice(&10i32.to_le_bytes());
    bytes[20..24].copy_from_slice(&128i32.to_le_bytes());
    bytes[24..28].copy_from_slice(&1024i32.to_le_bytes());
    bytes[28..36].copy_from_slice(&1i64.to_le_bytes());
    bytes
}

// Aeron's max policy in Rust, so both sides compute the same limit
struct MaxFlowControl;
impl FlowControl for MaxFlowControl {
    type Shared = ();
    fn new(_: &FlowControlArgs<'_>) -> Result<(Self, ()), AeronCError> {
        Ok((Self, ()))
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
}

fn flow_strategy(
    context: &AeronDriverContext,
    supplier: aeron_flow_control_strategy_supplier_func_t,
) -> *mut aeron_flow_control_strategy_t {
    let channel = aeron_udp_channel_t::default();
    let mut raw = std::ptr::null_mut();
    // SAFETY: both suppliers only read the context and channel during this call
    let result = unsafe {
        supplier.unwrap()(
            &mut raw,
            context.get_inner(),
            std::ptr::null_mut(),
            &channel,
            7,
            8,
            9,
            9,
            65536,
        )
    };
    assert_eq!(result, 0, "flow-control supplier failed");
    raw
}

fn flow_control(c: &mut Criterion) {
    let context = AeronDriverContext::new().unwrap();
    context.set_unicast_flow_control::<MaxFlowControl>().unwrap();
    // SAFETY: the context is alive and the supplier was just installed
    let rust = flow_strategy(&context, unsafe {
        aeron_driver_context_get_unicast_flowcontrol_supplier(context.get_inner())
    });
    let native = flow_strategy(&context, Some(aeron_unicast_flow_control_strategy_supplier));
    let message = status_message();
    let mut address = sockaddr_storage::default();
    let mut group = c.benchmark_group("flow_control_on_status_message");
    for (name, strategy) in [("native_c", native), ("rust_max", rust)] {
        // SAFETY: the strategy stays alive until fini below
        let table = unsafe { *strategy };
        let on_status_message = table.on_status_message.unwrap();
        let mut call = |limit| unsafe {
            on_status_message(
                table.state,
                message.as_ptr(),
                message.len(),
                &mut address,
                limit,
                9,
                16,
                1,
            )
        };
        // Term 10 is one 64 KiB term past initial term 9, so both must reach the same limit
        assert_eq!(call(0), 65_536 + 128 + 1024, "{name} computed a different limit");
        group.bench_function(name, |b| b.iter(|| call(black_box(0))));
    }
    group.finish();
    for strategy in [native, rust] {
        // SAFETY: each strategy is released once, after its last callback
        unsafe { (*strategy).fini.unwrap()(strategy) };
    }
}

#[derive(Default)]
struct Count(u64);
impl OutgoingInterceptor for Count {
    fn inspect(&mut self, packet: &OutgoingPacket<'_>) {
        self.0 += packet.len() as u64;
    }
}

unsafe extern "C" fn sink(
    _: *mut std::ffi::c_void,
    _: *mut aeron_udp_channel_outgoing_interceptor_t,
    _: *mut aeron_udp_channel_transport_t,
    _: *mut sockaddr_storage,
    _: *mut iovec,
    length: usize,
    bytes_sent: *mut i64,
) -> i32 {
    unsafe { *bytes_sent += length as i64 };
    0
}

fn interceptor_send(c: &mut Criterion) {
    let context = AeronDriverContext::new().unwrap();
    context.add_outgoing_interceptor::<Count>().unwrap();
    let ctx = context.get_inner();
    // SAFETY: the context owns the table it just installed
    let table = unsafe { &*aeron_driver_context_get_udp_channel_outgoing_interceptors(ctx) };
    let mut state = std::ptr::null_mut();
    let affinity = aeron_udp_channel_transport_affinity_t::AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER;
    // SAFETY: init writes one owned instance that close releases below
    assert_eq!(
        unsafe { table.outgoing_init_func.unwrap()(&mut state, ctx, affinity) },
        0
    );
    let mut next = aeron_udp_channel_outgoing_interceptor_t {
        outgoing_send_func: Some(sink),
        ..Default::default()
    };
    let mut transport = aeron_udp_channel_transport_t::default();
    let mut address = sockaddr_storage::default();
    let mut payload = [0u8; 64];
    let mut iov = iovec {
        iov_base: payload.as_mut_ptr().cast(),
        iov_len: payload.len(),
    };
    let mut bytes_sent = 0i64;
    let mut group = c.benchmark_group("outgoing_interceptor_send");
    for (name, (send, interceptor_state)) in [
        (
            "direct_next",
            (
                sink as unsafe extern "C" fn(_, _, _, _, _, _, _) -> _,
                std::ptr::null_mut(),
            ),
        ),
        ("rust_inspect", (table.outgoing_send_func.unwrap(), state)),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| unsafe {
                send(
                    interceptor_state,
                    &mut next,
                    &mut transport,
                    &mut address,
                    &mut iov,
                    black_box(1),
                    &mut bytes_sent,
                )
            })
        });
    }
    group.finish();
    black_box(bytes_sent);
    // SAFETY: the last callback has returned
    assert_eq!(unsafe { table.outgoing_close_func.unwrap()(state) }, 0);
}

criterion_group!(benches, flow_control, interceptor_send);
criterion_main!(benches);
