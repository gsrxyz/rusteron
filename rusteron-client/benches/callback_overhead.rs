//! Isolates callback storage from native trampolines, the driver and transport.
//!
//! Every case performs the same callback work. Differences include the storage's
//! borrow check, synchronization and dispatch. These are per-call costs, not
//! message latency percentiles. No other thread accesses the shared handlers, so
//! their measurements exclude contention and scheduler delays.
//!
//! Run with `cargo bench -p rusteron-client --features static --bench callback_overhead`.

use criterion::{Criterion, criterion_group, criterion_main};
use rusteron_client::{AeronErrorHandlerCallback, AeronErrorHandlerSlot, Handler, SharedHandler};
use std::hint::black_box;

fn callback() -> impl FnMut(i32, &str) + Send {
    let mut total = 0u64;
    move |code, message| {
        total = total.wrapping_add(code as u64 + message.len() as u64);
        black_box(total);
    }
}

fn callback_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("callback_overhead");
    let message = "callback";

    let mut owned = callback();
    group.bench_function("owned", |b| {
        b.iter(|| black_box(&mut owned).handle_aeron_error_handler(black_box(1), black_box(message)))
    });

    let mut local = Handler::new(callback());
    group.bench_function("local_handler", |b| {
        b.iter(|| black_box(&mut local).handle_aeron_error_handler(black_box(1), black_box(message)))
    });

    let mut shared = SharedHandler::new_shared(callback());
    group.bench_function("shared_handler", |b| {
        b.iter(|| black_box(&mut shared).handle_aeron_error_handler(black_box(1), black_box(message)))
    });

    let slot = AeronErrorHandlerSlot::new();
    slot.set(callback());
    let mut replaceable = SharedHandler::new_shared(slot);
    group.bench_function("shared_handler_with_slot", |b| {
        b.iter(|| black_box(&mut replaceable).handle_aeron_error_handler(black_box(1), black_box(message)))
    });

    group.finish();
}

criterion_group!(benches, callback_overhead);
criterion_main!(benches);
