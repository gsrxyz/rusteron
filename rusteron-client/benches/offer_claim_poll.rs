//! Latency micro-benchmarks for the publish/claim hot paths.
//!
//! Guards the latency prime directive: the typed `offer_result` / `try_claim_owned`
//! wrappers must add ~zero overhead vs the raw `i64`-returning `offer` / `try_claim`.
//! Run with `cargo bench -p rusteron-client`.

use criterion::{criterion_group, criterion_main, Criterion};
use rusteron_client::*;
use rusteron_media_driver::{AeronDriver, AeronDriverContext};
use std::ffi::CStr;
use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::time::Duration;

const STREAM_ID: i32 = 7001;
static CHANNEL: &CStr = AERON_IPC_STREAM;
const PAYLOAD_LEN: usize = 32;

/// Bring up an embedded driver + connected IPC publication/subscription.
struct Harness {
    publisher: AeronPublication,
    _subscription: AeronSubscription,
    _aeron: Aeron,
    _ctx: AeronContext,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: std::thread::JoinHandle<Result<(), rusteron_media_driver::AeronCError>>,
}

fn harness() -> Harness {
    let driver_ctx = AeronDriverContext::new().unwrap();
    driver_ctx.set_dir_delete_on_start(true).unwrap();
    driver_ctx.set_dir_delete_on_shutdown(true).unwrap();
    driver_ctx
        .set_dir(&format!("{}bench-offer", driver_ctx.get_dir()).into_c_string())
        .unwrap();
    let (stop, handle) = AeronDriver::launch_embedded(driver_ctx.clone(), false);

    let ctx = AeronContext::new().unwrap();
    ctx.set_dir(&driver_ctx.get_dir().into_c_string()).unwrap();
    let aeron = Aeron::new(&ctx).unwrap();
    aeron.start().unwrap();

    let publisher = aeron
        .async_add_publication(CHANNEL, STREAM_ID)
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();
    let subscription = aeron
        .async_add_subscription(
            CHANNEL,
            STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();

    // Wait until the publication sees the subscriber image.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !publisher.is_connected() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(publisher.is_connected(), "publication never connected");

    Harness {
        publisher,
        _subscription: subscription,
        _aeron: aeron,
        _ctx: ctx,
        stop,
        handle,
    }
}

fn bench_offer(c: &mut Criterion) {
    let h = harness();
    let payload = vec![0u8; PAYLOAD_LEN];
    let claim_buf = AeronBufferClaim::default();

    {
        let mut g = c.benchmark_group("offer");
        g.bench_function("raw_offer_i64", |b| {
            b.iter(|| {
                let _ = black_box(
                    h.publisher
                        .offer(black_box(&payload), Handlers::no_reserved_value_supplier_handler()),
                );
            });
        });
        g.bench_function("offer_result_simple", |b| {
            b.iter(|| {
                let _ = black_box(h.publisher.offer_result_simple(black_box(&payload)));
            });
        });
        g.finish();
    }

    {
        let mut g = c.benchmark_group("claim");
        g.bench_function("raw_try_claim_commit", |b| {
            b.iter(|| {
                if h.publisher.try_claim(PAYLOAD_LEN, &claim_buf) >= 0 {
                    let _ = claim_buf.commit();
                }
            });
        });
        g.bench_function("try_claim_owned_commit", |b| {
            b.iter(|| {
                if let Ok(claim) = h.publisher.try_claim_owned(PAYLOAD_LEN) {
                    let _ = claim.commit();
                }
            });
        });
        g.finish();
    }

    drop(h.publisher);
    h.stop.store(true, Ordering::SeqCst);
    let _ = h.handle.join();
}

criterion_group!(benches, bench_offer);
criterion_main!(benches);
