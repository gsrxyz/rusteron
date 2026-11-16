//! Latency micro-benchmarks for the publish/claim hot paths.
//!
//! Compare typed, raw and direct C APIs with the same application work. `claim`
//! measures reservation and commit; `claim_copy` also copies the same 32-byte payload.
//! These are amortized operation costs, not message latency percentiles.
//! Each arm publishes in batches; between them, untimed, it drains the subscription and waits
//! for room in the publication window, so every publish lands instead of returning back pressure.
//! Run with `cargo bench -p rusteron-client --features static --bench offer_claim_poll`.

use criterion::{Criterion, criterion_group, criterion_main};
use rusteron_client::bindings::{aeron_buffer_claim_commit, aeron_publication_offer, aeron_publication_try_claim};
use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::ffi::CStr;
use std::hint::black_box;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 7001;
static CHANNEL: &CStr = AERON_IPC_STREAM;
const PAYLOAD_LEN: usize = 32;
// a 32-byte payload takes a 64-byte frame, so a batch is 256KB, far below the IPC publication window
const FRAME_LEN: i64 = 64;
const BATCH: u64 = 4096;

/// An embedded driver with a connected IPC publication and subscription.
struct Harness {
    publisher: AeronPublication,
    subscription: AeronSubscription,
    _aeron: Aeron,
    _ctx: AeronContext,
    // last, so the client closes before the driver stops
    _driver: EmbeddedDriver,
}

fn harness() -> Harness {
    // The default three 64 MiB IPC terms can exceed a container's /dev/shm.
    let directory = std::env::temp_dir().join(format!(
        "rusteron-claim-bench-{}-{}",
        std::process::id(),
        Aeron::nano_clock()
    ));
    let driver = EmbeddedDriver::launch_with(|ctx| {
        ctx.set_dir(&directory.to_string_lossy().into_c_string())?;
        Ok(())
    })
    .unwrap();
    let ctx = AeronContext::new().unwrap();
    ctx.set_dir(&driver.dir().into_c_string()).unwrap();
    let aeron = Aeron::new(&ctx).unwrap();
    aeron.start().unwrap();

    let publisher = aeron
        .async_add_publication(CHANNEL, STREAM_ID)
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();
    let subscription = aeron
        .async_add_subscription(CHANNEL, STREAM_ID, Handlers::NONE, Handlers::NONE)
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();

    // Wait until the publication sees the subscriber image.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !publisher.is_connected() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(publisher.is_connected(), "publication never connected");

    Harness {
        publisher,
        subscription,
        _aeron: aeron,
        _ctx: ctx,
        _driver: driver,
    }
}

/// Times `iters` successful publishes, including identical retry behaviour in
/// every arm. Window waits and receive draining stay outside the timer.
fn timed(h: &Harness, iters: u64, mut publish: impl FnMut() -> bool) -> Duration {
    let mut elapsed = Duration::ZERO;
    let mut left = iters;
    while left > 0 {
        let batch = left.min(BATCH);
        // the driver reopens the window only as fast as it cleans old terms, which a
        // saturating publisher can outrun
        while h.publisher.position_limit() - h.publisher.position() < batch as i64 * FRAME_LEN {
            std::thread::yield_now();
        }
        let start = Instant::now();
        for _ in 0..batch {
            // Occasional ADMIN_ACTION at term rotation is part of every arm.
            while !publish() {}
        }
        elapsed += start.elapsed();
        let mut received = 0;
        let deadline = Instant::now() + Duration::from_secs(5);
        while received < batch {
            received += h.subscription.poll_fn(|_, _| {}, 1024).expect("drain subscription") as u64;
            assert!(Instant::now() < deadline, "subscription did not receive the batch");
        }
        assert_eq!(received, batch);
        left -= batch;
    }
    elapsed
}

fn bench_offer(c: &mut Criterion) {
    let h = harness();
    let payload = vec![0u8; PAYLOAD_LEN];
    let claim_buf = AeronBufferClaim::default();
    // read once, as a C caller holds its pointers
    let publication = h.publisher.get_inner();
    let claim = claim_buf.get_inner();

    // Touch the complete three-term ring before Criterion's own per-arm warmup.
    // This is setup work: its returned duration is intentionally discarded.
    let constants = AeronPublicationConstants::new_zeroed_on_stack();
    h.publisher.constants(&constants).unwrap();
    let ring_messages = 3 * constants.term_buffer_length() as u64 / FRAME_LEN as u64 + 1;
    let _ = timed(&h, ring_messages, || h.publisher.offer(&payload).is_ok());

    {
        let mut g = c.benchmark_group("offer");
        g.bench_function("c_offer", |b| {
            b.iter_custom(|iters| {
                timed(&h, iters, || {
                    // SAFETY: the publication outlives the bench and the payload outlives the call.
                    black_box(unsafe {
                        aeron_publication_offer(
                            publication,
                            black_box(payload.as_ptr()),
                            PAYLOAD_LEN,
                            None,
                            std::ptr::null_mut(),
                        )
                    }) >= 0
                })
            })
        });
        g.bench_function("raw_offer_i64", |b| {
            b.iter_custom(|iters| {
                timed(&h, iters, || {
                    black_box(h.publisher.offer_raw(black_box(&payload), Handlers::NONE)) >= 0
                })
            })
        });
        g.bench_function("offer", |b| {
            b.iter_custom(|iters| timed(&h, iters, || black_box(h.publisher.offer(black_box(&payload))).is_ok()))
        });
        g.finish();
    }

    {
        let mut g = c.benchmark_group("claim");
        g.bench_function("c_try_claim_commit", |b| {
            b.iter_custom(|iters| {
                timed(&h, iters, || {
                    // SAFETY: the publication and the claim outlive the bench; the claim is
                    // committed only after a successful try_claim filled it.
                    unsafe {
                        if aeron_publication_try_claim(publication, PAYLOAD_LEN, claim) >= 0 {
                            assert_eq!(aeron_buffer_claim_commit(claim), 0);
                            true
                        } else {
                            false
                        }
                    }
                })
            })
        });
        g.bench_function("raw_try_claim_commit", |b| {
            b.iter_custom(|iters| {
                timed(&h, iters, || {
                    if h.publisher.try_claim_raw(PAYLOAD_LEN, &claim_buf) >= 0 {
                        claim_buf.commit().unwrap();
                        true
                    } else {
                        false
                    }
                })
            })
        });
        g.bench_function("try_claim_owned_commit", |b| {
            b.iter_custom(|iters| {
                timed(&h, iters, || {
                    if let Ok(claim) = h.publisher.try_claim_owned(PAYLOAD_LEN) {
                        claim.commit().unwrap();
                        true
                    } else {
                        false
                    }
                })
            })
        });
        g.finish();
    }

    {
        let mut g = c.benchmark_group("claim_copy");
        g.bench_function("c_try_claim_copy_commit", |b| {
            b.iter_custom(|iters| {
                timed(&h, iters, || {
                    // SAFETY: native success initializes the claim; it remains
                    // writable until commit. The payload has exactly PAYLOAD_LEN bytes.
                    unsafe {
                        if aeron_publication_try_claim(publication, PAYLOAD_LEN, claim) >= 0 {
                            std::slice::from_raw_parts_mut((*claim).data, (*claim).length)
                                .copy_from_slice(black_box(&payload));
                            assert_eq!(aeron_buffer_claim_commit(claim), 0);
                            true
                        } else {
                            false
                        }
                    }
                })
            })
        });
        g.bench_function("raw_try_claim_copy_commit", |b| {
            b.iter_custom(|iters| {
                timed(&h, iters, || {
                    if h.publisher.try_claim_raw(PAYLOAD_LEN, &claim_buf) >= 0 {
                        claim_buf.data().copy_from_slice(black_box(&payload));
                        claim_buf.commit().unwrap();
                        true
                    } else {
                        false
                    }
                })
            })
        });
        g.bench_function("try_claim_owned_copy_commit", |b| {
            b.iter_custom(|iters| {
                timed(&h, iters, || {
                    if let Ok(mut claim) = h.publisher.try_claim_owned(PAYLOAD_LEN) {
                        claim.data().copy_from_slice(black_box(&payload));
                        claim.commit().unwrap();
                        true
                    } else {
                        false
                    }
                })
            })
        });
        g.finish();
    }
}

criterion_group!(benches, bench_offer);
criterion_main!(benches);
