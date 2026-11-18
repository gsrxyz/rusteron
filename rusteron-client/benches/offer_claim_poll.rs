//! Amortised operation costs for the publish/claim hot paths.
//!
//! Compare typed, raw and direct C APIs with the same application work. `claim`
//! measures reservation and commit; `claim_copy` also copies the payload. Each API
//! is measured with the same 32-, 256- and 1,024-byte message sizes.
//! These are amortised operation costs, not message latency percentiles.
//! Each arm publishes in batches; between them, untimed, it drains the subscription and waits
//! for room in the publication window, so every publish lands instead of returning back pressure.
//! Run with `cargo bench -p rusteron-client --features static --bench offer_claim_poll`

use criterion::{Criterion, criterion_group, criterion_main};
use rusteron_client::bindings::{
    AERON_DATA_HEADER_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT, aeron_buffer_claim_commit, aeron_publication_offer,
    aeron_publication_try_claim,
};
use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::ffi::CStr;
use std::hint::black_box;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 7001;
static CHANNEL: &CStr = AERON_IPC_STREAM;
const PAYLOAD_LENGTHS: [usize; 3] = [32, 256, 1024];
// Even the largest payload's batch is below the default IPC publication window
const BATCH: u64 = 4096;
const POLL_LIMIT: usize = 1024;
const TIMEOUT: Duration = Duration::from_secs(5);

fn frame_length(payload_len: usize) -> usize {
    let alignment = AERON_LOGBUFFER_FRAME_ALIGNMENT as usize;
    (payload_len + AERON_DATA_HEADER_LENGTH as usize).div_ceil(alignment) * alignment
}

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
    // The default three 64 MiB IPC terms can exceed a container's /dev/shm
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
/// every arm. Window waits and receive draining stay outside the timer
fn timed(h: &Harness, iters: u64, payload: &[u8], check_payload: bool, mut publish: impl FnMut() -> bool) -> Duration {
    let frame_len = frame_length(payload.len()) as i64;
    let mut elapsed = Duration::ZERO;
    let mut left = iters;
    while left > 0 {
        let batch = left.min(BATCH);
        // the driver reopens the window only as fast as it cleans old terms, which a
        // saturating publisher can outrun
        let deadline = Instant::now() + TIMEOUT;
        while h.publisher.position_limit() - h.publisher.position() < batch as i64 * frame_len {
            assert!(Instant::now() < deadline, "publication window did not reopen");
            std::thread::yield_now();
        }
        let deadline = Instant::now() + TIMEOUT;
        let start = Instant::now();
        for _ in 0..batch {
            // Occasional ADMIN_ACTION at term rotation is part of every arm
            while !publish() {
                assert!(Instant::now() < deadline, "publication did not accept the batch");
            }
        }
        elapsed += start.elapsed();
        let mut received = 0;
        let deadline = Instant::now() + Duration::from_secs(5);
        while received < batch {
            received += h
                .subscription
                .poll_fn(
                    |data, _| {
                        assert_eq!(data.len(), payload.len());
                        if check_payload {
                            assert_eq!(data, payload, "published payload differs");
                        }
                    },
                    POLL_LIMIT,
                )
                .expect("drain subscription") as u64;
            assert!(Instant::now() < deadline, "subscription did not receive the batch");
        }
        assert_eq!(received, batch);
        left -= batch;
    }
    elapsed
}

fn bench_offer(c: &mut Criterion) {
    let h = harness();
    let warmup_payload = [0xA5; 32];
    let claim_buf = AeronBufferClaim::default();
    // read once, as a C caller holds its pointers
    let publication = h.publisher.get_inner();
    let claim = claim_buf.get_inner();

    // Touch the complete three-term ring before Criterion's own per-arm warmup.
    // This is setup work: its returned duration is intentionally discarded
    let constants = h.publisher.get_constants().unwrap();
    let ring_messages = 3 * constants.term_buffer_length() as u64 / frame_length(warmup_payload.len()) as u64 + 1;
    let _ = timed(&h, ring_messages, &warmup_payload, true, || {
        h.publisher.offer(&warmup_payload).is_ok()
    });

    for payload_len in PAYLOAD_LENGTHS {
        // Allocation and initialisation happen once, outside all timed loops
        let payload: Vec<u8> = (0..payload_len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(1))
            .collect();

        {
            let mut g = c.benchmark_group(format!("offer/{payload_len}B"));
            g.bench_function("c_offer", |b| {
                b.iter_custom(|iters| {
                    timed(&h, iters, &payload, true, || {
                        // SAFETY: the publication outlives the bench and the payload outlives the call
                        black_box(unsafe {
                            aeron_publication_offer(
                                publication,
                                black_box(payload.as_ptr()),
                                payload_len,
                                None,
                                std::ptr::null_mut(),
                            )
                        }) >= 0
                    })
                })
            });
            g.bench_function("raw_offer_i64", |b| {
                b.iter_custom(|iters| {
                    timed(&h, iters, &payload, true, || {
                        black_box(h.publisher.offer_raw(black_box(&payload), Handlers::NONE)) >= 0
                    })
                })
            });
            g.bench_function("offer", |b| {
                b.iter_custom(|iters| {
                    timed(&h, iters, &payload, true, || {
                        black_box(h.publisher.offer(black_box(&payload))).is_ok()
                    })
                })
            });
            g.finish();
        }

        {
            let mut g = c.benchmark_group(format!("claim/{payload_len}B"));
            g.bench_function("c_try_claim_commit", |b| {
                b.iter_custom(|iters| {
                    timed(&h, iters, &payload, false, || {
                        // SAFETY: the publication and the claim outlive the bench; the claim is
                        // committed only after a successful try_claim filled it
                        unsafe {
                            if aeron_publication_try_claim(publication, payload_len, claim) >= 0 {
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
                    timed(&h, iters, &payload, false, || {
                        if h.publisher.try_claim_raw(payload_len, &claim_buf) >= 0 {
                            // SAFETY: this thread holds the successful raw claim.
                            unsafe { claim_buf.commit().unwrap() };
                            true
                        } else {
                            false
                        }
                    })
                })
            });
            g.bench_function("try_claim_owned_commit", |b| {
                b.iter_custom(|iters| {
                    timed(&h, iters, &payload, false, || {
                        if let Ok(claim) = h.publisher.try_claim_owned(payload_len) {
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
            let mut g = c.benchmark_group(format!("claim_copy/{payload_len}B"));
            g.bench_function("c_try_claim_copy_commit", |b| {
                b.iter_custom(|iters| {
                    timed(&h, iters, &payload, true, || {
                        // SAFETY: native success initialises the claim; it remains
                        // writable until commit. The payload has exactly payload_len bytes
                        unsafe {
                            if aeron_publication_try_claim(publication, payload_len, claim) >= 0 {
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
                    timed(&h, iters, &payload, true, || {
                        if h.publisher.try_claim_raw(payload_len, &claim_buf) >= 0 {
                            // SAFETY: this thread has exclusive access to the successful
                            // claim's payload until commit.
                            unsafe {
                                claim_buf.data().copy_from_slice(black_box(&payload));
                                claim_buf.commit().unwrap();
                            }
                            true
                        } else {
                            false
                        }
                    })
                })
            });
            g.bench_function("try_claim_owned_copy_commit", |b| {
                b.iter_custom(|iters| {
                    timed(&h, iters, &payload, true, || {
                        if let Ok(mut claim) = h.publisher.try_claim_owned(payload_len) {
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
}

criterion_group!(benches, bench_offer);
criterion_main!(benches);
