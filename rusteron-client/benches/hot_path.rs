//! Per-operation cost of the hot paths over IPC: `offer`, `offerv` and `try_claim` + `commit` on
//! concurrent and exclusive publications, `poll` per fragment, header reads inside the poll handler (the slope between 0, 1 and 4 reads), and the
//! generated `get_constants` getters.
//! Used to compare build setups such as cross-language LTO.
//! Run with `cargo bench -p rusteron-client --features static --bench hot_path`.

use criterion::{Criterion, criterion_group, criterion_main};
use rusteron_client::bindings::{
    aeron_exclusive_publication_offerv, aeron_exclusive_publication_t, aeron_header_t, aeron_iovec_t,
    aeron_subscription_poll,
};
use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::ffi::{CStr, c_void};
use std::hint::black_box;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 7003;
const EXCLUSIVE_STREAM_ID: i32 = 7004;
static CHANNEL: &CStr = AERON_IPC_STREAM;
const PAYLOAD_LEN: usize = 32;
// a 32-byte payload takes a 64-byte frame
const FRAME_LEN: i64 = 64;
const BATCH: u64 = 1024;

struct Harness {
    publisher: AeronPublication,
    subscription: AeronSubscription,
    exclusive: AeronExclusivePublication,
    exclusive_subscription: AeronSubscription,
    claim: AeronBufferClaim,
    payload: [u8; PAYLOAD_LEN],
    _aeron: Aeron,
    _ctx: AeronContext,
    // last, so the client closes before the driver stops
    _driver: EmbeddedDriver,
}

fn harness() -> Harness {
    let driver = EmbeddedDriver::launch().unwrap();
    let ctx = AeronContext::new().unwrap();
    ctx.set_dir(&driver.dir().into_c_string()).unwrap();
    let aeron = Aeron::new(&ctx).unwrap();
    aeron.start().unwrap();
    let timeout = Duration::from_secs(5);
    let publisher = aeron.add_publication(CHANNEL, STREAM_ID, timeout).unwrap();
    let subscription = aeron
        .add_subscription(CHANNEL, STREAM_ID, Handlers::NONE, Handlers::NONE, timeout)
        .unwrap();
    let exclusive = aeron
        .add_exclusive_publication(CHANNEL, EXCLUSIVE_STREAM_ID, timeout)
        .unwrap();
    let exclusive_subscription = aeron
        .add_subscription(CHANNEL, EXCLUSIVE_STREAM_ID, Handlers::NONE, Handlers::NONE, timeout)
        .unwrap();
    let deadline = Instant::now() + timeout;
    while !publisher.is_connected() || !exclusive.is_connected() {
        assert!(Instant::now() < deadline, "publication never connected");
        std::thread::sleep(Duration::from_millis(1));
    }
    Harness {
        publisher,
        subscription,
        exclusive,
        exclusive_subscription,
        claim: AeronBufferClaim::default(),
        payload: [7u8; PAYLOAD_LEN],
        _aeron: aeron,
        _ctx: ctx,
        _driver: driver,
    }
}

/// Publishes once through every publish call before timing. Besides warming up, this gives each
/// Aeron C function a second call site, as a real application has: under LTO, LLVM inlines a
/// function called from one site only almost regardless of its size, which would flatter the
/// timed loops.
fn warm_up(h: &Harness) {
    let _ = h.publisher.offer(&h.payload);
    if h.publisher.try_claim_raw(PAYLOAD_LEN, &h.claim) >= 0 {
        let _ = h.claim.commit();
    }
    let _ = h.exclusive.offer(&h.payload);
    if h.exclusive.try_claim_raw(PAYLOAD_LEN, &h.claim) >= 0 {
        let _ = h.claim.commit();
    }
    offerv(h.exclusive.get_inner(), &h.payload);
    while h.subscription.poll_fn(|_, _| {}, 1024).expect("drain") > 0 {}
    while h.exclusive_subscription.poll_fn(|_, _| {}, 1024).expect("drain") > 0 {}
}

/// `aeron_exclusive_publication_offerv` of `payload` in two halves.
#[inline]
fn offerv(publication: *mut aeron_exclusive_publication_t, payload: &[u8]) -> i64 {
    let (head, tail) = payload.split_at(payload.len() / 2);
    let mut iov = [
        aeron_iovec_t {
            iov_base: head.as_ptr().cast_mut(),
            iov_len: head.len(),
        },
        aeron_iovec_t {
            iov_base: tail.as_ptr().cast_mut(),
            iov_len: tail.len(),
        },
    ];
    // SAFETY: the publication outlives the call, and C only reads through the vectors.
    unsafe { aeron_exclusive_publication_offerv(publication, iov.as_mut_ptr(), 2, None, std::ptr::null_mut()) }
}

/// A publication with the subscription that drains it.
trait Lane {
    fn position(&self) -> i64;
    fn position_limit(&self) -> i64;
    fn subscription(&self) -> &AeronSubscription;

    fn wait_for_window(&self, batch: u64) {
        while self.position_limit() - self.position() < batch as i64 * FRAME_LEN {
            std::thread::yield_now();
        }
    }

    /// Times `iters` publishes in batches, draining between batches untimed.
    fn time_publish(&self, iters: u64, mut publish: impl FnMut()) -> Duration {
        let mut elapsed = Duration::ZERO;
        let mut left = iters;
        while left > 0 {
            let batch = left.min(BATCH);
            self.wait_for_window(batch);
            let before = self.position();
            let start = Instant::now();
            for _ in 0..batch {
                publish();
            }
            elapsed += start.elapsed();
            let published = (self.position() - before) / FRAME_LEN;
            assert!(
                published + 1 >= batch as i64,
                "only {published} of {batch} publishes landed"
            );
            while self.subscription().poll_fn(|_, _| {}, 1024).expect("drain") > 0 {}
            left -= batch;
        }
        elapsed
    }
}

struct Concurrent<'a>(&'a Harness);
struct Exclusive<'a>(&'a Harness);

impl Lane for Concurrent<'_> {
    fn position(&self) -> i64 {
        self.0.publisher.position()
    }
    fn position_limit(&self) -> i64 {
        self.0.publisher.position_limit()
    }
    fn subscription(&self) -> &AeronSubscription {
        &self.0.subscription
    }
}

impl Lane for Exclusive<'_> {
    fn position(&self) -> i64 {
        self.0.exclusive.position()
    }
    fn position_limit(&self) -> i64 {
        self.0.exclusive.position_limit()
    }
    // the exclusive lane drains its own subscription, which clippy misreads as the wrong field
    #[allow(clippy::misnamed_getters)]
    fn subscription(&self) -> &AeronSubscription {
        &self.0.exclusive_subscription
    }
}

impl Harness {
    /// Publishes batches untimed and times polling every fragment back with `poll`.
    fn time_poll(&self, iters: u64, mut poll: impl FnMut(usize) -> usize) -> Duration {
        let mut elapsed = Duration::ZERO;
        let mut left = iters;
        while left > 0 {
            let batch = left.min(BATCH);
            Concurrent(self).wait_for_window(batch);
            for _ in 0..batch {
                while self.publisher.offer(&self.payload).is_err() {}
            }
            let mut read = 0;
            let start = Instant::now();
            while read < batch as usize {
                read += poll(batch as usize - read);
            }
            elapsed += start.elapsed();
            left -= batch;
        }
        elapsed
    }
}

unsafe extern "C-unwind" fn c_count(
    clientd: *mut c_void,
    _buffer: *const u8,
    length: usize,
    _header: *mut aeron_header_t,
) {
    // SAFETY: clientd is the `&mut u64` the poll call passed.
    unsafe { *clientd.cast::<u64>() += length as u64 };
}

fn hot_path(c: &mut Criterion) {
    let h = harness();
    warm_up(&h);
    let mut g = c.benchmark_group("hot_path");

    let (concurrent, exclusive) = (Concurrent(&h), Exclusive(&h));
    g.bench_function("offer_32b", |b| {
        b.iter_custom(|iters| {
            concurrent.time_publish(iters, || {
                let _ = black_box(h.publisher.offer(black_box(&h.payload)));
            })
        })
    });
    g.bench_function("try_claim_commit_32b", |b| {
        b.iter_custom(|iters| {
            concurrent.time_publish(iters, || {
                if h.publisher.try_claim_raw(PAYLOAD_LEN, &h.claim) >= 0 {
                    let _ = black_box(h.claim.commit());
                }
            })
        })
    });
    g.bench_function("exclusive_offer_32b", |b| {
        b.iter_custom(|iters| {
            exclusive.time_publish(iters, || {
                let _ = black_box(h.exclusive.offer(black_box(&h.payload)));
            })
        })
    });
    g.bench_function("exclusive_try_claim_commit_32b", |b| {
        b.iter_custom(|iters| {
            exclusive.time_publish(iters, || {
                if h.exclusive.try_claim_raw(PAYLOAD_LEN, &h.claim) >= 0 {
                    let _ = black_box(h.claim.commit());
                }
            })
        })
    });
    let c_exclusive = h.exclusive.get_inner();
    g.bench_function("exclusive_offerv_2x16b", |b| {
        b.iter_custom(|iters| {
            exclusive.time_publish(iters, || {
                black_box(offerv(c_exclusive, black_box(&h.payload)));
            })
        })
    });

    g.bench_function("publication_get_constants", |b| {
        b.iter(|| black_box(h.publisher.get_constants()))
    });
    g.bench_function("subscription_get_constants", |b| {
        b.iter(|| black_box(h.subscription.get_constants()))
    });
    let mut received = 0u64;
    let c_subscription = h.subscription.get_inner();
    g.bench_function("c_poll_per_fragment", |b| {
        b.iter_custom(|iters| {
            h.time_poll(iters, |limit| {
                // SAFETY: the subscription outlives the bench; the handler only writes `received`.
                let n = unsafe {
                    aeron_subscription_poll(c_subscription, Some(c_count), (&raw mut received).cast(), limit)
                };
                n as usize
            })
        })
    });
    g.bench_function("poll_per_fragment", |b| {
        b.iter_custom(|iters| {
            h.time_poll(iters, |limit| {
                h.subscription
                    .poll_fn(|msg, _| received += msg.len() as u64, limit)
                    .expect("poll") as usize
            })
        })
    });
    g.bench_function("poll_per_fragment_1_header_read", |b| {
        b.iter_custom(|iters| {
            h.time_poll(iters, |limit| {
                h.subscription
                    .poll_fn(
                        |msg, header| {
                            received += msg.len() as u64;
                            black_box(header.session_id());
                        },
                        limit,
                    )
                    .expect("poll") as usize
            })
        })
    });
    g.bench_function("poll_per_fragment_4_header_reads", |b| {
        b.iter_custom(|iters| {
            h.time_poll(iters, |limit| {
                h.subscription
                    .poll_fn(
                        |msg, header| {
                            received += msg.len() as u64;
                            black_box(header.session_id());
                            black_box(header.stream_id());
                            black_box(header.term_id());
                            black_box(header.reserved_value());
                        },
                        limit,
                    )
                    .expect("poll") as usize
            })
        })
    });
    g.finish();
    black_box(received);
}

criterion_group!(benches, hot_path);
criterion_main!(benches);
