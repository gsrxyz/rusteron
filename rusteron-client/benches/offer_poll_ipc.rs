//! One message through IPC on one thread: offer it, then poll it back. Measures the
//! C publish and receive paths per message without cross-thread scheduling noise.
//!
//! Each rusteron arm has a twin making the same Aeron C calls directly, polling either the
//! subscription or its retained image.

use criterion::{Criterion, criterion_group, criterion_main};
use rusteron_client::bindings::{
    aeron_exclusive_publication_offer, aeron_exclusive_publication_t, aeron_header_t, aeron_image_poll,
    aeron_subscription_poll,
};
use rusteron_client::*;
use rusteron_media_driver::{AeronDriver, AeronDriverContext};
use std::ffi::c_void;
use std::hint::{black_box, spin_loop};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 7002;

/// Retries only what a later offer can fix, as a real publisher would.
#[inline]
fn offer(publication: &AeronExclusivePublication, payload: &[u8]) {
    loop {
        match publication.offer(black_box(payload)) {
            Ok(_) => return,
            Err(e) if e.is_retryable() => spin_loop(),
            Err(e) => panic!("offer failed: {e}"),
        }
    }
}

/// The C-side twin of [`offer`]: retries the retryable sentinels (-1 to -3).
#[inline]
fn c_offer(publication: *mut aeron_exclusive_publication_t, payload: &[u8]) {
    loop {
        // SAFETY: the publication outlives the bench and the payload outlives the call.
        let position = unsafe {
            aeron_exclusive_publication_offer(
                publication,
                black_box(payload.as_ptr()),
                payload.len(),
                None,
                std::ptr::null_mut(),
            )
        };
        match position {
            0.. => return,
            -3..=-1 => spin_loop(),
            _ => panic!("offer failed: {position}"),
        }
    }
}

unsafe extern "C-unwind" fn count_bytes(
    clientd: *mut c_void,
    _buffer: *const u8,
    length: usize,
    _header: *mut aeron_header_t,
) {
    // SAFETY: clientd is the `&mut u64` the poll call passed.
    unsafe { *clientd.cast::<u64>() += length as u64 };
}

fn offer_poll(c: &mut Criterion) {
    let driver = AeronDriverContext::new().unwrap();
    driver.set_dir_delete_on_start(true).unwrap();
    driver.set_dir_delete_on_shutdown(true).unwrap();
    driver
        .set_dir(&format!("{}bench-offer-poll", driver.get_dir()).into_c_string())
        .unwrap();
    let (stop, handle) = AeronDriver::launch_embedded(driver.clone(), false);

    let ctx = AeronContext::new().unwrap();
    ctx.set_dir(&driver.get_dir().into_c_string()).unwrap();
    let aeron = Aeron::new(&ctx).unwrap();
    aeron.start().unwrap();
    let timeout = Duration::from_secs(5);
    let publication = aeron
        .add_exclusive_publication(AERON_IPC_STREAM, STREAM_ID, timeout)
        .unwrap();
    let subscription = aeron
        .add_subscription(AERON_IPC_STREAM, STREAM_ID, Handlers::NONE, Handlers::NONE, timeout)
        .unwrap();
    let deadline = Instant::now() + timeout;
    let image = loop {
        if publication.is_connected()
            && let Some(image) = subscription.image_at_index(0)
        {
            break image;
        }
        assert!(Instant::now() < deadline, "publication never connected");
        std::thread::sleep(Duration::from_millis(1));
    };
    // read once, as a C caller holds its pointers
    let (c_publication, c_subscription, c_image) =
        (publication.get_inner(), subscription.get_inner(), image.get_inner());

    let payload = [7u8; 32];
    let mut received = 0u64;
    let mut g = c.benchmark_group("offer_poll_ipc_32b");
    g.bench_function("subscription", |b| {
        b.iter(|| {
            offer(&publication, &payload);
            while subscription
                .poll_fn(|message, _| received += message.len() as u64, 1)
                .expect("poll")
                == 0
            {
                spin_loop();
            }
        })
    });
    g.bench_function("c_subscription", |b| {
        b.iter(|| {
            c_offer(c_publication, &payload);
            loop {
                // SAFETY: the subscription outlives the bench; the handler only writes `received`.
                let fragments = unsafe {
                    aeron_subscription_poll(c_subscription, Some(count_bytes), (&raw mut received).cast(), 1)
                };
                match fragments {
                    0 => spin_loop(),
                    1.. => break,
                    _ => panic!("poll failed: {fragments}"),
                }
            }
        })
    });
    g.bench_function("image", |b| {
        b.iter(|| {
            offer(&publication, &payload);
            while image
                .poll_fn(|message, _| received += message.len() as u64, 1)
                .expect("poll")
                == 0
            {
                spin_loop();
            }
        })
    });
    g.bench_function("c_image", |b| {
        b.iter(|| {
            c_offer(c_publication, &payload);
            loop {
                // SAFETY: the retained image outlives the bench; the handler only writes `received`.
                let fragments = unsafe { aeron_image_poll(c_image, Some(count_bytes), (&raw mut received).cast(), 1) };
                match fragments {
                    0 => spin_loop(),
                    1.. => break,
                    _ => panic!("poll failed: {fragments}"),
                }
            }
        })
    });
    g.finish();
    black_box(received);

    drop((image, subscription, publication, aeron, ctx));
    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();
}

criterion_group!(benches, offer_poll);
criterion_main!(benches);
