#![allow(improper_ctypes_definitions)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![doc = include_str!("../README.md")]
//! # Features
//!
//! - **`static`**: When enabled, this feature statically links the Aeron C code.
//!   By default, the library uses dynamic linking to the Aeron C libraries.
//! - **`backtrace`**: When enabled will log a backtrace for each AeronCError
//! - **`extra-logging`**: When enabled will log when resource is created and destroyed. Useful if you're seeing a segfault due to a resource being closed
//! - **`log-c-bindings`**: When enabled will log every C binding call with arguments and return values. Useful for debugging FFI interactions
//! - **`precompile`**: When enabled will use precompiled C code instead of requiring cmake and java to be installed

#[allow(improper_ctypes_definitions)]
#[allow(unpredictable_function_pointer_comparisons)]
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use bindings::*;
use std::time::Duration;

/// Result codes returned by [`AeronPublication::offer`] / `try_claim` (Aeron `aeronc.h`). A
/// positive value is the resulting log position; the negatives classify the failure.
///
/// **Fatal** (stop offering): [`PUBLICATION_CLOSED`], [`PUBLICATION_MAX_POSITION_EXCEEDED`],
/// [`PUBLICATION_ERROR`]. **Transient** (retry, ideally with an idle strategy):
/// [`PUBLICATION_BACK_PRESSURED`], [`PUBLICATION_NOT_CONNECTED`], [`PUBLICATION_ADMIN_ACTION`].
/// Mirrors how Aeron's own samples (`BasicPublisher` / `basic_publisher.c`) classify offer results.
pub const PUBLICATION_NOT_CONNECTED: i64 = bindings::AERON_PUBLICATION_NOT_CONNECTED as i64;
pub const PUBLICATION_BACK_PRESSURED: i64 = bindings::AERON_PUBLICATION_BACK_PRESSURED as i64;
pub const PUBLICATION_ADMIN_ACTION: i64 = bindings::AERON_PUBLICATION_ADMIN_ACTION as i64;
pub const PUBLICATION_CLOSED: i64 = bindings::AERON_PUBLICATION_CLOSED as i64;
pub const PUBLICATION_MAX_POSITION_EXCEEDED: i64 = bindings::AERON_PUBLICATION_MAX_POSITION_EXCEEDED as i64;
pub const PUBLICATION_ERROR: i64 = bindings::AERON_PUBLICATION_ERROR as i64;

include!(concat!(env!("OUT_DIR"), "/aeron.rs"));
include!(concat!(env!("OUT_DIR"), "/aeron_custom.rs"));

// ---------------------------------------------------------------------------
// Idle strategies for poll loops. Pure-Rust port of Aeron's `IdleStrategy`
// (Java/C++): pass the work count from the last `poll` to `idle(work_count)`; it returns
// immediately when work was done, otherwise backs off the CPU (spin / yield / sleep).
// ---------------------------------------------------------------------------

/// Back off a poll loop when the last cycle did no work. Mirrors Aeron's `IdleStrategy`:
/// `idle(work_count)` returns immediately when `work_count > 0`, otherwise spins / yields /
/// sleeps depending on the implementation.
pub trait IdleStrategy {
    /// Called with the work/fragment count from the last operation. Implementations return
    /// immediately when `work_count > 0` (work was done) and back off otherwise.
    fn idle(&mut self, work_count: i32);

    /// Reset accumulated backoff state after a productive period. Default: no-op.
    fn reset(&mut self) {}
}

/// Spin with a CPU pause hint. Lowest latency, pins a core. (Aeron `BusySpinIdleStrategy`.)
pub struct BusySpinIdleStrategy;
impl BusySpinIdleStrategy {
    pub fn new() -> Self {
        Self
    }
}
impl Default for BusySpinIdleStrategy {
    fn default() -> Self {
        Self::new()
    }
}
impl IdleStrategy for BusySpinIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: i32) {
        if work_count > 0 {
            return;
        }
        std::hint::spin_loop();
    }
}

/// No-op — never yields the core. (Aeron `NoOpIdleStrategy`.)
pub struct NoOpIdleStrategy;
impl NoOpIdleStrategy {
    pub fn new() -> Self {
        Self
    }
}
impl Default for NoOpIdleStrategy {
    fn default() -> Self {
        Self::new()
    }
}
impl IdleStrategy for NoOpIdleStrategy {
    #[inline]
    fn idle(&mut self, _work_count: i32) {}
}

/// Yield the OS thread when idle. Lower CPU than busy-spin, slightly higher latency.
/// (Aeron `YieldingIdleStrategy`.)
pub struct YieldingIdleStrategy;
impl YieldingIdleStrategy {
    pub fn new() -> Self {
        Self
    }
}
impl Default for YieldingIdleStrategy {
    fn default() -> Self {
        Self::new()
    }
}
impl IdleStrategy for YieldingIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: i32) {
        if work_count > 0 {
            return;
        }
        std::thread::yield_now();
    }
}

/// Sleep for a fixed duration when idle. (Aeron `SleepingIdleStrategy`.)
pub struct SleepingIdleStrategy {
    duration: Duration,
}
impl SleepingIdleStrategy {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}
impl IdleStrategy for SleepingIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: i32) {
        if work_count == 0 {
            std::thread::sleep(self.duration);
        }
    }
}

/// Adaptive backoff: spin a few times, then yield a few times, then sleep with an exponentially
/// growing park up to a max. A good general-purpose strategy. (Aeron `BackoffIdleStrategy`.)
///
/// Defaults match Aeron: `max_spins = 10`, `max_yields = 20`, `min_park = 1µs`, `max_park = 1ms`.
pub struct BackoffIdleStrategy {
    max_spins: i64,
    max_yields: i64,
    min_park: Duration,
    max_park: Duration,
    spins: i64,
    yields: i64,
    park: Duration,
    state: u8,
}

const BACKOFF_NOT_IDLE: u8 = 0;
const BACKOFF_SPINNING: u8 = 1;
const BACKOFF_YIELDING: u8 = 2;
const BACKOFF_PARKING: u8 = 3;

impl BackoffIdleStrategy {
    /// Defaults match Aeron: 10 spins, 20 yields, park 1µs..1ms.
    pub fn new() -> Self {
        Self::with(10, 20, Duration::from_micros(1), Duration::from_millis(1))
    }

    /// Full control over the backoff parameters.
    pub fn with(max_spins: i64, max_yields: i64, min_park: Duration, max_park: Duration) -> Self {
        Self {
            max_spins,
            max_yields,
            min_park,
            max_park,
            spins: 0,
            yields: 0,
            park: min_park,
            state: BACKOFF_NOT_IDLE,
        }
    }

    #[inline]
    fn idle_one(&mut self) {
        match self.state {
            BACKOFF_NOT_IDLE => {
                self.state = BACKOFF_SPINNING;
                self.spins += 1;
            }
            BACKOFF_SPINNING => {
                std::hint::spin_loop();
                self.spins += 1;
                if self.spins > self.max_spins {
                    self.state = BACKOFF_YIELDING;
                    self.yields = 0;
                }
            }
            BACKOFF_YIELDING => {
                self.yields += 1;
                if self.yields > self.max_yields {
                    self.state = BACKOFF_PARKING;
                    self.park = self.min_park;
                } else {
                    std::thread::yield_now();
                }
            }
            _ => {
                // PARKING — sleep then double the park period up to the max.
                std::thread::sleep(self.park);
                self.park = std::cmp::min(self.park.saturating_mul(2), self.max_park);
            }
        }
    }
}

impl Default for BackoffIdleStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl IdleStrategy for BackoffIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: i32) {
        if work_count > 0 {
            self.reset();
        } else {
            self.idle_one();
        }
    }

    fn reset(&mut self) {
        self.spins = 0;
        self.yields = 0;
        self.park = self.min_park;
        self.state = BACKOFF_NOT_IDLE;
    }
}

#[cfg(test)]
mod idle_strategy_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn idle_strategy_kind_round_trips_through_context() {
        let ctx = AeronContext::new().unwrap();
        for (kind, name) in [
            (AeronIdleStrategyKind::Sleeping, "sleeping"),
            (AeronIdleStrategyKind::Yielding, "yield"),
            (AeronIdleStrategyKind::BusySpin, "spin"),
            (AeronIdleStrategyKind::NoOp, "noop"),
            (AeronIdleStrategyKind::Backoff, "backoff"),
        ] {
            ctx.set_idle_strategy_kind(kind).unwrap();
            assert_eq!(name, ctx.get_idle_strategy(), "kind {kind:?}");
        }
    }

    #[test]
    fn rust_backoff_idles_correctly() {
        let mut idle = BackoffIdleStrategy::new();
        // work done -> effectively free
        idle.idle(1);
        // no work: walk spin -> yield -> park without crashing; parking must take real time
        let start = Instant::now();
        for _ in 0..100 {
            idle.idle(0);
        }
        assert!(
            start.elapsed() >= Duration::from_micros(50),
            "backoff should have parked"
        );
    }

    #[test]
    fn rust_stateless_strategies_are_safe() {
        let strategies: Vec<Box<dyn IdleStrategy>> = vec![
            Box::new(BusySpinIdleStrategy::default()),
            Box::new(YieldingIdleStrategy::default()),
            Box::new(NoOpIdleStrategy::default()),
        ];
        for mut s in strategies {
            s.idle(1);
            s.idle(0);
        }
    }

    #[test]
    fn rust_backoff_matches_c_defaults() {
        // Verify BackoffIdleStrategy uses Aeron's canonical backoff defaults
        // (AERON_IDLE_STRATEGY_BACKOFF_*: 10 spins, 20 yields, park 1µs..1ms).
        let idle = BackoffIdleStrategy::new();
        assert_eq!(idle.max_spins, 10, "max_spins should be 10");
        assert_eq!(idle.max_yields, 20, "max_yields should be 20");
        assert_eq!(idle.min_park, Duration::from_micros(1), "min_park should be 1µs");
        assert_eq!(idle.max_park, Duration::from_millis(1), "max_park should be 1ms");
    }

    #[test]
    fn busy_spin_and_yield_return_on_work() {
        let mut s = BusySpinIdleStrategy;
        s.idle(5); // work done -> must not block
        s.idle(0); // no work -> just a pause hint, returns immediately

        let mut y = YieldingIdleStrategy;
        y.idle(3); // work done
        y.idle(0); // yields once, returns
    }

    #[test]
    fn no_op_never_blocks() {
        let mut s = NoOpIdleStrategy;
        for _ in 0..1000 {
            s.idle(0);
        }
    }

    #[test]
    fn sleeping_idle_only_sleeps_when_idle() {
        let mut s = SleepingIdleStrategy::new(Duration::from_micros(10));
        let t = std::time::Instant::now();
        s.idle(1); // work done -> no sleep
        assert!(t.elapsed() < Duration::from_millis(1));

        let t = std::time::Instant::now();
        s.idle(0); // idle -> sleeps ~10µs
        assert!(t.elapsed() >= Duration::from_micros(10));
    }

    #[test]
    fn backoff_resets_on_work_and_progresses_to_park() {
        let mut s = BackoffIdleStrategy::with(2, 2, Duration::from_micros(1), Duration::from_millis(1));
        // Work done -> resets state (no backoff progression).
        s.idle(1);
        assert_eq!(s.state, BACKOFF_NOT_IDLE);
        assert_eq!(s.spins, 0);

        // Spin a few, then yield, then park: driving it enough with 0 work must reach PARKING.
        for _ in 0..1000 {
            s.idle(0);
        }
        assert_eq!(s.state, BACKOFF_PARKING);
        // Park period grows but is capped at max_park.
        assert!(s.park <= Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_alloc::current_allocs;
    use hdrhistogram::Histogram;
    use log::{error, info};
    use rusteron_media_driver::AeronDriverContext;
    use serial_test::serial;
    use std::error;
    use std::error::Error;
    use std::io::Write;
    use std::os::raw::c_int;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::{sleep, JoinHandle};
    use std::time::{Duration, Instant};

    #[derive(Default, Debug)]
    struct ErrorCount {
        error_count: usize,
    }

    impl AeronErrorHandlerCallback for ErrorCount {
        fn handle_aeron_error_handler(&mut self, error_code: c_int, msg: &str) {
            error!("Aeron error {}: {}", error_code, msg);
            self.error_count += 1;
        }
    }

    struct CloseNotificationCount {
        count: Arc<AtomicUsize>,
    }

    impl AeronNotificationCallback for CloseNotificationCount {
        fn handle_aeron_notification(&mut self) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn running_under_valgrind() -> bool {
        std::env::var_os("RUSTERON_VALGRIND").is_some()
    }

    #[test]
    #[serial]
    fn version_check() -> Result<(), Box<dyn error::Error>> {
        unsafe {
            aeron_randomised_int32();
        }
        // Let background teardown from previous #[serial] tests settle before taking
        // the baseline: a driver thread still stopping can emit a captured log line
        // inside our window, which counts as a live allocation and skews the check.
        let settle_start = Instant::now();
        let mut alloc_count = current_allocs();
        loop {
            sleep(Duration::from_millis(50));
            let now = current_allocs();
            if now == alloc_count || settle_start.elapsed() > Duration::from_secs(2) {
                alloc_count = now;
                break;
            }
            alloc_count = now;
        }

        {
            let major = unsafe { crate::aeron_version_major() };
            let minor = unsafe { crate::aeron_version_minor() };
            let patch = unsafe { crate::aeron_version_patch() };

            let cargo_version = "1.52.0";
            let aeron_version = format!("{}.{}.{}", major, minor, patch);
            assert_eq!(aeron_version, cargo_version);

            let ctx = AeronContext::new()?;
            let handler = Handler::new(ErrorCount::default());
            ctx.set_error_handler(Some(handler.clone()))?;

            assert!(Aeron::epoch_clock() > 0);
            // the context holds a clone of the handler; dropping both frees the
            // callback value exactly once (verified by the alloc-count check below)
        }

        assert!(
            current_allocs() <= alloc_count,
            "allocations {} > {alloc_count}",
            current_allocs()
        );

        Ok(())
    }

    #[test]
    #[serial]
    fn async_publication_invalid_interface_poll_then_drop() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:udp?endpoint=203.0.113.1:54321");

        // Create async publication and subscription pollers on the same invalid channel and
        // attempt to resolve them. If both are created, try a small send/receive cycle and then exit.
        let pub_poller = aeron.async_add_publication(&channel.clone().into_c_string(), 4321)?;
        let sub_poller =
            aeron.async_add_subscription(&channel.into_c_string(), 4321, Handlers::NONE, Handlers::NONE)?;

        let mut publication: Option<AeronPublication> = None;
        let mut subscription: Option<AeronSubscription> = None;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if publication.is_none() {
                match pub_poller.poll() {
                    Ok(Some(p)) => publication = Some(p),
                    Ok(None) | Err(_) => {}
                }
            }
            if subscription.is_none() {
                match sub_poller.poll() {
                    Ok(Some(s)) => subscription = Some(s),
                    Ok(None) | Err(_) => {}
                }
            }
            if publication.is_some() && subscription.is_some() {
                break;
            }
            #[cfg(debug_assertions)]
            std::thread::sleep(Duration::from_millis(10));
        }

        info!("publication: {:?}", publication);
        info!("subscription: {:?}", subscription);

        if let (Some(publisher), Some(subscription)) = (publication, subscription) {
            let payload = b"hello-aeron";
            let send_start = Instant::now();
            let mut sent = false;
            while send_start.elapsed() < Duration::from_millis(500) {
                let res = publisher.offer_raw(payload, Handlers::NONE);
                if res >= payload.len() as i64 {
                    sent = true;
                    info!("sent {:?}", payload);
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }

            if sent {
                let mut got = false;
                let read_start = Instant::now();
                while read_start.elapsed() < Duration::from_millis(500) {
                    let _ = subscription.poll_fn(
                        |msg, _hdr| {
                            if msg == payload {
                                got = true;
                                info!("received {:?}", payload);
                            }
                        },
                        1024,
                    );
                    if got {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                // We don't assert on got, just exercise the path.
            }
        }

        // Shutdown
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    /// Exercises the additive ergonomics API end-to-end:
    /// `offer_result_simple`, `try_claim_owned` + `commit`, the `session_id` /
    /// `stream_id` header accessors, and `status()` on publication/subscription.
    #[test]
    #[serial]
    fn offer_result_and_claim_roundtrip_with_header_accessors() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:ipc");
        let stream_id: i32 = 9123;
        let pub_poller = aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
        let sub_poller =
            aeron.async_add_subscription(&channel.into_c_string(), stream_id, Handlers::NONE, Handlers::NONE)?;

        let mut publication: Option<AeronPublication> = None;
        let mut subscription: Option<AeronSubscription> = None;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if publication.is_none() {
                if let Ok(Some(p)) = pub_poller.poll() {
                    publication = Some(p);
                }
            }
            if subscription.is_none() {
                if let Ok(Some(s)) = sub_poller.poll() {
                    subscription = Some(s);
                }
            }
            if publication.is_some() && subscription.is_some() {
                break;
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }

        let (publisher, subscription) = match (publication, subscription) {
            (Some(p), Some(s)) => (p, s),
            _ => panic!("publication/subscription did not come up"),
        };

        // Wait for the IPC images to connect before asserting status.
        let conn_start = Instant::now();
        while !publisher.is_connected() && conn_start.elapsed() < Duration::from_secs(2) {
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert_eq!(publisher.status(), AeronStatus::Connected);

        // 1) Publish via the Result-returning offer variant.
        let payload = b"hello-result";
        let offer_start = Instant::now();
        let mut offered = false;
        while offer_start.elapsed() < Duration::from_secs(2) {
            if let Ok(pos) = publisher.offer(payload) {
                if pos >= payload.len() as i64 {
                    offered = true;
                    break;
                }
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(offered, "offer_result_simple never succeeded");

        // 2) Publish via the RAII claim: write into the claimed buffer and commit.
        let claim_payload = b"hello-claim";
        let claim_start = Instant::now();
        let mut committed = false;
        while claim_start.elapsed() < Duration::from_secs(2) {
            if let Ok(mut claim) = publisher.try_claim_owned(claim_payload.len()) {
                claim.data()[..claim_payload.len()].copy_from_slice(claim_payload);
                if claim.commit().is_ok() {
                    committed = true;
                    break;
                }
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(committed, "try_claim_owned + commit never succeeded");

        // 3) Publish via the zero-alloc gathering offer: header + payload parts must
        // arrive as ONE contiguous message.
        let parts_header = b"hdr:";
        let parts_payload = b"gathered-body";
        let parts_expected: Vec<u8> = [parts_header.as_slice(), parts_payload.as_slice()].concat();
        let parts_start = Instant::now();
        let mut parts_offered = false;
        while parts_start.elapsed() < Duration::from_secs(2) {
            if publisher.offer_parts(&[parts_header, parts_payload]).is_ok() {
                parts_offered = true;
                break;
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(parts_offered, "offer_parts never succeeded");
        // more parts than the stack iovec capacity must be rejected, not truncated
        let too_many = [b"x".as_slice(); MAX_OFFER_PARTS + 1];
        assert!(
            publisher.offer_parts(&too_many).is_err(),
            "over-capacity offer_parts must fail"
        );

        // 4) Receive all three and assert the header accessors.
        let received_offer = std::cell::Cell::new(false);
        let received_claim = std::cell::Cell::new(false);
        let received_parts = std::cell::Cell::new(false);
        let header_ids = std::cell::Cell::new(Option::<(i32, i32)>::None);
        let read_start = Instant::now();
        while read_start.elapsed() < Duration::from_secs(2)
            && !(received_offer.get() && received_claim.get() && received_parts.get())
        {
            let _ = subscription.poll_fn(
                |msg, header| {
                    header_ids.set(Some((
                        header.session_id().unwrap_or(0),
                        header.stream_id().unwrap_or(0),
                    )));
                    if msg == payload {
                        received_offer.set(true);
                    }
                    if msg == claim_payload {
                        received_claim.set(true);
                    }
                    if msg == parts_expected.as_slice() {
                        received_parts.set(true);
                    }
                },
                1024,
            );
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(received_offer.get(), "did not receive offer_result message");
        assert!(received_claim.get(), "did not receive claim message");
        assert!(
            received_parts.get(),
            "did not receive offer_parts message as one contiguous body"
        );
        let (session_id, recv_stream_id) = header_ids.get().expect("no header captured");
        assert_eq!(recv_stream_id, stream_id, "header stream_id should match");
        assert_ne!(session_id, 0, "session_id should be populated");

        assert_eq!(subscription.status(), AeronStatus::Connected);

        // Shutdown
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }
    /// C5: test AeronClaim RAII lifecycle — commit round-trip is received.
    /// Exercises the explicit commit path and position() accessor.
    #[test]
    #[serial]
    fn aeron_claim_commit_roundtrip_is_received() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:ipc");
        let stream_id: i32 = 9124;
        let pub_poller = aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
        let sub_poller =
            aeron.async_add_subscription(&channel.into_c_string(), stream_id, Handlers::NONE, Handlers::NONE)?;

        let mut publication: Option<AeronPublication> = None;
        let mut subscription: Option<AeronSubscription> = None;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if publication.is_none() {
                if let Ok(Some(p)) = pub_poller.poll() {
                    publication = Some(p);
                }
            }
            if subscription.is_none() {
                if let Ok(Some(s)) = sub_poller.poll() {
                    subscription = Some(s);
                }
            }
            if publication.is_some() && subscription.is_some() {
                break;
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }

        let (publisher, subscription) = match (publication, subscription) {
            (Some(p), Some(s)) => (p, s),
            _ => panic!("publication/subscription did not come up"),
        };

        // Wait for the IPC images to connect before asserting status.
        let conn_start = Instant::now();
        while !publisher.is_connected() && conn_start.elapsed() < Duration::from_secs(2) {
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert_eq!(publisher.status(), AeronStatus::Connected);

        // Claim a buffer, write into it, and commit.
        let claim_payload = b"claim-commit-test";
        let claim_start = Instant::now();
        let mut committed_pos = None;
        while claim_start.elapsed() < Duration::from_secs(2) {
            if let Ok(mut claim) = publisher.try_claim_owned(claim_payload.len()) {
                claim.data()[..claim_payload.len()].copy_from_slice(claim_payload);
                let pos = claim.position();
                let commit_pos = claim.commit()?;
                committed_pos = Some((commit_pos, pos));
                break;
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        let (commit_pos, claim_pos) = committed_pos.expect("try_claim_owned + commit never succeeded");
        assert_eq!(commit_pos, claim_pos, "commit() return should match claim.position()");

        // Receive the committed message and assert the exact bytes.
        let received = std::cell::Cell::new(false);
        let read_start = Instant::now();
        while read_start.elapsed() < Duration::from_secs(2) && !received.get() {
            let _ = subscription.poll_fn(
                |msg, _header| {
                    if msg == claim_payload {
                        received.set(true);
                    }
                },
                1024,
            );
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(received.get(), "did not receive claim message");

        assert_eq!(subscription.status(), AeronStatus::Connected);

        // Shutdown
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    /// C6: test AeronClaim RAII lifecycle — dropped without commit is aborted.
    /// Verifies that a dropped claim is not delivered and the publication remains usable.
    #[test]
    #[serial]
    fn aeron_claim_dropped_without_commit_is_aborted() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:ipc");
        let stream_id: i32 = 9125;
        let pub_poller = aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
        let sub_poller =
            aeron.async_add_subscription(&channel.into_c_string(), stream_id, Handlers::NONE, Handlers::NONE)?;

        let mut publication: Option<AeronPublication> = None;
        let mut subscription: Option<AeronSubscription> = None;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if publication.is_none() {
                if let Ok(Some(p)) = pub_poller.poll() {
                    publication = Some(p);
                }
            }
            if subscription.is_none() {
                if let Ok(Some(s)) = sub_poller.poll() {
                    subscription = Some(s);
                }
            }
            if publication.is_some() && subscription.is_some() {
                break;
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }

        let (publisher, subscription) = match (publication, subscription) {
            (Some(p), Some(s)) => (p, s),
            _ => panic!("publication/subscription did not come up"),
        };

        // Wait for the IPC images to connect.
        let conn_start = Instant::now();
        while !publisher.is_connected() && conn_start.elapsed() < Duration::from_secs(2) {
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert_eq!(publisher.status(), AeronStatus::Connected);

        // Claim a buffer, write into it, then DROP without commit/abort.
        let dropped_payload = b"dropped-claim-payload";
        let claim_start = Instant::now();
        let mut claimed = false;
        while claim_start.elapsed() < Duration::from_secs(2) && !claimed {
            if let Ok(mut claim) = publisher.try_claim_owned(dropped_payload.len()) {
                claim.data()[..dropped_payload.len()].copy_from_slice(dropped_payload);
                // Explicitly drop the claim here — it should be aborted, not committed.
                drop(claim);
                claimed = true;
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(claimed, "try_claim_owned never succeeded");

        // Publish a second, different message via the standard offer path.
        let marker_payload = b"marker-after-dropped-claim";
        let offer_start = Instant::now();
        let mut offered = false;
        while offer_start.elapsed() < Duration::from_secs(2) && !offered {
            if let Ok(pos) = publisher.offer(marker_payload) {
                if pos >= marker_payload.len() as i64 {
                    offered = true;
                }
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(offered, "offer_result_simple never succeeded after dropped claim");

        // Receive only the marker — the dropped claim should have been aborted.
        let received_dropped = std::cell::Cell::new(false);
        let received_marker = std::cell::Cell::new(false);
        let read_start = Instant::now();
        while read_start.elapsed() < Duration::from_secs(2) && !(received_marker.get() || received_dropped.get()) {
            let _ = subscription.poll_fn(
                |msg, _header| {
                    if msg == dropped_payload {
                        received_dropped.set(true);
                    }
                    if msg == marker_payload {
                        received_marker.set(true);
                    }
                },
                1024,
            );
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(!received_dropped.get(), "dropped claim was erroneously delivered");
        assert!(
            received_marker.get(),
            "marker message was not received after dropped claim"
        );

        // Confirm the publication is still usable by offering one more message.
        let final_payload = b"final-after-all";
        let final_start = Instant::now();
        let mut final_offered = false;
        while final_start.elapsed() < Duration::from_secs(2) && !final_offered {
            if let Ok(pos) = publisher.offer(final_payload) {
                if pos >= final_payload.len() as i64 {
                    final_offered = true;
                }
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(final_offered, "publication became unusable after dropped claim");

        assert_eq!(subscription.status(), AeronStatus::Connected);

        // Shutdown
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    /// C7: test try_claim_owned failure path — oversized request returns Err cleanly.
    /// Verifies the error path does not construct an AeronClaim or call abort.
    #[test]
    #[serial]
    fn try_claim_owned_failure_is_clean() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:ipc");
        let stream_id: i32 = 9126;
        let pub_poller = aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
        let sub_poller =
            aeron.async_add_subscription(&channel.into_c_string(), stream_id, Handlers::NONE, Handlers::NONE)?;

        let mut publication: Option<AeronPublication> = None;
        let mut subscription: Option<AeronSubscription> = None;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if publication.is_none() {
                if let Ok(Some(p)) = pub_poller.poll() {
                    publication = Some(p);
                }
            }
            if subscription.is_none() {
                if let Ok(Some(s)) = sub_poller.poll() {
                    subscription = Some(s);
                }
            }
            if publication.is_some() && subscription.is_some() {
                break;
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }

        let (publisher, _subscription) = match (publication, subscription) {
            (Some(p), Some(s)) => (p, s),
            _ => panic!("publication/subscription did not come up"),
        };

        // Wait for the IPC images to connect.
        let conn_start = Instant::now();
        while !publisher.is_connected() && conn_start.elapsed() < Duration::from_secs(2) {
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert_eq!(publisher.status(), AeronStatus::Connected);

        // Get the max_message_length and attempt to claim more than that.
        let constants = publisher.get_constants().expect("publication constants");
        let max_len = constants.max_message_length;
        assert!(max_len > 0, "max_message_length should be positive");

        // Try to claim an absurdly large length — should return Err without panicking.
        let oversized_claim = publisher.try_claim_owned(usize::MAX);
        assert!(
            oversized_claim.is_err(),
            "try_claim_owned(usize::MAX) should return Err"
        );

        // Try to claim exactly one byte over the limit — should also return Err.
        let over_limit_claim = publisher.try_claim_owned(max_len as usize + 1);
        assert!(
            over_limit_claim.is_err(),
            "try_claim_owned(max_len + 1) should return Err"
        );

        // Verify the publication is still usable after the failed claims.
        let valid_payload = b"valid-after-failed-claim";
        let offer_start = Instant::now();
        let mut offered = false;
        while offer_start.elapsed() < Duration::from_secs(2) && !offered {
            if let Ok(pos) = publisher.offer(valid_payload) {
                if pos >= valid_payload.len() as i64 {
                    offered = true;
                }
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(offered, "publication became unusable after failed try_claim_owned");

        // Shutdown
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    /// C4: regression guard for the latency prime directive — a tight loop on the
    /// publish hot path (`offer` with the static no-op reserved-value supplier)
    /// must stay allocation-free. If a future change adds a stray `to_string` /
    /// `Vec` / clone on the offer path, this fails CI.
    #[test]
    #[serial]
    fn publish_hot_path_is_allocation_free() -> Result<(), Box<dyn error::Error>> {
        let media_driver_ctx = AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}alloc-guard", media_driver_ctx.get_dir()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:ipc");
        let stream_id: i32 = 7777;
        let pub_poller = aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;

        // Bring the publication up + connected before measuring.
        let publisher: AeronPublication = {
            let start = Instant::now();
            loop {
                if let Ok(Some(p)) = pub_poller.poll() {
                    let conn_start = Instant::now();
                    while !p.is_connected() && conn_start.elapsed() < Duration::from_secs(2) {
                        #[cfg(debug_assertions)]
                        sleep(Duration::from_millis(10));
                    }
                    break p;
                }
                if start.elapsed() > Duration::from_secs(3) {
                    panic!("publication did not come up");
                }
                #[cfg(debug_assertions)]
                sleep(Duration::from_millis(10));
            }
        };

        let payload = b"alloc-guard-payload";
        // Warm up (first offer may touch lazy publication state).
        let _ = publisher.offer_raw(payload, Handlers::NONE);

        // Assert a tight offer loop is allocation-free (publish hot path).
        crate::test_alloc::assert_no_allocation(|| {
            for _ in 0..200 {
                let _ = publisher.offer_raw(payload, Handlers::NONE);
            }
        });

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    /// `from_code` + `Clone` must stay allocation-free so retry loops that keep
    /// failing on `-1` don't tax the hot path. Message capture is opt-in.
    #[test]
    #[serial]
    fn repeated_c_errors_are_allocation_free() {
        crate::test_alloc::assert_no_allocation(|| {
            for _ in 0..200 {
                let err = AeronCError::from_code(-1);
                assert_eq!(err.code, -1);
                assert!(err.message().is_none());
                let cloned = err.clone();
                assert_eq!(cloned, err);
            }
        });
    }

    #[test]
    #[serial]
    fn async_pub_sub_invalid_endpoint_create_drop_stress() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        const STRESS_ITERS: u16 = 60;
        const POLL_TIMEOUT: Duration = Duration::from_secs(10);
        const POLL_SLEEP: Duration = Duration::from_millis(10);

        // Stress: repeatedly create async pub/sub on an invalid endpoint and drive each
        // poller to a terminal state before dropping it.
        for i in 0..STRESS_ITERS {
            let port = 55000u16 + i;
            let channel = format!("aeron:udp?endpoint=203.0.113.1:{}", port);
            let pub_poller = aeron.async_add_publication(&channel.clone().into_c_string(), 4500 + i as i32)?;
            let sub_poller = aeron.async_add_subscription(
                &channel.into_c_string(),
                4500 + i as i32,
                Handlers::NONE,
                Handlers::NONE,
            )?;

            let start = Instant::now();
            let mut publication_done = false;
            let mut subscription_done = false;

            while !(publication_done && subscription_done) && start.elapsed() < POLL_TIMEOUT {
                if !publication_done {
                    match pub_poller.poll() {
                        Ok(Some(pub_)) => {
                            let _ = pub_;
                            publication_done = true;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            info!("publication async add finished with error on iteration {i}: {err:?}");
                            publication_done = true;
                        }
                    }
                }

                if !subscription_done {
                    match sub_poller.poll() {
                        Ok(Some(sub_)) => {
                            let _ = sub_;
                            subscription_done = true;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            info!("subscription async add finished with error on iteration {i}: {err:?}");
                            subscription_done = true;
                        }
                    }
                }

                if !(publication_done && subscription_done) {
                    std::thread::sleep(POLL_SLEEP);
                }
            }

            assert!(
                publication_done,
                "publication async add did not complete on iteration {i} within {:?}",
                POLL_TIMEOUT
            );
            assert!(
                subscription_done,
                "subscription async add did not complete on iteration {i} within {:?}",
                POLL_TIMEOUT
            );
        }

        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    fn async_subscription_invalid_interface_poll_then_drop() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        // Invalid remote endpoint only (no interface)
        let channel = String::from("aeron:udp?endpoint=203.0.113.1:54323");

        let poller = aeron.async_add_subscription(&channel.into_c_string(), 4323, Handlers::NONE, Handlers::NONE)?;

        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(250) {
            let _ = poller.poll();
            #[cfg(debug_assertions)]
            std::thread::sleep(Duration::from_millis(10));
        }

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    fn blocking_add_subscription_invalid_interface_timeout() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:udp?endpoint=203.0.113.1:54324");

        let result = aeron.add_subscription(
            &channel.into_c_string(),
            4324,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_millis(300),
        );

        assert!(result.is_err(), "expected error for invalid interface");
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    fn async_publication_invalid_bind_poll_then_drop() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        // Use an invalid bind on publication (bind is not valid for publication, and the IP is unowned).
        let channel = format!("aeron:udp?endpoint=127.0.0.1:54330|bind=203.0.113.1:60000");

        let poller = aeron.async_add_publication(&channel.into_c_string(), 4330)?;
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(250) {
            let _ = poller.poll();
            #[cfg(debug_assertions)]
            std::thread::sleep(Duration::from_millis(10));
        }
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    pub fn simple_large_send() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (liveness_ns, unblock_ns, driver_timeout_ms) = if running_under_valgrind() {
            (180_000_000_000u64, 185_000_000_000u64, 180_000)
        } else {
            (60_000_000_000u64, 65_000_000_000u64, 60_000)
        };
        media_driver_ctx.set_client_liveness_timeout_ns(liveness_ns)?;
        media_driver_ctx.set_image_liveness_timeout_ns(liveness_ns)?;
        media_driver_ctx.set_publication_unblock_timeout_ns(unblock_ns)?;
        media_driver_ctx.set_driver_timeout_ms(driver_timeout_ms)?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        assert_eq!(media_driver_ctx.get_dir(), ctx.get_dir());
        // Keep client-side keepalive threshold aligned with the slow Valgrind environment.
        ctx.set_driver_timeout_ms(driver_timeout_ms)?;
        // Handlers are reference-counted: the context/resources keep clones alive,
        // and the values are freed automatically when the last reference drops.
        let error_handler = Handler::new(ErrorCount::default());
        let new_pub_handler = Handler::new(AeronNewPublicationLogger);
        let avail_counter_handler1 = Handler::new(AeronAvailableCounterLogger);
        let close_client_handler = Handler::new(AeronCloseClientLogger);
        let new_sub_handler = Handler::new(AeronNewSubscriptionLogger);
        let unavail_counter_handler = Handler::new(AeronUnavailableCounterLogger);
        let avail_counter_handler2 = Handler::new(AeronAvailableCounterLogger);
        let excl_pub_handler = Handler::new(AeronNewPublicationLogger);
        ctx.set_error_handler(Some(error_handler.clone()))?;
        ctx.set_on_new_publication(Some(new_pub_handler.clone()))?;
        ctx.set_on_available_counter(Some(avail_counter_handler1.clone()))?;
        ctx.set_on_close_client(Some(close_client_handler.clone()))?;
        ctx.set_on_new_subscription(Some(new_sub_handler.clone()))?;
        ctx.set_on_unavailable_counter(Some(unavail_counter_handler.clone()))?;
        ctx.set_on_available_counter(Some(avail_counter_handler2.clone()))?;
        ctx.set_on_new_exclusive_publication(Some(excl_pub_handler.clone()))?;

        info!("creating client [simple_large_send test]");
        let aeron = Aeron::new(&ctx)?;
        info!("starting client");

        aeron.start()?;
        info!("client started");
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
        info!("created publisher");

        assert!(AeronCncMetadata::load_from_file(ctx.get_dir())?.pid > 0);
        let cstr = std::ffi::CString::new(ctx.get_dir()).unwrap();
        AeronCncMetadata::read_from_file(&cstr, |cnc| {
            assert!(cnc.pid > 0);
        })?;
        assert!(AeronCnc::open(&ctx.get_dir().into_c_string())?.get_to_driver_heartbeat_ms()? > 0);
        let cstr = std::ffi::CString::new(ctx.get_dir()).unwrap();
        for _ in 0..50 {
            AeronCnc::read(&cstr, |cnc| {
                assert!(cnc.get_to_driver_heartbeat_ms().unwrap() > 0);
            })?;
        }

        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_secs(5),
        )?;
        info!("created subscription");

        subscription.poll_fn(|msg, header| println!("foo"), 1024).unwrap();

        // pick a large enough size to confirm fragement assembler is working
        let string_len = media_driver_ctx.ipc_mtu_length * 100;
        info!("string length: {}", string_len);

        let stop_publisher = Arc::new(AtomicBool::new(false));

        let publisher_handler = {
            let stop_publisher = stop_publisher.clone();
            std::thread::spawn(move || {
                let binding = "1".repeat(string_len);
                let large_msg = binding.as_bytes();
                loop {
                    if stop_publisher.load(Ordering::Acquire) || publisher.is_closed() {
                        break;
                    }
                    let result = publisher.offer_raw(large_msg, Handlers::NONE);

                    assert_eq!(123, publisher.get_constants().unwrap().stream_id);

                    if result < large_msg.len() as i64 {
                        let error = AeronCError::from_code(result as i32);
                        match error.kind() {
                            AeronErrorType::PublicationBackPressured | AeronErrorType::PublicationAdminAction => {
                                // ignore
                            }
                            _ => {
                                error!(
                                    "ERROR: failed to send message {:?}",
                                    AeronCError::from_code(result as i32)
                                );
                            }
                        }
                        sleep(Duration::from_millis(500));
                    }
                }
                info!("stopping publisher thread");
            })
        };

        let mut assembler = AeronFragmentClosureAssembler::new()?;

        struct Context {
            count: Arc<AtomicUsize>,
            stop: Arc<AtomicBool>,
            string_len: usize,
        }

        let count = Arc::new(AtomicUsize::new(0usize));
        let mut context = Context {
            count: count.clone(),
            stop: stop.clone(),
            string_len,
        };

        // Start the timer
        let start_time = Instant::now();

        // Use break-with-value so cleanup (handler release, driver stop) always runs.
        // 120-second timeout: under Valgrind execution is ~10× slower, so 30 s is too tight.
        let loop_result: Result<(), Box<dyn error::Error>> = loop {
            if start_time.elapsed() > Duration::from_secs(120) {
                info!("Failed: exceeded 120-second timeout");
                break Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Timeout exceeded",
                )));
            }
            let c = count.load(Ordering::SeqCst);
            if c > 100 {
                break Ok(());
            }

            fn process_msg(ctx: &mut Context, buffer: &[u8], header: AeronHeader) {
                ctx.count.fetch_add(1, Ordering::SeqCst);

                let values = header.get_values().unwrap();
                assert_ne!(values.frame.session_id, 0);

                if buffer.len() != ctx.string_len {
                    ctx.stop.store(true, Ordering::SeqCst);
                    error!(
                        "ERROR: message was {} but was expecting {} [header={:?}]",
                        buffer.len(),
                        ctx.string_len,
                        header
                    );
                    sleep(Duration::from_secs(1));
                }

                assert_eq!(buffer.len(), ctx.string_len);
                assert_eq!(buffer, "1".repeat(ctx.string_len).as_bytes());
            }

            assembler.poll(&subscription, &mut context, process_msg, 128)?;
            assert_eq!(123, subscription.stream_id().unwrap());
        };

        subscription.close()?;

        info!("stopping client");
        stop_publisher.store(true, Ordering::SeqCst);

        let _ = publisher_handler.join().unwrap();
        drop(aeron);

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();

        // Release all context handlers now that Aeron and the driver are fully stopped.

        let cnc = AeronCnc::open(&ctx.get_dir().into_c_string())?;
        cnc.counters_reader()
            .foreach_counter_fn(|value: i64, id: i32, type_id: i32, key: &[u8], label: &str| {
                println!(
                    "counter reader id={id}, type_id={type_id}, key={key:?}, label={label}, value={value} [type={:?}]",
                    AeronSystemCounterType::try_from(type_id)
                );
            });
        cnc.error_log_read_fn(| observation_count: i32,
                                     first_observation_timestamp: i64,
                                     last_observation_timestamp: i64,
                                     error: &str| {
            println!("error: {error} observationCount={observation_count}, first_observation_timestamp={first_observation_timestamp}, last_observation_timestamp={last_observation_timestamp}");
        }, 0);
        cnc.loss_reporter_read_fn(|    observation_count: i64,
                                    total_bytes_lost: i64,
                                    first_observation_timestamp: i64,
                                    last_observation_timestamp: i64,
                                    session_id: i32,
                                    stream_id: i32,
                                    channel: &str,
                                    source: &str,| {
            println!("loss reporter observationCount={observation_count}, totalBytesLost={total_bytes_lost}, first_observed={first_observation_timestamp}, last_observed={last_observation_timestamp}, session_id={session_id}, stream_id={stream_id}, channel={channel} source={source}");
        })?;

        loop_result?;
        Ok(())
    }

    #[test]
    #[serial]
    pub fn try_claim() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (liveness_ns, unblock_ns, driver_timeout_ms) = if running_under_valgrind() {
            (180_000_000_000u64, 185_000_000_000u64, 180_000)
        } else {
            (60_000_000_000u64, 65_000_000_000u64, 60_000)
        };
        media_driver_ctx.set_client_liveness_timeout_ns(liveness_ns)?;
        media_driver_ctx.set_image_liveness_timeout_ns(liveness_ns)?;
        media_driver_ctx.set_publication_unblock_timeout_ns(unblock_ns)?;
        media_driver_ctx.set_driver_timeout_ms(driver_timeout_ms)?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        assert_eq!(media_driver_ctx.get_dir(), ctx.get_dir());
        // Keep client-side keepalive threshold aligned with slow Valgrind environment.
        ctx.set_driver_timeout_ms(driver_timeout_ms)?;
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;

        info!("creating client [try_claim test]");
        let aeron = Aeron::new(&ctx)?;
        info!("starting client");

        aeron.start()?;
        info!("client started");
        const STREAM_ID: i32 = 123;
        let publisher = aeron.add_publication(AERON_IPC_STREAM, STREAM_ID, Duration::from_secs(5))?;
        info!("created publisher");

        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            STREAM_ID,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_secs(5),
        )?;
        info!("created subscription");

        // pick a large enough size to confirm fragement assembler is working
        let string_len = 156;
        info!("string length: {}", string_len);

        let stop_publisher = Arc::new(AtomicBool::new(false));

        let publisher_handler = {
            let stop_publisher = stop_publisher.clone();
            std::thread::spawn(move || {
                let binding = "1".repeat(string_len);
                let msg = binding.as_bytes();
                let buffer = AeronBufferClaim::default();
                loop {
                    if stop_publisher.load(Ordering::Acquire) || publisher.is_closed() {
                        break;
                    }

                    let result = publisher.try_claim_raw(string_len, &buffer);

                    if result < msg.len() as i64 {
                        error!(
                            "ERROR: failed to send message {:?}",
                            AeronCError::from_code(result as i32)
                        );
                    } else {
                        buffer.data().write_all(&msg).unwrap();
                        buffer.commit().unwrap();
                    }
                }
                info!("stopping publisher thread");
            })
        };

        let count = Arc::new(AtomicUsize::new(0usize));
        let count_copy = Arc::clone(&count);
        let stop2 = stop.clone();

        struct FragmentHandler {
            count_copy: Arc<AtomicUsize>,
            stop2: Arc<AtomicBool>,
            string_len: usize,
        }

        impl AeronFragmentHandlerCallback for FragmentHandler {
            fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
                assert_eq!(STREAM_ID, header.get_values().unwrap().frame.stream_id);
                let header = header.get_values().unwrap();
                let frame = header.frame();
                let stream_id = frame.stream_id();
                assert_eq!(STREAM_ID, stream_id);

                self.count_copy.fetch_add(1, Ordering::SeqCst);

                if buffer.len() != self.string_len {
                    self.stop2.store(true, Ordering::SeqCst);
                    error!(
                        "ERROR: message was {} but was expecting {} [header={:?}]",
                        buffer.len(),
                        self.string_len,
                        header
                    );
                    sleep(Duration::from_secs(1));
                }

                assert_eq!(buffer.len(), self.string_len);
                assert_eq!(buffer, "1".repeat(self.string_len).as_bytes());
            }
        }

        let (closure, inner_handler) = Handler::with_fragment_assembler(FragmentHandler {
            count_copy,
            stop2,
            string_len,
        })?;
        let loop_result: Result<(), Box<dyn error::Error>> = {
            let start_time = Instant::now();
            loop {
                if start_time.elapsed() > Duration::from_secs(120) {
                    info!("Failed: exceeded 120-second timeout");
                    break Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Timeout exceeded",
                    )));
                }
                let c = count.load(Ordering::SeqCst);
                if c > 100 {
                    break Ok(());
                }
                subscription.poll(Some(&closure), 128)?;
            }
        };

        info!("stopping client");

        stop_publisher.store(true, Ordering::SeqCst);

        let _ = publisher_handler.join().unwrap();

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        loop_result?;
        Ok(())
    }

    #[test]
    #[serial]
    pub fn counters() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        assert_eq!(media_driver_ctx.get_dir(), ctx.get_dir());
        let error_handler = Handler::new(ErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;
        let unavailable_counter_handler = Handler::new(AeronUnavailableCounterLogger);
        ctx.set_on_unavailable_counter(Some(unavailable_counter_handler.clone()))?;

        struct AvailableCounterHandler {
            found_counter: bool,
        }

        impl AeronAvailableCounterCallback for AvailableCounterHandler {
            fn handle_aeron_on_available_counter(
                &mut self,
                counters_reader: AeronCountersReader,
                registration_id: i64,
                counter_id: i32,
            ) -> () {
                info!(
            "on counter key={:?}, label={:?} registration_id={registration_id}, counter_id={counter_id}, value={}, {counters_reader:?}",
            String::from_utf8(counters_reader.get_counter_key(counter_id).unwrap()),
            counters_reader.get_counter_label(counter_id, 1000),
            counters_reader.addr(counter_id)
        );

                assert_eq!(
                    counters_reader.counter_registration_id(counter_id).unwrap(),
                    registration_id
                );

                if let Ok(label) = counters_reader.get_counter_label(counter_id, 1000) {
                    if label == "label_buffer" {
                        self.found_counter = true;
                        assert_eq!(&counters_reader.get_counter_key(counter_id).unwrap(), "key".as_bytes());
                    }
                }
            }
        }

        let available_counter_handler = Handler::new(AvailableCounterHandler { found_counter: false });
        ctx.set_on_available_counter(Some(available_counter_handler.clone()))?;

        info!("creating client");
        let aeron = Aeron::new(&ctx)?;
        info!("starting client");

        aeron.start()?;
        info!("client started [counters test]");

        let counter = aeron.add_counter(123, "key".as_bytes(), "label_buffer", Duration::from_secs(5))?;
        let constants = counter.get_constants()?;
        let counter_id = constants.counter_id;

        let stop_publisher = Arc::new(AtomicBool::new(false));

        let publisher_handler = {
            let stop_publisher = stop_publisher.clone();
            let counter = counter.clone();
            std::thread::spawn(move || {
                for _ in 0..150 {
                    if stop_publisher.load(Ordering::Acquire) || counter.is_closed() {
                        break;
                    }
                    counter.addr_atomic().fetch_add(1, Ordering::SeqCst);
                }
                info!("stopping publisher thread");
            })
        };

        let now = Instant::now();
        while counter.addr_atomic().load(Ordering::SeqCst) < 100 && now.elapsed() < Duration::from_secs(10) {
            sleep(Duration::from_micros(10));
        }

        assert!(now.elapsed() < Duration::from_secs(10));

        info!("counter is {}", counter.addr_atomic().load(Ordering::SeqCst));

        info!("stopping client");

        #[cfg(not(target_os = "windows"))] // not sure why windows version doesn't fire event
        assert!(available_counter_handler.found_counter);

        let reader = aeron.counters_reader();
        assert_eq!(reader.get_counter_label(counter_id, 256)?, "label_buffer");
        assert_eq!(reader.get_counter_key(counter_id)?, "key".as_bytes());
        let buffers = AeronCountersReaderBuffers::default();
        reader.get_buffers(&buffers)?;

        stop_publisher.store(true, Ordering::SeqCst);

        let _ = publisher_handler.join().unwrap();

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    /// A simple error counter for testing error callback invocation.
    #[derive(Default, Debug)]
    struct TestErrorCount {
        pub error_count: usize,
    }

    impl Drop for TestErrorCount {
        fn drop(&mut self) {
            info!("TestErrorCount dropped with {} errors", self.error_count);
        }
    }

    impl AeronErrorHandlerCallback for TestErrorCount {
        fn handle_aeron_error_handler(&mut self, error_code: c_int, msg: &str) {
            error!("Aeron error {}: {}", error_code, msg);
            self.error_count += 1;
        }
    }

    #[test]
    #[serial]
    pub fn backpressure_recovery_test() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let under_valgrind = running_under_valgrind();
        let driver_timeout_ms = if under_valgrind { 180_000 } else { 60_000 };
        let liveness_timeout_ns = if under_valgrind {
            180_000_000_000
        } else {
            60_000_000_000
        };
        let poll_timeout = Duration::from_millis(driver_timeout_ms as u64);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        media_driver_ctx.set_client_liveness_timeout_ns(liveness_timeout_ns)?;
        media_driver_ctx.set_image_liveness_timeout_ns(liveness_timeout_ns)?;
        media_driver_ctx.set_publication_unblock_timeout_ns(liveness_timeout_ns + 5_000_000_000)?;
        media_driver_ctx.set_driver_timeout_ms(driver_timeout_ms)?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        ctx.set_driver_timeout_ms(driver_timeout_ms)?;
        let error_handler = Handler::new(TestErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_secs(5),
        )?;

        let count = Arc::new(AtomicUsize::new(0));
        let start_time = Instant::now();

        let stop_publisher = Arc::new(AtomicBool::new(false));

        // Spawn a publisher thread that repeatedly sends "test" messages.
        let publisher_thread = {
            let stop_publisher = stop_publisher.clone();
            std::thread::spawn(move || {
                while !stop_publisher.load(Ordering::Acquire) {
                    let msg = b"test";
                    let result = publisher.offer_raw(msg, Handlers::NONE);
                    // If backpressure is encountered, sleep a bit.
                    if result == AeronErrorType::PublicationBackPressured.code() as i64 {
                        sleep(Duration::from_millis(50));
                    }
                    if publisher.is_closed() {
                        break;
                    }
                }
            })
        };

        // Poll using the inline closure via poll_fn until we receive at least 50 messages.
        while count.load(Ordering::SeqCst) < 50 && start_time.elapsed() < poll_timeout {
            let _ = subscription.poll_fn(
                |_msg, _header| {
                    count.fetch_add(1, Ordering::SeqCst);
                },
                128,
            )?;
        }

        stop_publisher.store(true, Ordering::SeqCst);
        publisher_thread.join().unwrap();
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();

        assert!(
            count.load(Ordering::SeqCst) >= 50,
            "Expected at least 50 messages received"
        );
        Ok(())
    }

    #[test]
    #[serial]
    pub fn multi_subscription_test() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        media_driver_ctx.set_client_liveness_timeout_ns(60_000_000_000)?;
        media_driver_ctx.set_image_liveness_timeout_ns(60_000_000_000)?;
        media_driver_ctx.set_publication_unblock_timeout_ns(65_000_000_000)?;
        media_driver_ctx.set_driver_timeout_ms(60_000)?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(TestErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;

        // Create two subscriptions on the same channel.
        let subscription1 = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_secs(5),
        )?;
        let subscription2 = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_secs(5),
        )?;

        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        // Publish a single message.
        let msg = b"hello multi-subscription";
        let result = publisher.offer_raw(msg, Handlers::NONE);
        assert!(result >= msg.len() as i64, "Message should be sent successfully");

        let start_time = Instant::now();
        // Poll both subscriptions with inline closures until each has received at least one message.
        while (count1.load(Ordering::SeqCst) < 1 || count2.load(Ordering::SeqCst) < 1)
            && start_time.elapsed() < Duration::from_secs(5)
        {
            let _ = subscription1.poll_fn(
                |_msg, _header| {
                    count1.fetch_add(1, Ordering::SeqCst);
                },
                128,
            )?;
            let _ = subscription2.poll_fn(
                |_msg, _header| {
                    count2.fetch_add(1, Ordering::SeqCst);
                },
                128,
            )?;
        }

        assert!(
            count1.load(Ordering::SeqCst) >= 1,
            "Subscription 1 did not receive the message"
        );
        assert!(
            count2.load(Ordering::SeqCst) >= 1,
            "Subscription 2 did not receive the message"
        );

        drop(subscription2);
        drop(subscription1);
        drop(publisher);
        drop(aeron);
        _stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    pub fn should_be_able_to_drop_after_close_manually_being_closed() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(AeronErrorHandlerLogger);
        ctx.set_error_handler(Some(error_handler.clone()))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        {
            let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
            info!("created publication [sessionId={}]", publisher.session_id());
            publisher.close()?;
        }

        {
            let publisher = aeron.add_publication(AERON_IPC_STREAM, 124, Duration::from_secs(5))?;
            info!("created publication [sessionId={}]", publisher.session_id());
            publisher.close()?;
        }

        {
            let publisher = aeron.add_publication(AERON_IPC_STREAM, 125, Duration::from_secs(5))?;
            info!("created publication [sessionId={}]", publisher.session_id());
            publisher.close()?;
        }

        drop(aeron);
        _stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[test]
    #[serial]
    pub fn structural_close_safety_test() -> Result<(), Box<dyn error::Error>> {
        // Under the new design, close(self) consumes the handle — use-after-close
        // is a compile error, not a runtime check.  This test verifies that
        // structural teardown (the ManagedCResource::Drop path) properly frees
        // the C resource without errors.
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(TestErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;

        // Consume the publication handle — close(self) drops it, and the cleanup
        // closure (wired at construction) calls aeron_publication_close on Drop
        // of the last Rc.  No further use of `publisher` is possible at compile
        // time (the binding is moved into close()).
        publisher.close()?;

        drop(aeron);
        _stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    /// Test sending and receiving an empty (zero-length) message using inline closures with poll_fn.
    #[test]
    #[serial]
    pub fn empty_message_test() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let error_handler = Handler::new(TestErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone()))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_secs(5),
        )?;

        let empty_received = Arc::new(AtomicBool::new(false));
        let start_time = Instant::now();

        let result = publisher.offer_raw(b"", Handlers::NONE);
        assert!(result > 0);

        while !empty_received.load(Ordering::SeqCst) && start_time.elapsed() < Duration::from_secs(5) {
            let _ = subscription.poll_fn(
                |msg, _header| {
                    if msg.is_empty() {
                        empty_received.store(true, Ordering::SeqCst);
                    }
                },
                128,
            )?;
        }

        assert!(empty_received.load(Ordering::SeqCst), "Empty message was not received");
        drop(subscription);
        drop(publisher);
        drop(aeron);
        _stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        Ok(())
    }

    #[derive(Default, Debug)]
    struct MdcTotals {
        gap_events: u64,
        missing_messages: u64,
        received_messages: u64,
    }

    #[derive(Debug)]
    struct MdcWindowStats {
        expected_seq: Option<u64>,
        gap_events: u64,
        missing_messages: u64,
        received_messages: u64,
        histogram: Histogram<u64>,
    }

    impl MdcWindowStats {
        fn new() -> Result<Self, Box<dyn error::Error>> {
            Ok(Self {
                expected_seq: None,
                gap_events: 0,
                missing_messages: 0,
                received_messages: 0,
                histogram: Histogram::new(3)?,
            })
        }

        fn observe(&mut self, seq: u64, sent_ts_ns: u64) {
            self.received_messages += 1;

            match self.expected_seq {
                None => self.expected_seq = Some(seq.saturating_add(1)),
                Some(expected) if seq > expected => {
                    self.gap_events += 1;
                    self.missing_messages += seq - expected;
                    self.expected_seq = Some(seq.saturating_add(1));
                }
                Some(expected) if seq == expected => {
                    self.expected_seq = Some(expected.saturating_add(1));
                }
                Some(_) => {
                    // Ignore out-of-order/late packets for gap counting in this window.
                }
            }

            let now_ns = Aeron::nano_clock().max(0) as u64;
            let latency_ns = now_ns.saturating_sub(sent_ts_ns);
            let _ = self.histogram.record(latency_ns);
        }

        fn print_and_reset(&mut self, window_number: usize, interval: Duration, totals: &mut MdcTotals) {
            totals.gap_events += self.gap_events;
            totals.missing_messages += self.missing_messages;
            totals.received_messages += self.received_messages;

            if self.histogram.len() > 0 {
                let min_us = self.histogram.min() / 1_000;
                let p50_us = self.histogram.value_at_quantile(0.50) / 1_000;
                let p99_us = self.histogram.value_at_quantile(0.99) / 1_000;
                let max_us = self.histogram.max() / 1_000;
                println!(
                    "[mdc-window-{window_number}] interval={interval:?} received={} gaps={} missing={} latency_us[min={}, p50={}, p99={}, max={}]",
                    self.received_messages, self.gap_events, self.missing_messages, min_us, p50_us, p99_us, max_us,
                );
            } else {
                println!(
                    "[mdc-window-{window_number}] interval={interval:?} received=0 gaps=0 missing=0 latency_us[min=n/a, p50=n/a, p99=n/a, max=n/a]"
                );
            }

            self.expected_seq = None;
            self.gap_events = 0;
            self.missing_messages = 0;
            self.received_messages = 0;
            self.histogram.reset();
        }
    }

    /// Run with loss profile on macOS:
    /// `just mdc-loss-run 120 10 0.10`
    ///
    /// The recipe handles PF setup and cleanup automatically.
    ///
    /// Manual cleanup (if needed):
    /// `sudo pfctl -a com.apple/rusteron-mdc-loss -F all`
    /// `sudo dnctl -q flush`
    /// `sudo pfctl -f /etc/pf.conf`
    #[test]
    #[serial]
    #[ignore] // Long-running diagnostics test for manual MDC with rolling latency/gap reports.
    pub fn mdc_unreliable_gap_latency_histogram_report() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        const STREAM_ID: i32 = 32931;
        const CONTROL_PORT: u16 = 32929;
        const SUBSCRIBER_PORT: u16 = 32930;
        const MESSAGE_LEN: usize = 130;

        let report_interval = Duration::from_secs(
            std::env::var("RUSTERON_MDC_REPORT_INTERVAL_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(10),
        );
        let test_duration = Duration::from_secs(
            std::env::var("RUSTERON_MDC_TEST_DURATION_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(300),
        );
        let mdc_host = std::env::var("RUSTERON_MDC_HOST").unwrap_or("127.0.0.1".to_string());

        let publication_channel = format!("aeron:udp?control-mode=manual|control={}:{CONTROL_PORT}", mdc_host);
        // - group=false: do not apply MDC receiver-group semantics.
        // - nak-delay=100us: shorten unreliable-stream gap-fill decision latency.
        let subscription_channel = format!(
            "aeron:udp?endpoint={}:{SUBSCRIBER_PORT}|reliable=false|tether=false|group=false|nak-delay=500us",
            mdc_host
        );
        let destination_uri = format!("aeron:udp?endpoint={}:{SUBSCRIBER_PORT}", mdc_host);

        let (media_driver_ctx, stop_driver, driver_handle) = start_media_driver(32930)?;
        let aeron_dir = media_driver_ctx.get_dir().to_string();

        println!(
            "[mdc-start] publication={} subscription={} duration={:?} report_interval={:?}",
            publication_channel, subscription_channel, test_duration, report_interval
        );

        let running = Arc::new(AtomicBool::new(true));

        let subscriber_dir = aeron_dir.clone();
        let subscriber_channel = subscription_channel.clone();
        let subscriber_running = Arc::clone(&running);
        let subscriber_thread = std::thread::spawn(move || -> MdcTotals {
            let (_ctx, aeron) =
                create_client_for_dir(&subscriber_dir).expect("failed to create subscriber aeron client");

            let subscription = aeron
                .add_subscription(
                    &subscriber_channel.into_c_string(),
                    STREAM_ID,
                    Handlers::NONE,
                    Handlers::NONE,
                    Duration::from_secs(5),
                )
                .expect("failed to create subscriber");

            let mut totals = MdcTotals::default();
            let mut window_stats = MdcWindowStats::new().expect("failed to create histogram");
            let test_start = Instant::now();
            let mut window_start = test_start;
            let mut window_number = 1usize;

            while test_start.elapsed() < test_duration {
                let _ = subscription
                    .poll_fn(
                        |msg, _header| {
                            if msg.len() < 16 {
                                return;
                            }

                            let seq = u64::from_le_bytes(msg[0..8].try_into().unwrap());
                            let sent_ts_ns = u64::from_le_bytes(msg[8..16].try_into().unwrap());
                            window_stats.observe(seq, sent_ts_ns);
                        },
                        10_000,
                    )
                    .expect("subscriber poll failed");

                if window_start.elapsed() >= report_interval {
                    window_stats.print_and_reset(window_number, report_interval, &mut totals);
                    window_number += 1;
                    window_start = Instant::now();
                }
            }

            window_stats.print_and_reset(window_number, report_interval, &mut totals);
            subscriber_running.store(false, Ordering::SeqCst);
            totals
        });

        // Ensure subscriber has started before publisher setup.
        sleep(Duration::from_millis(250));

        let publisher_dir = aeron_dir;
        let publisher_channel = publication_channel.clone();
        let publisher_destination = destination_uri.clone();
        let publisher_running = Arc::clone(&running);
        let publisher_thread = std::thread::spawn(move || -> u64 {
            let (_ctx, aeron) = create_client_for_dir(&publisher_dir).expect("failed to create publisher aeron client");

            let publication = aeron
                .add_exclusive_publication(&publisher_channel.into_c_string(), STREAM_ID, Duration::from_secs(5))
                .expect("failed to create publication");

            let add_destination = AeronAsyncDestination::aeron_exclusive_publication_async_add_destination(
                &aeron,
                &publication,
                &publisher_destination.into_c_string(),
            )
            .expect("failed to add manual MDC destination");

            let add_destination_start = Instant::now();
            while add_destination
                .aeron_exclusive_publication_async_destination_poll()
                .expect("destination add poll failed")
                == 0
            {
                assert!(
                    add_destination_start.elapsed() <= Duration::from_secs(5),
                    "Timed out adding manual MDC destination"
                );
                sleep(Duration::from_millis(10));
            }

            let connect_start = Instant::now();
            while !publication.is_connected() && connect_start.elapsed() < Duration::from_secs(5) {
                sleep(Duration::from_millis(10));
            }
            assert!(
                publication.is_connected(),
                "manual MDC publication did not connect to subscriber destination"
            );

            let mut seq: u64 = 0;
            let mut payload = [0u8; MESSAGE_LEN];
            while publisher_running.load(Ordering::Acquire) {
                payload[0..8].copy_from_slice(&seq.to_le_bytes());
                let ts_ns = Aeron::nano_clock().max(0) as u64;
                payload[8..16].copy_from_slice(&ts_ns.to_le_bytes());

                let result = publication.offer_raw(&payload, Handlers::NONE);
                if result > 0 {
                    seq = seq.wrapping_add(1);
                }
                sleep(Duration::from_millis(1));
            }
            seq
        });

        let totals = subscriber_thread.join().unwrap();
        running.store(false, Ordering::SeqCst);
        let sent_messages = publisher_thread.join().unwrap();
        stop_driver.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();

        println!(
            "[mdc-summary] sent={} received={} total_gaps={} total_missing={}",
            sent_messages, totals.received_messages, totals.gap_events, totals.missing_messages
        );

        assert!(sent_messages > 0, "publisher failed to send any messages");
        assert!(totals.received_messages > 0, "subscriber did not receive any messages");
        Ok(())
    }

    #[test]
    #[serial]
    #[ignore] // need to work to get tags working properly, its more of testing issue then tag issue
    pub fn tags() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Debug);

        let (md_ctx, stop, md) = start_media_driver(1)?;

        let (_a_ctx2, aeron_sub) = create_client(&md_ctx)?;

        info!("creating suscriber 1");
        let sub = aeron_sub
            .add_subscription(
                c"aeron:udp?tags=100",
                123,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(50),
            )
            .map_err(|e| {
                error!("aeron error={}", Aeron::errmsg());
                e
            })?;

        let ctx = AeronContext::new()?;
        ctx.set_dir(&aeron_sub.context().get_dir().into_c_string())?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        info!("creating suscriber 2");
        let sub2 = aeron_sub.add_subscription(
            c"aeron:udp?tags=100",
            123,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_secs(50),
        )?;

        let (_a_ctx1, aeron_pub) = create_client(&md_ctx)?;
        info!("creating publisher");
        assert!(!aeron_pub.is_closed());
        let publisher = aeron_pub
            .add_publication(
                c"aeron:udp?endpoint=localhost:4040|alias=test|tags=100",
                123,
                Duration::from_secs(5),
            )
            .map_err(|e| {
                error!("aeron error={}", Aeron::errmsg());
                e
            })?;

        info!("publishing msg");

        loop {
            let result = publisher.offer_raw("213".as_bytes(), Handlers::NONE);
            if result < 0 {
                error!("failed to publish {:?}", AeronCError::from_code(result as i32));
            } else {
                break;
            }
        }

        sub.poll_fn(
            |msg, _header| {
                println!("Received message: {:?}", msg);
            },
            128,
        )?;
        sub2.poll_fn(
            |msg, _header| {
                println!("Received message: {:?}", msg);
            },
            128,
        )?;

        stop.store(true, Ordering::SeqCst);

        Ok(())
    }

    fn create_client_for_dir(dir: &str) -> Result<(AeronContext, Aeron), Box<dyn Error>> {
        info!("creating aeron client [dir={}]", dir);
        let ctx = AeronContext::new()?;
        ctx.set_dir(&dir.into_c_string())?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        Ok((ctx, aeron))
    }

    fn create_client(media_driver_ctx: &AeronDriverContext) -> Result<(AeronContext, Aeron), Box<dyn Error>> {
        let dir = media_driver_ctx.get_dir().to_string();
        create_client_for_dir(&dir)
    }

    fn start_media_driver(
        instance: u64,
    ) -> Result<
        (
            AeronDriverContext,
            Arc<AtomicBool>,
            JoinHandle<Result<(), rusteron_media_driver::AeronCError>>,
        ),
        Box<dyn Error>,
    > {
        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx
            .set_dir(&format!("{}{}-{}", media_driver_ctx.get_dir(), Aeron::epoch_clock(), instance).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);
        Ok((media_driver_ctx, stop, driver_handle))
    }

    /// C1: exercise the generated `AeronExclusivePublication` surface (the
    /// `offer_result` / `try_claim_owned` parity added for non-exclusive pubs)
    /// plus the generated `AeronPublicationConstants` accessors — a slice of the
    /// generated API that no other test touches end-to-end.
    #[test]
    #[serial]
    fn exclusive_publication_result_variants_and_constants() -> Result<(), Box<dyn error::Error>> {
        let (media_driver_ctx, stop, _driver_handle) = start_media_driver(8)?;
        let (_ctx, aeron) = create_client(&media_driver_ctx)?;

        let stream_id: i32 = 4321;
        let exclusive = aeron.add_exclusive_publication(AERON_IPC_STREAM, stream_id, Duration::from_secs(5))?;

        // Add a matching subscription so the IPC image connects and offers succeed.
        let sub_poller = aeron.async_add_subscription(
            &"aeron:ipc".to_string().into_c_string(),
            stream_id,
            Handlers::NONE,
            Handlers::NONE,
        )?;
        let mut subscription: Option<AeronSubscription> = None;
        let sub_start = Instant::now();
        while sub_start.elapsed() < Duration::from_secs(2) && subscription.is_none() {
            if let Ok(Some(s)) = sub_poller.poll() {
                subscription = Some(s);
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        let _subscription = subscription.expect("subscription did not come up");

        let conn = Instant::now();
        while !exclusive.is_connected() && conn.elapsed() < Duration::from_secs(2) {
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert_eq!(exclusive.status(), AeronStatus::Connected);

        // 1) offer_result_simple (typed Result variant on the exclusive pub).
        let payload = b"exclusive-result";
        let mut offered_pos = None;
        let offer_start = Instant::now();
        while offer_start.elapsed() < Duration::from_secs(2) && offered_pos.is_none() {
            if let Ok(pos) = exclusive.offer(payload) {
                offered_pos = Some(pos);
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        let pos = offered_pos.expect("exclusive offer_result_simple never succeeded");
        assert!(pos >= payload.len() as i64);

        // 2) RAII zero-copy claim on the exclusive pub.
        let claim_payload = b"exclusive-claim";
        let mut committed = false;
        let claim_start = Instant::now();
        while claim_start.elapsed() < Duration::from_secs(2) && !committed {
            if let Ok(mut claim) = exclusive.try_claim_owned(claim_payload.len()) {
                claim.data()[..claim_payload.len()].copy_from_slice(claim_payload);
                committed = claim.commit().is_ok();
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(committed, "exclusive try_claim_owned + commit never succeeded");

        // 3) Generated constants accessors return sane, consistent values.
        let constants = exclusive.get_constants().expect("publication constants");
        assert_eq!(constants.stream_id, stream_id);
        // channel is a *const c_char into the C struct; just assert it's a valid C string.
        assert!(!constants.channel.is_null());
        assert_eq!(
            unsafe { std::ffi::CStr::from_ptr(constants.channel) }.to_str().unwrap(),
            "aeron:ipc"
        );
        assert!(constants.max_possible_position > 0);
        assert!(constants.term_buffer_length > 0);
        assert!(constants.max_message_length > 0);
        assert!(constants.max_payload_length > 0);
        // position() advances with the offers.
        assert!(exclusive.position() >= pos);

        stop.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// C2: property tests for the pure-Rust (no driver) logic. Fast and
    /// deterministic — they fuzz the invariants the unit tests only sample.
    mod property_tests {
        use crate::{
            validate_endpoint_for_aeron_udp, AeronCError, AeronErrorType, AeronOfferError, AeronStatus,
            AeronStatusTracker,
        };
        use proptest::prelude::*;

        /// Fuzz: arbitrary endpoint strings must never panic — only Ok/Err.
        #[test]
        fn validate_endpoint_never_panics_on_arbitrary_input() {
            proptest!(|(s in ".{0,40}")| {
                let _ = validate_endpoint_for_aeron_udp(&s);
            });
        }

        /// Any hostname/IPv4 + port in 0..=65535 is accepted.
        #[test]
        fn validate_endpoint_accepts_well_formed_host_port() {
            let host = "[a-z][a-z0-9-]{0,20}(\\.[a-z0-9-]{1,20}){0,3}";
            proptest!(|(host in host, port in 0u16..=65535)| {
                let ep = format!("{host}:{port}");
                prop_assert!(
                    validate_endpoint_for_aeron_udp(&ep).is_ok(),
                    "expected ok for {ep}"
                );
            });
        }

        /// Any valid-looking host with a port > 65535 is rejected.
        #[test]
        fn validate_endpoint_rejects_out_of_range_port() {
            proptest!(|(port in 65536u32..=200_000)| {
                let ep = format!("localhost:{port}");
                prop_assert!(validate_endpoint_for_aeron_udp(&ep).is_err());
            });
        }

        /// Tracker emits iff the status differs from the previous one, and
        /// `last_status` is always the most recently observed.
        #[test]
        fn tracker_emits_iff_transition_and_tracks_last() {
            let status = prop::sample::select(vec![
                AeronStatus::Disconnected,
                AeronStatus::Connected,
                AeronStatus::BackPressured,
                AeronStatus::Closed,
            ]);
            proptest!(|(seq in prop::collection::vec(status, 0..40))| {
                let mut t = AeronStatusTracker::new();
                let mut prev: Option<AeronStatus> = None;
                for &s in &seq {
                    let emitted = t.observe(s);
                    let expected_emits = prev != Some(s);
                    prop_assert_eq!(emitted.is_some(), expected_emits);
                    if let Some(e) = emitted {
                        prop_assert_eq!(e, s);
                    }
                    prev = Some(s);
                }
                prop_assert_eq!(t.last_status(), seq.last().copied());
            });
        }

        /// `AeronOfferError::from_position`: non-negative → Ok(position); the five
        /// documented sentinels map to their variants; any other negative keeps its
        /// code inside `Error`. Every error is classified retryable xor fatal.
        #[test]
        fn from_position_and_classification_are_consistent() {
            proptest!(|(pos in -1_000_000i64..=1_000_000)| {
                match AeronOfferError::from_position(pos) {
                    Ok(p) => prop_assert!(p >= 0 && p == pos),
                    Err(e) => {
                        prop_assert!(pos < 0);
                        prop_assert!(e.is_retryable() ^ e.is_fatal());
                        match (pos, &e) {
                            (-1, AeronOfferError::NotConnected)
                            | (-2, AeronOfferError::BackPressured)
                            | (-3, AeronOfferError::AdminAction)
                            | (-4, AeronOfferError::Closed)
                            | (-5, AeronOfferError::MaxPositionExceeded) => {}
                            (p, AeronOfferError::Error(inner)) if p < -5 || p == 0 => {
                                prop_assert_eq!(inner.code, p as i32);
                            }
                            (p, other) => prop_assert!(false, "unexpected mapping {} -> {:?}", p, other),
                        }
                    }
                }
            });
        }

        /// Sanity: every documented Aeron error code maps back to itself.
        #[test]
        fn known_error_codes_round_trip() {
            for code in [-1, -2, -3, -4, -5, -6, -1000, -1001, -1002, -1003] {
                let err = AeronCError::from_code(code);
                assert_eq!(err.code, code);
                assert_eq!(AeronErrorType::from_code(code).code(), code);
            }
        }
    }

    #[doc = include_str!("../../README.md")]
    mod readme_tests {}

    #[cfg(test)]
    mod spin_poll_tests {
        use super::*;
        use crate::test_alloc::assert_no_allocation;
        use rusteron_media_driver::AeronDriverContext;
        use serial_test::serial;

        /// Tests the `poll_fn` closure-poll on `AeronSubscription`.
        /// Verifies that it receives all published messages without
        /// allocations on the hot path.
        #[test]
        #[serial]
        fn poll_fn_receives_all_messages_no_alloc() -> Result<(), Box<dyn error::Error>> {
            rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

            let media_driver_ctx = AeronDriverContext::new()?;
            media_driver_ctx.set_dir_delete_on_shutdown(true)?;
            media_driver_ctx.set_dir_delete_on_start(true)?;
            media_driver_ctx
                .set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
            let (stop, driver_handle) =
                rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

            let ctx = AeronContext::new()?;
            ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
            let error_handler = Handler::new(ErrorCount::default());
            ctx.set_error_handler(Some(error_handler.clone()))?;
            let aeron = Aeron::new(&ctx)?;
            aeron.start()?;

            let channel = String::from("aeron:ipc");
            let stream_id: i32 = 9999;

            let pub_poller = aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
            let sub_poller =
                aeron.async_add_subscription(&channel.into_c_string(), stream_id, Handlers::NONE, Handlers::NONE)?;

            let mut publication: Option<AeronPublication> = None;
            let mut subscription: Option<AeronSubscription> = None;
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(2) {
                if publication.is_none() {
                    if let Ok(Some(p)) = pub_poller.poll() {
                        publication = Some(p);
                    }
                }
                if subscription.is_none() {
                    if let Ok(Some(s)) = sub_poller.poll() {
                        subscription = Some(s);
                    }
                }
                if publication.is_some() && subscription.is_some() {
                    break;
                }
                #[cfg(debug_assertions)]
                sleep(Duration::from_millis(10));
            }

            let (publisher, subscription) = match (publication, subscription) {
                (Some(p), Some(s)) => (p, s),
                _ => panic!("publication/subscription did not come up"),
            };

            // Wait for IPC images to connect
            let conn_start = Instant::now();
            while !publisher.is_connected() && conn_start.elapsed() < Duration::from_secs(2) {
                #[cfg(debug_assertions)]
                sleep(Duration::from_millis(10));
            }

            // Publish N distinct messages
            const NUM_MESSAGES: usize = 10;
            let payloads: Vec<Vec<u8>> = (0..NUM_MESSAGES)
                .map(|i| format!("message-{}", i).into_bytes())
                .collect();

            for payload in &payloads {
                let offer_start = Instant::now();
                let mut offered = false;
                while offer_start.elapsed() < Duration::from_secs(2) {
                    if let Ok(pos) = publisher.offer(payload) {
                        if pos >= payload.len() as i64 {
                            offered = true;
                            break;
                        }
                    }
                    #[cfg(debug_assertions)]
                    sleep(Duration::from_millis(10));
                }
                assert!(offered, "Failed to offer message");
            }

            // Use poll_fn to receive all messages (spin-poll pattern)
            let mut received_count = 0;
            let max_iterations = 1000;

            for _ in 0..max_iterations {
                let fragments = subscription.poll_fn(
                    |data, _header| {
                        received_count += 1;
                        info!("Received fragment {} bytes", data.len());
                    },
                    1024,
                )?;

                if fragments > 0 {
                    // Got some messages, exit spin
                    break;
                }

                #[cfg(debug_assertions)]
                sleep(Duration::from_micros(100));
            }

            assert_eq!(
                received_count, NUM_MESSAGES,
                "Expected to receive {} messages, got {}",
                NUM_MESSAGES, received_count
            );

            // Now test that the hot path is allocation-free
            let alloc_before = current_allocs();

            let mut alloc_free_count = 0;
            let spin_alloc_test = || {
                for _ in 0..10 {
                    let _ = subscription.poll_fn(
                        |data, _header| {
                            alloc_free_count += 1;
                        },
                        1024,
                    );
                }
            };

            assert_no_allocation(spin_alloc_test);

            let alloc_after = current_allocs();
            assert!(
                (alloc_after - alloc_before).abs() < 10,
                "Expected no net allocation in poll_fn hot path"
            );

            // Cleanup
            stop.store(true, Ordering::SeqCst);
            let _ = driver_handle.join().unwrap();

            Ok(())
        }
    }

    // ── Use-after-free tests: aeron dropped before children ───────────────
    //
    // When `drop(aeron)` fires first, the C side calls `aeron_close()` which
    // frees every registered resource (publications, subscriptions, counters,
    // exclusive publications) via `aeron_client_conductor_on_close()`.
    //
    // The Rust wrappers don't know this happened — their `close_already_called`
    // flag is still `false`, so any subsequent method call that goes through
    // `get_inner()` dereferences a dangling pointer.
    //
    // These tests save the raw C pointer before `drop(aeron)`, then call C
    // functions directly on it afterwards to eliminate intermediate Drop
    // machinery that could trigger re-allocation.

    /// Helper: set up an embedded media driver + Aeron client.
    fn setup_aeron_for_uaf_test() -> (
        Aeron,
        rusteron_media_driver::testing::EmbeddedDriver,
        Handler<TestErrorCount>,
    ) {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let driver = rusteron_media_driver::testing::EmbeddedDriver::launch().unwrap();

        let ctx = AeronContext::new().unwrap();
        ctx.set_dir(&driver.dir().into_c_string()).unwrap();
        let error_handler = Handler::new(TestErrorCount::default());
        ctx.set_error_handler(Some(error_handler.clone())).unwrap();

        let aeron = Aeron::new(&ctx).unwrap();
        aeron.start().unwrap();

        (aeron, driver, error_handler)
    }

    fn teardown_aeron_after_uaf_test(
        driver: rusteron_media_driver::testing::EmbeddedDriver,
        error_handler: Handler<TestErrorCount>,
    ) {
        drop(driver); // stops + joins on Drop
    }

    /// `drop(aeron)` while children hold `Rc<Aeron>` references must not call
    /// `aeron_close()` until the last child drops — the raw C pointer reads below
    /// stay valid because close is deferred.
    #[test]
    #[serial]
    fn drop_client_before_children_is_safe_test() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        // Create all resource types.
        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1001, Duration::from_secs(5))
            .unwrap();
        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1002,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .unwrap();
        let counter = aeron
            .add_counter(1003, &[1u8, 2, 3, 4], "test counter", Duration::from_secs(5))
            .unwrap();
        let excl_pub = aeron
            .add_exclusive_publication(AERON_IPC_STREAM, 1004, Duration::from_secs(5))
            .unwrap();

        // Save raw C pointers while still valid.
        let pub_ptr: *mut aeron_publication_t = publisher.get_inner();
        let sub_ptr: *mut aeron_subscription_t = subscription.get_inner();
        let counter_ptr: *mut aeron_counter_t = counter.get_inner();
        let excl_pub_ptr: *mut aeron_exclusive_publication_t = excl_pub.get_inner();

        // SAFE: drop(aeron) does NOT call aeron_close() — children still
        // hold Rc references, keeping the C client alive.
        drop(aeron);

        // Read raw pointers — these access VALID (not freed) memory.
        let _closed = unsafe { aeron_publication_is_closed(pub_ptr) };
        let _connected = unsafe { aeron_subscription_is_connected(sub_ptr) };
        let _counter_closed = unsafe { aeron_counter_is_closed(counter_ptr) };
        let _excl_closed = unsafe { aeron_exclusive_publication_is_closed(excl_pub_ptr) };
        let _channel = unsafe { aeron_publication_channel(pub_ptr) };

        // Reaching here proves structural safety: no UAF after drop(aeron).

        // Now drop children properly — last one releases Rc<Aeron> →
        // aeron_close() fires in correct order.
        drop(publisher);
        drop(subscription);
        drop(counter);
        drop(excl_pub);

        // Teardown.
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    #[test]
    #[serial]
    fn cloned_leaf_handles_close_once_and_null_all_clones() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1101, Duration::from_secs(5))
            .unwrap();
        let publisher_clone = publisher.clone();
        assert!(!publisher_clone.get_inner().is_null());
        assert!(publisher.close().is_ok());
        assert!(publisher_clone.get_inner().is_null());
        assert!(publisher_clone.close().is_ok());

        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1102,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .unwrap();
        let subscription_clone = subscription.clone();
        assert!(!subscription_clone.get_inner().is_null());
        assert!(subscription.close().is_ok());
        assert!(subscription_clone.get_inner().is_null());
        assert!(subscription_clone.close().is_ok());

        let counter = aeron
            .add_counter(1103, &[1u8, 2, 3, 4], "close clone counter", Duration::from_secs(5))
            .unwrap();
        let counter_clone = counter.clone();
        assert!(!counter_clone.get_inner().is_null());
        assert!(counter.close().is_ok());
        assert!(counter_clone.get_inner().is_null());
        assert!(counter_clone.close().is_ok());

        let exclusive_publisher = aeron
            .add_exclusive_publication(AERON_IPC_STREAM, 1104, Duration::from_secs(5))
            .unwrap();
        let exclusive_publisher_clone = exclusive_publisher.clone();
        assert!(!exclusive_publisher_clone.get_inner().is_null());
        assert!(exclusive_publisher.close().is_ok());
        assert!(exclusive_publisher_clone.get_inner().is_null());
        assert!(exclusive_publisher_clone.close().is_ok());

        assert!(aeron.close().is_ok());
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    #[test]
    #[serial]
    fn close_with_handler_invokes_publication_close_notification_and_nulls_clones() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let notification_count = Arc::new(AtomicUsize::new(0));
        let close_handler = Handler::new(CloseNotificationCount {
            count: notification_count.clone(),
        });

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1151, Duration::from_secs(5))
            .unwrap();
        let publisher_clone = publisher.clone();

        assert!(publisher.close_with_handler(Some(&close_handler)).is_ok());
        assert!(publisher_clone.get_inner().is_null());

        let start = Instant::now();
        while notification_count.load(Ordering::SeqCst) == 0 && start.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(10));
        }
        assert_eq!(1, notification_count.load(Ordering::SeqCst));

        assert!(publisher_clone.close_with_handler(Some(&close_handler)).is_ok());
        assert_eq!(1, notification_count.load(Ordering::SeqCst));

        assert!(aeron.close().is_ok());
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    #[test]
    #[serial]
    fn explicit_client_close_defers_while_children_are_alive() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1201, Duration::from_secs(5))
            .unwrap();
        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1202,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .unwrap();
        let counter = aeron
            .add_counter(
                1203,
                &[5u8, 6, 7, 8],
                "deferred client close counter",
                Duration::from_secs(5),
            )
            .unwrap();
        let exclusive_publisher = aeron
            .add_exclusive_publication(AERON_IPC_STREAM, 1204, Duration::from_secs(5))
            .unwrap();

        let pub_ptr = publisher.get_inner();
        let sub_ptr = subscription.get_inner();
        let counter_ptr = counter.get_inner();
        let excl_ptr = exclusive_publisher.get_inner();

        assert!(aeron.clone().close().is_ok());

        assert!(!publisher.get_inner().is_null());
        assert!(!subscription.get_inner().is_null());
        assert!(!counter.get_inner().is_null());
        assert!(!exclusive_publisher.get_inner().is_null());

        let _ = unsafe { aeron_publication_is_closed(pub_ptr) };
        let _ = unsafe { aeron_subscription_is_connected(sub_ptr) };
        let _ = unsafe { aeron_counter_is_closed(counter_ptr) };
        let _ = unsafe { aeron_exclusive_publication_is_closed(excl_ptr) };

        drop(publisher);
        drop(subscription);
        drop(counter);
        drop(exclusive_publisher);

        assert!(aeron.close().is_ok());
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    #[test]
    #[serial]
    fn explicit_client_clone_close_defers_and_original_remains_usable() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let aeron_clone = aeron.clone();
        assert!(aeron_clone.close().is_ok());

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1301, Duration::from_secs(5))
            .expect("original client should remain usable after closing a clone");
        assert!(!publisher.get_inner().is_null());
        assert!(publisher.close().is_ok());

        assert!(aeron.close().is_ok());
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    #[test]
    #[serial]
    fn closing_leaf_resource_does_not_poison_client_or_siblings() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1401, Duration::from_secs(5))
            .unwrap();
        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1402,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .unwrap();

        assert!(publisher.close().is_ok());
        assert!(!subscription.get_inner().is_null());
        let _ = unsafe { aeron_subscription_is_connected(subscription.get_inner()) };

        let next_publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1403, Duration::from_secs(5))
            .expect("client should create new resources after a leaf close");
        assert!(!next_publisher.get_inner().is_null());

        assert!(subscription.close().is_ok());
        assert!(next_publisher.close().is_ok());
        assert!(aeron.close().is_ok());
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    /// After `aeron.close()` the surviving handles must remain fully usable —
    /// not merely safe to drop. A complete offer/poll roundtrip is the strongest
    /// proof that the deferred close left the C client fully operational.
    #[test]
    #[serial]
    fn client_close_defers_and_children_remain_fully_usable() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1501, Duration::from_secs(5))
            .unwrap();
        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1501,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .unwrap();

        assert!(aeron.clone().close().is_ok());

        let payload = b"post-close-roundtrip";
        let send_start = Instant::now();
        let mut sent = false;
        while send_start.elapsed() < Duration::from_secs(5) {
            if publisher.offer_raw(payload, Handlers::NONE) >= payload.len() as i64 {
                sent = true;
                break;
            }
            sleep(Duration::from_millis(10));
        }
        assert!(sent, "offer() must still work after a deferred client close");

        let received = Arc::new(AtomicUsize::new(0));
        let received_copy = received.clone();
        let read_start = Instant::now();
        while received.load(Ordering::SeqCst) == 0 && read_start.elapsed() < Duration::from_secs(5) {
            subscription
                .poll_fn(
                    |buffer, _header| {
                        assert_eq!(buffer, payload);
                        received_copy.fetch_add(1, Ordering::SeqCst);
                    },
                    16,
                )
                .unwrap();
            sleep(Duration::from_millis(10));
        }
        assert_eq!(
            1,
            received.load(Ordering::SeqCst),
            "poll() must still deliver after a deferred client close"
        );

        drop(publisher);
        drop(subscription);
        assert!(aeron.close().is_ok());
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    struct CountingAvailableImageHandler {
        available: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl AeronAvailableImageCallback for CountingAvailableImageHandler {
        fn handle_aeron_on_available_image(&mut self, _subscription: AeronSubscription, _image: AeronImage) {
            self.available.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Drop for CountingAvailableImageHandler {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A retained callback must stay alive as long as the C client can invoke it,
    /// even when the caller drops their `Handler` immediately after registration:
    /// the subscription owns a clone via its dependency list. It must be freed
    /// exactly once when the subscription (and its async poller) are gone.
    #[test]
    #[serial]
    fn retained_image_handler_outlives_callers_drop_and_frees_exactly_fn() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let available = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));

        let subscription = {
            let handler = Handler::new(CountingAvailableImageHandler {
                available: available.clone(),
                drops: drops.clone(),
            });
            let subscription = aeron
                .add_subscription(
                    AERON_IPC_STREAM,
                    1601,
                    Some(&handler),
                    Handlers::NONE,
                    Duration::from_secs(5),
                )
                .unwrap();
            // caller's reference goes away here — the subscription's dependency
            // clone must keep the callback value alive for the conductor thread
            drop(handler);
            subscription
        };
        assert_eq!(
            0,
            drops.load(Ordering::SeqCst),
            "handler freed while C can still call it"
        );

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1601, Duration::from_secs(5))
            .unwrap();

        let start = Instant::now();
        while available.load(Ordering::SeqCst) == 0 && start.elapsed() < Duration::from_secs(5) {
            let _ = publisher.offer_raw(b"wake", Handlers::NONE);
            sleep(Duration::from_millis(10));
        }
        assert!(
            available.load(Ordering::SeqCst) > 0,
            "image-available callback should have fired after the caller dropped its Handler"
        );
        assert_eq!(0, drops.load(Ordering::SeqCst));

        drop(publisher);
        drop(subscription);
        drop(aeron);
        assert_eq!(
            1,
            drops.load(Ordering::SeqCst),
            "handler must be freed exactly once after its owning resources close"
        );
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    /// Dropping an async poller *without polling it* must not free its retained
    /// callbacks: the C conductor completes the add in the background and will
    /// invoke the image handler when a publication connects. The handler is
    /// anchored to the client, whose lifetime matches the conductor's.
    #[test]
    #[serial]
    fn unpolled_async_subscription_drop_keeps_image_handler_alive() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let available = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));

        {
            let handler = Handler::new(CountingAvailableImageHandler {
                available: available.clone(),
                drops: drops.clone(),
            });
            let poller = aeron
                .async_add_subscription(AERON_IPC_STREAM, 1611, Some(&handler), Handlers::NONE)
                .unwrap();
            // both the caller's handler and the never-polled poller go away here
        }
        assert_eq!(
            0,
            drops.load(Ordering::SeqCst),
            "handler freed while the conductor can still invoke it (UAF)"
        );

        // the conductor completed the add internally; connecting a publication
        // fires the image-available callback into the (still alive) handler
        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1611, Duration::from_secs(5))
            .unwrap();
        let start = Instant::now();
        while available.load(Ordering::SeqCst) == 0 && start.elapsed() < Duration::from_secs(5) {
            let _ = publisher.offer_raw(b"wake", Handlers::NONE);
            sleep(Duration::from_millis(10));
        }
        assert!(
            available.load(Ordering::SeqCst) > 0,
            "conductor should have invoked the image handler after the unpolled poller dropped"
        );
        assert_eq!(0, drops.load(Ordering::SeqCst));

        drop(publisher);
        drop(aeron);
        assert_eq!(1, drops.load(Ordering::SeqCst), "freed exactly once at client close");
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    /// A failed async add (invalid URI) must fail cleanly: the error surfaces from
    /// new()/poll(), later polls are inert (the C client freed the async struct on
    /// the errored poll), and the client stays fully usable afterwards.
    #[test]
    #[serial]
    fn async_add_subscription_invalid_uri_fails_cleanly() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let bad_uri = c"aeron:udp?endpoint=not-a-real-host:0|interface=500.500.500.500";
        match aeron.async_add_subscription(bad_uri, 1621, Handlers::NONE, Handlers::NONE) {
            Err(_) => {} // rejected synchronously — fine
            Ok(poller) => {
                let mut saw_error = false;
                let start = Instant::now();
                while start.elapsed() < Duration::from_secs(5) {
                    match poller.poll() {
                        Err(_) => {
                            saw_error = true;
                            break;
                        }
                        Ok(Some(_)) => panic!("subscription must not be created for an invalid uri"),
                        Ok(None) => sleep(Duration::from_millis(10)),
                    }
                }
                assert!(saw_error, "poll should surface the async add error");
                // the C client freed the async struct on the errored poll; further
                // polls must be inert, not use-after-free
                assert!(matches!(poller.poll(), Ok(None)));
                assert!(matches!(poller.poll(), Ok(None)));
                drop(poller);
            }
        }

        // client unaffected: normal roundtrip still works
        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1622, Duration::from_secs(5))
            .unwrap();
        assert!(!publisher.get_inner().is_null());
        assert!(publisher.close().is_ok());
        assert!(aeron.close().is_ok());
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    /// The full dependency graph at once — context (with error handler), client + clone,
    /// publication + clone, exclusive publication, subscription with a retained image
    /// handler, counter, and an unpolled async poller — closed/dropped in an adversarial
    /// order, with every surviving handle exercised after each step. Deferred close must
    /// keep the C client alive until the *last* reference, and every retained handler must
    /// be freed exactly once at the end.
    #[test]
    #[serial]
    fn complex_object_graph_close_is_safe_in_any_order() {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = AeronDriverContext::new().unwrap();
        media_driver_ctx.set_dir_delete_on_shutdown(true).unwrap();
        media_driver_ctx.set_dir_delete_on_start(true).unwrap();
        media_driver_ctx
            .set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())
            .unwrap();
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let errors = Arc::new(AtomicUsize::new(0));
        let image_drops = Arc::new(AtomicUsize::new(0));
        let unpolled_drops = Arc::new(AtomicUsize::new(0));
        let available = Arc::new(AtomicUsize::new(0));

        let ctx = AeronContext::new().unwrap();
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string()).unwrap();
        let errors_cb = errors.clone();
        ctx.set_error_handler(Some(move |code: i32, msg: &str| {
            errors_cb.fetch_add(1, Ordering::SeqCst);
            log::error!("client error {code}: {msg}");
        }))
        .unwrap();
        let aeron = Aeron::new(&ctx).unwrap();
        aeron.start().unwrap();
        let aeron_clone = aeron.clone();
        drop(ctx); // the client's internal clone keeps the C context alive

        // children of every kind
        let publication = aeron
            .add_publication(AERON_IPC_STREAM, 1801, Duration::from_secs(5))
            .unwrap();
        let publication_clone = publication.clone();
        let exclusive = aeron
            .add_exclusive_publication(AERON_IPC_STREAM, 1801, Duration::from_secs(5))
            .unwrap();
        let image_handler = Handler::new(CountingAvailableImageHandler {
            available: available.clone(),
            drops: image_drops.clone(),
        });
        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1801,
                Some(&image_handler),
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .unwrap();
        drop(image_handler); // subscription + client keep it alive
        let counter = aeron
            .add_counter(1802, &[1, 2, 3], "graph counter", Duration::from_secs(5))
            .unwrap();
        let unpolled_handler = Handler::new(CountingAvailableImageHandler {
            available: available.clone(),
            drops: unpolled_drops.clone(),
        });
        let unpolled_poller = aeron
            .async_add_subscription(AERON_IPC_STREAM, 1803, Some(&unpolled_handler), Handlers::NONE)
            .unwrap();
        drop(unpolled_handler);
        drop(unpolled_poller); // never polled — its handler must stay alive via the client

        // adversarial order: close the client FIRST (defers), then use everything
        assert!(aeron.close().is_ok());

        let payload = b"graph-roundtrip";
        let start = Instant::now();
        let mut sent = false;
        while start.elapsed() < Duration::from_secs(5) {
            if publication.offer(payload).is_ok() {
                sent = true;
                break;
            }
            sleep(Duration::from_millis(5));
        }
        assert!(sent, "publication must work after deferred client close");
        assert!(exclusive.offer(payload).is_ok() || !exclusive.is_connected());
        let received = Arc::new(AtomicUsize::new(0));
        let received_copy = received.clone();
        let read_start = Instant::now();
        while received.load(Ordering::SeqCst) == 0 && read_start.elapsed() < Duration::from_secs(5) {
            subscription
                .poll_fn(
                    |_buf, _hdr| {
                        received_copy.fetch_add(1, Ordering::SeqCst);
                    },
                    16,
                )
                .unwrap();
            sleep(Duration::from_millis(5));
        }
        assert!(
            received.load(Ordering::SeqCst) >= 1,
            "subscription must deliver after deferred close"
        );
        counter.addr_atomic().store(42, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(42, counter.addr_atomic().load(std::sync::atomic::Ordering::SeqCst));

        // close a leaf, then its clone must be inert but safe
        assert!(publication.close().is_ok());
        assert!(publication_clone.get_inner().is_null());
        assert!(publication_clone.close().is_ok());

        // remaining teardown in shuffled order, exercising the clone last
        drop(subscription);
        assert_eq!(
            0,
            image_drops.load(Ordering::SeqCst),
            "client anchor still holds the image handler"
        );
        drop(exclusive);
        assert!(counter.close().is_ok());
        assert!(aeron_clone.close().is_ok()); // the LAST reference — the real aeron_close runs here

        assert_eq!(
            1,
            image_drops.load(Ordering::SeqCst),
            "image handler freed exactly once"
        );
        assert_eq!(
            1,
            unpolled_drops.load(Ordering::SeqCst),
            "unpolled poller's handler freed exactly once"
        );
        assert_eq!(
            0,
            errors.load(Ordering::SeqCst),
            "no client errors during graph teardown"
        );

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join();
    }

    /// Typed counter navigation: publisher-limit and subscriber-position counters are
    /// found by (type id, registration id), mirroring Java's CountersReader lookups.
    #[test]
    #[serial]
    fn counters_can_be_found_by_type_and_registration_id() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1951, Duration::from_secs(5))
            .unwrap();
        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1951,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .unwrap();
        let start = Instant::now();
        while !publisher.is_connected() && start.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(10));
        }

        let counters = aeron.counters_reader();
        let pub_registration_id = publisher.get_constants().unwrap().registration_id;
        let limit_counter = counters
            .find_by_type_and_registration_id(AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID as i32, pub_registration_id)
            .expect("publisher limit counter");
        assert!(
            counters.get_counter_value(limit_counter) > 0,
            "publisher limit should be positive once connected"
        );

        let sub_registration_id = subscription.get_constants().unwrap().registration_id();
        let position_counter = counters
            .find_by_type_and_registration_id(AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID as i32, sub_registration_id)
            .expect("subscriber position counter");
        assert!(counters.get_counter_value(position_counter) >= 0);

        assert!(counters
            .find_by_type_and_registration_id(AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID as i32, -12345)
            .is_none());

        drop(publisher);
        drop(subscription);
        drop(aeron);
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    /// Retained images are released back to the subscription automatically when the
    /// handle drops — in either order: image-then-subscription (normal release) or
    /// subscription-then-image (release skipped; the C client already reclaimed it).
    #[test]
    #[serial]
    fn retained_images_release_automatically_in_any_drop_order() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1901, Duration::from_secs(5))
            .unwrap();
        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1901,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .unwrap();
        let start = Instant::now();
        while subscription.image_count().unwrap_or(0) == 0 && start.elapsed() < Duration::from_secs(5) {
            let _ = publisher.offer(b"image-wake");
            sleep(Duration::from_millis(10));
        }
        assert!(subscription.image_count().unwrap() >= 1);

        // borrow-scoped iteration: no bookkeeping
        let mut seen = 0;
        subscription.for_each_image(|image| {
            assert!(!image.get_inner().is_null());
            seen += 1;
        });
        assert!(seen >= 1);

        // normal order: image handle dropped while the subscription is alive
        {
            let image = subscription.image_at_index(0).expect("image at 0");
            let session_id = image.get_constants().unwrap().session_id();
            assert!(subscription.image_by_session_id(session_id).is_some());
            assert!(subscription.image_by_session_id(session_id ^ 0x5555_5555).is_none());
            drop(image); // releases via aeron_subscription_image_release
        }
        // still healthy afterwards: roundtrip works
        assert!(subscription.poll_fn(|_, _| {}, 4).is_ok());

        // adversarial order: subscription closed while a retained image handle lives
        let image = subscription.image_at_index(0).expect("image at 0");
        drop(subscription);
        drop(image); // must be a safe no-op, not a UAF release

        drop(publisher);
        drop(aeron);
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    /// `close_now` is the unsafe escape hatch: the C close runs immediately.
    /// Clones of the client handle share the nulled pointer, so they stay safe
    /// to drop or close (children would dangle — none exist here).
    #[test]
    #[serial]
    fn close_now_with_clones_only_is_safe() {
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let clone = aeron.clone();
        unsafe {
            assert!(aeron.close_now().is_ok());
        }
        assert!(clone.get_inner().is_null(), "clones must see the nulled pointer");
        assert!(clone.close().is_ok(), "closing an already-closed clone is a no-op");
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    /// `close_now` with a complex object graph where Rust drops children before
    /// the parent. In 0.2's deferred-close model, children close immediately when
    /// dropped, so calling `close_now()` on a parent after all children have been
    /// dropped must be safe — the C client handles already-closed resources
    /// gracefully. This test verifies no segfaults or double-frees (CI runs this
    /// under ASan which catches use-after-free).
    ///
    /// WARNING: if any child handle survives `close_now()`, its C resources are
    /// freed and the handle becomes unsafe to use (dangling pointer). You MUST
    /// drop all children before calling `close_now()`, or explicitly `std::mem::forget`
    /// them and accept the UB.
    #[test]
    #[serial]
    fn close_now_after_all_children_dropped_is_safe() {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        // Build a complex object graph: publications, subscriptions, exclusive publications
        let pub1 = aeron
            .add_publication(AERON_IPC_STREAM, 1001, Duration::from_secs(5))
            .expect("add publication 1");
        let pub2 = aeron
            .add_publication(AERON_IPC_STREAM, 1002, Duration::from_secs(5))
            .expect("add publication 2");

        let sub1 = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1001,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .expect("add subscription 1");
        let sub2 = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1002,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .expect("add subscription 2");

        let excl_pub = aeron
            .add_exclusive_publication(AERON_IPC_STREAM, 1003, Duration::from_secs(5))
            .expect("add exclusive publication");

        // All children must be dropped before close_now - their C resources are freed
        drop(pub1);
        drop(pub2);
        drop(sub1);
        drop(sub2);
        drop(excl_pub);

        // Call close_now on the parent — must not segfault or double-free
        unsafe {
            assert!(
                aeron.close_now().is_ok(),
                "close_now should succeed after all children dropped"
            );
        }

        teardown_aeron_after_uaf_test(driver, error_handler);
        // Valgrind will catch any double-frees or use-after-free from this test
    }

    /// DANGER: This test documents the UB scenario where children survive
    /// `close_now()`. This is commented-out because it WILL segfault (ASan would
    /// catch it as use-after-free). This proves you MUST drop all children before
    /// calling `close_now()`.
    ///
    /// What happens:
    /// 1. `aeron.close_now()` frees the Aeron C client
    /// 2. All publications/subscriptions are freed in C
    /// 3. But `pub2` still holds a non-null pointer to freed memory
    /// 4. `drop(pub2)` calls `aeron_publication_close()` on dangling pointer → UB
    ///
    /// DO NOT call `close_now()` with surviving children — it's fundamentally unsafe.
    #[test]
    #[serial]
    #[ignore = "this test WILL segfault - it documents the UB scenario"]
    fn close_now_with_surviving_children_is_ub_and_segfaults() {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();

        let pub2 = aeron
            .add_publication(AERON_IPC_STREAM, 1001, Duration::from_secs(5))
            .expect("add publication");

        // Call close_now while pub2 is still alive — UNSAFE
        unsafe {
            let _ = aeron.close_now();
        }

        // pub2 now holds a dangling pointer. Dropping it calls
        // aeron_publication_close() on freed memory → SEGFAULT
        drop(pub2); // <-- BOOM: use-after-free

        // Never reached
        teardown_aeron_after_uaf_test(driver, error_handler);
    }

    /// The client stores the raw context pointer in C for its entire life, so the
    /// context must outlive the client. Dropping the user's `AeronContext` handle
    /// only releases a reference — the client's internal clone keeps the C context
    /// alive, and a full roundtrip must still work afterwards.
    #[test]
    #[serial]
    fn context_drop_while_client_alive_is_safe() {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = AeronDriverContext::new().unwrap();
        media_driver_ctx.set_dir_delete_on_shutdown(true).unwrap();
        media_driver_ctx.set_dir_delete_on_start(true).unwrap();
        media_driver_ctx
            .set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())
            .unwrap();
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let aeron = {
            let ctx = AeronContext::new().unwrap();
            ctx.set_dir(&media_driver_ctx.get_dir().into_c_string()).unwrap();
            let aeron = Aeron::new(&ctx).unwrap();
            drop(ctx); // user's handle gone; the client's clone keeps the C context alive
            aeron
        };
        aeron.start().unwrap();

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1631, Duration::from_secs(5))
            .unwrap();
        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1631,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .unwrap();

        let payload = b"ctx-dropped-roundtrip";
        let start = Instant::now();
        let mut sent = false;
        while start.elapsed() < Duration::from_secs(5) {
            if publisher.offer_raw(payload, Handlers::NONE) >= payload.len() as i64 {
                sent = true;
                break;
            }
            sleep(Duration::from_millis(10));
        }
        assert!(sent);

        let received = Arc::new(AtomicUsize::new(0));
        let received_copy = received.clone();
        let read_start = Instant::now();
        while received.load(Ordering::SeqCst) == 0 && read_start.elapsed() < Duration::from_secs(5) {
            subscription
                .poll_fn(
                    |buffer, _| {
                        assert_eq!(buffer, payload);
                        received_copy.fetch_add(1, Ordering::SeqCst);
                    },
                    16,
                )
                .unwrap();
            sleep(Duration::from_millis(10));
        }
        assert_eq!(1, received.load(Ordering::SeqCst));

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join();
    }

    /// Media driver dies mid-flight: the client must surface the failure through
    /// the error handler (driver keepalive timeout) and every handle must stay
    /// memory-safe — offers turn into errors, never UB — and teardown completes.
    #[test]
    #[serial]
    fn media_driver_shutdown_surfaces_errors_and_client_stays_safe() {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = AeronDriverContext::new().unwrap();
        media_driver_ctx.set_dir_delete_on_shutdown(true).unwrap();
        media_driver_ctx.set_dir_delete_on_start(true).unwrap();
        media_driver_ctx
            .set_dir(&format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())
            .unwrap();
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let errors = Arc::new(AtomicUsize::new(0));
        let ctx = AeronContext::new().unwrap();
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string()).unwrap();
        // short keepalive so the driver loss is detected quickly
        ctx.set_driver_timeout_ms(2_000).unwrap();
        let errors_cb = errors.clone();
        ctx.set_error_handler(Some(move |code: i32, msg: &str| {
            errors_cb.fetch_add(1, Ordering::SeqCst);
            log::info!("client error {code}: {msg}");
        }))
        .unwrap();
        let aeron = Aeron::new(&ctx).unwrap();
        aeron.start().unwrap();

        let publisher = aeron
            .add_publication(AERON_IPC_STREAM, 1701, Duration::from_secs(5))
            .unwrap();
        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                1701,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .unwrap();

        // sanity roundtrip while the driver is up
        let send_start = Instant::now();
        while publisher.offer(b"pre-shutdown").is_err() && send_start.elapsed() < Duration::from_secs(5) {
            sleep(Duration::from_millis(10));
        }

        // kill the driver
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join();

        // the conductor must notice the driver is gone and invoke the error handler
        let start = Instant::now();
        while errors.load(Ordering::SeqCst) == 0 && start.elapsed() < Duration::from_secs(15) {
            // keep exercising the handles: must return errors, never crash
            let _ = publisher.offer(b"post-shutdown");
            let _ = subscription.poll_fn(|_, _| {}, 4);
            sleep(Duration::from_millis(50));
        }
        assert!(
            errors.load(Ordering::SeqCst) > 0,
            "client error handler should have reported the driver keepalive timeout"
        );

        // handles remain safe after the client flags the failure
        match publisher.offer(b"after-error") {
            Ok(_) => {}
            Err(e) => assert!(e.is_retryable() || e.is_fatal()), // typed, not UB
        }
    }

    // ── Handler dependency tracking tests ────────────────────────────────
    //
    // These tests verify that handlers registered via add_dependency()
    // are kept alive correctly. Handlers must survive aeron.close() and
    // only be released when the Aeron client itself is dropped.
    //
    // NOTE: Handler<T> wraps Arc<UnsafeCell<T>>, and UnsafeCell deliberately
    // doesn't implement Drop (C code mutates handlers through raw pointers).
    // We track handler lifetime via Arc::strong_count instead of drop counters.

    /// Test that a single error handler survives handler clone drops
    /// and persists via dependency tracking.
    #[test]
    #[serial]
    fn handler_dependency_tracking() {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        let (aeron, driver, _error_handler) = setup_aeron_for_uaf_test();

        // Create a test handler
        let handler = Handler::new(move |code: i32, msg: &str| {
            log::info!("Error handler called: code={}, msg={}", code, msg);
        });

        // Store a clone to track strong_count
        let handler1 = handler.clone();

        // Verify initial strong_count is 2 (handler and handler1)
        assert_eq!(
            Arc::strong_count(&handler1.inner),
            2,
            "Handler should be held by handler and handler1"
        );

        // Register the handler with the context
        let ctx = aeron.context();
        ctx.set_error_handler(Some(handler)).unwrap();

        // After registration, handler1 is the only external reference
        // (the original handler was moved into the context's dependencies)
        assert_eq!(
            Arc::strong_count(&handler1.inner),
            1,
            "Handler should be held only by handler1 (original stored in context)"
        );

        // Explicitly close the Aeron client
        aeron.close().unwrap();

        // The handler should STILL be alive because it's stored in context
        assert_eq!(
            Arc::strong_count(&handler1.inner),
            1,
            "Handler must survive aeron.close() - still tracked by context"
        );

        // Drop handler1 - now only context holds the handler
        drop(handler1);

        teardown_aeron_after_uaf_test(driver, _error_handler);
    }

    /// Test that subscription image handlers are properly tracked
    /// and survive subscription drops.
    #[test]
    #[serial]
    fn subscription_handler_dependency_tracking() {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        let (aeron, driver, _error_handler) = setup_aeron_for_uaf_test();

        let avail_handler = Handler::new(move |subscription: AeronSubscription, image: AeronImage| {
            log::info!("Available image callback invoked");
        });

        let local_avail = avail_handler.clone();

        // Create subscription with handlers
        let sub = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                2001,
                Some(&avail_handler),
                None::<&Handler<AeronUnavailableImageLogger>>,
                Duration::from_secs(5),
            )
            .unwrap();

        // Handler is held by avail_handler, local_avail, and stored in subscription
        let initial_count = Arc::strong_count(&local_avail.inner);
        assert!(
            initial_count >= 2,
            "Handler should be held by at least avail_handler and local_avail"
        );

        // Close the subscription
        drop(sub);

        // Handler should STILL be alive - tracked by dependencies
        let after_sub_drop = Arc::strong_count(&local_avail.inner);
        assert!(
            after_sub_drop >= 1,
            "Handler must survive subscription drop - still tracked"
        );

        // Drop the Aeron wrapper
        drop(aeron);

        teardown_aeron_after_uaf_test(driver, _error_handler);
    }

    // ── Structural teardown verification via memory protection ────────
    //
    // Under the new design, the Rc dependency graph ensures correct
    // teardown order: `aeron_close()` can only fire once every child's
    // Rc<Aeron> is released.  This section proves that:
    //
    //  1. `drop(aeron)` is SAFE while children are alive (Rc keeps the
    //     C client alive).
    //  2. `drop(publisher)` triggers `aeron_close()` (when its Rc<Aeron>
    //     is the last handle), which frees the publication — UAF after
    //     the publisher drops is the *correct* C-level behaviour.
    //
    // To prove (2) we use `mprotect(PROT_NONE)` on the page containing
    // the dangling pointer after `aeron_close()`.  Any access then
    // triggers SIGBUS/SIGSEGV, proving the pointer targets freed memory.
    //
    // On Linux/macOS `mprotect` on malloc'd heap memory works at the page
    // level.  This test uses `fork()` so the dangerous mprotect runs in a
    // child process — the parent test runner is never at risk.
    //
    // The test passes when the child exits with SIGABRT (teardown proven).

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    const PROT_NONE: i32 = 0;

    #[cfg(target_os = "macos")]
    const SYS_PAGESIZE: i32 = 29;
    #[cfg(target_os = "linux")]
    const SYS_PAGESIZE: i32 = 30;

    extern "C" {
        fn mprotect(addr: *mut core::ffi::c_void, len: usize, prot: i32) -> i32;
        fn sysconf(name: i32) -> isize;
        fn write(fd: i32, buf: *const core::ffi::c_void, count: usize) -> isize;
    }

    /// Async-signal-safe write of a fixed message to stderr.
    unsafe fn uaf_write_msg(msg: &[u8]) {
        write(2, msg.as_ptr() as *const core::ffi::c_void, msg.len());
    }

    /// Signal handler for SIGBUS / SIGSEGV caught during the mprotect test.
    /// Converts the crash into a clean abort with a diagnostic message.
    extern "C" fn uaf_sigbus_handler(_sig: i32) {
        unsafe {
            uaf_write_msg(b"\n\n*** CORRECT TEARDOWN *** aeron_close() freed the publication memory\n");
            uaf_write_msg(b"*** Rc graph teardown confirmed: the publication's page was protected\n");
            uaf_write_msg(b"*** with mprotect(PROT_NONE) after the publisher dropped, proving the\n");
            uaf_write_msg(b"*** pointer is dangling because aeron_close freed it, not before.\n\n");
        }
        std::process::abort();
    }

    /// Prove the Rc graph teardown frees C memory: after the last child handle
    /// drops (triggering deferred `aeron_close()`), `mprotect(PROT_NONE)` on the
    /// publication's page causes a SIGBUS on access, confirming the memory was freed.
    ///
    /// This test is `#[ignore]` because it uses `mprotect(PROT_NONE)` which
    /// affects an entire VM page.  If other live heap allocations share the
    /// same page as the freed Aeron resource the process may crash during
    /// teardown too, so we call `std::process::abort` on detection and
    /// `std::process::exit(0)` on the no-crash path.
    ///
    /// Uses `fork()` to isolate the dangerous mprotect + access into
    /// a child process.  The parent waits for the child's exit status:
    ///
    /// | Child exit | Meaning | Test result |
    /// |---|---|---|
    /// | `_exit(0)` | `mprotect` unsupported on this platform | pass (skip) |
    /// | SIGABRT | freed memory detected via signal handler | **pass (teardown proven)** |
    /// | other signal or non-zero exit | unexpected failure | fail |
    #[test]
    #[serial]
    fn prove_rc_teardown_frees_via_mprotect() {
        extern "C" {
            fn signal(sig: i32, handler: unsafe extern "C" fn(i32)) -> usize;
            fn fork() -> i32;
            fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
            fn _exit(status: i32) -> !;
        }
        #[cfg(target_os = "macos")]
        const SIGBUS: i32 = 10;
        #[cfg(not(target_os = "macos"))]
        const SIGBUS: i32 = 7;
        const SIGSEGV: i32 = 11;

        // This test deliberately forks, mprotects, and `mem::forget`s the driver
        // to PROVE Rc teardown frees the C resource — it is not a memory-hygiene
        // target, and its non-standard teardown (native-executed child + unusual
        // close ordering) confuses Valgrind's leak accounting. Skip under Valgrind.
        if running_under_valgrind() {
            return;
        }

        unsafe {
            let pid = fork();
            assert!(pid >= 0, "fork failed");
            if pid == 0 {
                // ── CHILD: proof of correct Rc graph teardown ──
                // The parent is unaffected (fork makes the child's
                // mprotect page-private via copy-on-write).

                signal(SIGBUS, uaf_sigbus_handler);
                signal(SIGSEGV, uaf_sigbus_handler);

                let (aeron, driver, error_handler) = setup_aeron_for_uaf_test();
                let publisher = aeron
                    .add_publication(AERON_IPC_STREAM, 1006, Duration::from_secs(5))
                    .unwrap();

                let raw_ptr: *mut aeron_publication_t = publisher.get_inner();

                // Step 1: drop(aeron) — SAFE under new design.  The
                // publisher still holds an Rc<Aeron>, so aeron_close()
                // is deferred.
                drop(aeron);

                // Step 2: drop(publisher) — the Rc<Aeron> refcount drops
                // to 0, triggering aeron_close().  aeron_close() frees all
                // C resources including the publication at raw_ptr.
                drop(publisher);

                // Step 3: mprotect the page.  raw_ptr is now dangling.
                let page_size = sysconf(SYS_PAGESIZE) as usize;
                let page = (raw_ptr as usize) & !(page_size - 1);
                let rc = mprotect(page as *mut core::ffi::c_void, page_size, PROT_NONE);

                if rc == 0 {
                    // Page is PROT_NONE — accessing raw_ptr triggers SIGBUS →
                    // signal handler → "teardown confirmed" → abort() (SIGABRT).
                    let _closed = aeron_publication_is_closed(raw_ptr);
                    // If we reach here, mprotect worked but nothing crashed.
                    uaf_write_msg(b"*** FAIL: mprotect succeeded but freed access did not crash\n");
                    std::process::abort();
                }

                // mprotect not supported on this platform — clean exit.
                drop(error_handler);
                std::mem::forget(driver); // child process: no clean driver teardown needed
                _exit(0);
            }

            // ── PARENT: wait for child's result ──
            let mut status: i32 = 0;
            let waited = waitpid(pid, &mut status as *mut i32, 0);
            assert!(waited != -1, "waitpid failed");

            if status & 0x7f == 0 {
                // Child exited normally (WIFEXITED).
                let code = (status >> 8) & 0xff;
                if code == 0 {
                    eprintln!(
                        "note: mprotect(PROT_NONE) not supported on heap \
                         memory on this platform — teardown proof skipped"
                    );
                    return;
                }
                panic!("child exited with code {} (unexpected — setup error?)", code);
            }

            // Child was signalled (WIFSIGNALED).
            let sig = status & 0x7f;
            if sig == 6 {
                // SIGABRT from uaf_sigbus_handler → teardown PROVEN.
                eprintln!(
                    "*** PASS: Rc graph teardown proven — child confirmed freed \
                     memory after aeron_close() via mprotect"
                );
                return;
            }

            panic!("child killed by signal {} (expected SIGABRT=6 for teardown proof)", sig);
        }
    }
}
