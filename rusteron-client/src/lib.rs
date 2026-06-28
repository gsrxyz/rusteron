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
pub const PUBLICATION_MAX_POSITION_EXCEEDED: i64 =
    bindings::AERON_PUBLICATION_MAX_POSITION_EXCEEDED as i64;
pub const PUBLICATION_ERROR: i64 = bindings::AERON_PUBLICATION_ERROR as i64;

include!(concat!(env!("OUT_DIR"), "/aeron.rs"));
include!(concat!(env!("OUT_DIR"), "/aeron_custom.rs"));

// Retryable / unrecoverable classification for Aeron errors (hand-written — the codegen drops
// doc-commented methods from common.rs). GenericError(-1) and Unknown(_) are intentionally
// neither: `-1` is Aeron's catch-all and could mean anything, so the caller must decide.
impl AeronErrorType {
    /// Transient — retry the operation (back off first): back-pressure, admin action, a full
    /// client buffer, or a polling timeout.
    pub fn is_retryable(&self) -> bool {
        self == &AeronErrorType::PublicationBackPressured
            || self == &AeronErrorType::PublicationAdminAction
            || self == &AeronErrorType::ClientErrorBufferFull
            || self == &AeronErrorType::TimedOut
    }

    /// Definitively terminal — retrying will not help. Not exhaustive: ambiguous codes
    /// (`GenericError` / `Unknown`) are neither retryable nor unrecoverable.
    pub fn is_unrecoverable(&self) -> bool {
        self == &AeronErrorType::PublicationClosed
            || self == &AeronErrorType::PublicationMaxPositionExceeded
            || self == &AeronErrorType::PublicationError
            || self == &AeronErrorType::ClientErrorDriverTimeout
            || self == &AeronErrorType::ClientErrorClientTimeout
            || self == &AeronErrorType::ClientErrorConductorServiceTimeout
    }
}

impl AeronCError {
    /// Transient failure — retry the operation (back off first). See [`AeronErrorType::is_retryable`].
    pub fn is_retryable(&self) -> bool {
        self.kind().is_retryable()
    }

    /// Definitively terminal — abort the operation. See [`AeronErrorType::is_unrecoverable`].
    pub fn is_unrecoverable(&self) -> bool {
        self.kind().is_unrecoverable()
    }
}

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
impl IdleStrategy for NoOpIdleStrategy {
    #[inline]
    fn idle(&mut self, _work_count: i32) {}
}

/// Yield the OS thread when idle. Lower CPU than busy-spin, slightly higher latency.
/// (Aeron `YieldingIdleStrategy`.)
pub struct YieldingIdleStrategy;
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

    fn running_under_valgrind() -> bool {
        std::env::var_os("RUSTERON_VALGRIND").is_some()
    }

    #[test]
    #[serial]
    fn version_check() -> Result<(), Box<dyn error::Error>> {
        unsafe {
            aeron_randomised_int32();
        }
        let alloc_count = current_allocs();

        {
            let major = unsafe { crate::aeron_version_major() };
            let minor = unsafe { crate::aeron_version_minor() };
            let patch = unsafe { crate::aeron_version_patch() };

            let cargo_version = "1.51.0";
            let aeron_version = format!("{}.{}.{}", major, minor, patch);
            assert_eq!(aeron_version, cargo_version);

            let ctx = AeronContext::new()?;
            let error_count = 1;
            let mut handler = Handler::leak(ErrorCount::default());
            ctx.set_error_handler(Some(&handler))?;

            assert!(Aeron::epoch_clock() > 0);
            drop(ctx);
            assert!(handler.should_drop);
            handler.release();
            assert!(!handler.should_drop);
            drop(handler);
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
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:udp?endpoint=203.0.113.1:54321");

        // Create async publication and subscription pollers on the same invalid channel and
        // attempt to resolve them. If both are created, try a small send/receive cycle and then exit.
        let pub_poller = aeron.async_add_publication(&channel.clone().into_c_string(), 4321)?;
        let sub_poller = aeron.async_add_subscription(
            &channel.into_c_string(),
            4321,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?;

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
                let res = publisher.offer(payload, Handlers::no_reserved_value_supplier_handler());
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
                    let _ = subscription.poll_once(
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
        drop(sub_poller);
        drop(pub_poller);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
        Ok(())
    }

    /// Exercises the additive ergonomics API end-to-end:
    /// `offer_result_simple`, `try_claim_owned` + `commit`, the `session_id` /
    /// `stream_id` header accessors, and `status()` on publication/subscription.
    #[test]
    #[serial]
    fn offer_result_and_claim_roundtrip_with_header_accessors() -> Result<(), Box<dyn error::Error>>
    {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:ipc");
        let stream_id: i32 = 9123;
        let pub_poller =
            aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
        let sub_poller = aeron.async_add_subscription(
            &channel.into_c_string(),
            stream_id,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?;

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
            if let Ok(pos) = publisher.offer_result_simple(payload) {
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

        // 3) Receive both and assert the header accessors.
        let received_offer = std::cell::Cell::new(false);
        let received_claim = std::cell::Cell::new(false);
        let header_ids = std::cell::Cell::new(Option::<(i32, i32)>::None);
        let read_start = Instant::now();
        while read_start.elapsed() < Duration::from_secs(2)
            && !(received_offer.get() && received_claim.get())
        {
            let _ = subscription.poll_once(
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
                },
                1024,
            );
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(received_offer.get(), "did not receive offer_result message");
        assert!(received_claim.get(), "did not receive claim message");
        let (session_id, recv_stream_id) = header_ids.get().expect("no header captured");
        assert_eq!(recv_stream_id, stream_id, "header stream_id should match");
        assert_ne!(session_id, 0, "session_id should be populated");

        assert_eq!(subscription.status(), AeronStatus::Connected);

        // Shutdown
        drop(subscription);
        drop(publisher);
        drop(sub_poller);
        drop(pub_poller);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
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
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:ipc");
        let stream_id: i32 = 9124;
        let pub_poller =
            aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
        let sub_poller = aeron.async_add_subscription(
            &channel.into_c_string(),
            stream_id,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?;

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
        let (commit_pos, claim_pos) =
            committed_pos.expect("try_claim_owned + commit never succeeded");
        assert_eq!(
            commit_pos, claim_pos,
            "commit() return should match claim.position()"
        );

        // Receive the committed message and assert the exact bytes.
        let received = std::cell::Cell::new(false);
        let read_start = Instant::now();
        while read_start.elapsed() < Duration::from_secs(2) && !received.get() {
            let _ = subscription.poll_once(
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
        drop(subscription);
        drop(publisher);
        drop(sub_poller);
        drop(pub_poller);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
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
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:ipc");
        let stream_id: i32 = 9125;
        let pub_poller =
            aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
        let sub_poller = aeron.async_add_subscription(
            &channel.into_c_string(),
            stream_id,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?;

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
            if let Ok(pos) = publisher.offer_result_simple(marker_payload) {
                if pos >= marker_payload.len() as i64 {
                    offered = true;
                }
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(
            offered,
            "offer_result_simple never succeeded after dropped claim"
        );

        // Receive only the marker — the dropped claim should have been aborted.
        let received_dropped = std::cell::Cell::new(false);
        let received_marker = std::cell::Cell::new(false);
        let read_start = Instant::now();
        while read_start.elapsed() < Duration::from_secs(2)
            && !(received_marker.get() || received_dropped.get())
        {
            let _ = subscription.poll_once(
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
        assert!(
            !received_dropped.get(),
            "dropped claim was erroneously delivered"
        );
        assert!(
            received_marker.get(),
            "marker message was not received after dropped claim"
        );

        // Confirm the publication is still usable by offering one more message.
        let final_payload = b"final-after-all";
        let final_start = Instant::now();
        let mut final_offered = false;
        while final_start.elapsed() < Duration::from_secs(2) && !final_offered {
            if let Ok(pos) = publisher.offer_result_simple(final_payload) {
                if pos >= final_payload.len() as i64 {
                    final_offered = true;
                }
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(
            final_offered,
            "publication became unusable after dropped claim"
        );

        assert_eq!(subscription.status(), AeronStatus::Connected);

        // Shutdown
        drop(subscription);
        drop(publisher);
        drop(sub_poller);
        drop(pub_poller);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
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
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:ipc");
        let stream_id: i32 = 9126;
        let pub_poller =
            aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
        let sub_poller = aeron.async_add_subscription(
            &channel.into_c_string(),
            stream_id,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?;

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
            if let Ok(pos) = publisher.offer_result_simple(valid_payload) {
                if pos >= valid_payload.len() as i64 {
                    offered = true;
                }
            }
            #[cfg(debug_assertions)]
            sleep(Duration::from_millis(10));
        }
        assert!(
            offered,
            "publication became unusable after failed try_claim_owned"
        );

        // Shutdown
        drop(_subscription);
        drop(publisher);
        drop(sub_poller);
        drop(pub_poller);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
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
        media_driver_ctx
            .set_dir(&format!("{}alloc-guard", media_driver_ctx.get_dir()).into_c_string())?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:ipc");
        let stream_id: i32 = 7777;
        let pub_poller =
            aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;

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
        let _ = publisher.offer(payload, Handlers::no_reserved_value_supplier_handler());

        // Assert a tight offer loop is allocation-free (publish hot path).
        crate::test_alloc::assert_no_allocation(|| {
            for _ in 0..200 {
                let _ = publisher.offer(payload, Handlers::no_reserved_value_supplier_handler());
            }
        });

        drop(publisher);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
        Ok(())
    }

    #[test]
    #[serial]
    fn async_pub_sub_invalid_endpoint_create_drop_stress() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
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
            let pub_poller =
                aeron.async_add_publication(&channel.clone().into_c_string(), 4500 + i as i32)?;
            let sub_poller = aeron.async_add_subscription(
                &channel.into_c_string(),
                4500 + i as i32,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?;

            let start = Instant::now();
            let mut publication = None;
            let mut publication_done = false;
            let mut subscription = None;
            let mut subscription_done = false;

            while !(publication_done && subscription_done) && start.elapsed() < POLL_TIMEOUT {
                if !publication_done {
                    match pub_poller.poll() {
                        Ok(Some(pub_)) => {
                            publication = Some(pub_);
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
                            subscription = Some(sub_);
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

            drop(subscription);
            drop(publication);
            drop(sub_poller);
            drop(pub_poller);
        }

        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
        Ok(())
    }

    #[test]
    #[serial]
    fn async_subscription_invalid_interface_poll_then_drop() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        // Invalid remote endpoint only (no interface)
        let channel = String::from("aeron:udp?endpoint=203.0.113.1:54323");

        let poller = aeron.async_add_subscription(
            &channel.into_c_string(),
            4323,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?;

        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(250) {
            let _ = poller.poll();
            #[cfg(debug_assertions)]
            std::thread::sleep(Duration::from_millis(10));
        }

        drop(poller);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
        Ok(())
    }

    #[test]
    #[serial]
    fn blocking_add_subscription_invalid_interface_timeout() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let channel = String::from("aeron:udp?endpoint=203.0.113.1:54324");

        let result = aeron.add_subscription(
            &channel.into_c_string(),
            4324,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_millis(300),
        );

        assert!(result.is_err(), "expected error for invalid interface");
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
        Ok(())
    }

    #[test]
    #[serial]
    fn async_publication_invalid_bind_poll_then_drop() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
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
        drop(poller);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
        Ok(())
    }

    #[test]
    #[serial]
    pub fn simple_large_send() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        // Under Valgrind execution is ~10x slower; increase liveness timeouts so the
        // driver doesn't evict the client before the test finishes.
        media_driver_ctx.set_client_liveness_timeout_ns(60_000_000_000)?; // 60 s
        media_driver_ctx.set_image_liveness_timeout_ns(60_000_000_000)?; // 60 s
        media_driver_ctx.set_publication_unblock_timeout_ns(65_000_000_000)?; // 65 s
        media_driver_ctx.set_driver_timeout_ms(60_000)?; // 60 s
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        assert_eq!(media_driver_ctx.get_dir(), ctx.get_dir());
        // Keep client-side keepalive threshold aligned with the slow Valgrind environment.
        ctx.set_driver_timeout_ms(60_000)?;
        // Store all handlers so we can call release() after Aeron is stopped.
        // Anonymous Handler::leak() temporaries would be dropped without release(),
        // triggering the Drop impl warning and leaving heap allocations permanently orphaned.
        let mut error_handler = Handler::leak(ErrorCount::default());
        let mut new_pub_handler = Handler::leak(AeronNewPublicationLogger);
        let mut avail_counter_handler1 = Handler::leak(AeronAvailableCounterLogger);
        let mut close_client_handler = Handler::leak(AeronCloseClientLogger);
        let mut new_sub_handler = Handler::leak(AeronNewSubscriptionLogger);
        let mut unavail_counter_handler = Handler::leak(AeronUnavailableCounterLogger);
        let mut avail_counter_handler2 = Handler::leak(AeronAvailableCounterLogger);
        let mut excl_pub_handler = Handler::leak(AeronNewPublicationLogger);
        ctx.set_error_handler(Some(&error_handler))?;
        ctx.set_on_new_publication(Some(&new_pub_handler))?;
        ctx.set_on_available_counter(Some(&avail_counter_handler1))?;
        ctx.set_on_close_client(Some(&close_client_handler))?;
        ctx.set_on_new_subscription(Some(&new_sub_handler))?;
        ctx.set_on_unavailable_counter(Some(&unavail_counter_handler))?;
        ctx.set_on_available_counter(Some(&avail_counter_handler2))?;
        ctx.set_on_new_exclusive_publication(Some(&excl_pub_handler))?;

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
        assert!(AeronCnc::new_on_heap(ctx.get_dir())?.get_to_driver_heartbeat_ms()? > 0);
        let cstr = std::ffi::CString::new(ctx.get_dir()).unwrap();
        for _ in 0..50 {
            AeronCnc::read_on_partial_stack(&cstr, |cnc| {
                assert!(cnc.get_to_driver_heartbeat_ms().unwrap() > 0);
            })?;
        }

        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;
        info!("created subscription");

        subscription
            .poll_once(|msg, header| println!("foo"), 1024)
            .unwrap();

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
                    let result =
                        publisher.offer(large_msg, Handlers::no_reserved_value_supplier_handler());

                    assert_eq!(123, publisher.get_constants().unwrap().stream_id);

                    if result < large_msg.len() as i64 {
                        let error = AeronCError::from_code(result as i32);
                        match error.kind() {
                            AeronErrorType::PublicationBackPressured
                            | AeronErrorType::PublicationAdminAction => {
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

            subscription.poll(assembler.process(&mut context, process_msg), 128)?;
            assert_eq!(123, subscription.get_constants().unwrap().stream_id);
        };

        subscription.close(Handlers::no_notification_handler())?;

        info!("stopping client");
        stop_publisher.store(true, Ordering::SeqCst);

        let _ = publisher_handler.join().unwrap();
        drop(subscription);
        drop(aeron);

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();

        // Release all context handlers now that Aeron and the driver are fully stopped.
        error_handler.release();
        new_pub_handler.release();
        avail_counter_handler1.release();
        close_client_handler.release();
        new_sub_handler.release();
        unavail_counter_handler.release();
        avail_counter_handler2.release();
        excl_pub_handler.release();

        let cnc = AeronCnc::new_on_heap(ctx.get_dir())?;
        cnc.counters_reader().foreach_counter_once(
            |value: i64, id: i32, type_id: i32, key: &[u8], label: &str| {
                println!("counter reader id={id}, type_id={type_id}, key={key:?}, label={label}, value={value} [type={:?}]",
                AeronSystemCounterType::try_from(type_id));
            },
        );
        cnc.error_log_read_once(| observation_count: i32,
                                     first_observation_timestamp: i64,
                                     last_observation_timestamp: i64,
                                     error: &str| {
            println!("error: {error} observationCount={observation_count}, first_observation_timestamp={first_observation_timestamp}, last_observation_timestamp={last_observation_timestamp}");
        }, 0);
        cnc.loss_reporter_read_once(|    observation_count: i64,
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
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        // Under Valgrind execution is ~10x slower; increase liveness timeouts so the
        // driver doesn't evict the client before the test finishes.
        media_driver_ctx.set_client_liveness_timeout_ns(60_000_000_000)?; // 60 s
        media_driver_ctx.set_image_liveness_timeout_ns(60_000_000_000)?; // 60 s
        media_driver_ctx.set_publication_unblock_timeout_ns(65_000_000_000)?; // 65 s
        media_driver_ctx.set_driver_timeout_ms(60_000)?; // 60 s
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        assert_eq!(media_driver_ctx.get_dir(), ctx.get_dir());
        // Keep client-side keepalive threshold aligned with slow Valgrind environment.
        ctx.set_driver_timeout_ms(60_000)?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;

        info!("creating client [try_claim test]");
        let aeron = Aeron::new(&ctx)?;
        info!("starting client");

        aeron.start()?;
        info!("client started");
        const STREAM_ID: i32 = 123;
        let publisher =
            aeron.add_publication(AERON_IPC_STREAM, STREAM_ID, Duration::from_secs(5))?;
        info!("created publisher");

        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
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

                    let result = publisher.try_claim(string_len, &buffer);

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

        let (mut closure, mut inner_handler) =
            Handler::leak_with_fragment_assembler(FragmentHandler {
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
        drop(subscription);
        drop(aeron);

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        closure.release();
        inner_handler.release();
        error_handler.release();
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
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        assert_eq!(media_driver_ctx.get_dir(), ctx.get_dir());
        let mut error_handler = Handler::leak(ErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;
        let mut unavailable_counter_handler = Handler::leak(AeronUnavailableCounterLogger);
        ctx.set_on_unavailable_counter(Some(&unavailable_counter_handler))?;

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
                        assert_eq!(
                            &counters_reader.get_counter_key(counter_id).unwrap(),
                            "key".as_bytes()
                        );
                    }
                }
            }
        }

        let mut available_counter_handler = Handler::leak(AvailableCounterHandler {
            found_counter: false,
        });
        ctx.set_on_available_counter(Some(&available_counter_handler))?;

        info!("creating client");
        let aeron = Aeron::new(&ctx)?;
        info!("starting client");

        aeron.start()?;
        info!("client started [counters test]");

        let counter = aeron.add_counter(
            123,
            "key".as_bytes(),
            "label_buffer",
            Duration::from_secs(5),
        )?;
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
        while counter.addr_atomic().load(Ordering::SeqCst) < 100
            && now.elapsed() < Duration::from_secs(10)
        {
            sleep(Duration::from_micros(10));
        }

        assert!(now.elapsed() < Duration::from_secs(10));

        info!(
            "counter is {}",
            counter.addr_atomic().load(Ordering::SeqCst)
        );

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
        drop(counter);
        drop(aeron);

        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        available_counter_handler.release();
        unavailable_counter_handler.release();
        error_handler.release();
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
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        media_driver_ctx.set_client_liveness_timeout_ns(liveness_timeout_ns)?;
        media_driver_ctx.set_image_liveness_timeout_ns(liveness_timeout_ns)?;
        media_driver_ctx.set_publication_unblock_timeout_ns(liveness_timeout_ns + 5_000_000_000)?;
        media_driver_ctx.set_driver_timeout_ms(driver_timeout_ms)?;
        let (stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        ctx.set_driver_timeout_ms(driver_timeout_ms)?;
        let mut error_handler = Handler::leak(TestErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
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
                    let result =
                        publisher.offer(msg, Handlers::no_reserved_value_supplier_handler());
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

        // Poll using the inline closure via poll_once until we receive at least 50 messages.
        while count.load(Ordering::SeqCst) < 50 && start_time.elapsed() < poll_timeout {
            let _ = subscription.poll_once(
                |_msg, _header| {
                    count.fetch_add(1, Ordering::SeqCst);
                },
                128,
            )?;
        }

        stop_publisher.store(true, Ordering::SeqCst);
        publisher_thread.join().unwrap();
        drop(subscription);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();

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
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        media_driver_ctx.set_client_liveness_timeout_ns(60_000_000_000)?;
        media_driver_ctx.set_image_liveness_timeout_ns(60_000_000_000)?;
        media_driver_ctx.set_publication_unblock_timeout_ns(65_000_000_000)?;
        media_driver_ctx.set_driver_timeout_ms(60_000)?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(TestErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;

        // Create two subscriptions on the same channel.
        let subscription1 = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;
        let subscription2 = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;

        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        // Publish a single message.
        let msg = b"hello multi-subscription";
        let result = publisher.offer(msg, Handlers::no_reserved_value_supplier_handler());
        assert!(
            result >= msg.len() as i64,
            "Message should be sent successfully"
        );

        let start_time = Instant::now();
        // Poll both subscriptions with inline closures until each has received at least one message.
        while (count1.load(Ordering::SeqCst) < 1 || count2.load(Ordering::SeqCst) < 1)
            && start_time.elapsed() < Duration::from_secs(5)
        {
            let _ = subscription1.poll_once(
                |_msg, _header| {
                    count1.fetch_add(1, Ordering::SeqCst);
                },
                128,
            )?;
            let _ = subscription2.poll_once(
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
        error_handler.release();
        Ok(())
    }

    #[test]
    #[serial]
    pub fn should_be_able_to_drop_after_close_manually_being_closed(
    ) -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(AeronErrorHandlerLogger);
        ctx.set_error_handler(Some(&error_handler))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;

        {
            let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
            info!("created publication [sessionId={}]", publisher.session_id());
            publisher.close_with_no_args()?;
            drop(publisher);
        }

        {
            let publisher = aeron.add_publication(AERON_IPC_STREAM, 124, Duration::from_secs(5))?;
            info!("created publication [sessionId={}]", publisher.session_id());
            publisher.close(Handlers::no_notification_handler())?;
            drop(publisher);
        }

        {
            let publisher = aeron.add_publication(AERON_IPC_STREAM, 125, Duration::from_secs(5))?;
            publisher.close_once(|| println!("on close"))?;
            info!("created publication [sessionId={}]", publisher.session_id());
            drop(publisher);
        }

        drop(aeron);
        _stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
        Ok(())
    }

    #[test]
    #[serial]
    pub fn offer_on_closed_publication_error_test() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(TestErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;

        // Close the publication immediately.
        publisher.close(Handlers::no_notification_handler())?;

        // Attempt to send a message after the publication is closed.
        let result = publisher.offer(
            b"should fail",
            Handlers::no_reserved_value_supplier_handler(),
        );
        assert!(
            result < 0,
            "Offering on a closed publication should return a negative error code"
        );

        drop(publisher);
        drop(aeron);
        _stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
        Ok(())
    }

    /// Test sending and receiving an empty (zero-length) message using inline closures with poll_once.
    #[test]
    #[serial]
    pub fn empty_message_test() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        let media_driver_ctx = rusteron_media_driver::AeronDriverContext::new()?;
        media_driver_ctx.set_dir_delete_on_shutdown(true)?;
        media_driver_ctx.set_dir_delete_on_start(true)?;
        media_driver_ctx.set_dir(
            &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
        )?;
        let (_stop, driver_handle) =
            rusteron_media_driver::AeronDriver::launch_embedded(media_driver_ctx.clone(), false);

        let ctx = AeronContext::new()?;
        ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
        let mut error_handler = Handler::leak(TestErrorCount::default());
        ctx.set_error_handler(Some(&error_handler))?;

        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        let publisher = aeron.add_publication(AERON_IPC_STREAM, 123, Duration::from_secs(5))?;
        let subscription = aeron.add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;

        let empty_received = Arc::new(AtomicBool::new(false));
        let start_time = Instant::now();

        let result = publisher.offer(b"", Handlers::no_reserved_value_supplier_handler());
        assert!(result > 0);

        while !empty_received.load(Ordering::SeqCst)
            && start_time.elapsed() < Duration::from_secs(5)
        {
            let _ = subscription.poll_once(
                |msg, _header| {
                    if msg.is_empty() {
                        empty_received.store(true, Ordering::SeqCst);
                    }
                },
                128,
            )?;
        }

        assert!(
            empty_received.load(Ordering::SeqCst),
            "Empty message was not received"
        );
        drop(subscription);
        drop(publisher);
        drop(aeron);
        _stop.store(true, Ordering::SeqCst);
        let _ = driver_handle.join().unwrap();
        error_handler.release();
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

        fn print_and_reset(
            &mut self,
            window_number: usize,
            interval: Duration,
            totals: &mut MdcTotals,
        ) {
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

        let publication_channel = format!(
            "aeron:udp?control-mode=manual|control={}:{CONTROL_PORT}",
            mdc_host
        );
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
            let (_ctx, aeron) = create_client_for_dir(&subscriber_dir)
                .expect("failed to create subscriber aeron client");

            let subscription = aeron
                .add_subscription(
                    &subscriber_channel.into_c_string(),
                    STREAM_ID,
                    Handlers::no_available_image_handler(),
                    Handlers::no_unavailable_image_handler(),
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
                    .poll_once(
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
            let (_ctx, aeron) = create_client_for_dir(&publisher_dir)
                .expect("failed to create publisher aeron client");

            let publication = aeron
                .add_exclusive_publication(
                    &publisher_channel.into_c_string(),
                    STREAM_ID,
                    Duration::from_secs(5),
                )
                .expect("failed to create publication");

            let add_destination =
                AeronAsyncDestination::aeron_exclusive_publication_async_add_destination(
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

                let result =
                    publication.offer(&payload, Handlers::no_reserved_value_supplier_handler());
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
        assert!(
            totals.received_messages > 0,
            "subscriber did not receive any messages"
        );
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
                &"aeron:udp?tags=100".into_c_string(),
                123,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
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
            &"aeron:udp?tags=100".into_c_string(),
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(50),
        )?;

        let (_a_ctx1, aeron_pub) = create_client(&md_ctx)?;
        info!("creating publisher");
        assert!(!aeron_pub.is_closed());
        let publisher = aeron_pub
            .add_publication(
                &"aeron:udp?endpoint=localhost:4040|alias=test|tags=100".into_c_string(),
                123,
                Duration::from_secs(5),
            )
            .map_err(|e| {
                error!("aeron error={}", Aeron::errmsg());
                e
            })?;

        info!("publishing msg");

        loop {
            let result = publisher.offer(
                "213".as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            );
            if result < 0 {
                error!(
                    "failed to publish {:?}",
                    AeronCError::from_code(result as i32)
                );
            } else {
                break;
            }
        }

        sub.poll_once(
            |msg, _header| {
                println!("Received message: {:?}", msg);
            },
            128,
        )?;
        sub2.poll_once(
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

    fn create_client(
        media_driver_ctx: &AeronDriverContext,
    ) -> Result<(AeronContext, Aeron), Box<dyn Error>> {
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
        media_driver_ctx.set_dir(
            &format!(
                "{}{}-{}",
                media_driver_ctx.get_dir(),
                Aeron::epoch_clock(),
                instance
            )
            .into_c_string(),
        )?;
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
        let exclusive =
            aeron.add_exclusive_publication(AERON_IPC_STREAM, stream_id, Duration::from_secs(5))?;

        // Add a matching subscription so the IPC image connects and offers succeed.
        let sub_poller = aeron.async_add_subscription(
            &"aeron:ipc".to_string().into_c_string(),
            stream_id,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
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
            if let Ok(pos) = exclusive.offer_result_simple(payload) {
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
        assert!(
            committed,
            "exclusive try_claim_owned + commit never succeeded"
        );

        // 3) Generated constants accessors return sane, consistent values.
        let constants = exclusive.get_constants().expect("publication constants");
        assert_eq!(constants.stream_id, stream_id);
        // channel is a *const c_char into the C struct; just assert it's a valid C string.
        assert!(!constants.channel.is_null());
        assert_eq!(
            unsafe { std::ffi::CStr::from_ptr(constants.channel) }
                .to_str()
                .unwrap(),
            "aeron:ipc"
        );
        assert!(constants.max_possible_position > 0);
        assert!(constants.term_buffer_length > 0);
        assert!(constants.max_message_length > 0);
        assert!(constants.max_payload_length > 0);
        // position() advances with the offers.
        assert!(exclusive.position() >= pos);

        drop(exclusive);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// C2: property tests for the pure-Rust (no driver) logic. Fast and
    /// deterministic — they fuzz the invariants the unit tests only sample.
    mod property_tests {
        use crate::{
            publication_position_to_result, validate_endpoint_for_aeron_udp, AeronCError,
            AeronErrorType, AeronStatus, AeronStatusTracker,
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

        /// `position_to_result`: non-negative → Ok(position); negative → Err whose
        /// code is preserved. `from_code` round-trips every known code and keeps
        /// the code for unknown negatives.
        #[test]
        fn position_to_result_and_error_codes_are_consistent() {
            proptest!(|(pos in -1_000_000i64..=1_000_000)| {
                match publication_position_to_result(pos) {
                    Ok(p) => prop_assert!(p >= 0 && p == pos),
                    Err(e) => {
                        prop_assert!(pos < 0);
                        prop_assert_eq!(e.code, pos as i32);
                        // from_code round-trips the code.
                        prop_assert_eq!(AeronErrorType::from_code(e.code).code(), e.code);
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

        /// Tests the `for_each_fragment` ergonomic helper on `AeronSubscription`.
        /// Verifies that the new helper receives all published messages without
        /// allocations on the hot path.
        #[test]
        #[serial]
        fn for_each_fragment_receives_all_messages_no_alloc() -> Result<(), Box<dyn error::Error>> {
            rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

            let media_driver_ctx = AeronDriverContext::new()?;
            media_driver_ctx.set_dir_delete_on_shutdown(true)?;
            media_driver_ctx.set_dir_delete_on_start(true)?;
            media_driver_ctx.set_dir(
                &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
            )?;
            let (stop, driver_handle) = rusteron_media_driver::AeronDriver::launch_embedded(
                media_driver_ctx.clone(),
                false,
            );

            let ctx = AeronContext::new()?;
            ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
            let mut error_handler = Handler::leak(ErrorCount::default());
            ctx.set_error_handler(Some(&error_handler))?;
            let aeron = Aeron::new(&ctx)?;
            aeron.start()?;

            let channel = String::from("aeron:ipc");
            let stream_id: i32 = 9999;

            let pub_poller =
                aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
            let sub_poller = aeron.async_add_subscription(
                &channel.into_c_string(),
                stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?;

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
                    if let Ok(pos) = publisher.offer_result_simple(payload) {
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

            // Use for_each_fragment to receive all messages (spin-poll pattern)
            let mut received_count = 0;
            let max_iterations = 1000;

            for _ in 0..max_iterations {
                let fragments = subscription.for_each_fragment(1024, |data, _header| {
                    received_count += 1;
                    info!("Received fragment {} bytes", data.len());
                })?;

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
                    let _ = subscription.for_each_fragment(1024, |data, _header| {
                        alloc_free_count += 1;
                    });
                }
            };

            assert_no_allocation(spin_alloc_test);

            let alloc_after = current_allocs();
            assert!(
                (alloc_after - alloc_before).abs() < 10,
                "Expected no net allocation in for_each_fragment hot path"
            );

            // Cleanup
            drop(sub_poller);
            drop(pub_poller);
            drop(aeron);
            stop.store(true, Ordering::SeqCst);
            let _ = driver_handle.join().unwrap();
            error_handler.release();

            Ok(())
        }
    }

    mod latency_harness_tests {
        use super::*;
        use crate::test_alloc::assert_no_allocation;
        use serial_test::serial;

        /// `LatencyStats` records samples and estimates percentiles, all
        /// allocation-free.
        #[test]
        fn latency_stats_records_and_percentiles() {
            assert_no_allocation(|| {
                let mut stats = LatencyStats::new();
                for ns in [10u64, 20, 100, 1000, 10_000] {
                    stats.record(ns);
                }
                assert_eq!(stats.count(), 5);
                assert_eq!(stats.min_ns(), 10);
                assert_eq!(stats.max_ns(), 10_000);
                let p50 = stats.p50();
                assert!(p50 >= 10 && p50 <= 10_000, "p50 out of range: {p50}");
                assert!(stats.p99() >= p50, "p99 should be >= p50");
            });
        }

        /// End-to-end: `offer_timestamped` stamps a send time; the receiver
        /// measures latency via `AeronHeader::reserved_value` into `LatencyStats`.
        #[test]
        #[serial]
        fn offer_timestamped_round_trip_is_measured() -> Result<(), Box<dyn error::Error>> {
            rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

            let media_driver_ctx = AeronDriverContext::new()?;
            media_driver_ctx.set_dir_delete_on_shutdown(true)?;
            media_driver_ctx.set_dir_delete_on_start(true)?;
            media_driver_ctx.set_dir(
                &format!("{}{}", media_driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string(),
            )?;
            let (stop, driver_handle) = rusteron_media_driver::AeronDriver::launch_embedded(
                media_driver_ctx.clone(),
                false,
            );

            let ctx = AeronContext::new()?;
            ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;
            let mut error_handler = Handler::leak(ErrorCount::default());
            ctx.set_error_handler(Some(&error_handler))?;
            let aeron = Aeron::new(&ctx)?;
            aeron.start()?;

            let channel = String::from("aeron:ipc");
            let stream_id: i32 = 7700;
            let pub_poller =
                aeron.async_add_publication(&channel.clone().into_c_string(), stream_id)?;
            let sub_poller = aeron.async_add_subscription(
                &channel.into_c_string(),
                stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?;

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
            let conn_start = Instant::now();
            while !publisher.is_connected() && conn_start.elapsed() < Duration::from_secs(2) {
                #[cfg(debug_assertions)]
                sleep(Duration::from_millis(10));
            }

            // Publish timestamped messages.
            let payload = b"latency-probe";
            let mut offered = 0;
            let offer_start = Instant::now();
            while offered < 50 && offer_start.elapsed() < Duration::from_secs(2) {
                if publisher.offer_timestamped(payload)? >= 0 {
                    offered += 1;
                }
                #[cfg(debug_assertions)]
                sleep(Duration::from_millis(1));
            }
            assert!(offered > 0, "never offered any timestamped messages");

            // Poll with the latency recorder until a sample lands.
            let mut stats = LatencyStats::new();
            let poll_start = Instant::now();
            while stats.count() == 0 && poll_start.elapsed() < Duration::from_secs(2) {
                subscription.for_each_fragment(1024, latency_recorder(&mut stats))?;
                #[cfg(debug_assertions)]
                sleep(Duration::from_micros(200));
            }
            assert!(stats.count() > 0, "no latency samples recorded");
            // IPC on the same host: latency must be non-negative and sub-second.
            assert!(
                stats.min_ns() < 1_000_000_000,
                "min latency implausibly large: {} ns",
                stats.min_ns()
            );

            drop(subscription);
            drop(publisher);
            drop(sub_poller);
            drop(pub_poller);
            drop(aeron);
            stop.store(true, Ordering::SeqCst);
            let _ = driver_handle.join().unwrap();
            error_handler.release();
            Ok(())
        }
    }
}

#[cfg(test)]
mod idle_strategy_tests {
    use super::*;

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
        let mut s =
            BackoffIdleStrategy::with(2, 2, Duration::from_micros(1), Duration::from_millis(1));
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
