//! Negative control for the ThreadSanitizer job. Ordinary test runs skip it.
use rusteron_media_driver::extensions::{IdleStrategy, IdleStrategyRole};
use rusteron_media_driver::{AeronDriver, AeronDriverContext, AeronThreadingMode, IntoCString};
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;

struct DeliberateRace(UnsafeCell<u64>);

// Intentionally unsound. Only the ignored negative control uses this type.
// TSan must reject its shared mutable state inside a Rust callback from C.
unsafe impl Sync for DeliberateRace {}

impl DeliberateRace {
    #[inline(never)]
    fn tsan_rust_callback_write(&self) {
        // Deliberate data race for TSan's negative control, guarded by the test's
        // sanitizer-build assertion. Never copy this into an extension.
        unsafe {
            let value = self.0.get().read_volatile();
            self.0.get().write_volatile(value.wrapping_add(1));
        }
    }
}

struct RacyIdle(Arc<DeliberateRace>);
impl IdleStrategy for RacyIdle {
    const BUSY_SPIN: bool = true;

    fn idle(&mut self, _: i32) {
        self.0.tsan_rust_callback_write();
    }
}

#[test]
#[ignore = "deliberately races; scripts/test-tsan.sh requires a TSan report"]
fn rust_callback_race_is_detected() {
    assert_eq!(option_env!("RUSTERON_SANITIZE"), Some("thread"));
    let context = AeronDriverContext::new().unwrap();
    let directory = std::env::temp_dir().join(format!("rusteron-tsan-negative-{}", std::process::id()));
    context.set_dir(&directory.to_string_lossy().into_c_string()).unwrap();
    context.set_dir_delete_on_start(true).unwrap();
    context.set_dir_delete_on_shutdown(true).unwrap();
    context.set_threading_mode(AeronThreadingMode::Dedicated).unwrap();
    let shared = Arc::new(DeliberateRace(UnsafeCell::new(0)));
    for role in [IdleStrategyRole::Sender, IdleStrategyRole::Receiver] {
        context.set_idle_strategy(role, RacyIdle(shared.clone())).unwrap();
    }
    let driver = AeronDriver::new(&context).unwrap();
    driver.start(false).unwrap();
    std::thread::sleep(Duration::from_millis(250));
    drop(driver);
    panic!("ThreadSanitizer failed to report the deliberate callback race");
}

#[test]
#[ignore = "deliberately races; scripts/test-tsan.sh requires a TSan report"]
fn native_race_is_detected() {
    assert_eq!(option_env!("RUSTERON_SANITIZE"), Some("thread"));
    let context = AeronDriverContext::new().unwrap();
    let address = context.get_inner() as usize;
    let start = Barrier::new(2);
    std::thread::scope(|scope| {
        for value in [true, false] {
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for _ in 0..10_000 {
                    // Deliberate C data race. The context stays allocated through the join.
                    // Safe context handles forbid this sharing; the negative control bypasses it.
                    unsafe {
                        rusteron_media_driver::bindings::aeron_driver_context_set_dir_delete_on_shutdown(
                            address as *mut _,
                            value,
                        );
                    }
                }
            });
        }
    });
    panic!("ThreadSanitizer failed to report the deliberate native race");
}
