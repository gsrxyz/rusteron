//! Keep the longest measured duty cycle in nanoseconds. Each installed timer has its own baseline.
//! Run with `cargo run -p rusteron-media-driver --features static --example duty_cycle_tracker`.
//! Connect clients to the printed directory. Press Enter to stop.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

use std::sync::atomic::{AtomicU64, Ordering};
pub static MAX_CYCLE_NS: AtomicU64 = AtomicU64::new(0);

/// Install this extension before creating the driver.
pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    for role in [
        DutyCycleRole::Conductor,
        DutyCycleRole::Sender,
        DutyCycleRole::Receiver,
        DutyCycleRole::NameResolver,
    ] {
        context.set_duty_cycle_tracker(
            role,
            DutyCycleTimer::new(|elapsed_ns| {
                MAX_CYCLE_NS.fetch_max(elapsed_ns as u64, Ordering::Relaxed);
            }),
        )?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    println!(
        "Longest duty cycle in nanoseconds: {}",
        MAX_CYCLE_NS.load(Ordering::Relaxed)
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[serial_test::serial(driver_extensions)]
    fn every_role_records_a_maximum_without_adding_durations_together() {
        let context = AeronDriverContext::new().unwrap();
        configure(&context).unwrap();
        // SAFETY: no driver has started. Each installed callback owns its state
        // until this local context closes, and these calls do not overlap.
        unsafe {
            let raw = &*context.get_inner();
            for tracker in [
                raw.conductor_duty_cycle_tracker,
                raw.sender_duty_cycle_tracker,
                raw.receiver_duty_cycle_tracker,
                raw.name_resolver_time_tracker,
            ] {
                MAX_CYCLE_NS.store(0, Ordering::Relaxed);
                let tracker = &*tracker;
                tracker.update.unwrap()(tracker.state, 10);
                tracker.measure_and_update.unwrap()(tracker.state, 30);
                assert_eq!(MAX_CYCLE_NS.load(Ordering::Relaxed), 20);
                tracker.measure_and_update.unwrap()(tracker.state, 45);
                assert_eq!(MAX_CYCLE_NS.load(Ordering::Relaxed), 20);
                tracker.measure_and_update.unwrap()(tracker.state, 100);
                assert_eq!(MAX_CYCLE_NS.load(Ordering::Relaxed), 55);
            }
        }
    }
}
