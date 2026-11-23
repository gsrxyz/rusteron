//! Spin for 100 empty cycles, then yield. Reset the spin budget when work arrives.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

use std::sync::atomic::{AtomicU64, Ordering};
pub static IDLE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Spin for a bounded number of empty cycles, then yield. Work resets the count.
#[derive(Default)]
struct SpinThenYield {
    empty_cycles: u32,
}
impl IdleStrategy for SpinThenYield {
    fn idle(&mut self, work: i32) {
        IDLE_CALLS.fetch_add(1, Ordering::Relaxed);
        if work > 0 {
            self.empty_cycles = 0;
        } else if self.empty_cycles < 100 {
            self.empty_cycles += 1;
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
    }
}

pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    for role in [
        IdleStrategyRole::Conductor,
        IdleStrategyRole::Sender,
        IdleStrategyRole::Receiver,
        IdleStrategyRole::SharedNetwork,
        IdleStrategyRole::Shared,
        IdleStrategyRole::NativeResourceAgent,
    ] {
        context.set_idle_strategy(role, SpinThenYield::default())?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    println!("Idle calls: {}", IDLE_CALLS.load(Ordering::Relaxed));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[serial_test::serial(driver_extensions)]
    fn spins_for_100_empty_cycles_and_resets_when_work_arrives() {
        let mut idle = SpinThenYield::default();
        for _ in 0..101 {
            idle.idle(0);
        }
        assert_eq!(idle.empty_cycles, 100);
        idle.idle(1);
        assert_eq!(idle.empty_cycles, 0);
        idle.idle(-1);
        assert_eq!(idle.empty_cycles, 1);
    }
}
