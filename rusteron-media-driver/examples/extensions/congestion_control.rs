//! Use a fixed receiver window capped at half the term length. Do not sample round-trip times.
//! Run with `cargo run -p rusteron-media-driver --features static --example congestion_control`.
//! Connect clients to the printed directory. Press Enter to stop.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

use std::sync::atomic::{AtomicU64, Ordering};
pub static REBUILDS: AtomicU64 = AtomicU64::new(0);

/// A fixed receiver window without round-trip time sampling.
struct FixedWindow;
impl CongestionControl for FixedWindow {
    type Receiver = ();
    type Conductor = ReceiverWindow;
    type Shared = ();
    fn new(args: &CongestionControlArgs<'_>) -> Result<((), ReceiverWindow, ()), AeronCError> {
        let window = args.context.initial_receiver_window.min(args.term_length as usize / 2);
        Ok(((), ReceiverWindow::new(window as i32), ()))
    }
    fn initial_window_length(window: &mut ReceiverWindow, _: &()) -> ReceiverWindow {
        *window
    }
    fn max_window_length(window: &mut ReceiverWindow, _: &()) -> ReceiverWindow {
        *window
    }
    fn on_track_rebuild(window: &mut ReceiverWindow, _: &(), _: TrackRebuild, force: &mut bool) -> ReceiverWindow {
        REBUILDS.fetch_add(1, Ordering::Relaxed);
        *force = false;
        *window
    }
}

/// Install this extension before creating the driver.
pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    context.set_congestion_control::<FixedWindow>()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    println!("Rebuild callbacks: {}", REBUILDS.load(Ordering::Relaxed));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusteron_media_driver::bindings::*;

    #[test]
    #[serial_test::serial(driver_extensions)]
    fn window_stays_fixed_and_never_exceeds_half_the_term() {
        let context = AeronDriverContext::new().unwrap();
        context.set_rcv_initial_window_length(8192).unwrap();
        configure(&context).unwrap();
        let mut channel = aeron_udp_channel_t::default();
        let mut control = sockaddr_storage::default();
        let mut source = sockaddr_storage::default();
        for (term_length, expected) in [(4096, 2048), (65536, 8192)] {
            let mut raw = std::ptr::null_mut();
            // SAFETY: all inputs are live local storage. Finalization releases
            // the strategy before the context or its borrowed arguments drop.
            unsafe {
                let supplier = aeron_driver_context_get_congestioncontrol_supplier(context.get_inner()).unwrap();
                assert_eq!(
                    supplier(
                        &mut raw,
                        &mut channel,
                        1,
                        2,
                        3,
                        term_length,
                        1408,
                        &mut control,
                        &mut source,
                        context.get_inner(),
                        std::ptr::null_mut()
                    ),
                    0
                );
                let table = &*raw;
                assert_eq!(table.initial_window_length.unwrap()(table.state), expected);
                assert_eq!(table.max_window_length.unwrap()(table.state), expected);
                for loss_occurred in [false, true] {
                    let mut force = true;
                    assert_eq!(
                        table.on_track_rebuild.unwrap()(table.state, &mut force, 1, 2, 3, 4, 5, 6, loss_occurred),
                        expected
                    );
                    assert!(!force);
                }
                assert!(!table.should_measure_rtt.unwrap()(table.state, 0));
                assert_eq!(table.fini.unwrap()(raw), 0);
            }
        }
    }
}
