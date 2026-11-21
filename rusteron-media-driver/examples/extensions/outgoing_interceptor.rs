//! Count outgoing batches and forward each batch once without changing its buffers.
//! Run with `cargo run -p rusteron-media-driver --features static --example outgoing_interceptor`.
//! Connect clients to the printed directory. Press Enter to stop.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

use std::sync::atomic::{AtomicU64, Ordering};
pub static OUTGOING_BATCHES: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct ObserveOutgoing;
impl OutgoingInterceptor for ObserveOutgoing {
    fn inspect(&mut self, _: &OutgoingPacket<'_>) {
        OUTGOING_BATCHES.fetch_add(1, Ordering::Relaxed);
    }
}

/// Install this extension before creating the driver.
pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    context.add_outgoing_interceptor::<ObserveOutgoing>()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    println!("Outgoing batches: {}", OUTGOING_BATCHES.load(Ordering::Relaxed));
    Ok(())
}
