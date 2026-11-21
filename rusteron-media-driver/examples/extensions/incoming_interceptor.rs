//! Count received packets and forward each packet once without changing its bytes.
//! Run with `cargo run -p rusteron-media-driver --features static --example incoming_interceptor`.
//! Connect clients to the printed directory. Press Enter to stop.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

use std::sync::atomic::{AtomicU64, Ordering};
pub static INCOMING_PACKETS: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct ObserveIncoming;
impl IncomingInterceptor for ObserveIncoming {
    fn inspect(&mut self, _: &IncomingPacket<'_>) {
        INCOMING_PACKETS.fetch_add(1, Ordering::Relaxed);
    }
}

/// Install this extension before creating the driver.
pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    context.add_incoming_interceptor::<ObserveIncoming>()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    println!("Incoming packets: {}", INCOMING_PACKETS.load(Ordering::Relaxed));
    Ok(())
}
