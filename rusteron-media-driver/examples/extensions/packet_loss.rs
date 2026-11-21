//! Drop every seventh outgoing batch after a fixed seeded offset. Use only for retransmission tests.
//! Run with `cargo run -p rusteron-media-driver --features static --example packet_loss`.
//! Connect clients to the printed directory. Press Enter to stop.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

/// Install this extension before creating the driver.
pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    context.add_outgoing_interceptor::<SeededLoss<7, 3>>()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    Ok(())
}
