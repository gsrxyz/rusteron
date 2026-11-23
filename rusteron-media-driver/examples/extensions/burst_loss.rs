//! Apply seeded burst loss to one UDP destination. Other destinations pass unchanged.
//! Run with `cargo run -p rusteron-media-driver --features static --example burst_loss -- 127.0.0.1:40123`.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};
use std::net::SocketAddr;

pub fn configure(context: &AeronDriverContext, destination: SocketAddr) -> Result<(), AeronCError> {
    // Probabilities are parts per million. Bad periods last four batches on
    // average, with 60% loss. Good periods have 2% loss.
    let config = GilbertElliottConfig {
        seed: 7,
        good_to_bad: 20_000,
        bad_to_good: 250_000,
        good_loss: 20_000,
        bad_loss: 600_000,
    };
    context.add_outgoing_interceptor_with_state::<DestinationLoss<GilbertElliottLoss>>((destination, config))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let destination = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:40123".into())
        .parse()?;
    let driver =
        rusteron_media_driver::testing::EmbeddedDriver::launch_with(|context| configure(context, destination))?;
    println!("Driver directory: {}", driver.dir());
    println!("Burst loss applies to {destination}. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    Ok(())
}
