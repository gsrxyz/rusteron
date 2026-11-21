//! Allow 50 milliseconds for a missing tagged receiver before healthy subscribers can advance the sender.
//! Run with `cargo run -p rusteron-media-driver --features static --example bounded_flow_control`.
//! Connect clients to the printed directory. Press Enter to stop.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

// Omit fc= in channel URIs so Aeron uses the installed supplier.

/// Install this extension before creating the driver.
pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    context.set_flow_control_group_tag(555)?;
    context.set_flow_control_group_min_size(1)?;
    context.set_unicast_flow_control::<BoundedTaggedFlowControl<50_000_000>>()?;
    context.set_multicast_flow_control::<BoundedTaggedFlowControl<50_000_000>>()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    Ok(())
}
