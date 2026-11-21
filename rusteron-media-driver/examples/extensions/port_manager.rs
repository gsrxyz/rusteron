//! Preserve requested ports, including zero for kernel allocation. Count reservations and releases.
//! Run with `cargo run -p rusteron-media-driver --features static --example port_manager`.
//! Connect clients to the printed directory. Press Enter to stop.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};
pub static PORT_REQUESTS: AtomicU64 = AtomicU64::new(0);
pub static PORT_RELEASES: AtomicU64 = AtomicU64::new(0);

/// Preserve explicit ports and let the kernel allocate wildcard ports.
struct KernelPorts;
impl PortManager for KernelPorts {
    fn get_managed_port(&mut self, _: ChannelRef<'_>, requested: SocketAddr) -> Result<u16, AeronCError> {
        PORT_REQUESTS.fetch_add(1, Ordering::Relaxed);
        Ok(requested.port())
    }
    fn free_managed_port(&mut self, _: SocketAddr) {
        PORT_RELEASES.fetch_add(1, Ordering::Relaxed);
    }
}

/// Install this extension before creating the driver.
pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    context.set_port_manager(PortManagerRole::Sender, KernelPorts)?;
    context.set_port_manager(PortManagerRole::Receiver, KernelPorts)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    println!(
        "Port requests: {}. Releases: {}.",
        PORT_REQUESTS.load(Ordering::Relaxed),
        PORT_RELEASES.load(Ordering::Relaxed)
    );
    Ok(())
}
