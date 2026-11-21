//! Count native UDP sends and forward the unchanged arguments to Aeron. Other callbacks stay native.
//! Run with `cargo run -p rusteron-media-driver --features static --example native_transport`.
//! Connect clients to the printed directory. Press Enter to stop.

use rusteron_media_driver::{AeronCError, AeronDriverContext, bindings::*, extensions::*};

use std::sync::atomic::{AtomicU64, Ordering};
pub static TRANSPORT_SENDS: AtomicU64 = AtomicU64::new(0);

unsafe extern "C" fn observed_send(
    paths: *mut aeron_udp_channel_data_paths_t,
    transport: *mut aeron_udp_channel_transport_t,
    address: *mut sockaddr_storage,
    vectors: *mut iovec,
    length: usize,
    bytes: *mut i64,
) -> i32 {
    TRANSPORT_SENDS.fetch_add(1, Ordering::Relaxed);
    // SAFETY: forward every argument unchanged under the native callback contract.
    unsafe { aeron_udp_channel_transport_send(paths, transport, address, vectors, length, bytes) }
}

/// Install this extension before creating the driver.
pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    let mut callbacks = NativeTransportBindings::udp().callbacks();
    callbacks.send_func = Some(observed_send);
    // SAFETY: the replacement forwards the native ABI unchanged and uses only
    // a static atomic. UDP supplies the other callbacks and static metadata.
    let bindings = unsafe { NativeTransportBindings::from_callbacks(callbacks, ()) }?;
    context.set_native_transport_bindings(TransportRole::Data, bindings)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    println!("Native sends: {}", TRANSPORT_SENDS.load(Ordering::Relaxed));
    Ok(())
}
