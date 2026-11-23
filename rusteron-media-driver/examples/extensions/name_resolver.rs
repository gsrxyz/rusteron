//! Resolve example.local to loopback and accept IP literals without a DNS request.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

use std::{
    ffi::CStr,
    net::{IpAddr, Ipv4Addr},
    sync::atomic::{AtomicU64, Ordering},
};
pub static RESOLUTIONS: AtomicU64 = AtomicU64::new(0);

/// Resolve one application hostname without a DNS call on a driver thread.
struct LocalResolver;
impl NameResolver for LocalResolver {
    fn resolve(&mut self, name: &CStr, _: &CStr, _: bool) -> Result<IpAddr, AeronCError> {
        RESOLUTIONS.fetch_add(1, Ordering::Relaxed);
        if name == c"example.local" {
            return Ok(Ipv4Addr::LOCALHOST.into());
        }
        name.to_str()
            .ok()
            .and_then(|name| name.parse().ok())
            .ok_or_else(|| AeronCError::with_message(-1, "the example resolver accepts example.local and IP literals"))
    }
}

pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    context.set_name_resolver_instance(LocalResolver)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=example.local:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    println!("Name resolutions: {}", RESOLUTIONS.load(Ordering::Relaxed));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[serial_test::serial(driver_extensions)]
    fn resolves_the_application_name_and_ip_literals_but_rejects_unknown_names() {
        let mut resolver = LocalResolver;
        for name in [c"example.local", c"127.0.0.1", c"::1"] {
            assert!(resolver.resolve(name, c"endpoint", false).unwrap().is_loopback());
        }
        assert!(resolver.resolve(c"unknown.local", c"endpoint", false).is_err());
    }
}
