//! Test/example support: an embedded media driver with RAII teardown.
//!
//! Replaces the five-line launch incantation and two-line teardown that tests and examples
//! previously copied everywhere. The driver stops and joins on `Drop`, so teardown ordering
//! lives in exactly one place.

use crate::{Aeron, AeronCError, AeronDriver, AeronDriverContext, IntoCString};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

/// An embedded media driver running on a background thread, with a unique directory,
/// stopped and joined automatically on `Drop`.
///
/// ```no_run
/// # use rusteron_media_driver::testing::EmbeddedDriver;
/// let driver = EmbeddedDriver::launch().unwrap();
/// // connect clients against driver.dir() ...
/// // driver stops + joins when it goes out of scope
/// ```
pub struct EmbeddedDriver {
    dir: String,
    context: AeronDriverContext,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<(), AeronCError>>>,
}

impl EmbeddedDriver {
    /// Launch an embedded driver in a unique directory (deleted on start and shutdown).
    pub fn launch() -> Result<Self, AeronCError> {
        Self::launch_with(|_| Ok(()))
    }

    /// Launch with extra [`AeronDriverContext`] configuration (timeouts, idle strategies…)
    /// applied before the driver starts.
    pub fn launch_with(
        configure: impl FnOnce(&AeronDriverContext) -> Result<(), AeronCError>,
    ) -> Result<Self, AeronCError> {
        let context = AeronDriverContext::new()?;
        context.set_dir_delete_on_shutdown(true)?;
        context.set_dir_delete_on_start(true)?;
        context.set_dir(&format!("{}{}", context.get_dir(), Aeron::nano_clock()).into_c_string())?;
        configure(&context)?;
        let dir = context.get_dir().to_string();
        let (stop, handle) = AeronDriver::launch_embedded(context.clone(), false);
        Ok(Self {
            dir,
            context,
            stop,
            handle: Some(handle),
        })
    }

    /// The aeron directory clients should connect to.
    #[inline]
    pub fn dir(&self) -> &str {
        &self.dir
    }

    /// The driver's context, e.g. for reading configured values.
    #[inline]
    pub fn context(&self) -> &AeronDriverContext {
        &self.context
    }

    /// Stop the driver now instead of waiting for `Drop`.
    pub fn stop(mut self) {
        self.stop_and_join();
    }

    fn stop_and_join(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for EmbeddedDriver {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

/// First free UDP port at or above `start` (binds a probe socket to check).
pub fn find_unused_udp_port(start: u16) -> Option<u16> {
    (start..65535).find(|p| std::net::UdpSocket::bind(("127.0.0.1", *p)).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_driver_launches_and_stops_on_drop() {
        let driver = EmbeddedDriver::launch().unwrap();
        assert!(!driver.dir().is_empty());
        let dir = driver.dir().to_string();
        drop(driver); // stops + joins; dir deleted on shutdown
        assert!(!std::path::Path::new(&dir).join("cnc.dat").exists());
    }

    #[test]
    fn find_unused_udp_port_returns_bindable_port() {
        let port = find_unused_udp_port(23000).unwrap();
        assert!(std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok());
    }
}
