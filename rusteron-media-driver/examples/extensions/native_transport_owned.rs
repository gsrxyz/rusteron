//! Retain a native runtime until the driver joins and its context closes.
//!
//! Replace the demonstration allocation and UDP table with your C library's
//! runtime-open and bindings-table functions. Keep its library loaded too if
//! the callback code comes from a dynamically loaded library.

use rusteron_media_driver::{AeronCError, AeronDriverContext, bindings::*, extensions::*};
use std::{ffi::c_void, ptr::NonNull};

/// Own one runtime handle and call its native close function exactly once.
pub struct NativeRuntime {
    handle: NonNull<c_void>,
    close: unsafe extern "C" fn(*mut c_void),
}

// SAFETY: from_raw requires concurrent access and permits close on any thread.
unsafe impl Send for NativeRuntime {}
unsafe impl Sync for NativeRuntime {}

impl NativeRuntime {
    pub fn as_ptr(&self) -> *mut c_void {
        self.handle.as_ptr()
    }

    /// Take exclusive ownership of a live native runtime.
    ///
    /// # Safety
    /// `handle` must be accepted by `close`, which must release it once without
    /// unwinding and may be called on any thread. The runtime must support
    /// concurrent sender and receiver use. Its close function and all transport
    /// callbacks must remain loaded until this guard drops.
    pub unsafe fn from_raw(handle: NonNull<c_void>, close: unsafe extern "C" fn(*mut c_void)) -> Self {
        Self { handle, close }
    }
}

impl Drop for NativeRuntime {
    fn drop(&mut self) {
        // SAFETY: this non-Clone guard owns the handle and may close on any
        // thread. Once installed, the context retains it until native shutdown.
        unsafe { (self.close)(self.handle.as_ptr()) };
    }
}

pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    let mut handle = std::ptr::null_mut();
    // SAFETY: aeron_alloc initialises the output on success. This demonstration
    // uses native allocation/free as the runtime's open/close pair.
    let result = unsafe { aeron_alloc(&mut handle, 1) };
    let Some(handle) = NonNull::new(handle).filter(|_| result >= 0) else {
        return Err(AeronCError::from_code(result).capture_errmsg());
    };
    // SAFETY: the native allocation has one owner and aeron_free accepts it.
    let runtime = unsafe { NativeRuntime::from_raw(handle, aeron_free) };
    let table = NativeTransportBindings::udp().callbacks();
    // SAFETY: UDP supplies every callback and static metadata. In a vendor
    // integration, copy its static table here and retain its runtime guard.
    let bindings = unsafe { NativeTransportBindings::from_callbacks(table, runtime) }?;
    context.set_native_transport_bindings(TransportRole::Data, bindings)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("The context owns the native runtime. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    Ok(())
}
