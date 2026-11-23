//! Typed media driver callbacks.
//!
//! Aeron calls one monomorphised C function per callback. Mutable state belongs
//! to one agent. Cross-agent state must implement `Sync`. Callback panics abort.
//! Configuration is frozen by `AeronDriver::new`.
//!
//! Raw suppliers require the caller to uphold the native contracts.
//!
//! ```compile_fail
//! use rusteron_media_driver::AeronDriverContext;
//! let context = AeronDriverContext::new().unwrap();
//! context.set_unicast_flowcontrol_supplier(None).unwrap();
//! ```

use crate::{AeronCError, AeronDriverContext, ResourceStorageExt, bindings::*};
use std::{cell::UnsafeCell, ffi::CStr, marker::PhantomData, os::raw::c_void};

mod congestion;
mod duty_cycle;
mod flow;
mod idle;
mod interceptors;
mod ports;
mod resolver;
mod transport;
mod views;
pub use congestion::*;
pub use duty_cycle::*;
pub use flow::*;
pub use idle::*;
pub use interceptors::*;
pub use ports::*;
pub use resolver::*;
pub use transport::*;
pub use views::*;

/// Heap state whose address C keeps until context close.
///
/// A `Box` asserts unique access when it moves, which invalidates pointers already
/// given to C. This owner holds a raw pointer, so moving it into the context is a
/// plain copy of the address
struct NativeOwned<T>(std::ptr::NonNull<T>);
impl<T> NativeOwned<T> {
    fn new(value: T) -> Self {
        Self(std::ptr::NonNull::from(Box::leak(Box::new(value))))
    }
    fn as_ptr(&self) -> *mut T {
        self.0.as_ptr()
    }
}
impl<T> std::ops::Deref for NativeOwned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: the allocation lives until drop and is only mutated through C's pointers
        unsafe { self.0.as_ref() }
    }
}
impl<T> Drop for NativeOwned<T> {
    fn drop(&mut self) {
        // SAFETY: `new` leaked this Box and nothing else frees it
        drop(unsafe { Box::from_raw(self.0.as_ptr()) });
    }
}
// SAFETY: ownership matches `Box<T>`
unsafe impl<T: Send> Send for NativeOwned<T> {}
unsafe impl<T: Sync> Sync for NativeOwned<T> {}

// errno EINVAL, the same value on every supported platform
const EINVAL: i32 = 22;

#[inline]
fn check(result: i32) -> Result<(), AeronCError> {
    if result < 0 {
        Err(AeronCError::from_code(result))
    } else {
        Ok(())
    }
}

#[cold]
#[inline(never)]
unsafe fn supplier_error(error: AeronCError) -> i32 {
    error.with_last_err_message(|message| {
        // Bound the stack copy to Aeron's error capacity. Escape interior NULs
        // and pass percent signs as data to the native formatter.
        let mut text = [0u8; AERON_ERROR_MAX_TOTAL_LENGTH as usize];
        let mut length = 0;
        for byte in message.bytes() {
            let bytes = if byte == 0 {
                &b"\\0"[..]
            } else {
                std::slice::from_ref(&byte)
            };
            if length + bytes.len() >= text.len() {
                break;
            }
            text[length..length + bytes.len()].copy_from_slice(bytes);
            length += bytes.len();
        }
        // SAFETY: text is NUL-terminated, and aeron_err_set copies it before
        // returning. with_last_err_message snapshots native text before reuse.
        unsafe {
            aeron_err_set(
                EINVAL,
                c"rusteron extension".as_ptr(),
                c"extensions".as_ptr(),
                0,
                c"Rust extension error %d: %s".as_ptr(),
                error.code,
                text.as_ptr().cast::<std::os::raw::c_char>(),
            )
        };
    });
    -1
}

/// Values copied from the frozen context during initialisation.
#[derive(Clone, Copy, Debug)]
pub struct StrategyContext {
    pub initial_receiver_window: usize,
    pub receiver_timeout_ns: u64,
    pub group_tag: i64,
    pub group_min_size: i32,
    pub unicast_retransmit_window_multiple: usize,
    pub multicast_retransmit_window_multiple: usize,
}

impl StrategyContext {
    unsafe fn from_raw(context: *mut aeron_driver_context_t) -> Self {
        unsafe {
            Self {
                initial_receiver_window: (*context).initial_window_length,
                receiver_timeout_ns: (*context).flow_control.receiver_timeout_ns,
                group_tag: (*context).flow_control.group_tag,
                group_min_size: (*context).flow_control.group_min_size,
                unicast_retransmit_window_multiple: (*context).unicast_flow_control_rrwm,
                multicast_retransmit_window_multiple: (*context).multicast_flow_control_rrwm,
            }
        }
    }
}

/// An initialisation-only view of a native channel.
#[derive(Clone, Copy)]
pub struct ChannelRef<'a> {
    raw: *const aeron_udp_channel_t,
    _life: PhantomData<&'a aeron_udp_channel_t>,
}
impl<'a> ChannelRef<'a> {
    unsafe fn from_raw(raw: *const aeron_udp_channel_t) -> Self {
        Self {
            raw,
            _life: PhantomData,
        }
    }
    /// The original channel URI, without a trailing NUL.
    pub fn uri(self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts((*self.raw).original_uri.as_ptr().cast(), (*self.raw).uri_length) }
    }
    pub fn is_multicast(self) -> bool {
        unsafe { (*self.raw).is_multicast }
    }
}

// The unit tests compile the example policies so they also cover them
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../examples/extensions/bounded_flow_control.rs"]
mod bounded_example;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../examples/extensions/burst_loss.rs"]
mod burst_loss_example;
#[cfg(test)]
mod extension_tests;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../examples/extensions/packet_loss.rs"]
mod packet_loss_example;
#[cfg(test)]
mod tests;
