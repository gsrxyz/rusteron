//! Typed media driver callbacks.
//!
//! Aeron calls one monomorphized C function per callback. Mutable state belongs
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

mod bounded;
mod congestion;
mod duty_cycle;
mod flow;
mod idle;
mod interceptors;
mod loss;
mod ports;
mod resolver;
mod transport;
mod views;
pub use bounded::*;
pub use congestion::*;
pub use duty_cycle::*;
pub use flow::*;
pub use idle::*;
pub use interceptors::*;
pub use loss::*;
pub use ports::*;
pub use resolver::*;
pub use transport::*;
pub use views::*;

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
                22,
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

/// Values copied from the frozen context during initialization.
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

/// An initialization-only view of a native channel.
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

#[cfg(test)]
mod extension_tests;
#[cfg(test)]
mod tests;
