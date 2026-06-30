/**/
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![doc = include_str!("../README.md")]
//! # Features
//!
//! - **`static`**: When enabled, this feature statically links the Aeron C code.
//!   By default, the library uses dynamic linking to the Aeron C libraries.
//! - **`backtrace`**: When enabled will log a backtrace for each AeronCError
//! - **`extra-logging`**: When enabled will log when resource is created and destroyed. Useful if you're seeing a segfault due to a resource being closed
//! - **`log-c-bindings`**: When enabled will log every C binding call with arguments and return values. Useful for debugging FFI interactions
//! - **`precompile`**: When enabled will use precompiled C code instead of requiring cmake and java to be installed

#[allow(improper_ctypes_definitions)]
#[allow(unpredictable_function_pointer_comparisons)]
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use bindings::*;
use std::cell::Cell;
use std::os::raw::c_int;
use std::time::{Duration, Instant};

/// Result codes returned by `AeronPublication::offer` / `try_claim` (Aeron `aeronc.h`). A positive
/// value is the resulting log position; the negatives classify the failure.
///
/// **Fatal** (stop offering): [`PUBLICATION_CLOSED`], [`PUBLICATION_MAX_POSITION_EXCEEDED`],
/// [`PUBLICATION_ERROR`]. **Transient** (retry): [`PUBLICATION_BACK_PRESSURED`],
/// [`PUBLICATION_NOT_CONNECTED`], [`PUBLICATION_ADMIN_ACTION`].
pub const PUBLICATION_NOT_CONNECTED: i64 = bindings::AERON_PUBLICATION_NOT_CONNECTED as i64;
pub const PUBLICATION_BACK_PRESSURED: i64 = bindings::AERON_PUBLICATION_BACK_PRESSURED as i64;
pub const PUBLICATION_ADMIN_ACTION: i64 = bindings::AERON_PUBLICATION_ADMIN_ACTION as i64;
pub const PUBLICATION_CLOSED: i64 = bindings::AERON_PUBLICATION_CLOSED as i64;
pub const PUBLICATION_MAX_POSITION_EXCEEDED: i64 =
    bindings::AERON_PUBLICATION_MAX_POSITION_EXCEEDED as i64;
pub const PUBLICATION_ERROR: i64 = bindings::AERON_PUBLICATION_ERROR as i64;

pub mod testing;

#[cfg(test)]
pub mod persistent_subscription_integration;
#[cfg(test)]
pub mod persistent_subscription_tests;

include!(concat!(env!("OUT_DIR"), "/aeron.rs"));
include!(concat!(env!("OUT_DIR"), "/aeron_custom.rs"));

// Retryable / unrecoverable classification for Aeron errors. Lives in hand-written code rather
// than the codegen `common.rs` because the generator drops methods that carry multi-line doc
// comments. GenericError(-1) and Unknown(_) are intentionally neither — `-1` is Aeron's catch-all
// and could mean anything, so the caller must decide (treat as fatal if unsure).
impl AeronErrorType {
    /// Transient — retry the operation (back off first): back-pressure, admin action, a full
    /// client buffer, or a polling timeout.
    pub fn is_retryable(&self) -> bool {
        self == &AeronErrorType::PublicationBackPressured
            || self == &AeronErrorType::PublicationAdminAction
            || self == &AeronErrorType::ClientErrorBufferFull
            || self == &AeronErrorType::TimedOut
    }

    /// Definitively terminal — retrying will not help: the publication is closed / exhausted /
    /// errored, or the driver or client has timed out (effectively dead). Not exhaustive: an
    /// ambiguous code (`GenericError` / `Unknown`) is neither retryable nor unrecoverable.
    pub fn is_unrecoverable(&self) -> bool {
        self == &AeronErrorType::PublicationClosed
            || self == &AeronErrorType::PublicationMaxPositionExceeded
            || self == &AeronErrorType::PublicationError
            || self == &AeronErrorType::ClientErrorDriverTimeout
            || self == &AeronErrorType::ClientErrorClientTimeout
            || self == &AeronErrorType::ClientErrorConductorServiceTimeout
    }
}

impl AeronCError {
    /// Transient failure — retry the operation (back off first). See [`AeronErrorType::is_retryable`].
    pub fn is_retryable(&self) -> bool {
        self.kind().is_retryable()
    }

    /// Definitively terminal — abort the operation. See [`AeronErrorType::is_unrecoverable`].
    /// Not exhaustive: ambiguous codes are neither retryable nor unrecoverable.
    pub fn is_unrecoverable(&self) -> bool {
        self.kind().is_unrecoverable()
    }
}

pub type SourceLocation = bindings::aeron_archive_source_location_t;
pub const SOURCE_LOCATION_LOCAL: aeron_archive_source_location_en =
    SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL;
pub const SOURCE_LOCATION_REMOTE: aeron_archive_source_location_en =
    SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_REMOTE;

pub struct RecordingPos;
impl RecordingPos {
    pub fn find_counter_id_by_session(
        counter_reader: &AeronCountersReader,
        session_id: i32,
    ) -> i32 {
        unsafe {
            aeron_archive_recording_pos_find_counter_id_by_session_id(
                counter_reader.get_inner(),
                session_id,
            )
        }
    }
    pub fn find_counter_id_by_recording_id(
        counter_reader: &AeronCountersReader,
        recording_id: i64,
    ) -> i32 {
        unsafe {
            aeron_archive_recording_pos_find_counter_id_by_recording_id(
                counter_reader.get_inner(),
                recording_id,
            )
        }
    }

    /// Return the recordingId embedded in the key of the given counter
    /// if it is indeed a "recording position" counter. Otherwise return -1.
    pub fn get_recording_id_block(
        counters_reader: &AeronCountersReader,
        counter_id: i32,
        wait: Duration,
    ) -> Result<i64, AeronCError> {
        let mut result = Self::get_recording_id(counters_reader, counter_id);
        let instant = Instant::now();

        while result.is_err() && instant.elapsed() < wait {
            result = Self::get_recording_id(counters_reader, counter_id);
            #[cfg(debug_assertions)]
            std::thread::sleep(Duration::from_millis(10));
        }

        return result;
    }

    /// Return the recordingId embedded in the key of the given counter
    /// if it is indeed a "recording position" counter. Otherwise return -1.
    pub fn get_recording_id(
        counters_reader: &AeronCountersReader,
        counter_id: i32,
    ) -> Result<i64, AeronCError> {
        /// The type id for an Aeron Archive recording position counter.
        /// In Aeron Java, this is AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID (which is typically 100).
        pub const RECORDING_POSITION_TYPE_ID: i32 = 100;

        /// from Aeron Java code
        pub const RECORD_ALLOCATED: i32 = 1;

        /// A constant to mean "no valid recording ID".
        pub const NULL_RECORDING_ID: i64 = -1;

        if counter_id < 0 {
            return Err(AeronCError::from_code(NULL_RECORDING_ID as i32));
        }

        let state = counters_reader.counter_state(counter_id)?;
        if state != RECORD_ALLOCATED {
            return Err(AeronCError::from_code(NULL_RECORDING_ID as i32));
        }

        let type_id = counters_reader.counter_type_id(counter_id)?;
        if type_id != RECORDING_POSITION_TYPE_ID {
            return Err(AeronCError::from_code(NULL_RECORDING_ID as i32));
        }

        // Read the key area. For a RECORDING_POSITION_TYPE_ID counter:
        //    - offset 0..8 => the i64 recording_id
        //    - offset 8..12 => the session_id (int)
        //    etc...
        // only need the first 8 bytes to get the recordingId.
        let recording_id = Cell::new(-1);
        counters_reader.foreach_counter_once(|value, id, type_id, key, label| {
            if id == counter_id && type_id == RECORDING_POSITION_TYPE_ID {
                let mut val = [0u8; 8];
                val.copy_from_slice(&key[0..8]);
                let Ok(value) = i64::from_le_bytes(val).try_into();
                recording_id.set(value);
            }
        });
        let recording_id = recording_id.get();
        if recording_id < 0 {
            return Err(AeronCError::from_code(NULL_RECORDING_ID as i32));
        }

        Ok(recording_id)
    }
}

impl AeronArchive {
    pub fn aeron(&self) -> Aeron {
        self.get_archive_context().get_aeron()
    }

    /// Find the latest recording matching a predicate.
    /// Returns the recording with the highest recording_id that matches the predicate.
    pub fn find_recording<F>(
        &self,
        mut predicate: F,
    ) -> Result<Option<RecordingDescriptor>, AeronCError>
    where
        F: FnMut(&RecordingDescriptor) -> bool,
    {
        // Find the latest matching recording by fetching all recordings in one call.
        // Uses record_count=i32::MAX to fetch all available recordings.
        let mut result = None;
        let mut count = 0;
        self.list_recordings_once(&mut count, 0, i32::MAX, |desc| {
            let descriptor = self.descriptor_to_owned(&desc);
            if predicate(&descriptor) {
                if result.is_none()
                    || result
                        .as_ref()
                        .map(|r: &RecordingDescriptor| r.recording_id)
                        .unwrap_or(0)
                        < descriptor.recording_id
                {
                    result = Some(descriptor);
                }
            }
        })?;
        Ok(result)
    }

    /// Find the latest recording for a given stream ID.
    pub fn find_recording_for_stream(
        &self,
        stream_id: i32,
    ) -> Result<Option<RecordingDescriptor>, AeronCError> {
        self.find_recording(|desc| desc.stream_id == stream_id)
    }

    /// Collect all recordings matching a predicate.
    pub fn collect_recordings<F>(
        &self,
        mut predicate: F,
    ) -> Result<Vec<RecordingDescriptor>, AeronCError>
    where
        F: FnMut(&RecordingDescriptor) -> bool,
    {
        // Collect all matching recordings by fetching all recordings in one call.
        // Uses record_count=i32::MAX to fetch all available recordings.
        let mut recordings = Vec::new();
        let mut count = 0;
        self.list_recordings_once(&mut count, 0, i32::MAX, |desc| {
            let descriptor = self.descriptor_to_owned(&desc);
            if predicate(&descriptor) {
                recordings.push(descriptor);
            }
        })?;
        Ok(recordings)
    }

    /// Convert a callback-scoped recording descriptor to an owned struct.
    fn descriptor_to_owned(&self, desc: &AeronArchiveRecordingDescriptor) -> RecordingDescriptor {
        let start_position = desc.start_position();
        let stop_position = desc.stop_position();
        RecordingDescriptor {
            recording_id: desc.recording_id(),
            start_position,
            stop_position,
            start_timestamp: desc.start_timestamp(),
            stop_timestamp: desc.stop_timestamp(),
            position: stop_position.saturating_sub(start_position),
            recording_length: stop_position.saturating_sub(start_position),
            control_session_id: desc.control_session_id() as i32,
            correlation_id: desc.correlation_id(),
            session_id: desc.session_id(),
            stream_id: desc.stream_id(),
            channel: desc.stripped_channel().to_string(),
            source_identity: desc.source_identity().to_string(),
            original_channel: desc.original_channel().to_string(),
        }
    }
}

impl AeronArchiveAsyncConnect {
    #[inline]
    /// recommend using this method instead of standard `new` as it will link the archive to aeron so if a drop occurs archive is dropped before aeron
    pub fn new_with_aeron(ctx: &AeronArchiveContext, aeron: &Aeron) -> Result<Self, AeronCError> {
        let resource_async = Self::new(ctx)?;
        resource_async.inner.add_dependency(aeron.clone());
        Ok(resource_async)
    }
}

macro_rules! impl_archive_position_methods {
    ($pub_type:ty) => {
        impl $pub_type {
            /// Retrieves the current active live archive position using the Aeron counters.
            /// Returns an error if not found.
            pub fn get_archive_position(&self) -> Result<i64, AeronCError> {
                if let Some(aeron) = self.inner.get_dependency::<Aeron>() {
                    let counter_reader = &aeron.counters_reader();
                    self.get_archive_position_with(counter_reader)
                } else {
                    Err(AeronCError::from_code(-1))
                }
            }

            /// Retrieves the current active live archive position using the provided counter reader.
            /// Returns an error if not found.
            pub fn get_archive_position_with(
                &self,
                counters: &AeronCountersReader,
            ) -> Result<i64, AeronCError> {
                let session_id = self.get_constants()?.session_id();
                let counter_id = RecordingPos::find_counter_id_by_session(counters, session_id);
                if counter_id < 0 {
                    return Err(AeronCError::from_code(counter_id));
                }
                let position = counters.get_counter_value(counter_id);
                if position < 0 {
                    return Err(AeronCError::from_code(position as i32));
                }
                Ok(position)
            }

            /// Checks if the publication's current position is within a specified inclusive length
            /// of the archive position.
            pub fn is_archive_position_with(&self, length_inclusive: usize) -> bool {
                let archive_position = self.get_archive_position().unwrap_or(-1);
                if archive_position < 0 {
                    return false;
                }
                self.position() - archive_position <= length_inclusive as i64
            }
        }
    };
}

impl_archive_position_methods!(AeronPublication);
impl_archive_position_methods!(AeronExclusivePublication);

/// Recording descriptor for owned recording data
#[derive(Debug, Clone)]
pub struct RecordingDescriptor {
    pub recording_id: i64,
    pub start_position: i64,
    pub stop_position: i64,
    pub start_timestamp: i64,
    pub stop_timestamp: i64,
    pub position: i64,
    pub recording_length: i64,
    pub control_session_id: i32,
    pub correlation_id: i64,
    pub session_id: i32,
    pub stream_id: i32,
    pub channel: String,
    pub source_identity: String,
    pub original_channel: String,
}

/// Wrapper for `Box<dyn PersistentSubscriptionListener>` that provides a stable
/// thin pointer for C FFI callbacks.
struct ListenerHolder {
    listener: Box<dyn PersistentSubscriptionListener>,
}

// Hand-written trampolines: the code generator only wires single-callback args,
// but this listener has 3 callbacks sharing one clientd.
unsafe extern "C" fn persistent_subscription_on_live_joined(clientd: *mut std::ffi::c_void) {
    if !clientd.is_null() {
        // SAFETY: clientd is the ListenerHolder kept alive as a dependency of the subscription.
        let holder = &*(clientd as *const ListenerHolder);
        holder.listener.on_live_joined();
    }
}

unsafe extern "C" fn persistent_subscription_on_live_left(clientd: *mut std::ffi::c_void) {
    if !clientd.is_null() {
        let holder = &*(clientd as *const ListenerHolder);
        holder.listener.on_live_left();
    }
}

unsafe extern "C" fn persistent_subscription_on_error(
    clientd: *mut std::ffi::c_void,
    error_code: c_int,
    error_message: *const std::os::raw::c_char,
) {
    if !clientd.is_null() {
        let holder = &*(clientd as *const ListenerHolder);
        let msg = if !error_message.is_null() {
            std::ffi::CStr::from_ptr(error_message).to_string_lossy()
        } else {
            std::borrow::Cow::Borrowed("")
        };
        holder.listener.on_error(error_code, &msg);
    }
}

/// Safe creation method for AeronArchivePersistentSubscription
impl AeronArchivePersistentSubscription {
    /// Create a persistent subscription from a context.
    ///
    /// If `listener` is `Some`, it is wired into the context (the C layer copies
    /// the callback pointers + `clientd`) and kept alive for the subscription's
    /// lifetime; it is freed once the subscription is closed.
    ///
    /// The context is consumed and owned by the subscription from this point on —
    /// it will be closed when the subscription is closed, so it must not be used
    /// afterwards.
    pub fn create(
        ctx: AeronArchivePersistentSubscriptionContext,
        listener: Option<Box<dyn PersistentSubscriptionListener>>,
    ) -> Result<Self, AeronCError> {
        use std::os::raw::c_void;

        // Box the listener so we can hand C a stable clientd; a Box's heap address
        // never moves, so the pointer C copies stays valid until the box drops.
        let holder_box: Option<Box<ListenerHolder>> = match listener {
            Some(listener) => {
                let mut hb = Box::new(ListenerHolder { listener });
                let holder_ptr: *mut ListenerHolder = &mut *hb;
                let c_listener = match AeronArchivePersistentSubscriptionListener::new(
                    Some(persistent_subscription_on_live_joined),
                    Some(persistent_subscription_on_live_left),
                    Some(persistent_subscription_on_error),
                    holder_ptr as *mut c_void,
                ) {
                    Ok(l) => l,
                    Err(e) => return Err(e),
                };
                if let Err(e) = ctx.set_listener(c_listener.get_inner()) {
                    return Err(e);
                }
                Some(hb)
            }
            None => None,
        };

        let mut raw_ptr: *mut aeron_archive_persistent_subscription_t = std::ptr::null_mut();
        unsafe {
            let result =
                aeron_archive_persistent_subscription_create(&mut raw_ptr, ctx.get_inner());
            if result < 0 {
                return Err(AeronCError::from_code(result));
            }
        }

        // The C subscription now owns the context. Mark it already-closed so dropping
        // `ctx` frees the Rust bookkeeping without re-running the C context close.
        if let Some(inner) = ctx.inner.as_owned() {
            inner.close_already_called.set(true);
        }

        // C close, run on drop if close() wasn't called explicitly.
        let resource = match ManagedCResource::new(
            move |ctx_field| unsafe {
                *ctx_field = raw_ptr;
                0
            },
            Some(Box::new(move |ctx_field| unsafe {
                aeron_archive_persistent_subscription_close(*ctx_field)
            })),
            true,
            None,
        ) {
            Ok(r) => r,
            Err(e) => {
                unsafe {
                    aeron_archive_persistent_subscription_close(raw_ptr);
                }
                return Err(e);
            }
        };

        // Reclaim via a dependency, not the cleanup closure — the generated
        // close() bypasses the closure, but a field always drops.
        if let Some(hb) = holder_box {
            resource.add_dependency(hb);
        }

        Ok(Self {
            inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource)),
        })
    }

    /// Get the failure reason as a tuple of (error_code, error_message).
    /// Returns None if there is no failure. This is a safe wrapper around the C function.
    pub fn get_failure_reason(&self) -> Option<(i32, String)> {
        let mut error_code: i32 = 0;
        let mut reason_ptr: *const std::os::raw::c_char = std::ptr::null();
        let has_reason = unsafe {
            bindings::aeron_archive_persistent_subscription_failure_reason(
                self.get_inner(),
                &mut error_code,
                &mut reason_ptr,
            )
        };
        if has_reason && !reason_ptr.is_null() {
            let cstr = unsafe { std::ffi::CStr::from_ptr(reason_ptr) };
            Some((error_code, cstr.to_string_lossy().into_owned()))
        } else {
            None
        }
    }
}

/// Sentinel for [`PersistentSubscriptionBuilder::start_position`]: replay from the
/// beginning of the recording. Maps to Aeron's `AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START`.
pub const PERSISTENT_SUBSCRIPTION_FROM_START: i64 = -1;

/// Sentinel for [`PersistentSubscriptionBuilder::start_position`]: skip replay and join
/// the live stream immediately. Maps to Aeron's `AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE`.
pub const PERSISTENT_SUBSCRIPTION_FROM_LIVE: i64 = -2;

/// Returns a builder for configuring a persistent subscription context
pub fn persistent_subscription_builder() -> Result<PersistentSubscriptionBuilder, AeronCError> {
    PersistentSubscriptionBuilder::new()
}

/// Trait for persistent subscription event listeners.
/// This provides a safe Rust alternative to using raw C function pointers.
///
/// In the poll loop, prefer the state queries `is_live()` / `is_replaying()` /
/// `has_failed()` for control flow and treat this listener as observational
/// (logging/metrics). See Aeron's `PersistentSubscriptionListener`.
pub trait PersistentSubscriptionListener: Send + 'static {
    /// Called when the persistent subscription transitions to consuming from the
    /// live stream. Can fire more than once: if the live image is lost the
    /// subscription falls back to replay and this fires again on rejoin.
    fn on_live_joined(&self) {}

    /// Called when the persistent subscription stops consuming from the live
    /// stream (e.g. the live image closed). The subscription automatically falls
    /// back to replay; no user action required. Can fire repeatedly.
    fn on_live_left(&self) {}

    /// Called for **both** non-terminal and terminal errors. Non-terminal errors
    /// (timeouts, transient resource unavailability) are retried automatically.
    /// A terminal failure flips [`AeronArchivePersistentSubscription::has_failed`]
    /// to true — check it in the poll loop and read the reason with
    /// [`AeronArchivePersistentSubscription::get_failure_reason`].
    fn on_error(&self, error_code: i32, error_message: &str) {}
}

/// Builder for configuring and creating a persistent subscription.
/// This provides a fluent interface for setting up a persistent subscription
/// with proper CString handling.
pub struct PersistentSubscriptionBuilder {
    ctx: AeronArchivePersistentSubscriptionContext,
    listener: Option<Box<dyn PersistentSubscriptionListener>>,
}

impl PersistentSubscriptionBuilder {
    /// Create a new builder with a default context.
    pub fn new() -> Result<Self, AeronCError> {
        Ok(Self {
            ctx: AeronArchivePersistentSubscriptionContext::new()?,
            listener: None,
        })
    }

    /// Set the Aeron client to use.
    pub fn aeron(self, aeron: &Aeron) -> Result<Self, AeronCError> {
        self.ctx.set_aeron(aeron)?;
        Ok(self)
    }

    /// Set the archive context to use.
    pub fn archive_context(self, ctx: &AeronArchiveContext) -> Result<Self, AeronCError> {
        self.ctx.set_archive_context(ctx)?;
        Ok(self)
    }

    /// Set the live channel (accepts &str, handles CString conversion internally).
    pub fn live_channel(self, channel: &str) -> Result<Self, AeronCError> {
        let channel = std::ffi::CString::new(channel).map_err(|_| AeronCError::from_code(-1))?;
        self.ctx.set_live_channel(&channel)?;
        Ok(self)
    }

    /// Set the live stream ID.
    pub fn live_stream_id(self, id: i32) -> Result<Self, AeronCError> {
        self.ctx.set_live_stream_id(id)?;
        Ok(self)
    }

    /// Set the replay channel (accepts &str, handles CString conversion internally).
    pub fn replay_channel(self, channel: &str) -> Result<Self, AeronCError> {
        let channel = std::ffi::CString::new(channel).map_err(|_| AeronCError::from_code(-1))?;
        self.ctx.set_replay_channel(&channel)?;
        Ok(self)
    }

    /// Set the replay stream ID.
    pub fn replay_stream_id(self, id: i32) -> Result<Self, AeronCError> {
        self.ctx.set_replay_stream_id(id)?;
        Ok(self)
    }

    /// Set the start position.
    pub fn start_position(self, pos: i64) -> Result<Self, AeronCError> {
        self.ctx.set_start_position(pos)?;
        Ok(self)
    }

    /// Replay from the beginning of the recording (Aeron `FROM_START`).
    pub fn start_from_beginning(self) -> Result<Self, AeronCError> {
        self.start_position(PERSISTENT_SUBSCRIPTION_FROM_START)
    }

    /// Skip replay and join the live stream immediately (Aeron `FROM_LIVE`).
    pub fn start_from_live(self) -> Result<Self, AeronCError> {
        self.start_position(PERSISTENT_SUBSCRIPTION_FROM_LIVE)
    }

    /// Set the recording ID.
    pub fn recording_id(self, id: i64) -> Result<Self, AeronCError> {
        self.ctx.set_recording_id(id)?;
        Ok(self)
    }

    /// Set the listener for events.
    pub fn listener<L: PersistentSubscriptionListener>(
        mut self,
        listener: L,
    ) -> Result<Self, AeronCError> {
        self.listener = Some(Box::new(listener));
        Ok(self)
    }

    /// Pre-allocate the state counter so an external observer can read the PS state-machine
    /// state. If unset the PS allocates one itself. Maps to Aeron's `Context.stateCounter`.
    pub fn state_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        self.ctx.set_state_counter(counter)?;
        Ok(self)
    }

    /// Counter holding the byte gap between replay and live when the live image is added.
    /// Maps to Aeron's `Context.joinDifferenceCounter`.
    pub fn join_difference_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        self.ctx.set_join_difference_counter(counter)?;
        Ok(self)
    }

    /// Counter holding the number of times the PS has dropped off the live stream.
    /// Maps to Aeron's `Context.liveLeftCounter`.
    pub fn live_left_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        self.ctx.set_live_left_counter(counter)?;
        Ok(self)
    }

    /// Counter holding the number of times the PS has switched to the live stream.
    /// Maps to Aeron's `Context.liveJoinedCounter`.
    pub fn live_joined_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        self.ctx.set_live_joined_counter(counter)?;
        Ok(self)
    }

    /// Build the persistent subscription.
    pub fn build(mut self) -> Result<AeronArchivePersistentSubscription, AeronCError> {
        AeronArchivePersistentSubscription::create(self.ctx, self.listener.take())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::{error, info};

    use crate::testing::EmbeddedArchiveMediaDriverProcess;
    use serial_test::serial;
    use std::cell::Cell;
    use std::error;
    use std::error::Error;
    use std::os::raw::c_int;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread::{sleep, JoinHandle};
    use std::time::{Duration, Instant};

    #[derive(Default, Debug)]
    struct ErrorCount {
        error_count: usize,
    }

    impl AeronErrorHandlerCallback for ErrorCount {
        fn handle_aeron_error_handler(&mut self, error_code: c_int, msg: &str) {
            error!("Aeron error {}: {}", error_code, msg);
            self.error_count += 1;
        }
    }

    pub const ARCHIVE_CONTROL_REQUEST: &str = "aeron:udp?endpoint=localhost:8010";
    pub const ARCHIVE_CONTROL_RESPONSE: &str = "aeron:udp?endpoint=localhost:8011";
    pub const ARCHIVE_RECORDING_EVENTS: &str =
        "aeron:udp?control-mode=dynamic|control=localhost:8012";

    #[test]
    fn test_uri_string_builder() -> Result<(), AeronCError> {
        let builder = AeronUriStringBuilder::default();

        builder.init_new()?;
        builder
            .media(Media::Udp)? // very important to set media else set_initial_position will give an error of -1
            .mtu_length(1024 * 64)?
            .set_initial_position(127424949617280, 1182294755, 65536)?;
        let uri = builder.build(1024)?;
        assert_eq!("aeron:udp?term-id=-1168322114|term-length=65536|mtu=65536|init-term-id=1182294755|term-offset=33408", uri);

        builder.init_new()?;
        let uri = builder
            .media(Media::Udp)?
            .control_mode(ControlMode::Dynamic)?
            .reliable(false)?
            .ttl(2)?
            .endpoint("localhost:1235")?
            .control("localhost:1234")?
            .build(1024)?;
        assert_eq!("aeron:udp?ttl=2|control-mode=dynamic|endpoint=localhost:1235|control=localhost:1234|reliable=false", uri);

        let uri = AeronUriStringBuilder::from_str("aeron:udp?endpoint=localhost:8010")?
            .ttl(5)?
            .build(1024)?;

        assert_eq!("aeron:udp?ttl=5|endpoint=localhost:8010", uri);

        let uri = uri.parse::<AeronUriStringBuilder>()?.ttl(6)?.build(1024)?;

        assert_eq!("aeron:udp?ttl=6|endpoint=localhost:8010", uri);

        Ok(())
    }

    pub const STREAM_ID: i32 = 1033;
    pub const MESSAGE_PREFIX: &str = "Message-Prefix-";
    pub const CONTROL_ENDPOINT: &str = "localhost:23265";
    pub const RECORDING_ENDPOINT: &str = "localhost:23266";
    pub const LIVE_ENDPOINT: &str = "localhost:23267";
    pub const REPLAY_ENDPOINT: &str = "localhost:0";
    // pub const REPLAY_ENDPOINT: &str = "localhost:23268";

    #[test]
    #[serial]
    fn test_simple_replay_merge() -> Result<(), AeronCError> {
        // Skip test under Valgrind due to timeout issues
        if std::env::var_os("RUSTERON_VALGRIND").is_some() {
            return Ok(());
        }

        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes()
            .expect("failed to kill all java processes");

        assert!(is_udp_port_available(23265));
        assert!(is_udp_port_available(23266));
        assert!(is_udp_port_available(23267));
        assert!(is_udp_port_available(23268));
        let id = Aeron::nano_clock();
        let aeron_dir = format!("target/aeron/{}/shm", id);
        let archive_dir = format!("target/aeron/{}/archive", id);

        info!("starting archive media driver");
        let media_driver = EmbeddedArchiveMediaDriverProcess::build_and_start(
            &aeron_dir,
            &format!("{}/archive", aeron_dir),
            ARCHIVE_CONTROL_REQUEST,
            ARCHIVE_CONTROL_RESPONSE,
            ARCHIVE_RECORDING_EVENTS,
        )
        .expect("Failed to start embedded media driver");

        info!("connecting to archive");
        let (archive, aeron) = media_driver
            .archive_connect()
            .expect("Could not connect to archive client");

        let running = Arc::new(AtomicBool::new(true));

        info!("connected to archive, adding publication");
        assert!(!aeron.is_closed());

        let (session_id, publisher_thread) =
            reply_merge_publisher(&archive, aeron.clone(), running.clone())?;

        {
            let context = AeronContext::new()?;
            context.set_dir(&media_driver.aeron_dir)?;
            let mut error_handler = Handler::leak(ErrorCount::default());
            context.set_error_handler(Some(&error_handler))?;
            context.set_driver_timeout_ms(60_000)?;

            // Wrap fallible code so release() is always called even on error/panic
            let inner: Result<(), AeronCError> = (|| {
                let aeron = Aeron::new(&context)?;
                aeron.start()?;
                let source_archive_context = archive.get_archive_context();
                let aeron_archive_context = AeronArchiveContext::new()?;
                aeron_archive_context.set_aeron(&aeron)?;
                aeron_archive_context.set_control_request_channel(
                    &source_archive_context
                        .get_control_request_channel()
                        .into_c_string(),
                )?;
                aeron_archive_context.set_control_response_channel(
                    &source_archive_context
                        .get_control_response_channel()
                        .into_c_string(),
                )?;
                aeron_archive_context.set_recording_events_channel(
                    &source_archive_context
                        .get_recording_events_channel()
                        .into_c_string(),
                )?;
                aeron_archive_context.set_message_timeout_ns(60_000_000_000)?;
                aeron_archive_context.set_error_handler(Some(&error_handler))?;
                let merge_archive =
                    AeronArchiveAsyncConnect::new_with_aeron(&aeron_archive_context, &aeron)?
                        .poll_blocking(Duration::from_secs(60))?;
                replay_merge_subscription(&merge_archive, aeron.clone(), session_id)?;
                Ok(())
            })();

            error_handler.release();
            inner?;
        }

        running.store(false, Ordering::Release);
        publisher_thread.join().unwrap();
        drop(archive);
        drop(aeron);
        drop(media_driver);

        Ok(())
    }

    fn reply_merge_publisher(
        archive: &AeronArchive,
        aeron: Aeron,
        running: Arc<AtomicBool>,
    ) -> Result<(i32, JoinHandle<()>), AeronCError> {
        let publication = aeron.add_publication(
            // &format!("aeron:udp?control={CONTROL_ENDPOINT}|control-mode=dynamic|term-length=65536|fc=tagged,g:99901/1,t:5s"),
            &format!("aeron:udp?control={CONTROL_ENDPOINT}|control-mode=dynamic|term-length=65536")
                .into_c_string(),
            STREAM_ID,
            Duration::from_secs(5),
        )?;

        info!(
            "publication {} [status={:?}]",
            publication.channel(),
            publication.channel_status()
        );
        assert_eq!(1, publication.channel_status());

        let session_id = publication.session_id();
        let recording_channel = format!(
            // "aeron:udp?endpoint={RECORDING_ENDPOINT}|control={CONTROL_ENDPOINT}|session-id={session_id}|gtag=99901"
            "aeron:udp?endpoint={RECORDING_ENDPOINT}|control={CONTROL_ENDPOINT}|session-id={session_id}"
        );
        info!("recording channel {}", recording_channel);
        archive.start_recording(
            &recording_channel.into_c_string(),
            STREAM_ID,
            SOURCE_LOCATION_REMOTE,
            true,
        )?;

        info!("waiting for publisher to be connected");
        while !publication.is_connected() {
            thread::sleep(Duration::from_millis(100));
        }
        info!("publisher to be connected");
        let counters_reader = aeron.counters_reader();
        let mut caught_up_count = 0;
        let publisher_thread = thread::spawn(move || {
            let mut message_count = 0;

            while running.load(Ordering::Acquire) {
                let message = format!("{}{}", MESSAGE_PREFIX, message_count);
                while publication.offer(
                    message.as_bytes(),
                    Handlers::no_reserved_value_supplier_handler(),
                ) <= 0
                {
                    thread::sleep(Duration::from_millis(10));
                }
                message_count += 1;
                if message_count % 10_000 == 0 {
                    info!(
                        "Published {} messages [position={}]",
                        message_count,
                        publication.position()
                    );
                }
                // slow down publishing so can catch up
                if message_count > 10_000 {
                    // ensure archiver is caught up
                    while !publication.is_archive_position_with(0) {
                        thread::sleep(Duration::from_micros(300));
                    }
                    caught_up_count += 1;
                }
            }
            assert!(caught_up_count > 0);
            if let Err(err) = publication.close(Handlers::no_notification_handler()) {
                info!("publisher close returned error: {err:?}");
            }
            info!("Publisher thread terminated");
        });
        Ok((session_id, publisher_thread))
    }

    fn replay_merge_subscription(
        archive: &AeronArchive,
        aeron: Aeron,
        session_id: i32,
    ) -> Result<(), AeronCError> {
        // let replay_channel = format!("aeron:udp?control-mode=manual|session-id={session_id}");
        let replay_channel = format!("aeron:udp?session-id={session_id}").into_c_string();
        info!("replay channel {:?}", replay_channel);

        let replay_destination = format!("aeron:udp?endpoint={REPLAY_ENDPOINT}").into_c_string();
        info!("replay destination {:?}", replay_destination);

        let live_destination =
            format!("aeron:udp?endpoint={LIVE_ENDPOINT}|control={CONTROL_ENDPOINT}")
                .into_c_string();
        info!("live destination {:?}", live_destination);

        let counters_reader = aeron.counters_reader();
        let mut counter_id = -1;

        while counter_id < 0 {
            counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        }
        info!(
            "counter id {} {:?}",
            counter_id,
            counters_reader.get_counter_label(counter_id, 1024)
        );
        info!(
            "counter id {} position={:?}",
            counter_id,
            counters_reader.get_counter_value(counter_id)
        );

        // let recording_id = Cell::new(-1);
        // let start_position = Cell::new(-1);

        // let mut count = 0;
        // assert!(
        //     archive.list_recordings_once(&mut count, 0, 1000, |descriptor| {
        //         info!("Recording descriptor: {:?}", descriptor);
        //         recording_id.set(descriptor.recording_id);
        //         start_position.set(descriptor.start_position);
        //         assert_eq!(descriptor.session_id, session_id);
        //         assert_eq!(descriptor.stream_id, STREAM_ID);
        //     })? >= 0
        // );
        // assert!(count > 0);
        // assert!(recording_id.get() >= 0);

        // let record_id = RecordingPos::get_recording_id(&aeron.counters_reader(), counter_id)?;
        // assert_eq!(recording_id.get(), record_id);
        //
        // let recording_id = recording_id.get();
        // let start_position = start_position.get();
        let start_position = 0;
        let recording_id = RecordingPos::get_recording_id_block(
            &aeron.counters_reader(),
            counter_id,
            Duration::from_secs(5),
        )?;

        let subscribe_channel =
            format!("aeron:udp?control-mode=manual|session-id={session_id}").into_c_string();
        info!("subscribe channel {:?}", subscribe_channel);
        let subscription = aeron.add_subscription(
            &subscribe_channel,
            STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;

        let replay_merge = AeronArchiveReplayMerge::new(
            &subscription,
            &archive,
            &replay_channel,
            &replay_destination,
            &live_destination,
            recording_id,
            start_position,
            Aeron::epoch_clock(),
            60_000,
        )?;

        info!(
            "ReplayMerge initialization: recordingId={}, startPosition={}, subscriptionChannel={:?}, replayChannel={:?}, replayDestination={:?}, liveDestination={:?}",
            recording_id,
            start_position,
            subscribe_channel,
            &replay_channel,
            &replay_destination,
            &live_destination
        );

        // media_driver
        //     .run_aeron_stats()
        //     .expect("Failed to run aeron stats");

        // info!("Waiting for subscription to connect...");
        // while !subscription.is_connected() {
        //     thread::sleep(Duration::from_millis(100));
        // }
        // info!("Subscription connected");

        info!(
            "about to start_replay [maxRecordPosition={:?}]",
            archive.get_max_recorded_position(recording_id)
        );

        let mut reply_count = 0;
        while !replay_merge.is_merged() {
            assert!(!replay_merge.has_failed());
            if replay_merge.poll_once(
                |buffer, _header| {
                    reply_count += 1;
                    if reply_count % 10_000 == 0 {
                        info!(
                            "replay-merge [count={}, isMerged={}, isLive={}]",
                            reply_count,
                            replay_merge.is_merged(),
                            replay_merge.is_live_added()
                        );
                    }
                },
                100,
            )? == 0
            {
                let err = archive.poll_for_error_response_as_string(4096)?;
                if !err.is_empty() {
                    panic!("{}", err);
                }
                if Aeron::errmsg().len() > 0 && "no error" != Aeron::errmsg() {
                    panic!("{}", Aeron::errmsg());
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
        assert!(!replay_merge.has_failed());
        assert!(replay_merge.is_live_added());
        assert!(reply_count > 10_000, "no replay-merge fragments received");
        if let Err(err) = replay_merge.close() {
            info!("replay merge close returned error: {err:?}");
        }
        if let Err(err) = subscription.close(Handlers::no_notification_handler()) {
            info!("replay subscription close returned error: {err:?}");
        }
        Ok(())
    }

    #[test]
    fn version_check() {
        let major = unsafe { crate::aeron_version_major() };
        let minor = unsafe { crate::aeron_version_minor() };
        let patch = unsafe { crate::aeron_version_patch() };

        let aeron_version = format!("{}.{}.{}", major, minor, patch);

        let cargo_version = "1.51.0";
        assert_eq!(aeron_version, cargo_version);
    }

    use std::thread;

    fn start_aeron_archive() -> Result<
        (
            Aeron,
            AeronArchiveContext,
            EmbeddedArchiveMediaDriverProcess,
            Handler<AeronPublicationErrorFrameHandlerLogger>,
            Handler<ErrorCount>,
        ),
        Box<dyn Error>,
    > {
        let id = Aeron::nano_clock();
        let aeron_dir = format!("target/aeron/{}/shm", id);
        let archive_dir = format!("target/aeron/{}/archive", id);

        let request_port = find_unused_udp_port(8000).expect("Could not find port");
        let response_port = find_unused_udp_port(request_port + 1).expect("Could not find port");
        let recording_event_port =
            find_unused_udp_port(response_port + 1).expect("Could not find port");
        let request_control_channel = &format!("aeron:udp?endpoint=localhost:{}", request_port);
        let response_control_channel = &format!("aeron:udp?endpoint=localhost:{}", response_port);
        let recording_events_channel =
            &format!("aeron:udp?endpoint=localhost:{}", recording_event_port);
        assert_ne!(request_control_channel, response_control_channel);

        let archive_media_driver = EmbeddedArchiveMediaDriverProcess::build_and_start(
            &aeron_dir,
            &archive_dir,
            request_control_channel,
            response_control_channel,
            recording_events_channel,
        )
        .expect("Failed to start Java process");

        let aeron_context = AeronContext::new()?;
        aeron_context.set_dir(&aeron_dir.into_c_string())?;
        aeron_context.set_client_name(&"test".into_c_string())?;
        let mut pub_error_frame_handler = Handler::leak(AeronPublicationErrorFrameHandlerLogger);
        aeron_context.set_publication_error_frame_handler(Some(&pub_error_frame_handler))?;
        let mut error_handler = Handler::leak(ErrorCount::default());
        aeron_context.set_error_handler(Some(&error_handler))?;

        // Use inner closure so we can call release() on any error path after handlers are created
        let inner: Result<(Aeron, AeronArchiveContext), Box<dyn Error>> = (|| {
            let aeron = Aeron::new(&aeron_context)?;
            aeron.start()?;
            let archive_context = AeronArchiveContext::new()?;
            archive_context.set_aeron(&aeron)?;
            archive_context
                .set_control_request_channel(&request_control_channel.as_str().into_c_string())?;
            archive_context
                .set_control_response_channel(&response_control_channel.as_str().into_c_string())?;
            archive_context
                .set_recording_events_channel(&recording_events_channel.as_str().into_c_string())?;
            archive_context.set_error_handler(Some(&error_handler))?;
            Ok((aeron, archive_context))
        })();

        match inner {
            Ok((aeron, archive_context)) => Ok((
                aeron,
                archive_context,
                archive_media_driver,
                pub_error_frame_handler,
                error_handler,
            )),
            Err(e) => {
                pub_error_frame_handler.release();
                error_handler.release();
                Err(e)
            }
        }
    }

    #[test]
    #[serial]
    pub fn test_aeron_archive() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes()
            .expect("failed to kill all java processes");

        let (aeron, archive_context, media_driver, mut pub_error_frame_handler, mut error_handler) =
            start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            assert!(!aeron.is_closed());

            info!("connected to aeron");

            let archive_connector =
                AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
            let archive = archive_connector
                .poll_blocking(Duration::from_secs(30))
                .expect("failed to connect to aeron archive media driver");

            assert!(archive.get_archive_id() > 0);

            let channel = AERON_IPC_STREAM;
            let stream_id = 10;

            let subscription_id =
                archive.start_recording(channel, stream_id, SOURCE_LOCATION_LOCAL, true)?;

            assert!(subscription_id >= 0);
            info!("subscription id {}", subscription_id);

            let publication = aeron
                .async_add_exclusive_publication(channel, stream_id)?
                .poll_blocking(Duration::from_secs(5))?;

            for i in 0..11 {
                while publication.offer(
                    "123456".as_bytes(),
                    Handlers::no_reserved_value_supplier_handler(),
                ) <= 0
                {
                    sleep(Duration::from_millis(50));
                    let err = archive.poll_for_error_response_as_string(4096)?;
                    if !err.is_empty() {
                        return Err(std::io::Error::other(err).into());
                    }
                    archive.idle();
                }
                info!("sent message {i} [test_aeron_archive]");
            }

            archive.idle();
            let session_id = publication.get_constants()?.session_id;
            info!("publication session id {}", session_id);
            // since this is single threaded need to make sure it did write to archiver, usually not required in multi-proccess app
            let stop_position = publication.position();
            info!(
                "publication stop position {} [publication={:?}]",
                stop_position,
                publication.get_constants()
            );
            let counters_reader = aeron.counters_reader();
            info!("counters reader ready {:?}", counters_reader);

            let mut counter_id = -1;

            let start = Instant::now();
            while counter_id <= 0 && start.elapsed() < Duration::from_secs(5) {
                counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
                info!("counter id {}", counter_id);
            }

            assert!(counter_id >= 0);

            info!("counter id {counter_id}, session id {session_id}");
            while counters_reader.get_counter_value(counter_id) < stop_position {
                info!(
                    "current archive publication stop position {}",
                    counters_reader.get_counter_value(counter_id)
                );
                sleep(Duration::from_millis(50));
            }
            info!(
                "found archive publication stop position {}",
                counters_reader.get_counter_value(counter_id)
            );

            archive.stop_recording_channel_and_stream(channel, stream_id)?;
            drop(publication);

            info!("list recordings");
            let found_recording_id = Cell::new(-1);
            let start_pos = Cell::new(-1);
            let end_pos = Cell::new(-1);
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(5) && found_recording_id.get() == -1 {
                let mut count = 0;
                archive.list_recordings_for_uri_once(
                    &mut count,
                    0,
                    i32::MAX,
                    channel,
                    stream_id,
                    |d: AeronArchiveRecordingDescriptor| {
                        assert_eq!(d.stream_id, stream_id);
                        info!("found recording {:#?}", d);
                        info!(
                            "strippedChannel={}, originalChannel={}",
                            d.stripped_channel(),
                            d.original_channel()
                        );
                        if d.stop_position > d.start_position && d.stop_position > 0 {
                            found_recording_id.set(d.recording_id);
                            start_pos.set(d.start_position);
                            end_pos.set(d.stop_position);
                        }

                        // verify clone_struct works
                        let copy = d.clone_struct();
                        assert_eq!(copy.deref(), d.deref());
                        assert_eq!(copy.recording_id, d.recording_id);
                        assert_eq!(copy.control_session_id, d.control_session_id);
                        assert_eq!(copy.mtu_length, d.mtu_length);
                        assert_eq!(copy.source_identity_length, d.source_identity_length);
                    },
                )?;
                let err = archive.poll_for_error_response_as_string(4096)?;
                if !err.is_empty() {
                    return Err(std::io::Error::other(err).into());
                }
            }
            assert!(start.elapsed() < Duration::from_secs(5));
            info!("start replay");
            let params = AeronArchiveReplayParams::new(
                0,
                i32::MAX,
                start_pos.get(),
                end_pos.get() - start_pos.get(),
                0,
                0,
            )?;
            info!("replay params {:#?}", params);
            let replay_stream_id = 45;
            let replay_session_id = archive.start_replay(
                found_recording_id.get(),
                channel,
                replay_stream_id,
                &params,
            )?;
            let session_id = replay_session_id as i32;

            info!("replay session id {}", replay_session_id);
            info!("session id {}", session_id);
            let channel_replay =
                format!("{}?session-id={}", channel.to_str().unwrap(), session_id).into_c_string();
            info!("archive id: {}", archive.get_archive_id());

            info!("add subscription {:?}", channel_replay);
            let mut avail_image_handler = Handler::leak(AeronAvailableImageLogger);
            let mut unavail_image_handler = Handler::leak(AeronUnavailableImageLogger);
            let replay_result: Result<(), Box<dyn error::Error>> = (|| {
                let subscription = aeron
                    .async_add_subscription(
                        &channel_replay,
                        replay_stream_id,
                        Some(&avail_image_handler),
                        Some(&unavail_image_handler),
                    )?
                    .poll_blocking(Duration::from_secs(10))?;

                #[derive(Default)]
                struct FragmentHandler {
                    count: Cell<usize>,
                }

                impl AeronFragmentHandlerCallback for FragmentHandler {
                    fn handle_aeron_fragment_handler(
                        &mut self,
                        buffer: &[u8],
                        _header: AeronHeader,
                    ) {
                        assert_eq!(buffer, "123456".as_bytes());

                        // Update count (using Cell for interior mutability)
                        self.count.set(self.count.get() + 1);
                    }
                }

                let mut poll = Handler::leak(FragmentHandler::default());
                let poll_result: Result<(), Box<dyn error::Error>> = (|| {
                    let wait_timeout = Duration::from_secs(30);
                    let start = Instant::now();
                    while start.elapsed() < wait_timeout
                        && subscription.poll(Some(&poll), 100)? <= 0
                    {
                        let err = archive.poll_for_error_response_as_string(4096)?;
                        if !err.is_empty() {
                            return Err(std::io::Error::other(err).into());
                        }
                    }

                    if start.elapsed() >= wait_timeout {
                        return Err(std::io::Error::other(format!(
                            "messages not received {:?}",
                            poll.count
                        ))
                        .into());
                    }

                    info!("aeron {:?}", aeron);
                    info!("ctx {:?}", archive_context);
                    if poll.count.get() != 11 {
                        return Err(std::io::Error::other(format!(
                            "expected 11 replayed messages, got {}",
                            poll.count.get()
                        ))
                        .into());
                    }
                    Ok(())
                })();

                drop(subscription);
                poll.release();
                poll_result
            })();

            avail_image_handler.release();
            unavail_image_handler.release();
            replay_result?;
            drop(archive);
            Ok(())
        })();

        drop(aeron);
        drop(media_driver);
        pub_error_frame_handler.release();
        error_handler.release();
        test_result
    }

    /// Archive-side regression test for the close-then-drop double-free / use-after-free.
    ///
    /// In the archive crate the publications / exclusive publications / subscriptions /
    /// counters are owned by the underlying `Aeron` client exactly as in the plain client
    /// crate, so the same ownership/ordering bug applied: closing the `Aeron` client (which
    /// in C frees every registered resource) while a resource handle was still alive used
    /// to double-free / use-after-free when that handle dropped.
    ///
    /// Asserts the process survives closing the client BEFORE dropping the handles.
    #[test]
    #[serial]
    pub fn archive_close_client_then_drop_resources_does_not_double_free(
    ) -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes()
            .expect("failed to kill all java processes");

        let (aeron, _archive_context, media_driver, mut pub_error_frame_handler, mut error_handler) =
            start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            // Acquire one of every client-owned resource type and keep the handles alive.
            let publication =
                aeron.add_publication(AERON_IPC_STREAM, 1001, Duration::from_secs(10))?;
            let exclusive_publication =
                aeron.add_exclusive_publication(AERON_IPC_STREAM, 1002, Duration::from_secs(10))?;
            let subscription = aeron.add_subscription(
                AERON_IPC_STREAM,
                1003,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
                Duration::from_secs(10),
            )?;
            let counter =
                aeron.add_counter(101, &[1, 2, 3, 4], "test-counter", Duration::from_secs(10))?;

            // Close the client FIRST; in C this frees every resource above.
            aeron.close()?;

            // Dropping the still-alive handles must NOT free them again.
            drop(publication);
            drop(exclusive_publication);
            drop(subscription);
            drop(counter);
            Ok(())
        })();

        drop(aeron);
        drop(media_driver);
        pub_error_frame_handler.release();
        error_handler.release();
        test_result
    }

    #[test]
    #[serial]
    fn test_invalid_recording_channel() -> Result<(), Box<dyn Error>> {
        let (aeron, archive_context, media_driver, mut pub_error_frame_handler, mut error_handler) =
            start_aeron_archive()?;
        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        let invalid_channel = "invalid:channel".into_c_string();
        let result =
            archive.start_recording(&invalid_channel, STREAM_ID, SOURCE_LOCATION_LOCAL, true);
        assert!(
            result.is_err(),
            "Expected error when starting recording with an invalid channel"
        );
        drop(archive);
        drop(aeron);
        drop(media_driver);
        pub_error_frame_handler.release();
        error_handler.release();
        Ok(())
    }

    #[test]
    #[serial]
    fn test_stop_recording_on_nonexistent_channel() -> Result<(), Box<dyn Error>> {
        let (aeron, archive_context, media_driver, mut pub_error_frame_handler, mut error_handler) =
            start_aeron_archive()?;
        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        let nonexistent_channel = &"aeron:udp?endpoint=localhost:9999".into_c_string();
        let result = archive.stop_recording_channel_and_stream(nonexistent_channel, STREAM_ID);
        assert!(
            result.is_err(),
            "Expected error when stopping recording on a non-existent channel"
        );
        drop(archive);
        drop(aeron);
        drop(media_driver);
        pub_error_frame_handler.release();
        error_handler.release();
        Ok(())
    }

    #[test]
    #[serial]
    fn test_replay_with_invalid_recording_id() -> Result<(), Box<dyn Error>> {
        let (aeron, archive_context, media_driver, mut pub_error_frame_handler, mut error_handler) =
            start_aeron_archive()?;
        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        let invalid_recording_id = -999;
        let params = AeronArchiveReplayParams::new(0, i32::MAX, 0, 100, 0, 0)?;
        let result = archive.start_replay(
            invalid_recording_id,
            &"aeron:udp?endpoint=localhost:8888".into_c_string(),
            STREAM_ID,
            &params,
        );
        assert!(
            result.is_err(),
            "Expected error when starting replay with an invalid recording id"
        );
        drop(archive);
        drop(aeron);
        drop(media_driver);
        pub_error_frame_handler.release();
        error_handler.release();
        Ok(())
    }

    #[test]
    #[serial]
    fn test_archive_reconnect_after_close() -> Result<(), Box<dyn std::error::Error>> {
        let (aeron, archive_context, media_driver, mut pub_error_frame_handler, mut error_handler) =
            start_aeron_archive()?;
        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        drop(archive);

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
        let new_archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to reconnect to archive");
        assert!(
            new_archive.get_archive_id() > 0,
            "Reconnected archive should have a valid archive id"
        );

        drop(new_archive);
        drop(aeron);
        drop(media_driver);
        pub_error_frame_handler.release();
        error_handler.release();
        Ok(())
    }
}

// run `just slow-tests`
#[cfg(test)]
mod slow_consumer_test;

///////////////////////////////////////////////////////////////////////////////
// Backtest Tests
///////////////////////////////////////////////////////////////////////////////
