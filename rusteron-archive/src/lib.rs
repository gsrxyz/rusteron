/**/
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![allow(unsafe_op_in_unsafe_fn)]
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
#[cfg(feature = "multi-threaded")]
use std::sync::atomic::Ordering;
use std::time::Instant;

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
pub const PUBLICATION_MAX_POSITION_EXCEEDED: i64 = bindings::AERON_PUBLICATION_MAX_POSITION_EXCEEDED as i64;
pub const PUBLICATION_ERROR: i64 = bindings::AERON_PUBLICATION_ERROR as i64;

pub mod testing;

#[cfg(test)]
pub mod persistent_subscription_integration;
#[cfg(test)]
pub mod persistent_subscription_tests;

include!(concat!(env!("OUT_DIR"), "/aeron.rs"));
include!(concat!(env!("OUT_DIR"), "/aeron_custom.rs"));

pub type SourceLocation = bindings::aeron_archive_source_location_t;
pub const SOURCE_LOCATION_LOCAL: aeron_archive_source_location_en = SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL;
pub const SOURCE_LOCATION_REMOTE: aeron_archive_source_location_en =
    SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_REMOTE;

pub struct RecordingPos;
impl RecordingPos {
    /// Finds a recording counter in any archive. Prefer
    /// [`Self::find_counter_id_by_session_in_archive`] when archives share a driver.
    pub fn find_counter_id_by_session(counter_reader: &AeronCountersReader, session_id: i32) -> i32 {
        Self::find_counter_id_matching(counter_reader, |key| key[8..12] == session_id.to_ne_bytes())
    }
    /// Finds a recording counter in any archive. Prefer
    /// [`Self::find_counter_id_by_recording_id_in_archive`] when archives share a driver.
    pub fn find_counter_id_by_recording_id(counter_reader: &AeronCountersReader, recording_id: i64) -> i32 {
        Self::find_counter_id_matching(counter_reader, |key| key[..8] == recording_id.to_ne_bytes())
    }

    fn find_counter_id_matching(counter_reader: &AeronCountersReader, mut matches: impl FnMut(&[u8]) -> bool) -> i32 {
        let mut result = AERON_NULL_COUNTER_ID;
        counter_reader.foreach_counter_fn(|_, counter_id, type_id, key, _| {
            if result == AERON_NULL_COUNTER_ID
                && type_id == AERON_COUNTER_ARCHIVE_RECORDING_POSITION_TYPE_ID as i32
                && key.len() >= 12
                && matches(key)
            {
                result = counter_id;
            }
        });
        result
    }

    /// Finds the recording position counter for a session in the specified archive.
    pub fn find_counter_id_by_session_in_archive(
        counter_reader: &AeronCountersReader,
        session_id: i32,
        archive_id: i64,
    ) -> i32 {
        unsafe {
            aeron_archive_recording_pos_find_counter_id_by_session_id(
                counter_reader.get_inner(),
                session_id,
                archive_id,
            )
        }
    }

    /// Finds the recording position counter for a recording in the specified archive.
    pub fn find_counter_id_by_recording_id_in_archive(
        counter_reader: &AeronCountersReader,
        recording_id: i64,
        archive_id: i64,
    ) -> i32 {
        unsafe {
            aeron_archive_recording_pos_find_counter_id_by_recording_id(
                counter_reader.get_inner(),
                recording_id,
                archive_id,
            )
        }
    }

    /// Copies the recording's source identity, reserving one byte for C's NUL terminator.
    /// Returns the number of identity bytes copied, excluding the terminator.
    pub fn get_source_identity(
        counter_reader: &AeronCountersReader,
        counter_id: i32,
        destination: &mut [u8],
    ) -> Result<usize, AeronCError> {
        counter_reader.aeron_archive_recording_pos_get_source_identity(counter_id, destination)
    }

    /// Whether `counter_id` is still the position counter of `recording_id`. It turns false
    /// once the recording stops: its counter is freed, and the id may later be reused. Check
    /// it while waiting for a recording to reach a position, which a stopped one never does.
    ///
    /// # Errors
    ///
    /// Aeron could not read the counter.
    pub fn is_active(
        counters_reader: &AeronCountersReader,
        counter_id: i32,
        recording_id: i64,
    ) -> Result<bool, AeronCError> {
        counters_reader.aeron_archive_recording_pos_is_active(counter_id, recording_id)
    }

    /// The recording id in the key of `counter_id`, a recording position counter, retried
    /// until `wait` elapses.
    ///
    /// # Errors
    ///
    /// The counter is not an allocated recording position counter within `wait`.
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

    /// The recording id in the key of `counter_id`, a recording position counter.
    ///
    /// # Errors
    ///
    /// The counter is not an allocated recording position counter.
    pub fn get_recording_id(counters_reader: &AeronCountersReader, counter_id: i32) -> Result<i64, AeronCError> {
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
        counters_reader.foreach_counter_fn(|value, id, type_id, key, label| {
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

/// # Exclusive use of the archive client
///
/// The merge reads its replay and recording-position responses from the archive client's
/// shared control response stream. Until it has merged or failed, poll only the merge and use
/// that archive client for nothing else: `do_work`, `poll_for_error`, signal polls, blocking
/// calls such as `get_max_recorded_position` or `start_replay` all read the same stream and
/// skip responses that are not theirs, stalling the merge until its progress timeout. Archive errors reach the caller as `Err` from the merge's
/// own poll, and [`Self::has_failed`] reports a failed merge.
impl AeronArchiveReplayMerge {}

impl AeronArchive {
    pub fn aeron(&self) -> Aeron {
        self.get_archive_context().get_aeron()
    }

    /// One duty-cycle step for this archive and its Aeron client: runs the client's
    /// conductor when it uses the agent invoker, then hands one recording signal to the
    /// context's consumer, or one archive error to its error handler. Call it once a cycle,
    /// and poll each persistent subscription on its own. Do not call it while an
    /// [`AeronArchiveReplayMerge`] on this archive is in progress: the merge reads the same
    /// responses, and one taken here stalls it until its progress timeout.
    ///
    /// Every persistent subscription poll also runs an agent-invoker client's conductor,
    /// so with many persistent subscriptions on one client, give it its conductor thread.
    ///
    /// Client conductor faults go only to the client's error handler, and a lost archive
    /// is not reported here: [`Self::get_control_response_subscription`] stops being
    /// connected instead.
    ///
    /// # Errors
    ///
    /// The archive returned an error and the context has no error handler, or a control
    /// response could not be read.
    #[inline]
    pub fn do_work(&self) -> Result<i32, AeronCError> {
        let aeron = self.aeron();
        let conductor = if aeron.context().get_use_conductor_agent_invoker() {
            aeron.main_do_work()?
        } else {
            0
        };
        Ok(conductor + self.poll_for_recording_signals()?)
    }

    /// Find the latest recording matching a predicate.
    /// Returns the recording with the highest recording_id that matches the predicate.
    pub fn find_recording<F>(&self, mut predicate: F) -> Result<Option<RecordingDescriptor>, AeronCError>
    where
        F: FnMut(&RecordingDescriptor) -> bool,
    {
        // Find the latest matching recording by fetching all recordings in one call.
        // Uses record_count=i32::MAX to fetch all available recordings.
        let mut result = None;
        let mut count = 0;
        self.list_recordings_fn(&mut count, 0, i32::MAX, |desc| {
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
    pub fn find_recording_for_stream(&self, stream_id: i32) -> Result<Option<RecordingDescriptor>, AeronCError> {
        self.find_recording(|desc| desc.stream_id == stream_id)
    }

    /// Collect all recordings matching a predicate.
    pub fn collect_recordings<F>(&self, mut predicate: F) -> Result<Vec<RecordingDescriptor>, AeronCError>
    where
        F: FnMut(&RecordingDescriptor) -> bool,
    {
        // Collect all matching recordings by fetching all recordings in one call.
        // Uses record_count=i32::MAX to fetch all available recordings.
        let mut recordings = Vec::new();
        let mut count = 0;
        self.list_recordings_fn(&mut count, 0, i32::MAX, |desc| {
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
        // SAFETY: a terminal poll frees the connect and marks it released, so the cleanup
        // deletes only an abandoned one. The held client outlives it, which keeps cancelling
        // its adds safe.
        let resource_async = ManagedCResource::new(
            |connect| unsafe { aeron_archive_async_connect(connect, ctx.into()) },
            Some(Box::new(|connect| unsafe {
                aeron_archive_async_connect_delete(*connect)
            })),
            false,
        )?;
        let result = Self {
            inner: CResource::OwnedOnHeap(RcOrArc::new(resource_async)),
        };
        result.inner.add_dependency(ctx.clone());
        result.inner.add_dependency(aeron.clone());
        Ok(result)
    }
}

unsafe extern "C" {
    // Exported by the archive client; declared only in Aeron's internal headers.
    fn aeron_archive_async_connect_delete(connect: *mut aeron_archive_async_connect_t) -> c_int;
}

macro_rules! impl_archive_position_methods {
    ($pub_type:ty) => {
        impl $pub_type {
            /// Retrieves the current active live archive position using the Aeron counters.
            /// Returns an error if not found.
            ///
            /// Each call scans every counter for this session's recording position. In a loop,
            /// find the counter id once with [`RecordingPos::find_counter_id_by_session`] and read
            /// it with [`AeronCountersReader::get_counter_value`].
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
            pub fn get_archive_position_with(&self, counters: &AeronCountersReader) -> Result<i64, AeronCError> {
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
            ///
            /// Scans every counter on each call, as [`Self::get_archive_position`] does, and reads
            /// any error as `false`, so a wait on it spins forever once the recording stops. To wait
            /// in a loop, find the counter once with [`RecordingPos::find_counter_id_by_session`],
            /// then wait while [`AeronCountersReader::get_counter_value`] is below
            /// [`Self::position`], checking [`RecordingPos::is_active`] each time, as
            /// `examples/replay_merge.rs` does.
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
// but this listener has 3 callbacks sharing one clientd. They fire inside the persistent
// poll, so a panic is caught (unwinding through C would break its state) and the poll
// raises it on return
unsafe extern "C" fn persistent_subscription_on_live_joined(clientd: *mut std::ffi::c_void) {
    if !clientd.is_null() {
        // SAFETY: clientd is the ListenerHolder kept alive as a dependency of the subscription.
        let holder = &*(clientd as *const ListenerHolder);
        catch_panic((), || holder.listener.on_live_joined());
    }
}

unsafe extern "C" fn persistent_subscription_on_live_left(clientd: *mut std::ffi::c_void) {
    if !clientd.is_null() {
        let holder = &*(clientd as *const ListenerHolder);
        catch_panic((), || holder.listener.on_live_left());
    }
}

#[cold]
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
        catch_panic((), || holder.listener.on_error(error_code, &msg));
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
    /// afterwards. The context's Aeron client and archive context must outlive the
    /// subscription; [`PersistentSubscriptionBuilder::build`] keeps them open. Creating it
    /// points the archive context at that client; without one, it makes its own and closes
    /// it when it closes, after which the archive context must not be used again.
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
            let result = aeron_archive_persistent_subscription_create(&mut raw_ptr, ctx.get_inner());
            if result < 0 {
                return Err(AeronCError::from_code(result));
            }
        }

        // The C subscription now owns the context. Mark it already-closed so dropping
        // `ctx` frees the Rust bookkeeping without re-running the C context close.
        if let Some(inner) = ctx.inner.as_owned() {
            #[cfg(feature = "multi-threaded")]
            inner.close_already_called.store(true, Ordering::SeqCst);
            #[cfg(not(feature = "multi-threaded"))]
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
            inner: CResource::OwnedOnHeap(RcOrArc::new(resource)),
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
///
/// Callbacks run on the thread that polls the subscription, so a listener may hold
/// thread-local state such as `Rc`.
pub trait PersistentSubscriptionListener: 'static {
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
///
/// The subscription keeps the Aeron client and archive context given here open until it
/// closes. Building also points the archive context at the subscription's client, so
/// build with [`Self::new_with_aeron`], which sets one client on both, as below. Without
/// a client, the subscription makes its own and closes it when it closes, after which the
/// context must not be used to connect again.
///
/// # Examples
///
/// ```no_run
/// use rusteron_archive::*;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let context = AeronContext::new()?;
/// let aeron = Aeron::new(&context)?;
/// aeron.start()?;
/// let archive_context = AeronArchiveContext::new()?;
///
/// let subscription = PersistentSubscriptionBuilder::new_with_aeron(&archive_context, &aeron)?
///     .recording_id(0)?
///     .live_channel("aeron:ipc")?
///     .live_stream_id(1001)?
///     .replay_channel("aeron:udp?endpoint=localhost:0")?
///     .replay_stream_id(1002)?
///     .build()?;
/// while !subscription.is_live() && !subscription.has_failed() {
///     subscription.poll_fn(|message, _header| println!("{} bytes", message.len()), 10)?;
/// }
///
/// subscription.close()?;
/// # Ok(())
/// # }
/// ```
pub struct PersistentSubscriptionBuilder {
    ctx: AeronArchivePersistentSubscriptionContext,
    listener: Option<Box<dyn PersistentSubscriptionListener>>,
    aeron: Option<Aeron>,
    archive_context: Option<AeronArchiveContext>,
}

impl PersistentSubscriptionBuilder {
    /// Create a new builder with a default context.
    pub fn new() -> Result<Self, AeronCError> {
        Ok(Self {
            ctx: AeronArchivePersistentSubscriptionContext::new()?,
            listener: None,
            aeron: None,
            archive_context: None,
        })
    }

    /// A builder whose subscription and `archive_context` both use `aeron`, which it sets on
    /// the context too, so neither is left pointing at another client. The built
    /// subscription keeps both open until it closes.
    ///
    /// # Errors
    ///
    /// Returns the Aeron error if the context cannot be created or either setter fails.
    pub fn new_with_aeron(archive_context: &AeronArchiveContext, aeron: &Aeron) -> Result<Self, AeronCError> {
        archive_context.set_aeron(aeron)?;
        Self::new()?.aeron(aeron)?.archive_context(archive_context)
    }

    /// Set the Aeron client to use.
    ///
    /// # Errors
    ///
    /// Returns the Aeron error if the context rejects the client.
    pub fn aeron(mut self, aeron: &Aeron) -> Result<Self, AeronCError> {
        self.ctx.set_aeron(aeron)?;
        self.aeron = Some(aeron.clone());
        Ok(self)
    }

    /// Set the archive context to use.
    ///
    /// # Errors
    ///
    /// Returns the Aeron error if the context rejects the archive context.
    pub fn archive_context(mut self, ctx: &AeronArchiveContext) -> Result<Self, AeronCError> {
        self.ctx.set_archive_context(ctx)?;
        self.archive_context = Some(ctx.clone());
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
    pub fn listener<L: PersistentSubscriptionListener>(mut self, listener: L) -> Result<Self, AeronCError> {
        self.listener = Some(Box::new(listener));
        Ok(self)
    }

    /// Pre-allocate the state counter so an external observer can read the PS state-machine
    /// state. If unset the PS allocates one itself. Maps to Aeron's `Context.stateCounter`.
    /// The subscription takes the counter over: it closes it, as does setting the slot again.
    /// Keep the counter's client open until then.
    pub fn state_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        let previous = self.ctx.get_state_counter().get_inner();
        self.ctx.set_state_counter(counter)?;
        hand_over(previous, counter);
        Ok(self)
    }

    /// Counter holding the byte gap between replay and live when the live image is added.
    /// Maps to Aeron's `Context.joinDifferenceCounter`. Taken over as in [`Self::state_counter`].
    pub fn join_difference_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        let previous = self.ctx.get_join_difference_counter().get_inner();
        self.ctx.set_join_difference_counter(counter)?;
        hand_over(previous, counter);
        Ok(self)
    }

    /// Counter holding the number of times the PS has dropped off the live stream.
    /// Maps to Aeron's `Context.liveLeftCounter`. Taken over as in [`Self::state_counter`].
    pub fn live_left_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        let previous = self.ctx.get_live_left_counter().get_inner();
        self.ctx.set_live_left_counter(counter)?;
        hand_over(previous, counter);
        Ok(self)
    }

    /// Counter holding the number of times the PS has switched to the live stream.
    /// Maps to Aeron's `Context.liveJoinedCounter`. Taken over as in [`Self::state_counter`].
    pub fn live_joined_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        let previous = self.ctx.get_live_joined_counter().get_inner();
        self.ctx.set_live_joined_counter(counter)?;
        hand_over(previous, counter);
        Ok(self)
    }

    /// Build the persistent subscription, which keeps its Aeron client and archive context
    /// open until it closes.
    pub fn build(mut self) -> Result<AeronArchivePersistentSubscription, AeronCError> {
        let subscription = AeronArchivePersistentSubscription::create(self.ctx, self.listener.take())?;
        // the subscription uses both until its C close, and they drop after it in this order
        if let Some(inner) = subscription.inner.as_owned() {
            if let Some(context) = self.archive_context.take() {
                inner.add_dependency(context);
            }
            if let Some(aeron) = self.aeron.take() {
                inner.add_dependency(aeron);
            }
        }
        Ok(subscription)
    }
}

/// Leaves `counter` to the persistent subscription context, which closes every counter it
/// holds, so the Rust handle does not close it a second time. The context no longer holds
/// `previous`, the counter `counter` replaces, so that one is closed here.
fn hand_over(previous: *mut aeron_counter_t, counter: &AeronCounter) {
    if !previous.is_null() && previous != counter.get_inner() {
        // SAFETY: the context held `previous`, so it is open, and nothing else closes it.
        unsafe { aeron_counter_close(previous, None, std::ptr::null_mut()) };
    }
    if let Some(inner) = counter.inner.as_owned() {
        #[cfg(feature = "multi-threaded")]
        inner.close_already_called.store(true, Ordering::SeqCst);
        #[cfg(not(feature = "multi-threaded"))]
        inner.close_already_called.set(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::{error, info};

    use crate::testing::{EmbeddedArchiveMediaDriverProcess, valgrind_timeout};

    #[test]
    fn archive_error_parse_extracts_error_code() {
        // exact shape emitted by aeron_archive_client.c
        let msg = "(-11) generic error, see message\n[aeron_archive_poll_for_response, aeron_archive_client.c:2105] response for correlationId=32, errorCode=5, error: unknown recording id: 424242\n";
        let err = AeronArchiveError::parse(msg);
        assert_eq!(err.code, AeronArchiveErrorCode::UnknownRecording);
        assert!(err.message.contains("unknown recording id"));

        assert_eq!(
            AeronArchiveError::parse("errorCode=11, error: no space").code,
            AeronArchiveErrorCode::StorageSpace
        );
        assert!(AeronArchiveError::parse("errorCode=11, x").code.is_resource_exhausted());
        assert_eq!(
            AeronArchiveError::parse("errorCode=99, ?").code,
            AeronArchiveErrorCode::Unknown(99)
        );
        // client-mapped codes (wire + 200) as embedded by the error handler since 1.53.3
        assert_eq!(
            AeronArchiveError::parse("response for correlationId=7, errorCode=205, error: x").code,
            AeronArchiveErrorCode::UnknownRecording
        );
        assert_eq!(
            AeronArchiveError::parse("errorCode=216, x").code,
            AeronArchiveErrorCode::InvalidPosition
        );
        assert_eq!(
            AeronArchiveError::parse("errorCode=217, x").code,
            AeronArchiveErrorCode::Unknown(217)
        );
        // no code present -> Generic
        assert_eq!(
            AeronArchiveError::parse("subscription to archive is not connected").code,
            AeronArchiveErrorCode::Generic
        );
    }

    #[test]
    fn archive_error_codes_round_trip() {
        for code in 0..=16 {
            let parsed = AeronArchiveErrorCode::from_code(code);
            assert_ne!(
                parsed,
                AeronArchiveErrorCode::Unknown(code),
                "code {code} must map to a named variant"
            );
            assert_eq!(AeronArchiveErrorCode::from_code(code + 200), parsed);
        }
    }

    #[test]
    fn archive_client_error_codes_match_bindings() {
        for (constant, variant) in [
            (
                AERON_ARCHIVE_CLIENT_ERROR_REENTRANT_CALL,
                AeronArchiveErrorCode::ReentrantCall,
            ),
            (
                AERON_ARCHIVE_CLIENT_ERROR_NOT_CONNECTED,
                AeronArchiveErrorCode::NotConnected,
            ),
            (
                AERON_ARCHIVE_CLIENT_ERROR_ILLEGAL_STATE,
                AeronArchiveErrorCode::IllegalState,
            ),
        ] {
            let code = constant as i32;
            assert_eq!(AeronArchiveErrorCode::from_code(code), variant);
            assert_eq!(AeronArchiveErrorCode::from_code(-code), variant);
            assert_eq!(
                AeronArchiveError::parse(&format!("errorCode={code}, failure")).code,
                variant
            );
            // FFI functions return -1; the actual local client code lives in TLS.
            unsafe {
                aeron_err_set(
                    -code,
                    c"test".as_ptr(),
                    c"test".as_ptr(),
                    0,
                    c"local client failure".as_ptr(),
                );
            }
            assert_eq!(AeronArchiveError::from_code(-1).code, variant);
        }
        unsafe { aeron_err_clear() };
    }

    #[test]
    fn recording_position_key_layout_matches_wire_format() {
        use std::mem::{align_of, offset_of, size_of};
        assert_eq!(size_of::<aeron_archive_recording_position_key_layout_t>(), 16);
        assert_eq!(align_of::<aeron_archive_recording_position_key_layout_t>(), 4);
        assert_eq!(
            offset_of!(aeron_archive_recording_position_key_layout_t, recording_id),
            0
        );
        assert_eq!(offset_of!(aeron_archive_recording_position_key_layout_t, session_id), 8);
        assert_eq!(
            offset_of!(aeron_archive_recording_position_key_layout_t, source_identity_length),
            12
        );
    }

    #[test]
    #[serial_test::serial]
    fn persistent_subscription_context_borrow_and_transfer_close_in_order() -> Result<(), Box<dyn std::error::Error>> {
        let (aeron, archive_context, media_driver, publication_errors, errors) = start_aeron_archive()?;
        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            for transfer in [false, true] {
                let subscription = PersistentSubscriptionBuilder::new_with_aeron(&archive_context, &aeron)?
                    .recording_id(42)?
                    .live_channel("aeron:ipc")?
                    .live_stream_id(7)?
                    .replay_channel("aeron:ipc")?
                    .replay_stream_id(8)?
                    .build()?;
                // The subscription is open; borrowed pointers are only compared, never closed.
                let borrowed = unsafe { subscription.get_persistent_subscription_context() };
                assert!(!borrowed.is_null());
                if transfer {
                    let owned = unsafe { subscription.get_and_own_persistent_subscription_context() };
                    assert_eq!(borrowed, owned);
                    subscription.close()?;
                    // The client and archive context remain alive until after this close.
                    assert_eq!(unsafe { aeron_archive_persistent_subscription_context_close(owned) }, 0);
                } else {
                    subscription.close()?;
                }
            }
            Ok(())
        })();
        drop(aeron);
        drop(archive_context);
        drop(media_driver);
        drop(publication_errors);
        drop(errors);
        result
    }

    #[test]
    #[serial_test::serial]
    fn recording_counter_lookup_isolates_archives_and_copies_source_identity() -> Result<(), Box<dyn std::error::Error>>
    {
        let (aeron, archive_context, media_driver, publication_errors, errors) = start_aeron_archive()?;
        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            let make_key = |archive_id: i64| {
                let mut key = Vec::from(42_i64.to_ne_bytes());
                key.extend_from_slice(&7_i32.to_ne_bytes());
                key.extend_from_slice(&3_i32.to_ne_bytes());
                key.extend_from_slice(b"ipc");
                key.extend_from_slice(&archive_id.to_ne_bytes());
                key
            };
            let first = aeron.add_counter(
                AERON_COUNTER_ARCHIVE_RECORDING_POSITION_TYPE_ID as i32,
                &make_key(10),
                "first archive",
                Duration::from_secs(5),
            )?;
            let second = aeron.add_counter(
                AERON_COUNTER_ARCHIVE_RECORDING_POSITION_TYPE_ID as i32,
                &make_key(20),
                "second archive",
                Duration::from_secs(5),
            )?;
            let reader = aeron.counters_reader();
            let first_id = first.get_constants()?.counter_id;
            let second_id = second.get_constants()?.counter_id;
            assert_eq!(RecordingPos::find_counter_id_by_session(&reader, 7), first_id);
            assert_eq!(RecordingPos::find_counter_id_by_recording_id(&reader, 42), first_id);
            for (archive_id, expected) in [(10, first_id), (20, second_id), (-1, -1), (30, -1)] {
                assert_eq!(
                    RecordingPos::find_counter_id_by_session_in_archive(&reader, 7, archive_id),
                    expected
                );
                assert_eq!(
                    RecordingPos::find_counter_id_by_recording_id_in_archive(&reader, 42, archive_id),
                    expected
                );
            }
            assert_eq!(RecordingPos::find_counter_id_by_session(&reader, 99), -1);
            assert_eq!(RecordingPos::find_counter_id_by_recording_id(&reader, 99), -1);
            for (capacity, expected) in [(0, &b""[..]), (1, &b"\0"[..]), (3, &b"ip\0"[..]), (4, &b"ipc\0"[..])] {
                let mut buffer = vec![0xff; capacity];
                let copied = RecordingPos::get_source_identity(&reader, second_id, &mut buffer)?;
                assert_eq!(copied, capacity.saturating_sub(1).min(3));
                assert_eq!(buffer, expected);
            }
            assert!(RecordingPos::get_source_identity(&reader, -1, &mut [0; 4]).is_err());
            Ok(())
        })();
        drop(aeron);
        drop(archive_context);
        drop(media_driver);
        drop(publication_errors);
        drop(errors);
        result
    }

    /// Pins the enum to the aeron C source: every `ARCHIVE_ERROR_CODE_*` define the
    /// submodule has must map to a named variant. When `just update-aeron-version`
    /// pulls a release that adds or renumbers codes, this fails and points at the gap.
    #[test]
    fn archive_error_codes_match_the_c_source() {
        let header = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/aeron/aeron-archive/src/main/c/client/aeron_archive_client.c"
        ))
        .expect("aeron submodule source missing");
        let mut found = 0;
        for line in header.lines() {
            let Some(rest) = line.trim().strip_prefix("#define ARCHIVE_ERROR_CODE_") else {
                continue;
            };
            let mut parts = rest.split_whitespace();
            let name = parts.next().unwrap_or_default();
            let value: i32 = parts
                .next()
                .unwrap_or_default()
                .trim_matches(|c| c == '(' || c == ')')
                .parse()
                .unwrap_or_else(|_| panic!("unparseable value for ARCHIVE_ERROR_CODE_{name}"));
            let parsed = AeronArchiveErrorCode::from_code(value);
            assert_ne!(
                parsed,
                AeronArchiveErrorCode::Unknown(value),
                "C source defines ARCHIVE_ERROR_CODE_{name} = {value} but AeronArchiveErrorCode has no variant for it"
            );
            assert_eq!(
                format!("{parsed:?}").to_lowercase(),
                name.replace('_', "").to_lowercase(),
                "ARCHIVE_ERROR_CODE_{name} = {value} maps to the wrong AeronArchiveErrorCode variant"
            );
            found += 1;
        }
        assert!(
            found >= 17,
            "expected at least 17 ARCHIVE_ERROR_CODE_* defines, found {found}"
        );
    }
    use serial_test::serial;
    use std::cell::Cell;
    use std::error;
    use std::error::Error;
    use std::os::raw::c_int;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::{JoinHandle, sleep};
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
    pub const ARCHIVE_RECORDING_EVENTS: &str = "aeron:udp?control-mode=dynamic|control=localhost:8012";

    #[test]
    fn test_uri_string_builder() -> Result<(), AeronCError> {
        let builder = AeronUriStringBuilder::default();

        builder.init_new()?;
        builder
            .media(Media::Udp)? // very important to set media else set_initial_position will give an error of -1
            .mtu_length(1024 * 64)?
            .set_initial_position(127424949617280, 1182294755, 65536)?;
        let uri = builder.build(1024)?;
        assert_eq!(
            "aeron:udp?term-id=-1168322114|term-length=65536|mtu=65536|init-term-id=1182294755|term-offset=33408",
            uri
        );

        builder.init_new()?;
        let uri = builder
            .media(Media::Udp)?
            .control_mode(ControlMode::Dynamic)?
            .reliable(false)?
            .ttl(2)?
            .endpoint("localhost:1235")?
            .control("localhost:1234")?
            .build(1024)?;
        assert_eq!(
            "aeron:udp?ttl=2|control-mode=dynamic|endpoint=localhost:1235|control=localhost:1234|reliable=false",
            uri
        );

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

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().expect("failed to kill all java processes");

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

        let (session_id, publisher_thread) = reply_merge_publisher(&archive, &aeron_dir, running.clone())?;

        {
            let context = AeronContext::new()?;
            context.set_dir(&media_driver.aeron_dir)?;
            let error_handler = Handler::new(ErrorCount::default());
            context.set_error_handler(Some(error_handler.clone()))?;
            context.set_driver_timeout_ms(60_000)?;

            // Wrap fallible code so teardown ordering holds even on error/panic
            let inner: Result<(), AeronCError> = (|| {
                let aeron = Aeron::new(&context)?;
                aeron.start()?;
                let source_archive_context = archive.get_archive_context();
                let aeron_archive_context = AeronArchiveContext::new()?;
                aeron_archive_context.set_aeron(&aeron)?;
                aeron_archive_context.set_control_request_channel(
                    &source_archive_context.get_control_request_channel().into_c_string(),
                )?;
                aeron_archive_context.set_control_response_channel(
                    &source_archive_context.get_control_response_channel().into_c_string(),
                )?;
                aeron_archive_context.set_recording_events_channel(
                    &source_archive_context.get_recording_events_channel().into_c_string(),
                )?;
                aeron_archive_context.set_message_timeout_ns(60_000_000_000)?;
                aeron_archive_context.set_error_handler(Some(error_handler.clone()))?;
                let merge_archive = AeronArchiveAsyncConnect::new_with_aeron(&aeron_archive_context, &aeron)?
                    .poll_blocking(Duration::from_secs(60))?;
                replay_merge_subscription(&merge_archive, aeron.clone(), session_id)?;
                Ok(())
            })();

            inner?;
        }

        running.store(false, Ordering::Release);
        publisher_thread.join().unwrap();
        drop(media_driver);

        Ok(())
    }

    /// Publishes on its own thread and client (without `multi-threaded` a publication stays
    /// on the thread whose client created it), recorded by `archive` once the thread has
    /// sent its session id.
    fn reply_merge_publisher(
        archive: &AeronArchive,
        aeron_dir: &str,
        running: Arc<AtomicBool>,
    ) -> Result<(i32, JoinHandle<()>), AeronCError> {
        let (session_tx, session_rx) = std::sync::mpsc::channel();
        let aeron_dir = aeron_dir.to_string();
        let publisher_thread = thread::spawn(move || {
            let aeron = Aeron::connect_dir(&aeron_dir).expect("publisher client");
            let publication = aeron
                .add_publication(
                    &format!("aeron:udp?control={CONTROL_ENDPOINT}|control-mode=dynamic|term-length=65536")
                        .into_c_string(),
                    STREAM_ID,
                    Duration::from_secs(5),
                )
                .expect("publication");
            info!(
                "publication {} [status={:?}]",
                publication.channel(),
                publication.channel_status()
            );
            assert_eq!(1, publication.channel_status());
            let session_id = publication.session_id();
            session_tx.send(session_id).expect("session id");

            info!("waiting for publisher to be connected");
            while !publication.is_connected() && running.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(100));
            }
            info!("publisher to be connected");
            let counters_reader = aeron.counters_reader();
            let counter_id = crate::testing::find_counter_id_by_session_blocking(
                &counters_reader,
                session_id,
                Duration::from_secs(10),
            )
            .expect("recording counter");
            let recording_id =
                RecordingPos::get_recording_id_block(&counters_reader, counter_id, Duration::from_secs(5))
                    .expect("recording id");
            let mut caught_up_count = 0;
            let mut message_count = 0;

            while running.load(Ordering::Acquire) {
                let message = format!("{}{}", MESSAGE_PREFIX, message_count);
                while publication.offer_raw(message.as_bytes(), Handlers::NONE) <= 0 {
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
                    while counters_reader.get_counter_value(counter_id) < publication.position() {
                        assert!(
                            RecordingPos::is_active(&counters_reader, counter_id, recording_id).unwrap_or(false),
                            "the recording stopped"
                        );
                        thread::sleep(Duration::from_micros(300));
                    }
                    caught_up_count += 1;
                }
            }
            assert!(caught_up_count > 0);
            if let Err(err) = publication.close() {
                info!("publisher close returned error: {err:?}");
            }
            info!("Publisher thread terminated");
        });

        let session_id = session_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| AeronCError::with_message(-1, "the publisher thread sent no session id"))?;
        let recording_channel =
            format!("aeron:udp?endpoint={RECORDING_ENDPOINT}|control={CONTROL_ENDPOINT}|session-id={session_id}");
        info!("recording channel {}", recording_channel);
        archive.start_recording(
            &recording_channel.into_c_string(),
            STREAM_ID,
            SOURCE_LOCATION_REMOTE,
            true,
        )?;
        Ok((session_id, publisher_thread))
    }

    fn replay_merge_subscription(archive: &AeronArchive, aeron: Aeron, session_id: i32) -> Result<(), AeronCError> {
        // let replay_channel = format!("aeron:udp?control-mode=manual|session-id={session_id}");
        let replay_channel = format!("aeron:udp?session-id={session_id}").into_c_string();
        info!("replay channel {:?}", replay_channel);

        let replay_destination = format!("aeron:udp?endpoint={REPLAY_ENDPOINT}").into_c_string();
        info!("replay destination {:?}", replay_destination);

        let live_destination = format!("aeron:udp?endpoint={LIVE_ENDPOINT}|control={CONTROL_ENDPOINT}").into_c_string();
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
        //     archive.list_recordings_fn(&mut count, 0, 1000, |descriptor| {
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
        let recording_id =
            RecordingPos::get_recording_id_block(&aeron.counters_reader(), counter_id, Duration::from_secs(5))?;

        let subscribe_channel = format!("aeron:udp?control-mode=manual|session-id={session_id}").into_c_string();
        info!("subscribe channel {:?}", subscribe_channel);
        let subscription = aeron.add_subscription(
            &subscribe_channel,
            STREAM_ID,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_secs(5),
        )?;

        info!(
            "about to start_replay [maxRecordPosition={:?}]",
            archive.get_max_recorded_position(recording_id)
        );

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
            recording_id, start_position, subscribe_channel, &replay_channel, &replay_destination, &live_destination
        );

        // media_driver
        //     .run_aeron_stats()
        //     .expect("Failed to run aeron stats");

        // info!("Waiting for subscription to connect...");
        // while !subscription.is_connected() {
        //     thread::sleep(Duration::from_millis(100));
        // }
        // info!("Subscription connected");

        let mut reply_count = 0;
        while !replay_merge.is_merged() {
            assert!(!replay_merge.has_failed());
            if replay_merge.poll_fn(
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
                if Aeron::errmsg().len() > 0 && "no error" != Aeron::errmsg() {
                    panic!("{}", Aeron::errmsg());
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
        assert!(!replay_merge.has_failed());
        assert!(replay_merge.is_live_added());
        assert!(reply_count > 10_000, "no replay-merge fragments received");
        Ok(())
    }

    #[test]
    fn version_check() {
        let major = unsafe { crate::aeron_version_major() };
        let minor = unsafe { crate::aeron_version_minor() };
        let patch = unsafe { crate::aeron_version_patch() };

        let aeron_version = format!("{}.{}.{}", major, minor, patch);

        let cargo_version = include_str!("../aeron/version.txt").trim().split('-').next().unwrap();
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
        let recording_event_port = find_unused_udp_port(response_port + 1).expect("Could not find port");
        let request_control_channel = &format!("aeron:udp?endpoint=localhost:{}", request_port);
        let response_control_channel = &format!("aeron:udp?endpoint=localhost:{}", response_port);
        let recording_events_channel = &format!("aeron:udp?endpoint=localhost:{}", recording_event_port);
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
        aeron_context.set_client_name(c"test")?;
        let pub_error_frame_handler = Handler::new(AeronPublicationErrorFrameHandlerLogger);
        aeron_context.set_publication_error_frame_handler(Some(pub_error_frame_handler.clone()))?;
        let error_handler = Handler::new(ErrorCount::default());
        aeron_context.set_error_handler(Some(error_handler.clone()))?;

        // Use inner closure so teardown ordering holds on any error path after handlers are created
        let inner: Result<(Aeron, AeronArchiveContext), Box<dyn Error>> = (|| {
            let aeron = Aeron::new(&aeron_context)?;
            aeron.start()?;
            let archive_context = AeronArchiveContext::new()?;
            archive_context.set_aeron(&aeron)?;
            archive_context.set_control_request_channel(&request_control_channel.as_str().into_c_string())?;
            archive_context.set_control_response_channel(&response_control_channel.as_str().into_c_string())?;
            archive_context.set_recording_events_channel(&recording_events_channel.as_str().into_c_string())?;
            archive_context.set_error_handler(Some(error_handler.clone()))?;
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
            Err(e) => Err(e),
        }
    }

    /// Deep-graph close: closing a *clone* of the archive client defers the C close —
    /// the original stays fully usable (control session intact, error polling clean).
    #[test]
    #[serial]
    pub fn archive_clone_close_defers_and_original_remains_usable() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().expect("failed to kill all java processes");

        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            let archive = AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?
                .poll_blocking(Duration::from_secs(30))
                .expect("failed to connect to aeron archive media driver");

            let session_id = archive.control_session_id();
            let clone = archive.clone();
            assert!(clone.close().is_ok());

            // original remains fully usable after the clone's close
            assert_eq!(session_id, archive.control_session_id());
            assert!(archive.poll_for_error()?.is_none());
            let subscription_id = archive.start_recording(AERON_IPC_STREAM, 42, SOURCE_LOCATION_LOCAL, true)?;
            assert!(subscription_id >= 0);
            archive.stop_recording_subscription(subscription_id)?;

            assert!(archive.close().is_ok());
            Ok(())
        })();

        drop(aeron);
        drop(archive_context);
        drop(media_driver);
        drop(pub_error_frame_handler);
        drop(error_handler);
        test_result
    }

    /// A listener may hold thread-local state: its callbacks run on the polling thread.
    #[test]
    fn persistent_subscription_listener_may_hold_rc_state() -> Result<(), AeronCError> {
        struct Joins(std::rc::Rc<Cell<u32>>);
        impl PersistentSubscriptionListener for Joins {
            fn on_live_joined(&self) {
                self.0.set(self.0.get() + 1);
            }
        }
        let joins = std::rc::Rc::new(Cell::new(0));
        PersistentSubscriptionBuilder::new()?.listener(Joins(std::rc::Rc::clone(&joins)))?;
        assert_eq!(
            std::rc::Rc::strong_count(&joins),
            1,
            "the unbuilt builder drops its listener"
        );
        Ok(())
    }

    /// `do_work` runs an agent-invoker client's conductor and hands the archive's recording
    /// signals to the context's consumer, and needs nothing from a client with its own
    /// conductor thread.
    #[test]
    #[serial]
    pub fn do_work_drives_the_client_conductor_and_recording_signals() -> Result<(), Box<dyn error::Error>> {
        use std::sync::atomic::AtomicUsize;

        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().expect("failed to kill all java processes");
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            let threaded = AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?
                .poll_blocking(Duration::from_secs(30))
                .expect("failed to connect to aeron archive media driver");
            threaded.do_work()?;

            let client_context = AeronContext::new()?;
            client_context.set_dir(&aeron.context().get_dir().into_c_string())?;
            client_context.set_use_conductor_agent_invoker(true)?;
            let client = Aeron::new(&client_context)?;
            client.start()?;
            let signals = Arc::new(AtomicUsize::new(0));
            let counted = Arc::clone(&signals);
            let context = AeronArchiveContext::new()?;
            context.set_aeron(&client)?;
            context.set_control_request_channel(&archive_context.get_control_request_channel().into_c_string())?;
            context.set_control_response_channel(&archive_context.get_control_response_channel().into_c_string())?;
            context.set_recording_signal_consumer(Some(move |_signal: AeronArchiveRecordingSignal| {
                counted.fetch_add(1, Ordering::SeqCst);
            }))?;
            // the blocking connect drives the client's conductor itself
            let archive =
                AeronArchiveAsyncConnect::new_with_aeron(&context, &client)?.poll_blocking(Duration::from_secs(30))?;
            archive.start_recording(AERON_IPC_STREAM, 7401, SOURCE_LOCATION_LOCAL, true)?;

            // nothing else runs this client's conductor, so the publication arrives through do_work
            let adding = client.async_add_exclusive_publication(AERON_IPC_STREAM, 7401)?;
            let start = Instant::now();
            let publication = loop {
                archive.do_work()?;
                if let Some(publication) = adding.poll()? {
                    break publication;
                }
                assert!(
                    start.elapsed() < Duration::from_secs(10),
                    "the publication never arrived"
                );
                thread::yield_now();
            };
            let start = Instant::now();
            while signals.load(Ordering::SeqCst) == 0 {
                archive.do_work()?;
                let _ = publication.offer(b"do-work");
                assert!(start.elapsed() < Duration::from_secs(10), "no recording signal arrived");
                thread::yield_now();
            }
            Ok(())
        })();

        drop(aeron);
        drop(archive_context);
        drop(media_driver);
        drop(pub_error_frame_handler);
        drop(error_handler);
        test_result
    }

    /// The context's blocking connects drive an agent-invoker client given only through
    /// `set_aeron`, as nothing else runs its conductor.
    #[test]
    #[serial]
    pub fn context_connect_drives_an_agent_invoker_client() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().expect("failed to kill all java processes");
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            let client_context = AeronContext::new()?;
            client_context.set_dir(&aeron.context().get_dir().into_c_string())?;
            client_context.set_use_conductor_agent_invoker(true)?;
            let client = Aeron::new(&client_context)?;
            client.start()?;
            let context = AeronArchiveContext::new()?;
            context.set_aeron(&client)?;
            context.set_control_request_channel(&archive_context.get_control_request_channel().into_c_string())?;
            context.set_control_response_channel(&archive_context.get_control_response_channel().into_c_string())?;

            let connected = context.aeron_archive_connect(Duration::from_secs(10))?;
            let polled = AeronArchiveAsyncConnect::new(&context)?.poll_blocking(Duration::from_secs(10))?;
            drop(polled);
            drop(connected);
            Ok(())
        })();

        drop(aeron);
        drop(archive_context);
        drop(media_driver);
        drop(pub_error_frame_handler);
        drop(error_handler);
        test_result
    }

    /// A borrowed context (e.g. from `get_archive_context()`) cannot keep a client alive, so
    /// `set_aeron` on it is refused before C is changed, rather than leaving C a pointer the
    /// caller may free. The owning context still accepts it
    #[test]
    #[serial]
    pub fn set_aeron_on_borrowed_context_is_rejected() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes()?;
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            let other_context = AeronContext::new()?;
            other_context.set_dir(&aeron.context().get_dir().into_c_string())?;
            let other = Aeron::new(&other_context)?;
            other.start()?;

            let owned = AeronArchiveContext::new()?;
            owned.set_aeron(&aeron)?;
            let borrowed = AeronArchiveContext::from(owned.get_inner());

            let Err(err) = borrowed.set_aeron(&other) else {
                return Err("a borrowed context must refuse set_aeron".into());
            };
            assert!(err.message().is_some_and(|m| m.contains("borrowed")), "{err:?}");
            // C was not changed: the owner still has its own client
            assert_eq!(owned.get_aeron().get_inner(), aeron.get_inner());

            // the owner can still change it, and keeps the new client alive
            owned.set_aeron(&other)?;
            drop(other);
            assert!(!owned.get_aeron().is_closed());
            Ok(())
        })();

        drop(aeron);
        drop(archive_context);
        drop(media_driver);
        drop(pub_error_frame_handler);
        drop(error_handler);
        test_result
    }

    /// The archive's descriptor pollers are borrowed, so they cannot keep a consumer alive and
    /// `reset` must refuse before C stores a pointer the caller may free
    #[test]
    #[serial]
    pub fn reset_on_archive_descriptor_pollers_is_rejected() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes()?;
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            let archive = AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?
                .poll_blocking(Duration::from_secs(30))?;

            let poller = archive.recording_descriptor_poller();
            let (correlation_id, clientd) = (poller.correlation_id(), poller.recording_descriptor_consumer_clientd());
            let consumer = Handler::new(AeronArchiveRecordingDescriptorConsumerFuncLogger);
            let Err(err) = poller.reset(correlation_id + 1, 1, Some(&consumer)) else {
                return Err("a borrowed recording descriptor poller must refuse reset".into());
            };
            assert!(err.message().is_some_and(|m| m.contains("borrowed")), "{err:?}");
            assert_eq!(poller.correlation_id(), correlation_id);
            assert_eq!(poller.recording_descriptor_consumer_clientd(), clientd);

            let poller = archive.recording_subscription_descriptor_poller();
            let (correlation_id, clientd) = (
                poller.correlation_id(),
                poller.recording_subscription_descriptor_consumer_clientd(),
            );
            let consumer = Handler::new(AeronArchiveRecordingSubscriptionDescriptorConsumerFuncLogger);
            let Err(err) = poller.reset(correlation_id + 1, 1, Some(&consumer)) else {
                return Err("a borrowed recording subscription descriptor poller must refuse reset".into());
            };
            assert!(err.message().is_some_and(|m| m.contains("borrowed")), "{err:?}");
            assert_eq!(poller.correlation_id(), correlation_id);
            assert_eq!(poller.recording_subscription_descriptor_consumer_clientd(), clientd);

            archive.close()?;
            Ok(())
        })();

        drop(aeron);
        drop(archive_context);
        drop(media_driver);
        drop(pub_error_frame_handler);
        drop(error_handler);
        test_result
    }

    /// The context keeps every client passed to `set_aeron` alive, even after the caller drops
    /// theirs or a later call replaces it
    #[test]
    #[serial]
    pub fn set_aeron_keeps_clients_alive_after_caller_drops_them() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes()?;
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            let new_client = || -> Result<Aeron, Box<dyn error::Error>> {
                let ctx = AeronContext::new()?;
                ctx.set_dir(&aeron.context().get_dir().into_c_string())?;
                let client = Aeron::new(&ctx)?;
                client.start()?;
                Ok(client)
            };
            let context = AeronArchiveContext::new()?;
            let (first, second) = (new_client()?, new_client()?);
            let (first_id, second_id) = (first.client_id(), second.client_id());
            context.set_aeron(&first)?;
            context.set_aeron(&second)?;
            drop(first);
            drop(second);

            // the replaced client stays alive, as objects made from it may still use it
            let replaced = context.inner.get_dependency::<Aeron>().ok_or("first client not kept")?;
            assert_eq!(replaced.client_id(), first_id);
            assert!(!replaced.is_closed());

            // without the fix these would use a freed client
            let client = context.get_aeron();
            assert_eq!(client.client_id(), second_id);
            // the harness does not wait for the Java driver, which can take seconds to start on CI
            let timeout = Duration::from_secs(30);
            let subscription =
                client.add_subscription(AERON_IPC_STREAM, 7001, Handlers::NONE, Handlers::NONE, timeout)?;
            let publication = client.add_publication(AERON_IPC_STREAM, 7001, timeout)?;
            let start = Instant::now();
            let mut received = 0;
            while received == 0 {
                assert!(start.elapsed() < timeout, "no message through the context's client");
                let _ = publication.offer(b"alive");
                subscription.poll_fn(|_, _| received += 1, 10)?;
                sleep(Duration::from_millis(1));
            }
            // publication and subscription drop before the context that keeps their client
            Ok(())
        })();

        drop(aeron);
        drop(archive_context);
        drop(media_driver);
        drop(pub_error_frame_handler);
        drop(error_handler);
        test_result
    }

    /// Dropping a connect before it completes releases its control subscription and
    /// publication, as Aeron's own terminal outcomes do.
    #[test]
    #[serial]
    pub fn abandoned_async_connect_releases_its_subscription() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().expect("failed to kill all java processes");
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            let request_port = find_unused_udp_port(9000).expect("Could not find port");
            let response_port = find_unused_udp_port(request_port + 1).expect("Could not find port");
            let context = AeronArchiveContext::new()?;
            context.set_aeron(&aeron)?;
            context
                .set_control_request_channel(&format!("aeron:udp?endpoint=127.0.0.1:{request_port}").into_c_string())?;
            context.set_control_response_channel(
                &format!("aeron:udp?endpoint=127.0.0.1:{response_port}").into_c_string(),
            )?;
            let endpoint = format!("endpoint=127.0.0.1:{response_port}|");
            let receivers = || {
                let mut count = 0;
                aeron.counters_reader().foreach_counter_fn(|_, _, _, _, label: &str| {
                    if label.starts_with("rcv-channel:") && label.contains(&endpoint) {
                        count += 1;
                    }
                });
                count
            };

            let connect = AeronArchiveAsyncConnect::new_with_aeron(&context, &aeron)?;
            let start = Instant::now();
            while receivers() == 0 {
                assert!(
                    start.elapsed() < Duration::from_secs(10),
                    "the subscription never arrived"
                );
                thread::sleep(Duration::from_millis(10));
            }
            drop(connect);

            let start = Instant::now();
            while receivers() != 0 {
                assert!(
                    start.elapsed() < Duration::from_secs(10),
                    "the abandoned connect kept its subscription"
                );
                thread::sleep(Duration::from_millis(10));
            }
            Ok(())
        })();

        drop(aeron);
        drop(archive_context);
        drop(media_driver);
        drop(pub_error_frame_handler);
        drop(error_handler);
        test_result
    }

    #[test]
    #[serial]
    pub fn test_aeron_archive() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().expect("failed to kill all java processes");

        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            assert!(!aeron.is_closed());

            info!("connected to aeron");

            let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
            let archive = archive_connector
                .poll_blocking(Duration::from_secs(30))
                .expect("failed to connect to aeron archive media driver");

            assert!(archive.get_archive_id() > 0);

            let channel = AERON_IPC_STREAM;
            let stream_id = 10;

            let subscription_id = archive.start_recording(channel, stream_id, SOURCE_LOCATION_LOCAL, true)?;

            assert!(subscription_id >= 0);
            info!("subscription id {}", subscription_id);

            let publication = aeron
                .async_add_exclusive_publication(channel, stream_id)?
                .poll_blocking(Duration::from_secs(5))?;

            for i in 0..11 {
                while publication.offer_raw("123456".as_bytes(), Handlers::NONE) <= 0 {
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
                archive.list_recordings_for_uri_fn(
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
            let params =
                AeronArchiveReplayParams::new(0, i32::MAX, start_pos.get(), end_pos.get() - start_pos.get(), 0, 0)?;
            info!("replay params {:#?}", params);
            let replay_stream_id = 45;
            let replay_session_id =
                archive.start_replay(found_recording_id.get(), channel, replay_stream_id, &params)?;
            let session_id = replay_session_id as i32;

            info!("replay session id {}", replay_session_id);
            info!("session id {}", session_id);
            let channel_replay = format!("{}?session-id={}", channel.to_str().unwrap(), session_id).into_c_string();
            info!("archive id: {}", archive.get_archive_id());

            info!("add subscription {:?}", channel_replay);
            let avail_image_handler = Handler::new(AeronAvailableImageLogger);
            let unavail_image_handler = Handler::new(AeronUnavailableImageLogger);
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
                    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
                        assert_eq!(buffer, "123456".as_bytes());

                        // Update count (using Cell for interior mutability)
                        self.count.set(self.count.get() + 1);
                    }
                }

                let poll = Handler::new(FragmentHandler::default());
                let poll_result: Result<(), Box<dyn error::Error>> = (|| {
                    let wait_timeout = Duration::from_secs(30);
                    let start = Instant::now();
                    while start.elapsed() < wait_timeout && subscription.poll(Some(&poll), 100)? <= 0 {
                        let err = archive.poll_for_error_response_as_string(4096)?;
                        if !err.is_empty() {
                            return Err(std::io::Error::other(err).into());
                        }
                    }

                    if start.elapsed() >= wait_timeout {
                        return Err(std::io::Error::other(format!("messages not received {:?}", poll.count)).into());
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
                poll_result
            })();

            replay_result?;
            Ok(())
        })();

        drop(aeron);
        drop(media_driver);
        test_result
    }

    #[test]
    #[serial]
    fn test_invalid_recording_channel() -> Result<(), Box<dyn Error>> {
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;
        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        let invalid_channel = c"invalid:channel";
        let result = archive.start_recording(&invalid_channel, STREAM_ID, SOURCE_LOCATION_LOCAL, true);
        assert!(
            result.is_err(),
            "Expected error when starting recording with an invalid channel"
        );
        drop(media_driver);
        Ok(())
    }

    #[test]
    #[serial]
    fn test_stop_recording_on_nonexistent_channel() -> Result<(), Box<dyn Error>> {
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;
        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        let nonexistent_channel = c"aeron:udp?endpoint=localhost:9999";
        let result = archive.stop_recording_channel_and_stream(nonexistent_channel, STREAM_ID);
        assert!(
            result.is_err(),
            "Expected error when stopping recording on a non-existent channel"
        );
        drop(media_driver);
        Ok(())
    }

    #[test]
    #[serial]
    fn test_replay_with_invalid_recording_id() -> Result<(), Box<dyn Error>> {
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;
        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        let invalid_recording_id = -999;
        let params = AeronArchiveReplayParams::new(0, i32::MAX, 0, 100, 0, 0)?;
        let result = archive.start_replay(
            invalid_recording_id,
            c"aeron:udp?endpoint=localhost:8888",
            STREAM_ID,
            &params,
        );
        assert!(
            result.is_err(),
            "Expected error when starting replay with an invalid recording id"
        );
        drop(media_driver);
        Ok(())
    }

    #[test]
    #[serial]
    fn test_archive_reconnect_after_close() -> Result<(), Box<dyn std::error::Error>> {
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;
        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        archive.close()?;

        // Retry reconnection with exponential backoff to handle race condition
        // where close() is async and the endpoint may still be CLOSING
        let mut retry_delay = Duration::from_millis(100);
        let max_retries = 10;
        let mut new_archive = None;

        for attempt in 0..max_retries {
            let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
            match archive_connector.poll_blocking(valgrind_timeout(5)) {
                Ok(archive) => {
                    new_archive = Some(archive);
                    break;
                }
                Err(e) if attempt < max_retries - 1 => {
                    // Check if error is about CLOSING state - retry with backoff
                    let error_msg = e.to_string();
                    if error_msg.contains("CLOSING") || error_msg.contains("temporarily unavailable") {
                        std::thread::sleep(retry_delay);
                        retry_delay = retry_delay.saturating_mul(2);
                        continue;
                    }
                    // Other errors should fail immediately
                    return Err(format!("Failed to reconnect to archive: {}", e).into());
                }
                Err(e) => {
                    return Err(format!("Failed to reconnect to archive after {} retries: {}", max_retries, e).into());
                }
            }
        }

        let new_archive = new_archive.expect("failed to reconnect to archive after retries");
        assert!(
            new_archive.get_archive_id() > 0,
            "Reconnected archive should have a valid archive id"
        );

        drop(media_driver);
        Ok(())
    }

    #[test]
    #[serial]
    fn test_archive_close_defers_with_live_clone() -> Result<(), Box<dyn std::error::Error>> {
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;
        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");
        let archive_id = archive.get_archive_id();
        assert!(archive_id > 0);

        // Ordering 1: close clone first, original stays alive
        let clone = archive.clone();
        clone.close()?;
        assert_eq!(
            archive_id,
            archive.get_archive_id(),
            "clone close defers while original alive"
        );

        // Ordering 2: close original first, clone stays alive
        let clone2 = archive.clone();
        archive.close()?;
        assert_eq!(
            archive_id,
            clone2.get_archive_id(),
            "original close defers while clone alive"
        );

        drop(media_driver);
        Ok(())
    }

    #[test]
    #[serial]
    fn test_archive_close_does_not_close_aeron_client() -> Result<(), Box<dyn std::error::Error>> {
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;
        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        archive.close()?;

        let publication = aeron
            .add_publication(AERON_IPC_STREAM, 30, Duration::from_secs(5))
            .expect("Aeron client should remain usable after archive close");
        assert!(!publication.get_inner().is_null());
        publication.close()?;

        drop(media_driver);
        Ok(())
    }

    #[test]
    #[serial]
    fn close_now_after_all_archive_children_dropped_is_safe() -> Result<(), Box<dyn std::error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        let (aeron, archive_context, media_driver, _pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        // Build a complex object graph: Aeron subscriptions, archive recording
        let publication = aeron
            .add_publication(AERON_IPC_STREAM, 30, Duration::from_secs(5))
            .expect("add publication");

        let subscription = aeron
            .add_subscription(
                AERON_IPC_STREAM,
                30,
                Handlers::NONE,
                Handlers::NONE,
                Duration::from_secs(5),
            )
            .expect("add subscription");

        let archive = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        let recording_id = archive
            .start_recording(AERON_IPC_STREAM, 30, SOURCE_LOCATION_LOCAL, true)
            .expect("start recording");

        // Publish a few messages so the recording has content
        for _ in 0..5 {
            let _ = publication.offer(b"test data");
        }

        // Drop all Aeron and archive children before close_now
        drop(subscription);
        drop(publication);
        // recording_id is just a u64, not a resource handle

        // Call close_now on the archive — must not segfault or double-free
        unsafe {
            assert!(
                archive.close_now().is_ok(),
                "close_now should succeed after all children dropped"
            );
        }

        // Aeron client should still be usable (archive close doesn't close aeron)
        let pub2 = aeron
            .add_publication(AERON_IPC_STREAM, 31, Duration::from_secs(5))
            .expect("aeron still usable after archive close_now");
        drop(pub2);
        aeron.close()?;

        drop(media_driver);
        drop(error_handler);
        Ok(())
    }
}

// run `just slow-tests`
#[cfg(test)]
mod slow_consumer_test;

///////////////////////////////////////////////////////////////////////////////
// Backtest Tests
///////////////////////////////////////////////////////////////////////////////
