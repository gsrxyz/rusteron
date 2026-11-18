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

#[cfg(test)]
mod descriptor_poller_tests;
#[cfg(test)]
mod replaceable_error_handler_tests;

#[allow(improper_ctypes_definitions)]
#[allow(unpredictable_function_pointer_comparisons)]
// Preserve upstream Doxygen markup and bracketed IPv6 examples in the raw C comments.
#[allow(rustdoc::broken_intra_doc_links, rustdoc::invalid_html_tags)]
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[cfg(doctest)]
mod ownership_contracts;

// The migration guide covers client and archive code, and this crate exposes both APIs
#[cfg(doctest)]
#[doc = include_str!("../../docs/migrating_0.2_to_0.3.md")]
mod migration_guide_doctests {}

#[cfg(doctest)]
#[doc = include_str!("../../docs/archive-guide.md")]
mod archive_guide_doctests {}

/// Compile-time and doctest contracts for the public API.
///
/// The data header flags are exported:
/// ```
/// use rusteron_archive::bindings::*;
/// assert_eq!(AERON_DATA_HEADER_BEGIN_FLAG, 0x80);
/// assert_eq!(AERON_DATA_HEADER_END_FLAG, 0x40);
/// assert_eq!(AERON_DATA_HEADER_EOS_FLAG, 0x20);
/// assert_eq!(AERON_DATA_HEADER_REVOKED_FLAG, 0x10);
/// assert_eq!(AERON_DATA_HEADER_UNFRAGMENTED, 0xC0);
/// ```
///
/// A recording descriptor borrows the callback's stack frame, so it cannot leave the closure
/// ```compile_fail,E0521
/// use rusteron_archive::*;
/// fn escape(archive: &AeronArchive) {
///     let mut count = 0;
///     let mut kept = None;
///     let _ = archive.list_recordings_fn(&mut count, 0, 1, |d| kept = Some(d));
///     drop(kept);
/// }
/// ```
///
/// C keeps a retained handler after this call, so it must not borrow locals
/// ```compile_fail,E0373
/// use rusteron_archive::*;
/// let label = String::from("my archive");
/// let ctx = AeronArchiveContext::new().unwrap();
/// let _ = ctx.set_error_handler(Some(|code: i32, message: &str| {
///     eprintln!("{label}: {code}: {message}");
/// }));
/// ```
///
/// The archive owns its context, so the borrowed context cannot be cloned out of it
/// ```compile_fail,E0599
/// use rusteron_archive::*;
/// fn escape(archive: AeronArchive) -> Result<(), AeronCError> {
///     let context = (*archive.get_archive_context()).clone();
///     drop(archive);
///     context.set_control_request_channel(c"aeron:ipc")?;
///     Ok(())
/// }
/// ```
///
/// Nor can the borrow outlive the archive
/// ```compile_fail,E0505
/// use rusteron_archive::*;
/// fn escape(archive: AeronArchive) -> Result<(), AeronCError> {
///     let context = archive.get_archive_context();
///     drop(archive);
///     context.set_control_request_channel(c"aeron:ipc")?;
///     Ok(())
/// }
/// ```
#[cfg(doctest)]
mod issue_regressions {}

#[cfg(test)]
mod custom_tests;

use bindings::*;
#[cfg(test)]
use std::cell::Cell;
use std::os::raw::c_int;
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

/// A recording descriptor is borrowed from a C stack local that Aeron frees once the callback
/// returns, so it cannot be kept, nor cloned or rewrapped and kept. Copying a field out is fine:
///
/// ```no_run
/// # use rusteron_archive::*;
/// # fn f(archive: &AeronArchive) -> Result<(), AeronArchiveError> {
/// let (mut count, mut id) = (0, None);
/// archive.list_recordings_fn(&mut count, 0, 10, |d| id = Some(d.recording_id()))?;
/// # Ok(()) }
/// ```
///
/// ```compile_fail,E0521
/// # use rusteron_archive::*;
/// # fn f(archive: &AeronArchive) -> Result<(), AeronArchiveError> {
/// let (mut count, mut kept) = (0, None);
/// archive.list_recordings_fn(&mut count, 0, 10, |d| kept = Some(d))?;
/// # Ok(()) }
/// ```
///
/// ```compile_fail,E0521
/// # use rusteron_archive::*;
/// # fn f(archive: &AeronArchive) -> Result<(), AeronArchiveError> {
/// let (mut count, mut kept) = (0, None);
/// archive.list_recordings_fn(&mut count, 0, 10, |d| kept = Some(d.clone()))?;
/// # Ok(()) }
/// ```
///
/// ```compile_fail,E0308
/// # use rusteron_archive::*;
/// # fn f(archive: &AeronArchive) -> Result<(), AeronArchiveError> {
/// let (mut count, mut kept) = (0, None);
/// archive.list_recordings_fn(&mut count, 0, 10, |d| kept = Some(AeronArchiveRecordingDescriptor::from(d.get_inner())))?;
/// # Ok(()) }
/// ```
///
/// ```compile_fail,E0308
/// # use rusteron_archive::*;
/// # fn f(archive: &AeronArchive) -> Result<(), AeronArchiveError> {
/// let (mut count, mut kept) = (0, None);
/// archive.list_recordings_fn(&mut count, 0, 10, |d| kept = Some(AeronArchiveRecordingDescriptor::from(**d)))?;
/// # Ok(()) }
/// ```
#[allow(dead_code)]
mod callback_args_cannot_escape {}

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
    pub fn find_counter_id_by_session(
        counter_reader: &AeronCountersReader<impl ResourceStorage<aeron_counters_reader_t>>,
        session_id: i32,
    ) -> i32 {
        Self::find_counter_id_matching(counter_reader, |key| key[8..12] == session_id.to_ne_bytes())
    }
    /// Finds a recording counter in any archive. Prefer
    /// [`Self::find_counter_id_by_recording_id_in_archive`] when archives share a driver.
    pub fn find_counter_id_by_recording_id(
        counter_reader: &AeronCountersReader<impl ResourceStorage<aeron_counters_reader_t>>,
        recording_id: i64,
    ) -> i32 {
        Self::find_counter_id_matching(counter_reader, |key| key[..8] == recording_id.to_ne_bytes())
    }

    fn find_counter_id_matching(
        counter_reader: &AeronCountersReader<impl ResourceStorage<aeron_counters_reader_t>>,
        mut matches: impl FnMut(&[u8]) -> bool,
    ) -> i32 {
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
        counter_reader: &AeronCountersReader<impl ResourceStorage<aeron_counters_reader_t>>,
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
        counter_reader: &AeronCountersReader<impl ResourceStorage<aeron_counters_reader_t>>,
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
        counter_reader: &AeronCountersReader<impl ResourceStorage<aeron_counters_reader_t>>,
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
        counters_reader: &AeronCountersReader<impl ResourceStorage<aeron_counters_reader_t>>,
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
        counters_reader: &AeronCountersReader<impl ResourceStorage<aeron_counters_reader_t>>,
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
    #[inline]
    pub fn get_recording_id(
        counters_reader: &AeronCountersReader<impl ResourceStorage<aeron_counters_reader_t>>,
        counter_id: i32,
    ) -> Result<i64, AeronCError> {
        // Aeron validates the id, state and type, then reads this counter's key.
        // No need to scan the counter list for an id we already have.
        let recording_id = counters_reader.aeron_archive_recording_pos_get_recording_id(counter_id);
        if recording_id < 0 {
            Err(AeronCError::from_code(-1))
        } else {
            Ok(recording_id)
        }
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
    /// The client this archive uses. Getters chain without binding each step:
    ///
    /// ```
    /// # use rusteron_archive::{AeronArchive, Aeron, Ref};
    /// fn client(archive: &AeronArchive) -> Ref<'_, Aeron> {
    ///     archive.get_archive_context().get_aeron()
    /// }
    /// ```
    ///
    /// Like every getter returning a [`Ref`], the result cannot outlive the archive:
    ///
    /// ```compile_fail,E0505
    /// fn use_after_drop(archive: rusteron_archive::AeronArchive) {
    ///     let context = archive.get_archive_context();
    ///     drop(archive);
    ///     context.get_control_request_stream_id();
    /// }
    /// ```
    ///
    /// `close()` takes the archive by value, so it is rejected the same way:
    ///
    /// ```compile_fail,E0505
    /// fn use_after_close(archive: rusteron_archive::AeronArchive) {
    ///     let context = archive.get_archive_context();
    ///     let _ = archive.close();
    ///     context.get_control_request_stream_id();
    /// }
    /// ```
    ///
    /// Borrowed views do not implement `Clone`:
    ///
    /// ```compile_fail,E0599
    /// fn use_clone_after_drop(archive: rusteron_archive::AeronArchive) {
    ///     let context = archive.get_archive_context().clone();
    ///     drop(archive);
    ///     context.get_control_request_stream_id();
    /// }
    /// ```
    ///
    /// Cloning the dereferenced context cannot create an owned context:
    ///
    /// ```compile_fail
    /// use rusteron_archive::{AeronArchive, AeronArchiveContext};
    /// fn escape(archive: &AeronArchive) -> AeronArchiveContext {
    ///     (*archive.get_archive_context()).clone()
    /// }
    /// ```
    ///
    /// A raw pointer or copied native struct cannot create a safe wrapper either:
    ///
    /// ```compile_fail
    /// use rusteron_archive::{AeronArchive, AeronArchiveContext};
    /// fn escape(archive: &AeronArchive) -> AeronArchiveContext {
    ///     archive.get_archive_context().get_inner().into()
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// use rusteron_archive::{AeronArchive, AeronArchiveContext};
    /// fn escape(archive: &AeronArchive) -> AeronArchiveContext {
    ///     AeronArchiveContext::from(**archive.get_archive_context())
    /// }
    /// ```
    pub fn aeron(&self) -> Ref<'_, Aeron> {
        self.get_archive_context().get_aeron()
    }

    /// Retain this archive while using its Aeron client. Clone retains the
    /// archive owner; borrowing this handle only reads its cached native pointer.
    pub fn aeron_owned(&self) -> OwnedWithReference<ResourceRef<ManagedCResource<aeron_archive_t>>, Aeron> {
        let pointer = self.aeron().get_inner();
        // SAFETY: the archive retains its client and defers close until the
        // last owner, including this retained reference, is dropped.
        unsafe { OwnedWithReference::new(self.inner.as_owned().unwrap().clone(), pointer) }
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
            // SAFETY: archive handles stay local. Supplying an agent-invoker
            // client binds native archive calls to its conductor thread.
            unsafe { aeron.main_do_work()? }
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
    /// Start an archive connection that keeps `ctx` and `aeron` alive.
    ///
    /// Aeron copies the context's handler pointers during this call, so replacing
    /// a handler on `ctx` afterwards does not affect this connection. `ctx` must
    /// already use `aeron`.
    #[inline]
    pub fn new_with_aeron(ctx: &AeronArchiveContext, aeron: &Aeron) -> Result<Self, AeronCError> {
        let retained = ctx.retained_aeron()?;
        if retained.get_inner() != aeron.get_inner() {
            return Err(AeronCError::with_message(
                -1,
                "the archive context uses a different Aeron client",
            ));
        }
        Self::new(ctx)
    }
}

macro_rules! impl_archive_position_methods {
    ($pub_type:ty) => {
        impl $pub_type {
            /// Retrieves the current active live archive position using the Aeron counters.
            /// Returns an error if not found.
            /// Searches any archive; when multiple Archives record this session, use
            /// [`RecordingPos::find_counter_id_by_session_in_archive`] to select one.
            ///
            /// Each call scans every counter for this session's recording position. In a loop,
            /// find the counter id once with [`RecordingPos::find_counter_id_by_session`] and read
            /// it with [`AeronCountersReader::get_counter_value`].
            pub fn get_archive_position(&self) -> Result<i64, AeronCError> {
                if let Some(aeron) = self.inner.get_handle_dependency::<Aeron>() {
                    let counter_reader = &aeron.counters_reader();
                    self.get_archive_position_with(counter_reader)
                } else {
                    Err(AeronCError::from_code(-1))
                }
            }

            /// Retrieves the current active live archive position using the provided counter reader.
            /// Returns an error if not found.
            /// Searches any archive, like [`Self::get_archive_position`].
            pub fn get_archive_position_with(
                &self,
                counters: &AeronCountersReader<impl ResourceStorage<aeron_counters_reader_t>>,
            ) -> Result<i64, AeronCError> {
                let session_id = self.get_constants()?.session_id();
                let counter_id = RecordingPos::find_counter_id_by_session(counters, session_id);
                if counter_id < 0 {
                    return Err(AeronCError::from_code(counter_id));
                }
                let position = counters.get_counter_value(counter_id)?;
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

// Hand-written trampolines: the code generator only generates single-callback arguments,
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
    /// The subscription retains the context, its client, archive context and listener
    /// until native cleanup finishes. Context aliases remain valid, but configuration
    /// changes and creating a second subscription from the same context are rejected.
    /// Set a Rust-owned client on the context or on its archive context before creating.
    pub fn create(
        ctx: AeronArchivePersistentSubscriptionContext,
        listener: Option<Box<dyn PersistentSubscriptionListener>>,
    ) -> Result<Self, AeronCError> {
        use std::os::raw::c_void;

        let owner = ctx
            .inner
            .as_owned()
            .ok_or_else(|| AeronCError::with_message(-1, "persistent subscription requires an owning context"))?;
        owner.check_not_frozen()?;
        let archive = ctx
            .inner
            .get_handle_dependency::<AeronArchiveContext>()
            .ok_or_else(|| {
                AeronCError::with_message(-1, "set an owned archive context before creating the subscription")
            })?;
        let client = if let Some(client) = ctx.inner.get_handle_dependency::<Aeron>() {
            client
        } else {
            let client = archive.retained_aeron()?;
            ctx.set_aeron(&client)?;
            client
        };
        if unsafe { aeron_archive_context_get_aeron(archive.get_inner()) }.is_null() {
            archive.set_aeron(&client)?;
        }
        if archive.retained_aeron()?.get_inner() != client.get_inner() {
            return Err(AeronCError::with_message(
                -1,
                "persistent subscription and archive context must use the same client",
            ));
        }

        // Native reconnects copy this archive context again. Prevent replacement
        // of callback storage until the subscription has finished closing.
        let archive_owner = archive
            .inner
            .as_owned()
            .ok_or_else(|| AeronCError::with_message(-1, "persistent subscription requires an owning archive context"))?
            .clone();
        let archive_reservation = archive_owner.reserve_context()?;
        if let Some(listener) = listener {
            let mut holder = Box::new(ListenerHolder { listener });
            let holder_ptr: *mut ListenerHolder = &mut *holder;
            // SAFETY: C copies this listener. The context retains the heap allocation
            // before creation, including when creation fails and a context alias survives.
            unsafe {
                let c_listener = AeronArchivePersistentSubscriptionListener::new(
                    Some(persistent_subscription_on_live_joined),
                    Some(persistent_subscription_on_live_left),
                    Some(persistent_subscription_on_error),
                    holder_ptr as *mut c_void,
                )?;
                ctx.set_listener(c_listener.get_inner())?;
            }
            owner.replace_callback_dependency("persistent_subscription_listener", Some(holder));
        }

        let reservation = owner.reserve_context()?;
        let mut raw_ptr: *mut aeron_archive_persistent_subscription_t = std::ptr::null_mut();
        unsafe {
            let result = aeron_archive_persistent_subscription_create(&mut raw_ptr, ctx.get_inner());
            if result < 0 {
                return Err(AeronCError::from_code(result));
            }
            // Keep ownership in Rust so context aliases remain valid after closing the
            // subscription. Its dependency keeps the context alive during native close.
            aeron_archive_persistent_subscription_get_and_own_persistent_subscription_context(raw_ptr);
        }
        reservation.commit();
        archive_reservation.commit();

        let resource = ManagedCResource::<aeron_archive_persistent_subscription_t>::new(
            move |ctx_field| unsafe {
                *ctx_field = raw_ptr;
                0
            },
            Some(Box::new(move |ctx_field| unsafe {
                let result = aeron_archive_persistent_subscription_close(*ctx_field);
                if result >= 0 {
                    archive_owner.frozen.store(false, Ordering::Release);
                }
                result
            })),
            false,
        )?;
        resource.add_handle_dependency(&ctx);
        Ok(Self {
            inner: CResource::new(ResourceRef::new(resource)),
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
    /// to true. Check it in the poll loop and read the reason with
    /// [`AeronArchivePersistentSubscription::get_failure_reason`].
    fn on_error(&self, error_code: i32, error_message: &str) {}
}

/// Builder for configuring and creating a persistent subscription.
/// This provides a fluent interface for setting up a persistent subscription
/// with proper CString handling.
///
/// The subscription keeps the Aeron client and archive context given here open until it
/// closes. Use [`Self::new_with_aeron`] to set the same Rust-owned client on both
/// contexts. The archive context cannot be reconfigured while the subscription is
/// active, because native reconnects read its configuration and callback pointers.
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
        self.archive_context = Some(AeronArchiveContext::from_resource(ctx.inner.clone()));
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
    /// # Safety
    /// Do not use this counter or its aliases after this call. The builder and
    /// subscription own its native cleanup. Keep the counter's client alive.
    ///
    /// ```compile_fail,E0133
    /// fn transfer(builder: rusteron_archive::PersistentSubscriptionBuilder, counter: &rusteron_archive::AeronCounter) {
    ///     builder.state_counter(counter);
    /// }
    /// ```
    pub unsafe fn state_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        let previous = self
            .ctx
            .get_state_counter()
            .map_or(std::ptr::null_mut(), |view| view.get_inner());
        // SAFETY: the caller transfers native ownership under the contract above.
        unsafe { self.ctx.set_state_counter(counter)? };
        hand_over(previous, counter);
        Ok(self)
    }

    /// Counter holding the byte gap between replay and live when the live image is added.
    /// Maps to Aeron's `Context.joinDifferenceCounter`. Taken over as in [`Self::state_counter`].
    /// # Safety
    /// Do not use this counter or its aliases after this call. The builder and
    /// subscription own its native cleanup. Keep the counter's client alive.
    pub unsafe fn join_difference_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        let previous = self
            .ctx
            .get_join_difference_counter()
            .map_or(std::ptr::null_mut(), |view| view.get_inner());
        // SAFETY: the caller transfers native ownership under the contract above.
        unsafe { self.ctx.set_join_difference_counter(counter)? };
        hand_over(previous, counter);
        Ok(self)
    }

    /// Counter holding the number of times the PS has dropped off the live stream.
    /// Maps to Aeron's `Context.liveLeftCounter`. Taken over as in [`Self::state_counter`].
    /// # Safety
    /// Do not use this counter or its aliases after this call. The builder and
    /// subscription own its native cleanup. Keep the counter's client alive.
    pub unsafe fn live_left_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        let previous = self
            .ctx
            .get_live_left_counter()
            .map_or(std::ptr::null_mut(), |view| view.get_inner());
        // SAFETY: the caller transfers native ownership under the contract above.
        unsafe { self.ctx.set_live_left_counter(counter)? };
        hand_over(previous, counter);
        Ok(self)
    }

    /// Counter holding the number of times the PS has switched to the live stream.
    /// Maps to Aeron's `Context.liveJoinedCounter`. Taken over as in [`Self::state_counter`].
    /// # Safety
    /// Do not use this counter or its aliases after this call. The builder and
    /// subscription own its native cleanup. Keep the counter's client alive.
    pub unsafe fn live_joined_counter(self, counter: &AeronCounter) -> Result<Self, AeronCError> {
        let previous = self
            .ctx
            .get_live_joined_counter()
            .map_or(std::ptr::null_mut(), |view| view.get_inner());
        // SAFETY: the caller transfers native ownership under the contract above.
        unsafe { self.ctx.set_live_joined_counter(counter)? };
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
                inner.add_handle_dependency(&context);
            }
            if let Some(aeron) = self.aeron.take() {
                inner.add_handle_dependency(&aeron);
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
        inner.close_already_called.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::{error, info};

    use crate::testing::{EmbeddedArchiveMediaDriverProcess, find_unused_udp_port, valgrind_timeout};

    #[test]
    fn archive_message_timeout_duration_preserves_units_and_rejects_overflow() -> Result<(), AeronCError> {
        let context = AeronArchiveContext::new()?;
        let timeout = Duration::from_nanos(123_456_789);
        context.set_message_timeout(timeout)?;
        assert_eq!(context.get_message_timeout_ns(), 123_456_789);
        assert_eq!(context.get_message_timeout(), timeout);
        assert!(context.set_message_timeout(Duration::MAX).is_err());
        assert_eq!(context.get_message_timeout(), timeout);
        Ok(())
    }

    // Aeron 1.54.2 keeps `owns_idle_strategy` set when the idle strategy is replaced after
    // conclude, so close frees the Handler's state with `aeron_free`
    // TODO: un-ignore once a vendored Aeron release fixes https://github.com/aeron-io/aeron/issues/2250
    #[test]
    #[serial_test::serial]
    #[ignore = "Aeron 1.54.2 frees the caller's idle state, https://github.com/aeron-io/aeron/issues/2250"]
    fn replacing_the_idle_strategy_after_connect_keeps_the_handler() -> Result<(), Box<dyn std::error::Error>> {
        let (driver, root) = start_archive_driver()?;
        let (ready_archive, aeron) = driver.archive_connect()?;
        drop(ready_archive);
        let context = driver.archive_context(&aeron)?;
        let archive = AeronArchiveAsyncConnect::new_with_aeron(&context, &aeron)?.poll_blocking(valgrind_timeout(5))?;
        let calls = Arc::new(AtomicUsize::new(0));
        let lifetime = Arc::downgrade(&calls);
        let capture = calls.clone();
        drop(calls);
        context.set_idle_strategy(Some(move |_: i32| {
            capture.fetch_add(1, Ordering::SeqCst);
        }))?;
        drop(archive);
        drop(context);
        assert!(lifetime.upgrade().is_none());
        drop(driver);
        drop(root);
        Ok(())
    }

    #[test]
    fn uninitialised_archive_has_no_context_to_retain() {
        // SAFETY: this test only checks the empty owner; it makes no native calls.
        let archive = unsafe { AeronArchive::new_zeroed_on_heap() };
        assert!(archive.retain_archive_context().is_err());
    }

    // Uses a real archive because Aeron copies the callback pointers inside async connect.
    #[test]
    #[serial_test::serial]
    fn connected_archive_keeps_its_handler_snapshot_until_it_closes() -> Result<(), Box<dyn std::error::Error>> {
        let (driver, root) = start_archive_driver()?;
        // Wait for archive startup.
        let (ready_archive, aeron) = driver.archive_connect()?;

        for use_helper in [true, false] {
            let context = driver.archive_context(&aeron)?;
            let old_calls = Arc::new(AtomicUsize::new(0));
            let new_calls = Arc::new(AtomicUsize::new(0));
            let old_lifetime = Arc::downgrade(&old_calls);
            let new_lifetime = Arc::downgrade(&new_calls);

            let old_capture = old_calls.clone();
            // Ignore the returned Handler. The context must retain the closure itself.
            context.set_error_handler(Some(move |_, _: &str| {
                old_capture.fetch_add(1, Ordering::SeqCst);
            }))?;

            // C copies the context and its old callback pointer here, before polling.
            let connecting = if use_helper {
                AeronArchiveAsyncConnect::new_with_aeron(&context, &aeron)?
            } else {
                AeronArchiveAsyncConnect::new(&context)?
            };
            let unused = Arc::new(());
            let unused_lifetime = Arc::downgrade(&unused);
            context.set_error_handler(Some(move |_, _: &str| {
                std::hint::black_box(&unused);
            }))?;
            let new_capture = new_calls.clone();
            context.set_error_handler(Some(move |_, _: &str| {
                new_capture.fetch_add(1, Ordering::SeqCst);
            }))?;

            assert!(unused_lifetime.upgrade().is_none());
            let archive = connecting.poll_blocking(valgrind_timeout(5))?;
            let borrowed = archive.get_archive_context();
            let same_borrow = borrowed.as_ref();
            assert_eq!(borrowed.get_inner(), same_borrow.get_inner());
            let retained = archive.retain_archive_context()?;
            let retained_clone = retained.clone();
            assert_eq!(borrowed.get_inner(), retained.as_ref().get_inner());
            assert_eq!(retained.as_ref().get_inner(), retained_clone.as_ref().get_inner());
            drop(same_borrow);
            drop(borrowed);
            assert!(archive.get_archive_id() > 0);
            assert_eq!(old_calls.load(Ordering::SeqCst), 0);
            assert_eq!(new_calls.load(Ordering::SeqCst), 0);

            // Invoke directly to check which callback each context stored.
            archive
                .get_archive_context()
                .invoke_error_handler(1, 0, c"connected archive context");
            assert_eq!(old_calls.load(Ordering::SeqCst), 1);
            assert_eq!(new_calls.load(Ordering::SeqCst), 0);
            context.invoke_error_handler(2, 0, c"original context");
            assert_eq!(old_calls.load(Ordering::SeqCst), 1);
            assert_eq!(new_calls.load(Ordering::SeqCst), 1);

            drop(old_calls);
            drop(new_calls);
            assert!(old_lifetime.upgrade().is_some());
            archive.close()?;
            // The retained context keeps the archive's native allocation and
            // callback snapshot alive after the original archive is closed.
            assert!(old_lifetime.upgrade().is_some());
            retained
                .as_ref()
                .invoke_error_handler(3, 0, c"retained archive context");
            assert_eq!(old_lifetime.upgrade().unwrap().load(Ordering::SeqCst), 2);
            drop(retained);
            assert!(old_lifetime.upgrade().is_some());
            assert_eq!(
                retained_clone.as_ref().get_control_request_stream_id(),
                context.get_control_request_stream_id()
            );
            drop(retained_clone);
            assert!(old_lifetime.upgrade().is_none());
            drop(connecting);
            // Keeping the original context alive must not keep the old snapshot alive.
            assert!(old_lifetime.upgrade().is_none());
            assert!(new_lifetime.upgrade().is_some());
            drop(context);
            assert!(new_lifetime.upgrade().is_none());
        }
        drop(ready_archive);
        drop(aeron);
        drop(driver);
        std::fs::remove_dir(root)?;
        Ok(())
    }

    fn start_archive_driver()
    -> Result<(EmbeddedArchiveMediaDriverProcess, std::path::PathBuf), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!(
            "rusteron-context-{}-{}",
            std::process::id(),
            Aeron::nano_clock()
        ));
        let request = find_unused_udp_port(19000).unwrap();
        let response = find_unused_udp_port(request + 1).unwrap();
        let events = find_unused_udp_port(response + 1).unwrap();
        let driver = EmbeddedArchiveMediaDriverProcess::start(
            root.join("driver").to_str().unwrap(),
            root.join("archive").to_str().unwrap(),
            &format!("aeron:udp?endpoint=localhost:{request}"),
            &format!("aeron:udp?endpoint=localhost:{response}"),
            &format!("aeron:udp?endpoint=localhost:{events}"),
        )?;
        Ok((driver, root))
    }

    // a pointer-free struct gets a safe `clone_struct` whose copy is independent of the source
    #[test]
    fn clone_struct_of_pointer_free_struct_is_independent() {
        let signal = AeronArchiveRecordingSignal::default();
        unsafe { signal.get_inner_mut().recording_id = 7 };
        let copy = signal.clone_struct();
        unsafe { signal.get_inner_mut().recording_id = 8 };
        assert_eq!(copy.recording_id(), 7);
        assert_ne!(copy.get_inner(), signal.get_inner());
    }

    // Borrowing shares the native struct without cloning its owner.
    #[test]
    fn borrowed_view_shares_the_struct() {
        let signal = AeronArchiveRecordingSignal::default();
        let shared = signal.as_ref();
        unsafe { signal.get_inner_mut().recording_id = 7 };
        assert_eq!(shared.recording_id(), 7);
        assert_eq!(shared.get_inner(), signal.get_inner());
    }

    #[test]
    fn archive_error_uses_native_code_without_a_message_marker() {
        for code in [
            AERON_ARCHIVE_ERROR_CODE_GENERIC,
            AERON_ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING,
            AERON_ARCHIVE_ERROR_CODE_STORAGE_SPACE,
            AERON_ARCHIVE_CLIENT_ERROR_NOT_CONNECTED,
        ] {
            let native_code = -(code as i32);
            let error = capture_native_archive_error(native_code, c"plain error text");
            assert_eq!(error.code, AeronArchiveErrorCode::from_code(code as i32));
            assert!(error.message.contains("plain error text"));
        }
    }

    #[test]
    fn archive_error_ignores_error_code_text_in_the_message() {
        let error = capture_native_archive_error(
            -(AERON_ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING as i32),
            c"errorCode=11 is part of the message",
        );
        assert_eq!(error.code, AeronArchiveErrorCode::UnknownRecording);
        assert!(error.message.contains("errorCode=11"));
    }

    #[test]
    fn archive_error_preserves_unknown_native_codes_and_errno() {
        for code in [-217, -2003, -11, 5, 13, 22, 110] {
            let error = capture_native_archive_error(code, c"plain error text");
            assert_eq!(error.code, AeronArchiveErrorCode::Unknown(code));
        }
    }

    fn capture_native_archive_error(code: i32, message: &std::ffi::CStr) -> AeronArchiveError {
        // SAFETY: the format and message are NUL-terminated. Aeron copies them
        // into this thread's error buffer before returning.
        unsafe {
            aeron_err_set(
                code,
                c"test".as_ptr(),
                c"test".as_ptr(),
                0,
                c"%s".as_ptr(),
                message.as_ptr(),
            );
        }
        let error = AeronArchiveError::from_code(-1);
        unsafe { aeron_err_clear() };
        error
    }

    #[test]
    fn archive_errors_preserve_native_codes_when_converted_to_c_errors() {
        for code in (200..=216)
            .map(|code| -code)
            .chain([-2000, -2001, -2002, -217, 5, 13, 22, 110])
        {
            let error = capture_native_archive_error(code, c"captured error text");
            let expected = error.code;
            let error: AeronCError = error.into();
            assert_eq!(error.code, code);
            assert_eq!(AeronArchiveErrorCode::from_native_code(error.code), expected);
            assert!(error.message().unwrap().contains("captured error text"));
        }
    }

    #[test]
    fn archive_native_codes_do_not_use_the_wire_code_namespace() {
        for code in [0, 5, -5, 13, -13, 16, -16, i32::MIN, i32::MAX] {
            let typed = AeronArchiveErrorCode::from_native_code(code);
            assert_eq!(typed, AeronArchiveErrorCode::Unknown(code));
            assert_eq!(typed.as_native_code(), code);
        }
        assert_eq!(
            AeronArchiveErrorCode::from_code(5),
            AeronArchiveErrorCode::UnknownRecording
        );
    }

    #[test]
    fn archive_error_uses_the_return_value_when_no_native_code_was_set() {
        unsafe { aeron_err_clear() };
        assert_eq!(
            AeronArchiveError::from_code(-4321).code,
            AeronArchiveErrorCode::Unknown(-4321)
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
            assert_eq!(AeronArchiveErrorCode::from_code(-(code + 200)), parsed);
            // SAFETY: this native function maps an integer and has no pointer arguments.
            let native = unsafe { aeron_archive_client_map_archive_to_client_error_code(code) };
            assert_eq!(parsed.as_native_code(), -native);
        }
    }

    #[test]
    #[serial]
    fn archive_errors_use_native_codes_for_blocking_and_unsolicited_responses() -> Result<(), Box<dyn Error>> {
        crate::skip_unless_java!();
        let (aeron, context, driver, publication_errors, errors) = start_aeron_archive()?;
        let result = (|| -> Result<(), Box<dyn Error>> {
            let archive = context.aeron_archive_connect(valgrind_timeout(30))?;
            let error = archive.get_start_position(i64::MAX).unwrap_err();
            assert_eq!(error.code, AeronArchiveErrorCode::UnknownRecording);
            let error: AeronCError = error.into();
            assert_eq!(error.code, -(AERON_ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING as i32));

            // Leave the previous error in TLS. The unsolicited response has a
            // different code, which must come from the response poller.
            let correlation = aeron.next_correlation_id();
            let proxy = archive.proxy();
            let deadline = Instant::now() + valgrind_timeout(10);
            while !proxy.stop_recording_subscription(correlation, i64::MAX) {
                assert!(Instant::now() < deadline, "could not publish the Archive request");
                sleep(Duration::from_millis(1));
            }
            let error = loop {
                if let Some(error) = archive.poll_for_error()? {
                    break error;
                }
                assert!(Instant::now() < deadline, "Archive error response did not arrive");
                sleep(Duration::from_millis(1));
            };
            assert_eq!(error.code, AeronArchiveErrorCode::UnknownSubscription);
            assert!(!error.message.is_empty());
            assert!(!error.message.contains("errorCode="));
            assert!(archive.poll_for_error()?.is_none());

            Ok(())
        })();
        drop(context);
        drop(aeron);
        drop(driver);
        drop(publication_errors);
        drop(errors);
        result
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
    fn archive_context_configuration_round_trips() -> Result<(), AeronCError> {
        let context = AeronArchiveContext::new()?;
        context.set_client_name(c"archive-review")?;
        context.set_recording_events_stream_id(1540)?;
        context.set_control_term_buffer_length(128 * 1024)?;
        context.set_control_mtu_length(4096)?;
        // SAFETY: this expression consumes the text before any context mutation
        assert_eq!(unsafe { context.get_client_name() }, "archive-review");
        assert_eq!(context.get_recording_events_stream_id(), 1540);
        assert_eq!(context.get_control_term_buffer_length(), 128 * 1024);
        assert_eq!(context.get_control_mtu_length(), 4096);
        for sparse in [true, false] {
            context.set_control_term_buffer_sparse(sparse)?;
            assert_eq!(context.get_control_term_buffer_sparse(), sparse);
        }
        Ok(())
    }

    #[test]
    fn archive_client_name_enforces_the_byte_limit() -> Result<(), Box<dyn std::error::Error>> {
        let context = AeronArchiveContext::new()?;
        let limit = AERON_COUNTER_MAX_CLIENT_NAME_LENGTH as usize;
        for name in [String::new(), "a".repeat(limit), "é".repeat(limit / 2)] {
            context.set_client_name(&name.as_str().into_c_string())?;
            // SAFETY: this expression consumes the text before any context mutation
            assert_eq!(unsafe { context.get_client_name() }, name);
        }
        let previous = context.get_client_name_owned();
        for name in ["a".repeat(limit + 1), "é".repeat(limit / 2 + 1)] {
            let error = context.set_client_name(&name.into_c_string()).unwrap_err();
            assert_eq!(error.code, -1);
            Aeron::with_errmsg(|message| assert!(message.contains("client_name length")));
            // SAFETY: this expression consumes the text before any context mutation
            assert_eq!(unsafe { context.get_client_name() }, previous);
        }
        Ok(())
    }

    #[test]
    #[serial_test::serial]
    fn archive_propagates_its_client_name_and_control_session_info() -> Result<(), Box<dyn std::error::Error>> {
        let (client, original_context, _driver, _publication_errors, _errors) = start_aeron_archive()?;
        // SAFETY: the text is used at once, before any setter can change it
        let request = unsafe { original_context.get_control_request_channel() }.into_c_string();
        // SAFETY: the text is used at once, before any setter can change it
        let response = unsafe { original_context.get_control_response_channel() }.into_c_string();

        // Without a Rust-owned client the connect fails at once instead of timing out
        let context = AeronArchiveContext::new()?;
        context.set_control_request_channel(&request)?;
        context.set_control_response_channel(&response)?;
        let start = std::time::Instant::now();
        let error = context
            .aeron_archive_connect(valgrind_timeout(30))
            .err()
            .ok_or("connect needs a client")?;
        assert!(error.to_string().contains("Rust-owned Aeron client"), "{error}");
        assert!(start.elapsed() < Duration::from_secs(5));

        for name in [None, Some(c"upstream-port")] {
            let context = AeronArchiveContext::new()?;
            context.set_aeron(&client)?;
            context.set_control_request_channel(&request)?;
            context.set_control_response_channel(&response)?;
            if let Some(name) = name {
                context.set_client_name(name)?;
            }
            let archive = context.aeron_archive_connect(valgrind_timeout(30))?;
            let reader = client.counters_reader();
            let mut label = None;
            reader.foreach_typed_counter_fn(|_, _, counter_type, key, text| {
                if counter_type == AeronCounterTypeId::ArchiveControlSession
                    && key[..8] == archive.get_archive_id().to_ne_bytes()
                    && key[8..16] == archive.control_session_id().to_ne_bytes()
                {
                    label = Some(text.to_owned());
                }
            });
            let label = label.ok_or("Archive control session counter is missing")?;
            assert!(
                // SAFETY: this expression consumes the text before any context mutation
                label.contains(&format!("name={} version=", unsafe { context.get_client_name() })),
                "{label}"
            );
            assert!(
                label.contains(&format!("archiveId={}", archive.get_archive_id())),
                "{label}"
            );
        }
        Ok(())
    }

    #[test]
    #[serial_test::serial]
    fn persistent_subscription_context_closes_and_the_archive_context_builds_again()
    -> Result<(), Box<dyn std::error::Error>> {
        let (aeron, archive_context, media_driver, publication_errors, errors) = start_aeron_archive()?;
        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            // Rust owns the context after `build`, so closing the first subscription must leave
            // the archive context free for a second build
            for _ in 0..2 {
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
                subscription.close()?;
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
                AeronCounterTypeId::ArchiveRecordingPosition.code(),
                &make_key(10),
                "first archive",
                Duration::from_secs(5),
            )?;
            let second = aeron.add_counter(
                AeronCounterTypeId::ArchiveRecordingPosition.code(),
                &make_key(20),
                "second archive",
                Duration::from_secs(5),
            )?;
            let reader = aeron.counters_reader();
            let first_id = first.get_constants()?.counter_id();
            let second_id = second.get_constants()?.counter_id();
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
                let found =
                    testing::find_counter_id_by_session_in_archive_blocking(&reader, 7, archive_id, Duration::ZERO);
                if expected >= 0 {
                    assert_eq!(found?, expected);
                } else {
                    assert!(found.is_err(), "must not return another archive's recording counter");
                }
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
            // Upstream rejects negative lengths and clamps corrupt positive lengths
            // before copying bytes from the fixed-size counter key.
            for length in [-13_i32, 1_000_000] {
                let mut key = [b'x'; AERON_COUNTER_MAX_KEY_LENGTH as usize];
                key[..8].copy_from_slice(&43_i64.to_ne_bytes());
                key[8..12].copy_from_slice(&8_i32.to_ne_bytes());
                key[12..16].copy_from_slice(&length.to_ne_bytes());
                let counter = aeron.add_counter(
                    AeronCounterTypeId::ArchiveRecordingPosition.code(),
                    &key,
                    "malformed source identity",
                    Duration::from_secs(5),
                )?;
                let mut destination = [0x5a; 128];
                let result =
                    RecordingPos::get_source_identity(&reader, counter.get_constants()?.counter_id(), &mut destination);
                if length < 0 {
                    assert!(result.is_err());
                    assert_eq!(destination, [0x5a; 128]);
                } else {
                    let maximum = key.len() - 16 - std::mem::size_of::<i64>();
                    assert_eq!(result?, maximum);
                    assert_eq!(&destination[..maximum], vec![b'x'; maximum]);
                    assert_eq!(destination[maximum], 0);
                    assert!(destination[maximum + 1..].iter().all(|byte| *byte == 0x5a));
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
    use std::rc::Rc;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
            .media(Media::Udp)? // must set media, else set_initial_position will give an error of -1
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
            let error_handler = SharedHandler::new_shared(ErrorCount::default());
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
                    &source_archive_context
                        .get_control_request_channel_owned()
                        .into_c_string(),
                )?;
                aeron_archive_context.set_control_response_channel(
                    &source_archive_context
                        .get_control_response_channel_owned()
                        .into_c_string(),
                )?;
                aeron_archive_context.set_recording_events_channel(
                    &source_archive_context
                        .get_recording_events_channel_owned()
                        .into_c_string(),
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

    /// Publishes through a separate client on the sending thread. Recording starts
    /// once the thread has sent its session id.
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
                // throttle publishing so the archiver can keep up
                if message_count > 10_000 {
                    while counters_reader.get_counter_value(counter_id).unwrap() < publication.position() {
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
            counters_reader.get_counter_value(counter_id)?
        );

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
                Aeron::with_errmsg(|message| {
                    if !message.is_empty() && message != "no error" {
                        panic!("{message}");
                    }
                });
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
            SharedHandler<AeronPublicationErrorFrameHandlerLogger>,
            SharedHandler<ErrorCount>,
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
        let pub_error_frame_handler = SharedHandler::new_shared(AeronPublicationErrorFrameHandlerLogger);
        aeron_context.set_publication_error_frame_handler(Some(pub_error_frame_handler.clone()))?;
        let error_handler = SharedHandler::new_shared(ErrorCount::default());
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

    /// Deep-graph close: closing a *clone* of the archive client defers the C close.
    /// The original stays usable (control session intact, error polling clean).
    #[test]
    #[serial]
    pub fn archive_clone_close_defers_and_original_remains_usable() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().expect("failed to kill all java processes");

        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            let archive = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?
                .poll_blocking(Duration::from_secs(30))
                .expect("failed to connect to aeron archive media driver");

            let session_id = archive.control_session_id();
            let clone = archive.clone();
            assert!(clone.close().is_ok());

            // original remains usable after the clone's close
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
            let threaded = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?
                .poll_blocking(Duration::from_secs(30))
                .expect("failed to connect to aeron archive media driver");
            threaded.do_work()?;

            let client_context = AeronContext::new()?;
            // SAFETY: client initialization froze the source context configuration.
            client_context.set_dir(&unsafe { aeron.context().get_dir().into_c_string() })?;
            client_context.set_use_conductor_agent_invoker(true)?;
            let client = Aeron::new(&client_context)?;
            client.start()?;
            let signals = Arc::new(AtomicUsize::new(0));
            let counted = Arc::clone(&signals);
            let context = AeronArchiveContext::new()?;
            context.set_aeron(&client)?;
            context
                // SAFETY: the text is used at once, before any setter can change it
                .set_control_request_channel(
                    &unsafe { archive_context.get_control_request_channel() }.into_c_string(),
                )?;
            context
                // SAFETY: the text is used at once, before any setter can change it
                .set_control_response_channel(
                    &unsafe { archive_context.get_control_response_channel() }.into_c_string(),
                )?;
            context.set_recording_signal_consumer(Some(move |_signal: &AeronArchiveRecordingSignal| {
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
            // SAFETY: client initialization froze the source context configuration.
            client_context.set_dir(&unsafe { aeron.context().get_dir().into_c_string() })?;
            client_context.set_use_conductor_agent_invoker(true)?;
            let client = Aeron::new(&client_context)?;
            client.start()?;
            let context = AeronArchiveContext::new()?;
            context.set_aeron(&client)?;
            context
                // SAFETY: the text is used at once, before any setter can change it
                .set_control_request_channel(
                    &unsafe { archive_context.get_control_request_channel() }.into_c_string(),
                )?;
            context
                // SAFETY: the text is used at once, before any setter can change it
                .set_control_response_channel(
                    &unsafe { archive_context.get_control_response_channel() }.into_c_string(),
                )?;

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

    #[test]
    #[serial]
    fn recording_counters_isolate_two_live_archives_on_one_driver() -> Result<(), Box<dyn Error>> {
        let (aeron, context, driver, _publication_errors, _errors) = start_aeron_archive()?;
        let first = context.aeron_archive_connect(valgrind_timeout(30))?;
        let request = find_unused_udp_port(18000).ok_or("no request port")?;
        let response = find_unused_udp_port(request + 1).ok_or("no response port")?;
        let events = find_unused_udp_port(response + 1).ok_or("no events port")?;
        let second_process = driver.start_on_existing_driver(
            &format!("{}-second", driver.archive_dir.to_str()?),
            &format!("aeron:udp?endpoint=localhost:{request}"),
            &format!("aeron:udp?endpoint=localhost:{response}"),
            &format!("aeron:udp?endpoint=localhost:{events}"),
            first.get_archive_id() + 1,
        )?;
        // Both clients and both Archives use the very same driver's counter buffers.
        let second = second_process
            .archive_context(&aeron)?
            .aeron_archive_connect(valgrind_timeout(30))?;
        assert_ne!(first.get_archive_id(), second.get_archive_id());
        let channel = c"aeron:ipc?term-length=64k";
        let first_registration = first.start_recording(channel, 1541, SOURCE_LOCATION_LOCAL, true)?;
        let second_registration = second.start_recording(channel, 1541, SOURCE_LOCATION_LOCAL, true)?;
        let publication = aeron.add_exclusive_publication(channel, 1541, valgrind_timeout(10))?;
        let session_id = publication.get_constants()?.session_id();
        let reader = aeron.counters_reader();
        let first_counter = testing::find_counter_id_by_session_in_archive_blocking(
            &reader,
            session_id,
            first.get_archive_id(),
            valgrind_timeout(10),
        )?;
        let second_counter = testing::find_counter_id_by_session_in_archive_blocking(
            &reader,
            session_id,
            second.get_archive_id(),
            valgrind_timeout(10),
        )?;
        assert_ne!(first_counter, second_counter);
        let recording_id = RecordingPos::get_recording_id(&reader, first_counter)?;
        assert_eq!(RecordingPos::get_recording_id(&reader, second_counter)?, recording_id);
        for (archive, counter) in [(&first, first_counter), (&second, second_counter)] {
            assert_eq!(
                RecordingPos::find_counter_id_by_recording_id_in_archive(
                    &reader,
                    recording_id,
                    archive.get_archive_id()
                ),
                counter
            );
            assert!(RecordingPos::is_active(&reader, counter, recording_id)?);
        }
        assert_eq!(
            RecordingPos::find_counter_id_by_session_in_archive(&reader, session_id, i64::MAX),
            -1
        );

        let deadline = Instant::now() + valgrind_timeout(10);
        loop {
            match publication.offer(b"shared-session") {
                Ok(_) => break,
                Err(e) if e.is_retryable() => (),
                Err(e) => return Err(e.into()),
            }
            assert!(Instant::now() < deadline, "recording publication did not connect");
            sleep(Duration::from_millis(1));
        }
        let position = publication.position();
        while reader.get_counter_value(first_counter)? < position
            || reader.get_counter_value(second_counter)? < position
        {
            assert!(Instant::now() < deadline, "both Archives must record the same message");
            sleep(Duration::from_millis(1));
        }
        first.stop_recording_subscription(first_registration)?;
        // Stopping the first Archive must leave the second Archive's lookup and recording intact.
        assert_eq!(
            RecordingPos::find_counter_id_by_session_in_archive(&reader, session_id, second.get_archive_id()),
            second_counter
        );
        assert!(RecordingPos::is_active(&reader, second_counter, recording_id)?);
        publication.offer(b"second Archive still records")?;
        let deadline = Instant::now() + valgrind_timeout(10);
        while reader.get_counter_value(second_counter)? < publication.position() {
            assert!(Instant::now() < deadline, "second Archive stopped with the first");
            sleep(Duration::from_millis(1));
        }
        second.stop_recording_subscription(second_registration)?;
        drop(second);
        drop(second_process);
        assert!(
            std::path::Path::new(driver.aeron_dir.to_str()?)
                .join("cnc.dat")
                .exists()
        );
        first.get_max_recorded_position(recording_id)?;
        Ok(())
    }

    // Ported from the empty-recording replay cases in aeron_archive_test.cpp.
    fn with_empty_recording(
        check: impl FnOnce(&AeronArchive, &Aeron, &AeronPublication, i64, i64) -> Result<(), Box<dyn Error>>,
    ) -> Result<(), Box<dyn Error>> {
        let (aeron, context, _driver, _publication_errors, _errors) = start_aeron_archive()?;
        let archive = context.aeron_archive_connect(valgrind_timeout(30))?;
        let channel = c"aeron:ipc?term-length=64k";
        let registration = archive.start_recording(channel, 1552, SOURCE_LOCATION_LOCAL, true)?;
        let publication = aeron.add_publication(channel, 1552, valgrind_timeout(10))?;
        let reader = aeron.counters_reader();
        let counter = testing::find_counter_id_by_session_in_archive_blocking(
            &reader,
            publication.session_id(),
            archive.get_archive_id(),
            valgrind_timeout(10),
        )?;
        let recording_id = RecordingPos::get_recording_id(&reader, counter)?;
        assert_eq!(reader.get_counter_value(counter)?, 0);
        check(&archive, &aeron, &publication, recording_id, registration)
    }

    fn empty_recording_replay_is_rejected(stopped: bool) -> Result<(), Box<dyn Error>> {
        with_empty_recording(|archive, _, _, recording_id, registration| {
            if stopped {
                archive.stop_recording_subscription(registration)?;
                let deadline = Instant::now() + valgrind_timeout(10);
                while archive.get_stop_position(recording_id)? != 0 {
                    assert!(Instant::now() < deadline, "empty recording did not stop");
                    sleep(Duration::from_millis(1));
                }
            }
            // aeron_archive.h defines -2 as REPLAY_ALL_AND_STOP and -1 as NULL_POSITION.
            let params = AeronArchiveReplayParams::builder().position(-1).length(-2).build()?;
            let error = archive
                .start_replay(recording_id, c"aeron:ipc?term-length=64k", 1553, &params)
                .unwrap_err();
            assert_eq!(error.code, AeronArchiveErrorCode::EmptyRecording, "{error}");
            assert_eq!(archive.get_start_position(recording_id)?, 0);
            if !stopped {
                archive.stop_recording_subscription(registration)?;
            }
            Ok(())
        })
    }

    #[test]
    #[serial]
    fn replay_all_and_stop_rejects_an_empty_stopped_recording() -> Result<(), Box<dyn Error>> {
        empty_recording_replay_is_rejected(true)
    }

    #[test]
    #[serial]
    fn replay_all_and_stop_rejects_an_empty_live_recording() -> Result<(), Box<dyn Error>> {
        empty_recording_replay_is_rejected(false)
    }

    #[test]
    #[serial]
    fn replay_all_and_follow_waits_for_the_first_recorded_message() -> Result<(), Box<dyn Error>> {
        with_empty_recording(|archive, aeron, publication, recording_id, registration| {
            // aeron_archive.h defines -1 as REPLAY_ALL_AND_FOLLOW.
            let params = AeronArchiveReplayParams::builder().position(-1).length(-1).build()?;
            let replay = archive.start_replay(recording_id, c"aeron:ipc?term-length=64k", 1553, &params)?;
            let channel = format!("aeron:ipc?session-id={}", replay as i32).into_c_string();
            let subscription =
                aeron.add_subscription(&channel, 1553, Handlers::NONE, Handlers::NONE, valgrind_timeout(10))?;
            let deadline = Instant::now() + valgrind_timeout(10);
            while !subscription.is_connected() {
                assert!(Instant::now() < deadline, "live replay did not connect");
                sleep(Duration::from_millis(1));
            }
            let image = subscription.image_at_index(0).ok_or("live replay image is missing")?;
            assert!(!image.is_end_of_stream());
            assert_eq!(
                subscription.poll_fn(|_, _| panic!("empty recording produced a fragment"), 10)?,
                0
            );
            let payload = b"first message after replay starts";
            loop {
                match publication.offer(payload) {
                    Ok(_) => break,
                    Err(e) if e.is_retryable() => (),
                    Err(e) => return Err(e.into()),
                }
                assert!(Instant::now() < deadline, "recording publication did not connect");
                sleep(Duration::from_millis(1));
            }
            let mut received = None;
            while received.is_none() {
                subscription.poll_fn(|buffer, _| received = Some(buffer.to_vec()), 10)?;
                assert!(
                    Instant::now() < deadline,
                    "live replay did not deliver its first message"
                );
                sleep(Duration::from_millis(1));
            }
            assert_eq!(received.as_deref(), Some(payload.as_slice()));
            archive.stop_replay(replay)?;
            archive.stop_recording_subscription(registration)?;
            Ok(())
        })
    }

    #[test]
    #[serial]
    fn truncating_a_recording_stops_its_active_replay() -> Result<(), Box<dyn Error>> {
        with_empty_recording(|archive, aeron, publication, recording_id, registration| {
            let deadline = Instant::now() + valgrind_timeout(10);
            loop {
                match publication.offer(b"recording to truncate") {
                    Ok(_) => break,
                    Err(e) if e.is_retryable() => (),
                    Err(e) => return Err(e.into()),
                }
                assert!(Instant::now() < deadline, "recording publication did not connect");
                sleep(Duration::from_millis(1));
            }
            let reader = aeron.counters_reader();
            let counter = RecordingPos::find_counter_id_by_recording_id_in_archive(
                &reader,
                recording_id,
                archive.get_archive_id(),
            );
            assert!(counter >= 0);
            while reader.get_counter_value(counter)? < publication.position() {
                assert!(Instant::now() < deadline, "Archive did not record the message");
                sleep(Duration::from_millis(1));
            }
            archive.stop_recording_subscription(registration)?;
            while archive.get_stop_position(recording_id)? != publication.position() {
                assert!(Instant::now() < deadline, "recording did not stop");
                sleep(Duration::from_millis(1));
            }
            let params = AeronArchiveReplayParams::builder().position(0).length(-1).build()?;
            archive.start_replay(recording_id, c"aeron:ipc?term-length=64k", 1553, &params)?;
            // With no replay subscriber, the replay stays active until the truncation.
            let mut replay_count = None;
            reader.foreach_typed_counter_fn(|_, id, counter_type, key, _| {
                if counter_type == AeronCounterTypeId::ArchiveReplaySessionCount
                    && key[..8] == archive.get_archive_id().to_ne_bytes()
                {
                    replay_count = Some(id);
                }
            });
            let replay_count = replay_count.ok_or("Archive replay session counter is missing")?;
            while reader.get_counter_value(replay_count)? != 1 {
                assert!(Instant::now() < deadline, "replay did not start");
                sleep(Duration::from_millis(1));
            }
            archive.truncate_recording(recording_id, 0)?;
            assert_eq!(archive.get_stop_position(recording_id)?, 0);
            while reader.get_counter_value(replay_count)? != 0 {
                assert!(Instant::now() < deadline, "truncation left a replay running");
                sleep(Duration::from_millis(1));
            }
            Ok(())
        })
    }

    fn replay_with_agent_invoker(response_channel: bool) -> Result<(), Box<dyn Error>> {
        use std::sync::atomic::AtomicI64;

        let (threaded, threaded_context, driver, _publication_errors, _errors) = start_aeron_archive()?;
        let archive = threaded_context.aeron_archive_connect(valgrind_timeout(30))?;
        let channel = c"aeron:ipc?term-length=64k";
        let recording = archive.start_recording(channel, 1542, SOURCE_LOCATION_LOCAL, true)?;
        let publication = threaded.add_exclusive_publication(channel, 1542, valgrind_timeout(10))?;
        let expected = [b"first replay message".as_slice(), b"second replay message".as_slice()];
        let deadline = Instant::now() + valgrind_timeout(10);
        for message in expected {
            loop {
                match publication.offer(message) {
                    Ok(_) => break,
                    Err(e) if e.is_retryable() => (),
                    Err(e) => return Err(e.into()),
                }
                assert!(Instant::now() < deadline, "recording publication did not connect");
                sleep(Duration::from_millis(1));
            }
        }
        let reader = threaded.counters_reader();
        let counter = testing::find_counter_id_by_session_in_archive_blocking(
            &reader,
            publication.get_constants()?.session_id(),
            archive.get_archive_id(),
            valgrind_timeout(10),
        )?;
        let recording_id = RecordingPos::get_recording_id(&reader, counter)?;
        let position = publication.position();
        while reader.get_counter_value(counter)? < position {
            assert!(Instant::now() < deadline, "Archive did not catch up");
            sleep(Duration::from_millis(1));
        }
        archive.stop_recording_subscription(recording)?;

        let client_context = AeronContext::new()?;
        client_context.set_dir(&driver.aeron_dir)?;
        client_context.set_use_conductor_agent_invoker(true)?;
        let client = Aeron::new(&client_context)?;
        client.start()?;
        let context = driver.archive_context(&client)?;
        let replay_deadline = Arc::new(AtomicI64::new(i64::MAX));
        let stalled = Arc::new(AtomicBool::new(false));
        let idle_deadline = replay_deadline.clone();
        let idle_stalled = stalled.clone();
        let idle_client = client.clone();
        // Rescues a stalled C wait after the deadline so the test fails below instead of hanging
        context.set_idle_strategy(Some(move |_work_count: i32| {
            sleep(Duration::from_millis(1));
            if Aeron::nano_clock() > idle_deadline.load(Ordering::SeqCst) {
                idle_stalled.store(true, Ordering::SeqCst);
                // SAFETY: the idle callback runs between native invocations on
                // this archive's conductor thread.
                let _ = unsafe { idle_client.main_do_work() };
            }
        }))?;
        let invoker_archive = context.aeron_archive_connect(valgrind_timeout(30))?;
        let params = AeronArchiveReplayParams::builder()
            .position(0)
            .length(position)
            .build()?;
        let replay_channel = if response_channel {
            let port = find_unused_udp_port(18100).ok_or("no replay control port")?;
            format!("aeron:udp?control-mode=response|control=127.0.0.1:{port}|term-length=64k").into_c_string()
        } else {
            channel.to_owned()
        };

        // This C API creates the subscription and waits internally. No external
        // do_work loop can help it: each replay wait must invoke the client itself.
        // The generated Rust API exposes start_replay, so exercise replay via bindings.
        let mut raw_subscription = std::ptr::null_mut();
        replay_deadline.store(
            Aeron::nano_clock() + valgrind_timeout(10).as_nanos() as i64,
            Ordering::SeqCst,
        );
        let result = unsafe {
            aeron_archive_replay(
                &mut raw_subscription,
                invoker_archive.get_inner(),
                recording_id,
                replay_channel.as_ptr(),
                1543,
                params.get_inner(),
            )
        };
        Aeron::with_errmsg(|message| assert_eq!(result, 0, "{}", message));
        assert!(!raw_subscription.is_null());
        // Keep the client alive until this subscription has been closed, including
        // assertion-failure paths; the borrowed Rust view never owns the C pointer.
        struct ReplaySubscription(AeronSubscriptionRef);
        impl Drop for ReplaySubscription {
            fn drop(&mut self) {
                unsafe { aeron_subscription_close(self.0.get_inner(), None, std::ptr::null_mut()) };
            }
        }
        // SAFETY: the replay subscription stays alive for the scope of this test.
        let subscription = ReplaySubscription(unsafe { AeronSubscription::from_ptr(raw_subscription) });
        assert!(
            !stalled.load(Ordering::SeqCst),
            "replay did not drive its invoker client"
        );
        replay_deadline.store(i64::MAX, Ordering::SeqCst);
        let mut received = Vec::new();
        let deadline = Instant::now() + valgrind_timeout(10);
        while received.len() < expected.len() {
            invoker_archive.do_work()?;
            subscription.0.poll_fn(|buffer, _| received.push(buffer.to_vec()), 10)?;
            assert!(Instant::now() < deadline, "invoker replay did not deliver all messages");
            sleep(Duration::from_millis(1));
        }
        assert_eq!(received, expected);
        Ok(())
    }

    #[test]
    #[serial]
    fn replay_waits_drive_an_agent_invoker_client() -> Result<(), Box<dyn Error>> {
        replay_with_agent_invoker(false)
    }

    #[test]
    #[serial]
    fn response_replay_waits_drive_an_agent_invoker_client() -> Result<(), Box<dyn Error>> {
        replay_with_agent_invoker(true)
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
            // SAFETY: client initialization froze the source context configuration.
            other_context.set_dir(&unsafe { aeron.context().get_dir().into_c_string() })?;
            let other = Aeron::new(&other_context)?;
            other.start()?;

            let owned = AeronArchiveContext::new()?;
            owned.set_aeron(&aeron)?;
            // SAFETY: owned stays alive for all uses of this borrowed test handle.
            let borrowed = unsafe { AeronArchiveContext::from_ptr(owned.get_inner()) };

            let Err(err) = borrowed.set_aeron(&other) else {
                return Err("a borrowed context must refuse set_aeron".into());
            };
            assert!(
                err.message()
                    .is_some_and(|m| m.contains("borrowed context cannot be changed")),
                "{err:?}"
            );
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

    /// An owned poller lists recordings without blocking
    #[test]
    #[serial]
    pub fn owned_descriptor_poller_lists_recordings_without_blocking() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes()?;
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            const STREAM_ID: i32 = 7411;
            let archive = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?
                .poll_blocking(Duration::from_secs(30))?;
            let subscription_id = archive.start_recording(AERON_IPC_STREAM, STREAM_ID, SOURCE_LOCATION_LOCAL, true)?;
            let publication = aeron
                .async_add_exclusive_publication(AERON_IPC_STREAM, STREAM_ID)?
                .poll_blocking(Duration::from_secs(5))?;
            let start = Instant::now();
            while publication.offer(b"listed").is_err() {
                assert!(
                    start.elapsed() < Duration::from_secs(10),
                    "the publication never took a message"
                );
                thread::yield_now();
            }

            let found = Rc::new(Cell::new(-1));
            let seen = Rc::clone(&found);
            let consumer = Handler::new(move |d: &AeronArchiveRecordingDescriptor| {
                if d.stream_id() == STREAM_ID {
                    seen.set(d.recording_id());
                }
            });
            let mut poller = AeronArchiveRecordingDescriptorPoller::new(&archive, 10)?;
            assert!(poller.poll().is_err(), "polling needs an installed consumer");
            assert!(
                poller
                    .reset::<AeronArchiveRecordingDescriptorConsumerFuncLogger>(0, 1, None)
                    .is_err()
            );
            let proxy = archive.archive_proxy();
            let start = Instant::now();
            while found.get() == -1 {
                assert!(
                    start.elapsed() < Duration::from_secs(10),
                    "the recording was never listed"
                );
                let correlation_id = archive.next_correlation_id();
                poller.reset(correlation_id, i32::MAX, Some(&consumer))?;
                assert!(proxy.list_recordings_for_uri(correlation_id, 0, i32::MAX, c"aeron:ipc", STREAM_ID));
                while !poller.is_dispatch_complete() {
                    assert!(start.elapsed() < Duration::from_secs(10), "the listing never completed");
                    poller.poll()?;
                    thread::yield_now();
                }
            }
            assert!(found.get() >= 0);

            // the poller keeps the archive alive, so it still works after the archive is dropped
            drop(proxy);
            drop(archive);
            let _ = poller.poll()?;
            drop(poller);
            drop(publication);
            let _ = subscription_id;
            Ok(())
        })();

        drop(aeron);
        drop(archive_context);
        drop(media_driver);
        drop(pub_error_frame_handler);
        drop(error_handler);
        test_result
    }

    /// An owned poller lists recording subscriptions without blocking
    #[test]
    #[serial]
    pub fn owned_subscription_descriptor_poller_lists_without_blocking() -> Result<(), Box<dyn error::Error>> {
        rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes()?;
        let (aeron, archive_context, media_driver, pub_error_frame_handler, error_handler) = start_aeron_archive()?;

        let test_result: Result<(), Box<dyn error::Error>> = (|| {
            const STREAM_ID: i32 = 7412;
            let archive = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?
                .poll_blocking(Duration::from_secs(30))?;
            let subscription_id = archive.start_recording(AERON_IPC_STREAM, STREAM_ID, SOURCE_LOCATION_LOCAL, true)?;

            let found = Rc::new(Cell::new(-1));
            let seen = Rc::clone(&found);
            let consumer = Handler::new(move |d: &AeronArchiveRecordingSubscriptionDescriptor| {
                if d.stream_id() == STREAM_ID {
                    seen.set(d.subscription_id());
                }
            });
            let mut poller = AeronArchiveRecordingSubscriptionDescriptorPoller::new(&archive, 10)?;
            assert!(poller.poll().is_err(), "polling needs an installed consumer");
            assert!(
                poller
                    .reset::<AeronArchiveRecordingSubscriptionDescriptorConsumerFuncLogger>(0, 1, None)
                    .is_err()
            );
            let proxy = archive.archive_proxy();
            let correlation_id = archive.next_correlation_id();
            poller.reset(correlation_id, i32::MAX, Some(&consumer))?;
            assert!(proxy.list_recording_subscriptions(correlation_id, 0, i32::MAX, c"aeron:ipc", STREAM_ID, true));
            let start = Instant::now();
            while !poller.is_dispatch_complete() {
                assert!(start.elapsed() < Duration::from_secs(10), "the listing never completed");
                poller.poll()?;
                thread::yield_now();
            }
            assert_eq!(found.get(), subscription_id);
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
            let archive = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?
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
                // SAFETY: client initialization froze the source context configuration.
                ctx.set_dir(&unsafe { aeron.context().get_dir().into_c_string() })?;
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
            let replaced = context
                .inner
                .get_handle_dependency::<Aeron>()
                .ok_or("first client not kept")?;
            assert_eq!(replaced.client_id(), first_id);
            assert!(!replaced.is_closed());

            let view = context.get_aeron();
            assert_eq!(view.client_id(), second_id);
            let client = context
                .inner
                .get_handle_dependency_matching::<Aeron>(view.get_inner())
                .ok_or("current client not retained")?;
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

            for use_helper in [true, false] {
                let connect = if use_helper {
                    AeronArchiveAsyncConnect::new_with_aeron(&context, &aeron)?
                } else {
                    AeronArchiveAsyncConnect::new(&context)?
                };
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

            let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
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
            let session_id = publication.get_constants()?.session_id();
            info!("publication session id {}", session_id);
            // single thread: wait for the archiver before reading the position
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
            while counters_reader.get_counter_value(counter_id)? < stop_position {
                info!(
                    "current archive publication stop position {}",
                    counters_reader.get_counter_value(counter_id)?
                );
                sleep(Duration::from_millis(50));
            }
            info!(
                "found archive publication stop position {}",
                counters_reader.get_counter_value(counter_id)?
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
                    |d: &AeronArchiveRecordingDescriptor| {
                        assert_eq!(d.stream_id(), stream_id);
                        info!("found recording {:#?}", d);
                        info!(
                            "strippedChannel={}, originalChannel={}",
                            d.stripped_channel(),
                            d.original_channel()
                        );
                        if d.stop_position() > d.start_position() && d.stop_position() > 0 {
                            found_recording_id.set(d.recording_id());
                            start_pos.set(d.start_position());
                            end_pos.set(d.stop_position());
                        }

                        // SAFETY: `copy` is dropped within the callback and its strings are not read
                        let copy = unsafe { d.clone_struct() };
                        assert_eq!(unsafe { copy.get_inner_ref() }, unsafe { d.get_inner_ref() });
                        assert_eq!(copy.recording_id(), d.recording_id());
                        assert_eq!(copy.control_session_id(), d.control_session_id());
                        assert_eq!(copy.mtu_length(), d.mtu_length());
                        assert_eq!(copy.source_identity_length(), d.source_identity_length());
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
            let avail_image_handler = SharedHandler::new_shared(AeronAvailableImageLogger);
            let unavail_image_handler = SharedHandler::new_shared(AeronUnavailableImageLogger);
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
                    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: &AeronHeader) {
                        assert_eq!(buffer, "123456".as_bytes());

                        self.count.set(self.count.get() + 1);
                    }
                }

                let poll = Handler::new(FragmentHandler::default());
                let poll_result: Result<(), Box<dyn error::Error>> = (|| {
                    let wait_timeout = Duration::from_secs(30);
                    let start = Instant::now();
                    while start.elapsed() < wait_timeout && subscription.poll(&mut *poll.lock(), 100)? <= 0 {
                        let err = archive.poll_for_error_response_as_string(4096)?;
                        if !err.is_empty() {
                            return Err(std::io::Error::other(err).into());
                        }
                    }

                    if start.elapsed() >= wait_timeout {
                        return Err(
                            std::io::Error::other(format!("messages not received {:?}", poll.borrow().count)).into(),
                        );
                    }

                    info!("aeron {:?}", aeron);
                    info!("ctx {:?}", archive_context);
                    if poll.borrow().count.get() != 11 {
                        return Err(std::io::Error::other(format!(
                            "expected 11 replayed messages, got {}",
                            poll.borrow().count.get()
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
        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
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
        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
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
        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
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
        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
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
                    // Retry with backoff if the error is about the CLOSING state
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
        // recording_id is a u64, not a resource handle

        // Call close_now on the archive. It must not segfault or double-free
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
