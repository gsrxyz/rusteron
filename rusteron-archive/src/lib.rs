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
pub const PUBLICATION_MAX_POSITION_EXCEEDED: i64 = bindings::AERON_PUBLICATION_MAX_POSITION_EXCEEDED as i64;
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

/// Archive control-response error codes (`io.aeron.archive.client.ArchiveException`).
///
/// The C archive client reports control-session errors only as text (with the code
/// embedded as `errorCode=N`); [`AeronArchiveError::parse`] recovers the typed code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AeronArchiveErrorCode {
    Generic,
    ActiveListing,
    ActiveRecording,
    ActiveSubscription,
    UnknownSubscription,
    UnknownRecording,
    UnknownReplay,
    MaxReplays,
    MaxRecordings,
    InvalidExtension,
    AuthenticationRejected,
    StorageSpace,
    UnknownReplication,
    UnauthorisedAction,
    ReplicationConnectionFailure,
    EmptyRecording,
    InvalidPosition,
    Unknown(i32),
}

impl AeronArchiveErrorCode {
    /// Maps the wire error code to its variant. Codes 0–13 come straight from the
    /// bindgen'd `ARCHIVE_ERROR_CODE_*` constants in the aeron C header, so an Aeron
    /// upgrade that changes them is picked up automatically (and the
    /// `archive_error_codes_match_the_c_header` test fails if new ones appear).
    /// Codes 14–16 are defined by the Java/C++ `ArchiveException` but are not yet in
    /// the C header; a Java archive can still send them.
    pub fn from_code(code: i32) -> Self {
        use crate::bindings::*;
        const GENERIC: i32 = ARCHIVE_ERROR_CODE_GENERIC as i32;
        const ACTIVE_LISTING: i32 = ARCHIVE_ERROR_CODE_ACTIVE_LISTING as i32;
        const ACTIVE_RECORDING: i32 = ARCHIVE_ERROR_CODE_ACTIVE_RECORDING as i32;
        const ACTIVE_SUBSCRIPTION: i32 = ARCHIVE_ERROR_CODE_ACTIVE_SUBSCRIPTION as i32;
        const UNKNOWN_SUBSCRIPTION: i32 = ARCHIVE_ERROR_CODE_UNKNOWN_SUBSCRIPTION as i32;
        const UNKNOWN_RECORDING: i32 = ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING as i32;
        const UNKNOWN_REPLAY: i32 = ARCHIVE_ERROR_CODE_UNKNOWN_REPLAY as i32;
        const MAX_REPLAYS: i32 = ARCHIVE_ERROR_CODE_MAX_REPLAYS as i32;
        const MAX_RECORDINGS: i32 = ARCHIVE_ERROR_CODE_MAX_RECORDINGS as i32;
        const INVALID_EXTENSION: i32 = ARCHIVE_ERROR_CODE_INVALID_EXTENSION as i32;
        const AUTHENTICATION_REJECTED: i32 = ARCHIVE_ERROR_CODE_AUTHENTICATION_REJECTED as i32;
        const STORAGE_SPACE: i32 = ARCHIVE_ERROR_CODE_STORAGE_SPACE as i32;
        const UNKNOWN_REPLICATION: i32 = ARCHIVE_ERROR_CODE_UNKNOWN_REPLICATION as i32;
        const UNAUTHORISED_ACTION: i32 = ARCHIVE_ERROR_CODE_UNAUTHORISED_ACTION as i32;
        match code {
            GENERIC => Self::Generic,
            ACTIVE_LISTING => Self::ActiveListing,
            ACTIVE_RECORDING => Self::ActiveRecording,
            ACTIVE_SUBSCRIPTION => Self::ActiveSubscription,
            UNKNOWN_SUBSCRIPTION => Self::UnknownSubscription,
            UNKNOWN_RECORDING => Self::UnknownRecording,
            UNKNOWN_REPLAY => Self::UnknownReplay,
            MAX_REPLAYS => Self::MaxReplays,
            MAX_RECORDINGS => Self::MaxRecordings,
            INVALID_EXTENSION => Self::InvalidExtension,
            AUTHENTICATION_REJECTED => Self::AuthenticationRejected,
            STORAGE_SPACE => Self::StorageSpace,
            UNKNOWN_REPLICATION => Self::UnknownReplication,
            UNAUTHORISED_ACTION => Self::UnauthorisedAction,
            // Java/C++ ArchiveException codes not yet mirrored in the C header:
            14 => Self::ReplicationConnectionFailure,
            15 => Self::EmptyRecording,
            16 => Self::InvalidPosition,
            other => Self::Unknown(other),
        }
    }

    /// Resource-exhaustion codes — the operation can succeed later once capacity frees up
    /// (a replay/recording slot or storage). Everything else is a state/identity error:
    /// fix the request, don't retry it blindly.
    pub fn is_resource_exhausted(&self) -> bool {
        matches!(self, Self::MaxReplays | Self::MaxRecordings | Self::StorageSpace)
    }
}

/// A typed archive control-session error: the [`AeronArchiveErrorCode`] plus the full
/// message from the archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AeronArchiveError {
    pub code: AeronArchiveErrorCode,
    pub message: String,
}

impl AeronArchiveError {
    /// Parses the `errorCode=N` the C archive client embeds in error text (both
    /// `poll_for_error_response` payloads and `AeronCError` `lastError` messages).
    /// Falls back to [`AeronArchiveErrorCode::Generic`] when no code is present.
    pub fn parse(message: &str) -> Self {
        let code = message
            .split("errorCode=")
            .nth(1)
            .and_then(|rest| {
                let digits: String = rest.chars().take_while(|c| c.is_ascii_digit() || *c == '-').collect();
                digits.parse::<i32>().ok()
            })
            .map(AeronArchiveErrorCode::from_code)
            .unwrap_or(AeronArchiveErrorCode::Generic);
        Self {
            code,
            message: message.to_string(),
        }
    }
}

impl std::fmt::Display for AeronArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "archive error {:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for AeronArchiveError {}

/// Fluent builder for [`AeronArchiveReplayParams`], starting from aeron's defaults
/// (every field `AERON_NULL_VALUE`: replay from the recording start to its end, with the
/// context-default file IO length and no bounding counter).
#[derive(Debug, Clone)]
pub struct AeronArchiveReplayParamsBuilder {
    bounding_limit_counter_id: i32,
    file_io_max_length: i32,
    position: i64,
    length: i64,
    replay_token: i64,
    subscription_registration_id: i64,
}

impl AeronArchiveReplayParams {
    pub fn builder() -> AeronArchiveReplayParamsBuilder {
        AeronArchiveReplayParamsBuilder {
            bounding_limit_counter_id: AERON_NULL_VALUE,
            file_io_max_length: AERON_NULL_VALUE,
            position: AERON_NULL_VALUE as i64,
            length: AERON_NULL_VALUE as i64,
            replay_token: AERON_NULL_VALUE as i64,
            subscription_registration_id: AERON_NULL_VALUE as i64,
        }
    }
}

impl AeronArchiveReplayParamsBuilder {
    /// Start the replay from this position (default: the recording's start).
    pub fn position(mut self, position: i64) -> Self {
        self.position = position;
        self
    }

    /// Replay this many bytes (default: to the recording's end).
    pub fn length(mut self, length: i64) -> Self {
        self.length = length;
        self
    }

    /// Follow a live recording after the recorded portion (`length = i64::MAX`).
    pub fn follow_live(mut self) -> Self {
        self.length = i64::MAX;
        self
    }

    /// Bound the replay by this counter (triggers a bounded replay request).
    pub fn bounded_by(mut self, counter_id: i32) -> Self {
        self.bounding_limit_counter_id = counter_id;
        self
    }

    /// Maximum size of an archive file IO operation during the replay.
    pub fn file_io_max_length(mut self, length: i32) -> Self {
        self.file_io_max_length = length;
        self
    }

    /// Token for replays where the initiating image did not create the archive session.
    pub fn replay_token(mut self, token: i64) -> Self {
        self.replay_token = token;
        self
    }

    /// Subscription registration id for response-channel replays on an existing channel.
    pub fn subscription_registration_id(mut self, registration_id: i64) -> Self {
        self.subscription_registration_id = registration_id;
        self
    }

    pub fn build(self) -> Result<AeronArchiveReplayParams, AeronCError> {
        AeronArchiveReplayParams::new(
            self.bounding_limit_counter_id,
            self.file_io_max_length,
            self.position,
            self.length,
            self.replay_token,
            self.subscription_registration_id,
        )
    }
}

/// [`AeronArchiveReplicationParams`] plus the string storage its C struct points into.
///
/// The C params struct stores raw `char *` pointers, so the strings must outlive it —
/// this wrapper owns them. Deref gives the params for `archive.replicate(...)`.
pub struct AeronArchiveReplicationParamsOwned {
    params: AeronArchiveReplicationParams,
    _credentials: AeronArchiveEncodedCredentials,
    _strings: [std::ffi::CString; 4],
}

impl std::ops::Deref for AeronArchiveReplicationParamsOwned {
    type Target = AeronArchiveReplicationParams;

    fn deref(&self) -> &Self::Target {
        &self.params
    }
}

/// Fluent builder for [`AeronArchiveReplicationParams`], starting from aeron's defaults:
/// replicate into a **new** recording at the destination, no live merge, the context's
/// default replication channel, and no credentials.
#[derive(Debug, Clone, Default)]
pub struct AeronArchiveReplicationParamsBuilder {
    stop_position: Option<i64>,
    dst_recording_id: Option<i64>,
    live_destination: Option<String>,
    replication_channel: Option<String>,
    src_response_channel: Option<String>,
    channel_tag_id: Option<i64>,
    subscription_tag_id: Option<i64>,
    file_io_max_length: Option<i32>,
    replication_session_id: Option<i32>,
    encoded_credentials: Option<String>,
}

impl AeronArchiveReplicationParams {
    pub fn builder() -> AeronArchiveReplicationParamsBuilder {
        AeronArchiveReplicationParamsBuilder::default()
    }
}

impl AeronArchiveReplicationParamsBuilder {
    /// Stop the replication at this position (default: continuous until synced).
    pub fn stop_position(mut self, position: i64) -> Self {
        self.stop_position = Some(position);
        self
    }

    /// Extend this existing recording at the destination (default: create a new one).
    pub fn extend_recording(mut self, dst_recording_id: i64) -> Self {
        self.dst_recording_id = Some(dst_recording_id);
        self
    }

    /// Merge to this live destination once caught up (default: no merge).
    pub fn live_destination(mut self, destination: &str) -> Self {
        self.live_destination = Some(destination.to_string());
        self
    }

    /// Channel to replicate over (default: the context's replication channel).
    pub fn replication_channel(mut self, channel: &str) -> Self {
        self.replication_channel = Some(channel.to_string());
        self
    }

    /// Source archive response channel when replicating over response channels.
    pub fn src_response_channel(mut self, channel: &str) -> Self {
        self.src_response_channel = Some(channel.to_string());
        self
    }

    /// Tag for the destination archive's replication subscription channel.
    pub fn channel_tag_id(mut self, tag: i64) -> Self {
        self.channel_tag_id = Some(tag);
        self
    }

    /// Subscription tag for the destination archive's replication subscription.
    pub fn subscription_tag_id(mut self, tag: i64) -> Self {
        self.subscription_tag_id = Some(tag);
        self
    }

    /// Maximum size of a file IO operation during the replay driving the replication.
    pub fn file_io_max_length(mut self, length: i32) -> Self {
        self.file_io_max_length = Some(length);
        self
    }

    /// Session id for the replicated recording (default: keep the source's session id).
    pub fn replication_session_id(mut self, session_id: i32) -> Self {
        self.replication_session_id = Some(session_id);
        self
    }

    /// Credentials passed to the source archive (simple authentication only).
    pub fn encoded_credentials(mut self, credentials: &str) -> Self {
        self.encoded_credentials = Some(credentials.to_string());
        self
    }

    pub fn build(self) -> Result<AeronArchiveReplicationParamsOwned, AeronCError> {
        let live_destination = self.live_destination.unwrap_or_default().into_c_string();
        let replication_channel = self.replication_channel.unwrap_or_default().into_c_string();
        let src_response_channel = self.src_response_channel.unwrap_or_default().into_c_string();
        let credentials_str = self.encoded_credentials.clone().unwrap_or_default().into_c_string();
        let credentials_len = self.encoded_credentials.map(|c| c.len()).unwrap_or(0) as u32;
        let credentials = AeronArchiveEncodedCredentials::new(&credentials_str, credentials_len)?;
        let params = AeronArchiveReplicationParams::new(
            self.stop_position.unwrap_or(AERON_NULL_VALUE as i64),
            self.dst_recording_id.unwrap_or(AERON_NULL_VALUE as i64),
            &live_destination,
            &replication_channel,
            &src_response_channel,
            self.channel_tag_id.unwrap_or(AERON_NULL_VALUE as i64),
            self.subscription_tag_id.unwrap_or(AERON_NULL_VALUE as i64),
            self.file_io_max_length.unwrap_or(AERON_NULL_VALUE),
            self.replication_session_id.unwrap_or(AERON_NULL_VALUE),
            &credentials,
        )?;
        Ok(AeronArchiveReplicationParamsOwned {
            params,
            _credentials: credentials,
            _strings: [
                live_destination,
                replication_channel,
                src_response_channel,
                credentials_str,
            ],
        })
    }
}

impl AeronArchive {
    /// Connect to an Aeron Archive in one call: context wiring, async connect, and a
    /// bounded blocking poll.
    ///
    /// For tuned setups — error handlers, credentials, recording-signal consumers,
    /// message timeouts — build the [`AeronArchiveContext`] yourself and use
    /// [`AeronArchiveAsyncConnect`]. The context used here is retrievable afterwards via
    /// [`Self::get_archive_context`].
    pub fn connect(
        aeron: &Aeron,
        control_request_channel: &str,
        control_response_channel: &str,
        recording_events_channel: Option<&str>,
        timeout: std::time::Duration,
    ) -> Result<AeronArchive, AeronCError> {
        let ctx = AeronArchiveContext::new()?;
        ctx.set_aeron(aeron)?;
        ctx.set_control_request_channel(&control_request_channel.into_c_string())?;
        ctx.set_control_response_channel(&control_response_channel.into_c_string())?;
        if let Some(events) = recording_events_channel {
            ctx.set_recording_events_channel(&events.into_c_string())?;
        }
        AeronArchiveAsyncConnect::new_with_aeron(&ctx, aeron)?.poll_blocking(timeout)
    }
}

impl AeronArchive {
    /// Typed variant of [`Self::poll_for_error_response_as_string`]: polls the control
    /// response stream once and returns the parsed archive error, or `Ok(None)` when the
    /// stream is clean.
    pub fn poll_for_error(&self) -> Result<Option<AeronArchiveError>, AeronCError> {
        let message = self.poll_for_error_response_as_string(4096)?;
        if message.is_empty() {
            Ok(None)
        } else {
            Ok(Some(AeronArchiveError::parse(&message)))
        }
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
pub const SOURCE_LOCATION_LOCAL: aeron_archive_source_location_en = SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL;
pub const SOURCE_LOCATION_REMOTE: aeron_archive_source_location_en =
    SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_REMOTE;

pub struct RecordingPos;
impl RecordingPos {
    pub fn find_counter_id_by_session(counter_reader: &AeronCountersReader, session_id: i32) -> i32 {
        unsafe { aeron_archive_recording_pos_find_counter_id_by_session_id(counter_reader.get_inner(), session_id) }
    }
    pub fn find_counter_id_by_recording_id(counter_reader: &AeronCountersReader, recording_id: i64) -> i32 {
        unsafe { aeron_archive_recording_pos_find_counter_id_by_recording_id(counter_reader.get_inner(), recording_id) }
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
    pub fn find_recording<F>(&self, mut predicate: F) -> Result<Option<RecordingDescriptor>, AeronCError>
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
            let result = aeron_archive_persistent_subscription_create(&mut raw_ptr, ctx.get_inner());
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
    pub fn listener<L: PersistentSubscriptionListener>(mut self, listener: L) -> Result<Self, AeronCError> {
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
        }
    }

    /// Pins the enum to the aeron C header: every `ARCHIVE_ERROR_CODE_*` constant the
    /// submodule defines must map to a named variant. When `just update-aeron-version`
    /// pulls a release that adds or renumbers codes, this fails and points at the gap.
    #[test]
    fn archive_error_codes_match_the_c_header() {
        let header = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/aeron/aeron-archive/src/main/c/client/aeron_archive.h"
        ))
        .expect("aeron submodule header missing");
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
                "C header defines ARCHIVE_ERROR_CODE_{name} = {value} but AeronArchiveErrorCode has no variant for it"
            );
            found += 1;
        }
        assert!(
            found >= 14,
            "expected at least 14 ARCHIVE_ERROR_CODE_* defines, found {found}"
        );
    }
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

        let (session_id, publisher_thread) = reply_merge_publisher(&archive, aeron.clone(), running.clone())?;

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
            &format!("aeron:udp?control={CONTROL_ENDPOINT}|control-mode=dynamic|term-length=65536").into_c_string(),
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
                while publication.offer_raw(message.as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
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
            if let Err(err) = publication.close() {
                info!("publisher close returned error: {err:?}");
            }
            info!("Publisher thread terminated");
        });
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
        let recording_id =
            RecordingPos::get_recording_id_block(&aeron.counters_reader(), counter_id, Duration::from_secs(5))?;

        let subscribe_channel = format!("aeron:udp?control-mode=manual|session-id={session_id}").into_c_string();
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
        drop(replay_merge);
        drop(subscription);
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
        aeron_context.set_client_name(&"test".into_c_string())?;
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
                while publication.offer_raw("123456".as_bytes(), Handlers::no_reserved_value_supplier_handler()) <= 0 {
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
            drop(archive);
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

        let invalid_channel = "invalid:channel".into_c_string();
        let result = archive.start_recording(&invalid_channel, STREAM_ID, SOURCE_LOCATION_LOCAL, true);
        assert!(
            result.is_err(),
            "Expected error when starting recording with an invalid channel"
        );
        drop(archive);
        drop(aeron);
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

        let nonexistent_channel = &"aeron:udp?endpoint=localhost:9999".into_c_string();
        let result = archive.stop_recording_channel_and_stream(nonexistent_channel, STREAM_ID);
        assert!(
            result.is_err(),
            "Expected error when stopping recording on a non-existent channel"
        );
        drop(archive);
        drop(aeron);
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

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
        let new_archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to reconnect to archive");
        assert!(
            new_archive.get_archive_id() > 0,
            "Reconnected archive should have a valid archive id"
        );

        new_archive.close()?;
        aeron.close()?;
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

        clone2.close()?;
        aeron.close()?;
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

        aeron.close()?;
        drop(media_driver);
        Ok(())
    }
}

// run `just slow-tests`
#[cfg(test)]
mod slow_consumer_test;

///////////////////////////////////////////////////////////////////////////////
// Backtest Tests
///////////////////////////////////////////////////////////////////////////////
