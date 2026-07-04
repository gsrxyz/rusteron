// Archive-only hand-written wrapper code, appended after `aeron_custom.rs` into the
// generated `aeron_custom.rs` of rusteron-archive ONLY. Unlike the common custom file,
// this one may reference archive-only types (AeronArchive, AeronArchiveContext, ...).

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

    /// Mirror of [`AeronCError::from_c_code`] for archive control operations: snapshots
    /// the current thread-local `aeron_errmsg()` text and parses the embedded code out
    /// of it. Use this at FFI error sites on `aeron_archive_*` calls.
    ///
    /// Like its client counterpart, retryable codes skip the message snapshot to keep
    /// retry loops allocation-free; the message can be reconstructed later via
    /// [`AeronCError::get_last_err_message`] on the lossy conversion below if needed.
    pub fn from_c_code(code: i32) -> Self {
        let message = Aeron::errmsg();
        if message.contains("errorCode=") {
            Self::parse(message)
        } else {
            // No structured code in the buffer (e.g. generic C-level failure); preserve
            // the message verbatim and let the code fall through to the parsed form, or
            // Generic if nothing is recoverable.
            let parsed = Self::parse(message);
            if matches!(parsed.code, AeronArchiveErrorCode::Generic) && code < 0 {
                Self {
                    code: AeronArchiveErrorCode::Generic,
                    message: format!("{} (code {})", message, code),
                }
            } else {
                parsed
            }
        }
    }
}

/// Lossy interop: archive control errors flatten into [`AeronCError`] so `?` works in
/// mixed client/archive code paths. The original typed code is not preserved (the C
/// client error domain has no equivalent slot for it); use [`AeronArchiveError`]
/// directly at archive control sites to retain the typed code.
impl From<AeronArchiveError> for AeronCError {
    fn from(err: AeronArchiveError) -> Self {
        AeronCError::with_message(-1, err.message)
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

impl AeronArchiveRecordingSignal {
    /// Typed variant of [`Self::recording_signal_code`]: the wire signal as the
    /// `aeron_archive_client_recording_signal_t` enum (START/STOP/EXTEND/REPLICATE/…),
    /// or `None` for codes this client version does not know.
    pub fn signal(&self) -> Option<aeron_archive_client_recording_signal_t> {
        use aeron_archive_client_recording_signal_en::*;
        Some(match self.recording_signal_code() {
            0 => AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_START,
            1 => AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_STOP,
            2 => AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EXTEND,
            3 => AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE,
            4 => AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_MERGE,
            5 => AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_SYNC,
            6 => AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_DELETE,
            7 => AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE_END,
            _ => return None,
        })
    }
}

impl AeronArchiveReplayMerge {
    /// Poll the replay-merge delivering each fragment to `handler`, borrowing the
    /// closure only for this call (zero allocation). Named to match
    /// [`AeronSubscription::poll_fn`].
    #[inline]
    pub fn poll_fn<H: FnMut(&[u8], AeronHeader)>(
        &self,
        mut handler: H,
        fragment_limit: std::os::raw::c_int,
    ) -> Result<i32, AeronCError> {
        let result = unsafe {
            aeron_archive_replay_merge_poll(
                self.get_inner(),
                Some(aeron_fragment_handler_t_callback_for_once_closure::<H>),
                &mut handler as *mut _ as *mut std::os::raw::c_void,
                fragment_limit.into(),
            )
        };
        if result < 0 {
            Err(AeronCError::from_c_code(result))
        } else {
            Ok(result)
        }
    }
}
