// Archive-only hand-written wrapper code, appended after `aeron_custom.rs` into the
// generated `aeron_custom.rs` of rusteron-archive ONLY. Unlike the common custom file,
// this one may reference archive-only types (AeronArchive, AeronArchiveContext, ...).

impl<S: ResourceStorage<aeron_archive_context_t>> AeronArchiveContext<S> {
    /// Attach a client that Rust continues to own. Aeron Archive must not close it.
    pub fn set_aeron(&self, aeron: &Aeron) -> Result<i32, AeronCError> {
        self.inner.check_configurable()?;
        if self.get_owns_aeron_client() {
            return Err(AeronCError::with_message(
                -1,
                "archive context already owns a native client",
            ));
        }
        let Some(owner) = self.inner.as_owned() else {
            return Err(AeronCError::with_message(
                -1,
                "archive context requires owning heap storage",
            ));
        };
        let Some(client_owner) = aeron.inner.as_owned() else {
            return Err(AeronCError::with_message(-1, "client requires owning heap storage"));
        };
        client_owner.bind_conductor_thread()?;
        let result = unsafe { aeron_archive_context_set_aeron(self.get_inner(), aeron.get_inner()) };
        if result < 0 {
            return Err(AeronCError::from_code(result));
        }
        owner.add_handle_dependency(aeron);
        Ok(result)
    }

    /// Rust owns clients passed to `set_aeron`; transferring their ownership to C
    /// would leave every Rust handle pointing at freed memory.
    pub fn set_owns_aeron_client(&self, owns_aeron_client: bool) -> Result<i32, AeronCError> {
        self.inner.check_configurable()?;
        if owns_aeron_client || self.get_owns_aeron_client() {
            return Err(AeronCError::with_message(
                -1,
                "native ownership of the client cannot be changed",
            ));
        }
        let result = unsafe { aeron_archive_context_set_owns_aeron_client(self.get_inner(), false) };
        if result < 0 {
            Err(AeronCError::from_code(result))
        } else {
            Ok(result)
        }
    }
}

impl<S: ResourceStorage<aeron_archive_persistent_subscription_context_t>> AeronArchivePersistentSubscriptionContext<S> {
    /// Retain the client for as long as native code can use this context.
    pub fn set_aeron(&self, aeron: &Aeron) -> Result<i32, AeronCError> {
        self.inner.check_configurable()?;
        let owner = self.inner.as_owned().unwrap();
        let client_owner = aeron
            .inner
            .as_owned()
            .ok_or_else(|| AeronCError::with_message(-1, "client requires owning storage"))?;
        client_owner.bind_conductor_thread()?;
        let result =
            unsafe { aeron_archive_persistent_subscription_context_set_aeron(self.get_inner(), aeron.get_inner()) };
        if result < 0 {
            return Err(AeronCError::from_code(result));
        }
        owner.replace_callback_dependency("persistent_subscription_client", Some(client_owner.clone()));
        Ok(result)
    }

    /// Retain the archive context until the persistent subscription has closed.
    pub fn set_archive_context(&self, context: &AeronArchiveContext) -> Result<i32, AeronCError> {
        self.inner.check_configurable()?;
        let owner = self.inner.as_owned().unwrap();
        let context_owner = context
            .inner
            .as_owned()
            .ok_or_else(|| AeronCError::with_message(-1, "archive context requires owning storage"))?;
        let result = unsafe {
            aeron_archive_persistent_subscription_context_set_archive_context(self.get_inner(), context.get_inner())
        };
        if result < 0 {
            return Err(AeronCError::from_code(result));
        }
        owner.replace_callback_dependency("persistent_subscription_archive_context", Some(context_owner.clone()));
        Ok(result)
    }
}

impl AeronArchiveProxy {
    /// Release this proxy. Other owning clones keep its native allocation alive.
    pub fn delete(self) -> Result<(), AeronCError> {
        let result = self.inner.close_resource_deferred_if_shared();
        drop(self);
        result
    }
}

impl AeronArchiveErrorCode {
    /// Maps a wire error code or a native Archive error code to its variant.
    ///
    /// Wire codes 0 through 16 map to native codes 200 through 216. Other values
    /// use [`Self::from_native_code`]. For `aeron_errcode()` and error handlers,
    /// use `from_native_code` to avoid confusing system errors with wire codes.
    pub const fn from_code(code: i32) -> Self {
        let last_wire_code = (AERON_ARCHIVE_ERROR_CODE_INVALID_POSITION - AERON_ARCHIVE_ERROR_CODE_GENERIC) as i32;
        if code >= 0 && code <= last_wire_code {
            Self::from_native_code(code + AERON_ARCHIVE_ERROR_CODE_GENERIC as i32)
        } else {
            Self::from_native_code(code)
        }
    }

    /// Resource-exhaustion codes. The operation can succeed later once capacity frees up
    /// (a replay/recording slot or storage). Everything else is a state/identity error:
    /// fix the request, don't retry it blindly.
    pub const fn is_resource_exhausted(&self) -> bool {
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
    /// Captures the current thread's native Archive error after a failed C call.
    ///
    /// Reads the error code before copying its message. `code` is the failed call's
    /// return value, used only when Aeron has not set a thread-local error code.
    /// Message contents never determine the error code.
    #[cold]
    pub fn from_code(code: i32) -> Self {
        // SAFETY: Aeron returns the calling thread's integer error code.
        let native_code = unsafe { aeron_errcode() };
        Self {
            code: AeronArchiveErrorCode::from_native_code(if native_code == 0 { code } else { native_code }),
            message: Aeron::errmsg_owned(),
        }
    }
}

/// Preserves the native code and captured message when using `?` in code that
/// returns [`AeronCError`]. Use [`AeronArchiveErrorCode::from_native_code`] to
/// recover the Archive variant from its code.
impl From<AeronArchiveError> for AeronCError {
    fn from(err: AeronArchiveError) -> Self {
        AeronCError::with_message(err.code.as_native_code(), err.message)
    }
}

impl std::fmt::Display for AeronArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "archive error {:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for AeronArchiveError {}

impl<S: ResourceStorage<aeron_counters_reader_t>> AeronCountersReader<S> {
    /// Copies the source identity into `destination`, including a trailing NUL if nonempty.
    /// Returns the number of bytes copied, excluding that terminator.
    pub fn aeron_archive_recording_pos_get_source_identity(
        &self,
        counter_id: i32,
        destination: &mut [u8],
    ) -> Result<usize, AeronCError> {
        // len_p is an in/out capacity, not an output-only primitive.
        let mut length = destination.len();
        let result = unsafe {
            aeron_archive_recording_pos_get_source_identity(
                self.get_inner(),
                counter_id,
                destination.as_mut_ptr().cast(),
                &mut length,
            )
        };
        if result < 0 {
            return Err(AeronCError::from_code(result).capture_errmsg());
        }
        Ok(length)
    }
}

impl AeronArchivePersistentSubscription {
    /// Borrows the C context without transferring ownership.
    ///
    /// # Safety
    ///
    /// The subscription must be open. The returned pointer must not be closed or used
    /// after the subscription closes; it does not retain the subscription's dependencies.
    pub unsafe fn get_persistent_subscription_context(&self) -> *mut aeron_archive_persistent_subscription_context_t {
        unsafe { aeron_archive_persistent_subscription_get_persistent_subscription_context(self.get_inner()) }
    }

    /// Transfers responsibility for closing the C context to the caller.
    ///
    /// # Safety
    ///
    /// The subscription must be open, and ownership may only be taken once. Close the
    /// subscription before closing this context, keeping its client, archive context,
    /// counters and listener alive until both have closed.
    pub unsafe fn get_and_own_persistent_subscription_context(
        &self,
    ) -> *mut aeron_archive_persistent_subscription_context_t {
        unsafe { aeron_archive_persistent_subscription_get_and_own_persistent_subscription_context(self.get_inner()) }
    }
}

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
    pub const fn builder() -> AeronArchiveReplayParamsBuilder {
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
    pub const fn position(mut self, position: i64) -> Self {
        self.position = position;
        self
    }

    /// Replay this many bytes (default: to the recording's end).
    pub const fn length(mut self, length: i64) -> Self {
        self.length = length;
        self
    }

    /// Follow a live recording after the recorded portion (`length = i64::MAX`).
    pub const fn follow_live(mut self) -> Self {
        self.length = i64::MAX;
        self
    }

    /// Bound the replay by this counter (triggers a bounded replay request).
    pub const fn bounded_by(mut self, counter_id: i32) -> Self {
        self.bounding_limit_counter_id = counter_id;
        self
    }

    /// Maximum size of an archive file IO operation during the replay.
    pub const fn file_io_max_length(mut self, length: i32) -> Self {
        self.file_io_max_length = length;
        self
    }

    /// Token for replays where the initiating image did not create the archive session.
    pub const fn replay_token(mut self, token: i64) -> Self {
        self.replay_token = token;
        self
    }

    /// Subscription registration id for response-channel replays on an existing channel.
    pub const fn subscription_registration_id(mut self, registration_id: i64) -> Self {
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

/// Replication params with owned strings and credentials.
/// Clones of the params retain those allocations too.
pub struct AeronArchiveReplicationParamsOwned {
    params: AeronArchiveReplicationParams,
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
        let c_string = |value: Option<String>| {
            std::ffi::CString::new(value.unwrap_or_default())
                .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))
        };
        let live_destination = c_string(self.live_destination)?;
        let replication_channel = c_string(self.replication_channel)?;
        let src_response_channel = c_string(self.src_response_channel)?;
        let credentials_str = c_string(self.encoded_credentials)?;
        let credentials_len = u32::try_from(credentials_str.as_bytes().len())
            .map_err(|_| AeronCError::with_message(-1, "credentials exceed the native u32 length limit"))?;
        // Params and their clones must keep all native pointers valid.
        let credentials = unsafe { AeronArchiveEncodedCredentials::new(&credentials_str, credentials_len)? };
        let params = unsafe {
            AeronArchiveReplicationParams::new(
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
            )?
        };
        params.inner.add_dependency((
            credentials,
            [
                live_destination,
                replication_channel,
                src_response_channel,
                credentials_str,
            ],
        ));
        Ok(AeronArchiveReplicationParamsOwned { params })
    }
}

/// Borrow the context for as long as this retained handle is alive.
///
/// ```compile_fail,E0505
/// # use rusteron_archive::RetainedArchiveContext;
/// fn use_after_drop(handle: RetainedArchiveContext) {
///     let context = handle.as_ref();
///     drop(handle);
///     context.get_control_request_stream_id();
/// }
/// ```
///
/// ```compile_fail,E0505
/// # use rusteron_archive::RetainedArchiveContext;
/// fn nested_ref_after_drop(handle: RetainedArchiveContext) {
///     let client = handle.as_ref().get_aeron();
///     drop(handle);
///     client.is_closed();
/// }
/// ```
/// A connected archive context kept alive by its native archive owner.
pub type RetainedArchiveContext =
    OwnedWithReference<ResourceRef<ManagedCResource<aeron_archive_t>>, AeronArchiveContext>;

impl AeronArchive {
    /// Keep the archive's context alive after the original archive handle is
    /// dropped or closed. Cloning this handle also keeps the archive alive.
    /// Ordinary `get_archive_context()` borrows the archive without cloning it.
    ///
    /// This fails for an uninitialised archive or after native context ownership
    /// has been transferred away from the archive.
    ///
    /// ```no_run
    /// # use rusteron_archive::*;
    /// # fn example(archive: AeronArchive) -> Result<(), AeronCError> {
    /// let context = archive.retain_archive_context()?;
    /// archive.close()?;
    /// let _stream_id = context.as_ref().get_control_request_stream_id();
    /// # Ok(()) }
    /// ```
    pub fn retain_archive_context(&self) -> Result<RetainedArchiveContext, AeronCError> {
        let Some(archive_owner) = self.inner.as_owned() else {
            return Err(AeronCError::with_message(
                -1,
                "archive requires owning, live heap storage",
            ));
        };
        let archive = archive_owner.get();
        if archive.is_null() {
            return Err(AeronCError::with_message(
                -1,
                "archive requires owning, live heap storage",
            ));
        }
        // The checked heap owner keeps the native struct at this address.
        let (owns_context, context) = unsafe { ((*archive).owns_ctx, (*archive).ctx) };
        if !owns_context {
            return Err(AeronCError::with_message(-1, "archive does not own its context"));
        }
        if context.is_null() {
            return Err(AeronCError::with_message(-1, "archive context is null"));
        }
        // SAFETY: the native archive owns this context. Its shared Rust owner
        // defers close until this handle and all its clones have dropped.
        Ok(unsafe { OwnedWithReference::new(archive_owner.clone(), context) })
    }

    /// Connect to an Aeron Archive in one call: context wiring, async connect, and a
    /// bounded blocking poll.
    ///
    /// To tune error handlers, credentials, recording-signal consumers or
    /// message timeouts, build the [`AeronArchiveContext`] yourself and use
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
    /// response stream once and returns the archive error it carries, or `Ok(None)` when
    /// the stream is clean.
    ///
    /// Reads the numeric error code and full message from the native response poller.
    /// Returns an error response even when its message is empty.
    ///
    /// Do not call it, or any other request on this archive, while an `AeronArchiveReplayMerge`
    /// on it is in progress: the merge reads the same responses, and one taken here stalls it
    /// until its progress timeout. The merge reports archive errors itself, as `Err` from its
    /// poll.
    pub fn poll_for_error(&self) -> Result<Option<AeronArchiveError>, AeronCError> {
        // Aeron still fills the response poller when no text buffer is requested.
        // Reading that result avoids a temporary buffer and message truncation.
        self.poll_for_error_response(&mut [])?;
        let poller = self.control_response_poller();
        if !poller.is_poll_complete()
            || !poller.is_control_response()
            || !poller.is_code_error()
            || poller.control_session_id() != self.control_session_id()
        {
            return Ok(None);
        }
        let code = i32::try_from(poller.relevant_id())
            .map_err(|_| AeronCError::with_message(-1, "Archive error code exceeds the i32 range"))?;
        Ok(Some(AeronArchiveError {
            code: AeronArchiveErrorCode::from_code(code),
            message: poller.error_message_owned(),
        }))
    }
}

impl<S: ResourceStorage<aeron_archive_recording_signal_t>> AeronArchiveRecordingSignal<S> {
    /// Return the typed recording signal, or `None` for a null or unknown code.
    #[inline]
    pub fn signal(&self) -> Option<aeron_archive_client_recording_signal_t> {
        match aeron_archive_client_recording_signal_t::try_from(self.recording_signal_code()).ok()? {
            aeron_archive_client_recording_signal_en::AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_NULL_VALUE => None,
            signal => Some(signal),
        }
    }
}

/// Route a persistent subscription through [`AeronFragmentClosureAssembler`] for
/// API consistency with [`AeronSubscription`]. The C
/// `aeron_archive_persistent_subscription_poll` already reassembles fragments
/// internally (via `aeron_image_fragment_assembler_handler`), so the delegate
/// receives whole messages either way. The Rust assembler here is a pass-through.
/// Prefer `ps.poll_fn(handler, limit)` directly unless you want the shared API.
impl FragmentAssemblable for AeronArchivePersistentSubscription {
    #[inline]
    fn poll_with_assembler(
        &self,
        assembler: Option<&Handler<AeronFragmentAssembler>>,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        self.poll_with_handler(assembler, fragment_limit)
    }
}

/// Controlled-poll counterpart for [`AeronControlledFragmentClosureAssembler`].
impl ControlledFragmentAssemblable for AeronArchivePersistentSubscription {
    #[inline]
    fn controlled_poll_with_assembler(
        &self,
        assembler: Option<&Handler<AeronControlledFragmentAssembler>>,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        self.controlled_poll_with_handler(assembler, fragment_limit)
    }
}
