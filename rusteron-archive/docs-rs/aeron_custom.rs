
// code here is included in all modules and extends generated classes
pub static AERON_IPC_STREAM: &std::ffi::CStr = c"aeron:ipc";

/// Max buffer parts accepted by [`AeronPublication::offer_parts`] /
/// [`AeronExclusivePublication::offer_parts`]. The `aeron_iovec_t` array is
/// built on the stack, so it has a fixed capacity. Use the raw `offerv` with
/// your own iovec array for larger gathers.
pub const MAX_OFFER_PARTS: usize = 8;

// Threading policies and the shared-handle allowlist live in thread_safety.rs.

/// High-level connection state of a publication or subscription.
///
/// [`AeronPublication::status`] / [`AeronSubscription::status`] derive
/// `Disconnected` / `Connected` / `Closed` from the handle's `is_closed` /
/// `is_connected` flags. `BackPressured` is only observable at `offer` /
/// `try_claim` time (via the returned error) and is reported by
/// [`AeronStatus::from_error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AeronStatus {
    /// No publication/subscription image is currently connected.
    Disconnected,
    /// At least one image/publication is connected and the channel is usable.
    Connected,
    /// The publication is applying back-pressure; retry the offer shortly.
    BackPressured,
    /// The publication/subscription has been closed and is unusable.
    Closed,
}

impl AeronStatus {
    /// Derive a status from an `offer` / `try_claim` error when it corresponds
    /// to a known transition (`BackPressured` / `Closed`). Returns `None` for
    /// errors that are not status-like (e.g. `MaxPositionExceeded`).
    pub const fn from_error(error: &AeronOfferError) -> Option<Self> {
        match error {
            AeronOfferError::BackPressured => Some(Self::BackPressured),
            AeronOfferError::Closed => Some(Self::Closed),
            AeronOfferError::NotConnected => Some(Self::Disconnected),
            _ => None,
        }
    }
}

/// Aeron's named idle strategies, as accepted by the context / media-driver
/// `set_*_idle_strategy` options (the C `aeron_idle_strategy_load` symbol table).
///
/// Strategy parameters are configured separately via `set_idle_strategy_init_args`:
/// - `Sleeping`: sleep period in nanoseconds (e.g. `"1000000"` = 1ms)
/// - `Backoff`: `maxSpins-maxYields-minParkNs-maxParkNs` (e.g. `"10-20-1000-1000000"`)
/// - Others: init args ignored
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AeronIdleStrategyKind {
    /// Sleep when idle (`"sleeping"`); period from the init args.
    Sleeping,
    /// Yield the OS thread when idle (`"yield"`).
    Yielding,
    /// Busy-spin (`"spin"`). Lowest latency, pins a core.
    BusySpin,
    /// Never back off (`"noop"`).
    NoOp,
    /// Spin, then yield, then park with exponential backoff (`"backoff"`). This is the default.
    Backoff,
}

impl AeronIdleStrategyKind {
    /// The name the C loader expects.
    pub const fn name(&self) -> &'static str {
        match self {
            AeronIdleStrategyKind::Sleeping => "sleeping",
            AeronIdleStrategyKind::Yielding => "yield",
            AeronIdleStrategyKind::BusySpin => "spin",
            AeronIdleStrategyKind::NoOp => "noop",
            AeronIdleStrategyKind::Backoff => "backoff",
        }
    }

    /// [`Self::name`] as a compile-time C string, so setters can pass it straight to
    /// the FFI with no runtime allocation.
    pub const fn name_c(&self) -> &'static std::ffi::CStr {
        match self {
            AeronIdleStrategyKind::Sleeping => c"sleeping",
            AeronIdleStrategyKind::Yielding => c"yield",
            AeronIdleStrategyKind::BusySpin => c"spin",
            AeronIdleStrategyKind::NoOp => c"noop",
            AeronIdleStrategyKind::Backoff => c"backoff",
        }
    }
}

impl AeronIdleStrategyKind {
    /// Default init args accepted by this strategy's loader (the C client validates the
    /// currently-set init args when the strategy is set, so they must be coherent):
    /// sleeping takes a period in ns; backoff takes `maxSpins-maxYields-minParkNs-maxParkNs`
    /// (hyphen-separated); the rest ignore init args.
    pub const fn default_init_args(&self) -> &'static str {
        match self {
            AeronIdleStrategyKind::Sleeping => "1000000",           // 1ms
            AeronIdleStrategyKind::Backoff => "10-20-1000-1000000", // aeron defaults
            _ => "",
        }
    }

    /// [`Self::default_init_args`] as a compile-time C string (no runtime allocation).
    pub const fn default_init_args_c(&self) -> &'static std::ffi::CStr {
        match self {
            AeronIdleStrategyKind::Sleeping => c"1000000",           // 1ms
            AeronIdleStrategyKind::Backoff => c"10-20-1000-1000000", // aeron defaults
            _ => c"",
        }
    }
}

impl Aeron {
    /// Connect to a media driver in one call: context, client, and conductor start.
    ///
    /// `dir` is the media driver directory (`None` uses the aeron default). For tuned
    /// setups with custom error handlers, driver timeout or idle strategy, build the
    /// [`AeronContext`] yourself and use [`Aeron::new`].
    pub fn connect(dir: Option<&str>) -> Result<Aeron, AeronCError> {
        let ctx = AeronContext::new()?;
        if let Some(dir) = dir {
            ctx.set_dir(&dir.into_c_string())?;
        }
        let aeron = Aeron::new(&ctx)?;
        aeron.start()?;
        Ok(aeron)
    }

    /// Connect to a media driver in the default aeron directory. Equivalent to
    /// [`Self::connect`]`(None)`, but reads more naturally at the call site.
    #[inline]
    pub fn connect_default() -> Result<Aeron, AeronCError> {
        Self::connect(None)
    }

    /// Connect to a media driver in `dir`. Equivalent to
    /// [`Self::connect`]`(Some(dir))`.
    #[inline]
    pub fn connect_dir(dir: &str) -> Result<Aeron, AeronCError> {
        Self::connect(Some(dir))
    }
}

// Aeron uses signed nanosecond clocks even where its context setters accept u64
fn duration_to_nanos(value: std::time::Duration) -> Result<i64, AeronCError> {
    i64::try_from(value.as_nanos())
        .map_err(|_| AeronCError::with_message(-1, "duration exceeds Aeron's i64 nanosecond range"))
}

fn duration_to_millis(value: std::time::Duration) -> Result<u64, AeronCError> {
    let millis = value.as_nanos().div_ceil(1_000_000);
    if millis > (i64::MAX / 1_000_000) as u128 {
        return Err(AeronCError::with_message(
            -1,
            "rounded duration exceeds Aeron's i64 nanosecond range",
        ));
    }
    Ok(millis as u64)
}

impl AeronContext {
    /// Typed variant of [`Self::set_idle_strategy`]: configures the conductor's idle
    /// strategy without stringly-typed names, setting coherent default init args
    /// (override afterwards with [`Self::set_idle_strategy_init_args`] if needed).
    ///
    /// This is how to make the conductor busier: [`Self::set_idle_sleep_duration_ns`] does
    /// not change its sleep once the context exists, only how often it checks timeouts.
    pub fn set_idle_strategy_kind(&self, kind: AeronIdleStrategyKind) -> Result<i32, AeronCError> {
        self.set_idle_strategy_init_args(kind.default_init_args_c())?;
        self.set_idle_strategy(kind.name_c())
    }
}

/// **Counters and Control (CnC) file** is the shared-memory interface between the
/// media driver and its clients.
///
/// The CnC file contains:
/// - **Counters**: stream positions, recording positions, liveness indicators,
///   and application-defined counters.
/// - **Error log**: a ring-buffer of recent driver/client errors.
/// - **Loss reporter**: observed packet-loss entries.
/// - **To-driver / to-clients** command buffers.
///
/// `AeronCnc` is a read-only view of the CnC file. Use it to inspect driver
/// state, dump counters, or read the error log (the upstream `aeron_stat`,
/// `error_stat`, and `loss_stat` tools all do this).
///
/// # Two access patterns
///
/// **Scoped read** (zero allocation, preferred for one-shot queries):
/// ```ignore
/// AeronCnc::read(&cformat!("{}", driver_ctx.get_dir()), |cnc| {
///     cnc.counters_reader().foreach_counter_fn(|value, id, _type_id, _key, label| {
///         println!("{id}: {label} = {value}");
///     });
/// })?;
/// ```
/// Opens the CnC file, runs the closure, closes. The resource wrapper lives on
/// the stack and is freed immediately after.
///
/// **Owned handle** (for repeated polling, e.g. a stats dashboard):
/// ```ignore
/// let cnc = AeronCnc::open(&cformat!("{}", driver_ctx.get_dir()))?;
/// loop {
///     let heartbeat = cnc.to_driver_heartbeat();
///     // ... poll counters, error log, etc. ...
///     sleep(Duration::from_secs(1));
/// }
/// // closed on drop
/// ```
impl AeronCnc {
    /// Open the CnC file, run `handler`, close. **Zero heap allocation** for the
    /// resource wrapper (the C struct is stack-borrowed). Preferred for one-shot
    /// reads (dump counters, print the error log). Accepts `&CStr` so `c""`
    /// literals work directly.
    #[inline]
    pub fn read(aeron_dir: &std::ffi::CStr, mut handler: impl FnMut(&mut AeronCncRef)) -> Result<(), AeronCError> {
        let cnc = ManagedCResource::initialise(move |cnc| unsafe { aeron_cnc_init(cnc, aeron_dir.as_ptr(), 0) })?;
        // Close on unwind too, so a panicking handler does not leak the mapped file
        struct CloseOnDrop(*mut aeron_cnc_t);
        impl Drop for CloseOnDrop {
            fn drop(&mut self) {
                unsafe { aeron_cnc_close(self.0) };
            }
        }
        let _close = CloseOnDrop(cnc);
        let mut cnc = unsafe { AeronCnc::from_ptr(cnc) };
        handler(&mut cnc);
        Ok(())
    }

    /// Open the CnC file and return an **owned handle** you can store and poll
    /// repeatedly (e.g. a monitoring loop). Allocates the resource wrapper on
    /// the heap; closed on drop. Accepts `&CStr` so `c""` literals work.
    #[inline]
    pub fn open(aeron_dir: &std::ffi::CStr) -> Result<AeronCnc, AeronCError> {
        let resource = ManagedCResource::<aeron_cnc_t>::new(
            move |cnc| unsafe { aeron_cnc_init(cnc, aeron_dir.as_ptr(), 0) },
            Some(Box::new(move |cnc| unsafe {
                aeron_cnc_close(*cnc);
                0
            })),
            false,
        )?;

        let result = Self {
            inner: CResource::new(ResourceRef::new(resource)),
        };
        Ok(result)
    }
}
impl<S: ResourceStorage<aeron_cnc_t>> AeronCnc<S> {
    #[doc = " Gets the timestamp of the last heartbeat sent to the media driver from any client.\n\n @param aeron_cnc to query\n @return last heartbeat timestamp in ms."]
    #[inline]
    pub fn get_to_driver_heartbeat_ms(&self) -> Result<i64, AeronCError> {
        unsafe {
            let timestamp = aeron_cnc_to_driver_heartbeat(self.get_inner());
            if timestamp >= 0 {
                return Ok(timestamp);
            } else {
                return Err(AeronCError::from_code(timestamp as i32));
            }
        }
    }
}

impl<S: ResourceStorage<aeron_header_t>> AeronHeader<S> {
    /// The image for this header, borrowed for the header's lifetime.
    pub fn image(&self) -> Option<Ref<'_, AeronImage>> {
        let ptr = self.context();
        if ptr.is_null() {
            None
        } else {
            Some(Ref::new(unsafe { AeronImage::from_ptr(ptr as *mut aeron_image_t) }))
        }
    }

    /// The fragment's data frame, read in place; `None` for a null header.
    #[inline]
    fn frame_values(&self) -> Option<&aeron_data_header_t> {
        // SAFETY: `aeron_header_t` (aeron_image.h) starts with its `aeron_data_header_t *frame`,
        // which stays valid for the handler call; rusteron-client's
        // `aeron_claim_commit_roundtrip_is_received` test catches a layout change.
        unsafe { self.get_inner().cast::<*const aeron_data_header_t>().as_ref()?.as_ref() }
    }

    /// Session id of this fragment, or `None` if Aeron could not read the header.
    #[inline]
    pub fn session_id(&self) -> Option<i32> {
        self.frame_values().map(|f| f.session_id)
    }

    /// Stream id of this fragment, or `None` if Aeron could not read the header.
    #[inline]
    pub fn stream_id(&self) -> Option<i32> {
        self.frame_values().map(|f| f.stream_id)
    }

    /// Reserved value of this fragment, or `None` if Aeron could not read the header. A
    /// sender can stamp a timestamp here (see [`AeronClaim::set_reserved_value`]) so the
    /// receiver can measure end-to-end latency.
    #[inline]
    pub fn reserved_value(&self) -> Option<i64> {
        self.frame_values().map(|f| f.reserved_value)
    }

    /// Term id of this fragment, or `None` if Aeron could not read the header.
    #[inline]
    pub fn term_id(&self) -> Option<i32> {
        self.frame_values().map(|f| f.term_id)
    }

    /// Term offset of this fragment, or `None` if Aeron could not read the header.
    #[inline]
    pub fn term_offset(&self) -> Option<i32> {
        self.frame_values().map(|f| f.term_offset)
    }
}

impl AeronCncMetadata {
    #[inline]
    /// allocates on heap
    pub fn load_from_file(aeron_dir: &str) -> Result<Self, AeronCError> {
        let aeron_dir = std::ffi::CString::new(aeron_dir).map_err(|_| AeronCError::from_code(-1))?;
        // Shared between init and cleanup so the mapping populated by
        // aeron_cnc_map_existing_file_and_load_metadata stays live for aeron_unmap on drop.
        let mapped_file = std::rc::Rc::new(std::cell::RefCell::new(aeron_mapped_file_t {
            addr: std::ptr::null_mut(),
            length: 0,
        }));
        let mapped_file_for_cleanup = mapped_file.clone();
        let resource = ManagedCResource::<aeron_cnc_metadata_t>::new(
            move |ctx| {
                let mut g = mapped_file.borrow_mut();
                let result = unsafe {
                    aeron_cnc_map_existing_file_and_load_metadata(
                        aeron_dir.as_ptr(),
                        &mut *g as *mut aeron_mapped_file_t,
                        ctx,
                    )
                };
                if result == aeron_cnc_load_result_t::AERON_CNC_LOAD_SUCCESS {
                    1
                } else {
                    -1
                }
            },
            Some(Box::new(move |_ctx| {
                let mut g = mapped_file_for_cleanup.borrow_mut();
                unsafe { aeron_unmap(&mut *g as *mut aeron_mapped_file_t) };
                0
            })),
            false,
        )?;

        let result = Self {
            inner: CResource::new(ResourceRef::new(resource)),
        };
        Ok(result)
    }

    #[inline]
    /// allocates on stack
    pub fn read_from_file(
        aeron_dir: &std::ffi::CString,
        mut handler: impl FnMut(&AeronCncMetadataRef),
    ) -> Result<(), AeronCError> {
        let mut mapped_file = aeron_mapped_file_t {
            addr: std::ptr::null_mut(),
            length: 0,
        };
        let ctx = ManagedCResource::initialise(move |ctx| {
            let result = unsafe {
                aeron_cnc_map_existing_file_and_load_metadata(
                    aeron_dir.as_ptr(),
                    &mut mapped_file as *mut aeron_mapped_file_t,
                    ctx,
                )
            };
            if result == aeron_cnc_load_result_t::AERON_CNC_LOAD_SUCCESS {
                1
            } else {
                -1
            }
        })?;

        let result = unsafe { AeronCncMetadata::from_ptr(ctx) };

        handler(&result);
        unsafe { aeron_unmap(&mut mapped_file as *mut aeron_mapped_file_t) };
        Ok(())
    }
}

unsafe extern "C" fn rusteron_image_visitor<F: FnMut(&AeronImageRef)>(
    image: *mut aeron_image_t,
    clientd: *mut ::std::os::raw::c_void,
) {
    let f = &mut *(clientd as *mut F);
    let image = unsafe { AeronImage::from_ptr(image) };
    // caught so C can release the image list refcount, then raised by for_each_image
    catch_panic((), || f(&image));
}

/// Shared polling loop for the `add_destination`/`remove_destination[_by_id]` blocking
/// convenience wrappers on `AeronSubscription`/`AeronPublication`/`AeronExclusivePublication`.
///
/// Returns C errors, such as an invalid destination URI, as `Err` immediately.
/// Returns [`AeronErrorType::TimedOut`] if the operation remains pending until
/// `timeout` elapses.
fn poll_destination_op_to_completion<C: std::fmt::Debug>(
    client: &Aeron,
    result: &AeronAsyncDestination,
    timeout: std::time::Duration,
    context: C,
) -> Result<(), AeronCError> {
    let invoker = client.uses_agent_invoker();
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if invoker {
            client.inner.owner().bind_conductor_thread()?;
            // SAFETY: the synchronous wait runs on the assigned conductor thread.
            unsafe {
                client.main_do_work()?;
            }
        }
        match result.aeron_subscription_async_destination_poll() {
            Ok(v) if v > 0 => {
                let _ = result.inner.close_resource();
                return Ok(());
            }
            Ok(_) => {
                // still pending, keep polling until deadline
            }
            Err(e) => {
                let _ = result.inner.close_resource();
                log::error!("async destination op failed for {context:?} {result:?}: {e:?}");
                return Err(e);
            }
        }
        if std::time::Instant::now() >= deadline {
            log::error!("failed async poll for {context:?} {result:?}");
            return Err(AeronErrorType::TimedOut.into());
        }
        #[cfg(debug_assertions)]
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

impl<S: ResourceStorage<aeron_t>> Aeron<S> {
    /// Whether this client's conductor runs only in [`Self::main_do_work`], so blocking
    /// helpers must call it while they wait.
    #[inline]
    fn uses_agent_invoker(&self) -> bool {
        self.context().get_use_conductor_agent_invoker()
    }
}

impl AeronSubscription {
    /// A retained image handle for `index`, or `None` when no such image exists.
    ///
    /// The returned [`AeronImage`] releases the image when it drops. Until then the image
    /// stays valid even once it becomes unavailable: [`AeronImage::is_closed`] turns true,
    /// polling reads nothing more, and its log buffer stays mapped. The image keeps the
    /// native subscription open, so [`Self::close`] waits for the image to drop
    ///
    /// The borrow checker cannot see that the image and the subscription share a session
    /// position. Do not poll a retained image inside a poll of its own subscription, and do
    /// not keep polling it on this thread after the subscription moves to another thread.
    /// Either one races on the image position and can deliver fragments twice
    ///
    /// The image is one publisher session: a publisher that reconnects with a new session
    /// is a new image, so poll the subscription, or fetch the new image, to follow it. To
    /// keep an image passed to an image callback, whose arguments are valid only during the
    /// call, retain it here (or with [`Self::image_by_session_id`]) on your own subscription.
    pub fn image_at_index(&self, index: usize) -> Option<AeronImage> {
        let image = unsafe { aeron_subscription_image_at_index(self.get_inner(), index) };
        self.wrap_retained_image(image)
    }

    /// A retained image handle for `session_id`, or `None` when no such image exists.
    /// Same automatic-release semantics as [`Self::image_at_index`].
    pub fn image_by_session_id(&self, session_id: i32) -> Option<AeronImage> {
        let image = unsafe { aeron_subscription_image_by_session_id(self.get_inner(), session_id) };
        self.wrap_retained_image(image)
    }

    fn wrap_retained_image(&self, image: *mut aeron_image_t) -> Option<AeronImage> {
        if image.is_null() {
            return None;
        }
        let subscription = self.inner.owner().clone();
        let resource = ManagedCResource::<aeron_image_t>::new(
            move |ctx| {
                unsafe { *ctx = image };
                0
            },
            Some(Box::new(move |ctx| {
                // Capturing the owner keeps the native subscription open until this release
                let _ = &subscription;
                unsafe { aeron_image_release(*ctx) }
            })),
            false,
        )
        .ok()?;
        Some(AeronImage {
            inner: CResource::new(ResourceRef::new(resource)).into(),
        })
    }

    /// Borrow-scoped iteration over the current images, with zero retain/release bookkeeping.
    /// The borrowed [`AeronImage`] is only valid inside the closure; call
    /// [`Self::image_at_index`] / [`Self::image_by_session_id`] for a handle that outlives it.
    pub fn for_each_image<F: FnMut(&AeronImageRef)>(&self, mut f: F) {
        unsafe {
            aeron_subscription_for_each_image(
                self.get_inner(),
                Some(rusteron_image_visitor::<F>),
                &mut f as *mut _ as *mut ::std::os::raw::c_void,
            )
        }
        resume_caught_panic();
    }

}

impl<S: ResourceStorage<aeron_uri_t>> AeronUri<S> {
    /// Parse the first `uri_length` bytes of a channel URI.
    #[inline]
    pub fn parse(&self, uri_length: usize, uri: &std::ffi::CStr) -> Result<i32, AeronCError> {
        if uri_length > uri.to_bytes().len() {
            return Err(AeronCError::with_message(-1, "URI length exceeds the supplied string"));
        }
        // SAFETY: C receives initialized URI storage and a bounded input string.
        let result = unsafe { aeron_uri_parse(uri_length, uri.as_ptr(), self.get_inner()) };
        if result < 0 {
            Err(AeronCError::from_code(result))
        } else {
            Ok(result)
        }
    }
}

impl std::str::FromStr for AeronUriStringBuilder {
    type Err = AeronCError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let builder = AeronUriStringBuilder::default();
        let s = std::ffi::CString::new(s).map_err(|_| AeronCError::from_code(-1))?;
        builder.init_on_string(&s)?;
        Ok(builder)
    }
}

// AeronUriStringBuilder does not follow convention so manually adding Default method which calls close
impl Default for AeronUriStringBuilder {
    fn default() -> Self {
        let r_constructor = ManagedCResource::<aeron_uri_string_builder_t>::new(
            move |ctx_field| {
                let inst: aeron_uri_string_builder_t = unsafe { std::mem::zeroed() };
                let inner_ptr: *mut aeron_uri_string_builder_t = Box::into_raw(Box::new(inst));
                unsafe { *ctx_field = inner_ptr };
                0
            },
            Some(Box::new(move |ctx_field| unsafe {
                aeron_uri_string_builder_close(*ctx_field)
            })),
            true,
        )
        .expect("should not happen");
        Self {
            inner: CResource::new(ResourceRef::new(r_constructor)),
        }
    }
}

impl AeronCError {
    /// Borrow the captured message or the current native thread's error buffer.
    /// # Safety
    /// If no message was captured, keep this thread alive and do not call native
    /// code that changes its error buffer until the returned text is no longer used.
    pub unsafe fn get_last_err_message(&self) -> &str {
        self.message().unwrap_or_else(|| unsafe { Aeron::errmsg() })
    }

    /// Copy the message into an owned string. This allocates for nonempty text.
    pub fn get_last_err_message_owned(&self) -> String {
        self.message().map(str::to_owned).unwrap_or_else(Aeron::errmsg_owned)
    }

    /// Read the message without a heap allocation. Native text is copied to the
    /// stack before the callback, so the callback can make other Aeron calls.
    pub fn with_last_err_message<R>(&self, callback: impl FnOnce(&str) -> R) -> R {
        match self.message() {
            Some(message) => callback(message),
            None => Aeron::with_errmsg(callback),
        }
    }

    /// Attach the current `aeron_errmsg()` text to this error (one allocation).
    /// Call at the error site when the error will be stored, logged later, or sent
    /// across threads. Otherwise `Display` reads the live buffer, which a later
    /// error can overwrite.
    #[cold]
    pub fn capture_errmsg(mut self) -> Self {
        if self.msg.is_none() {
            Aeron::with_errmsg(|message| self.msg = Some(message.to_owned().into()));
        }
        self
    }
}

impl std::fmt::Debug for AeronCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.with_last_err_message(|message| f.debug_struct("AeronCError")
            .field("code", &self.code)
            .field("kind", &self.kind())
            .field("lastError", &message)
            .finish())
    }
}

impl std::fmt::Display for AeronCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.with_last_err_message(|message| write!(
            f,
            "Aeron error {}: {:?} [lastError={}]",
            self.code,
            self.kind(),
            message
        ))
    }
}

impl std::error::Error for AeronCError {}

impl Aeron {
    /// Read this thread's native error message without a heap allocation.
    /// The stack copy stays valid if the callback changes the native error buffer.
    #[cold]
    #[inline(never)]
    pub fn with_errmsg<R>(callback: impl FnOnce(&str) -> R) -> R {
        let mut buffer = [std::mem::MaybeUninit::<u8>::uninit(); AERON_ERROR_MAX_TOTAL_LENGTH as usize];
        // SAFETY: aeron_errmsg returns this thread's live error buffer. No native
        // call or user callback occurs between reading and copying its bytes.
        let length = unsafe {
            let pointer = aeron_errmsg();
            if pointer.is_null() {
                0
            } else {
                let bytes = std::ffi::CStr::from_ptr(pointer).to_bytes();
                let length = bytes.len().min(buffer.len());
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.as_mut_ptr().cast::<u8>(), length);
                length
            }
        };
        // SAFETY: the first length bytes were initialized above. The callback
        // only borrows this local copy, not native memory.
        let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), length) };
        callback(std::str::from_utf8(bytes).unwrap_or(""))
    }
}

const PARSE_CSTR_ERROR_CODE: i32 = -132131;

impl AeronUriStringBuilder {
    /// Fresh builder for an `aeron:ipc` channel.
    pub fn ipc() -> Result<Self, AeronCError> {
        let builder = AeronUriStringBuilder::default();
        builder.init_new()?;
        builder.media(Media::Ipc)?;
        Ok(builder)
    }

    /// Fresh builder for an `aeron:udp` channel with the given `endpoint` (`host:port`).
    pub fn udp(endpoint: &str) -> Result<Self, AeronCError> {
        let builder = AeronUriStringBuilder::default();
        builder.init_new()?;
        builder.media(Media::Udp)?.endpoint(endpoint)?;
        Ok(builder)
    }

    /// Fresh builder for an `aeron:udp` multi-destination channel with the given
    /// `control` endpoint and [`ControlMode`] (`Dynamic` for MDC publications,
    /// `Manual` for MDS subscriptions/publications, `Response` for response channels).
    pub fn udp_control(control: &str, mode: ControlMode) -> Result<Self, AeronCError> {
        let builder = AeronUriStringBuilder::default();
        builder.init_new()?;
        builder.media(Media::Udp)?.control(control)?.control_mode(mode)?;
        Ok(builder)
    }

    /// Close previous builder state and run a re-init function.
    ///
    /// Closes the previous C builder state directly (reserving the cleanup
    /// closure for the final Drop), runs `f`, and re-arms the cleanup gate on
    /// success so ManagedCResource::Drop calls `aeron_uri_string_builder_close`
    /// on the new state.
    #[inline]
    fn reinit_run<F>(&self, log_msg: &str, f: F) -> Result<i32, AeronCError>
    where
        F: FnOnce(*mut aeron_uri_string_builder_t) -> i32,
    {
        if let Some(inner) = self.inner.as_owned() {
            if !inner.close_already_called.load(std::sync::atomic::Ordering::SeqCst) {
                unsafe {
                    aeron_uri_string_builder_close(inner.get());
                }
                inner
                    .close_already_called
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        #[cfg(feature = "log-c-bindings")]
        log::info!("{}", log_msg);
        let result = unsafe { f(self.get_inner()) };
        if result < 0 {
            Err(AeronCError::from_code(result))
        } else {
            if let Some(inner) = self.inner.as_owned() {
                inner
                    .close_already_called
                    .store(false, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(result)
        }
    }

    #[inline]
    #[doc = "Initialize a new AeronUriStringBuilder. If already initialized, it will close the previous builder to prevent memory leaks."]
    pub fn init_new(&self) -> Result<i32, AeronCError> {
        self.reinit_run("aeron_uri_string_builder_init_new(self.get_inner())", |ptr| unsafe {
            aeron_uri_string_builder_init_new(ptr)
        })
    }

    #[inline]
    #[doc = "Initialize AeronUriStringBuilder with an existing URI string. If already initialized, it will close the previous builder."]
    pub fn init_on_string(&self, uri: &std::ffi::CStr) -> Result<i32, AeronCError> {
        self.reinit_run(
            "aeron_uri_string_builder_init_on_string(self.get_inner(), uri)",
            |ptr| unsafe { aeron_uri_string_builder_init_on_string(ptr, uri.as_ptr()) },
        )
    }

    #[inline]
    pub fn build(&self, max_str_length: usize) -> Result<String, AeronCError> {
        let mut result = String::with_capacity(max_str_length);
        self.build_into(&mut result)?;
        Ok(result)
    }

    pub fn put_str(&self, key: &std::ffi::CStr, value: &str) -> Result<&Self, AeronCError> {
        let value = std::ffi::CString::new(value).map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put(&key, &value)?;
        Ok(self)
    }

    #[inline]
    pub fn build_into(&self, dst: &mut String) -> Result<(), AeronCError> {
        self.sprint_into(dst)?;
        Ok(())
    }

    /// Remove a key from the URI builder by setting it to null.
    ///
    /// # Example
    /// ```ignore
    /// let builder: AeronUriStringBuilder = "aeron:udp?endpoint=localhost:20121|tags=1,2".parse()?;
    /// builder.remove(c"tags")?;
    /// ```
    #[inline]
    pub fn remove(&self, key: &std::ffi::CStr) -> Result<i32, AeronCError> {
        unsafe {
            let result = aeron_uri_string_builder_put(self.get_inner(), key.as_ptr(), std::ptr::null());
            if result < 0 {
                Err(AeronCError::from_code(result))
            } else {
                Ok(result)
            }
        }
    }

    /// Remove a key from the URI builder by string slice.
    ///
    /// # Example
    /// ```ignore
    /// let builder: AeronUriStringBuilder = "aeron:udp?endpoint=localhost:20121|tags=1,2".parse()?;
    /// builder.remove_str("tags")?;
    /// ```
    #[inline]
    pub fn remove_str(&self, key: &str) -> Result<i32, AeronCError> {
        let key = std::ffi::CString::new(key).map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.remove(&key)
    }
}

impl AeronCountersReader {
    /// A counter key has 112 bytes, as in Java's `CountersReader.MAX_KEY_LENGTH`.
    pub const MAX_KEY_LENGTH: usize = unsafe { std::mem::zeroed::<aeron_counter_metadata_descriptor_t>() }
        .key
        .len();
}

impl<S: ResourceStorage<aeron_counters_reader_t>> AeronCountersReader<S> {
    /// Atomic access to a counter value. The reader keeps its backing mapping alive.
    #[inline]
    pub fn addr_atomic(&self, counter_id: i32) -> &std::sync::atomic::AtomicI64 {
        assert!(
            counter_id >= 0 && counter_id <= self.max_counter_id(),
            "invalid counter id"
        );
        // SAFETY: Aeron aligns counter values to cache lines. The bounds check
        // keeps the pointer in the mapped values buffer.
        unsafe { std::sync::atomic::AtomicI64::from_ptr(self.addr(counter_id)) }
    }

    /// Find the first counter of `type_id` whose key matches `predicate`.
    ///
    /// The type ids are the bindgen'd `AERON_COUNTER_*_TYPE_ID` constants; the key layout
    /// is type-specific (see `aeron_counters.h`). Mirrors Java's `CountersReader` lookups.
    pub fn find_by_type_id(&self, type_id: i32, mut predicate: impl FnMut(&[u8]) -> bool) -> Option<i32> {
        let mut found = None;
        self.foreach_counter_fn(|_value, id, counter_type_id, key, _label| {
            if found.is_none() && counter_type_id == type_id && predicate(key) {
                found = Some(id);
            }
        });
        found
    }

    /// Find the first counter of `type_id` keyed by `registration_id`, the layout used by
    /// aeron's per-stream counters (publisher limit/position, sender/receiver positions,
    /// subscriber position). The key starts with the registration id as a little-endian
    /// `i64`. Mirrors Java's `findCounterIdByRegistration`-style helpers, e.g.:
    ///
    /// ```no_compile
    /// let registration_id = publication.get_constants()?.registration_id;
    /// let limit_counter = counters_reader
    ///     .find_by_type_and_registration_id(AERON_COUNTER_PUBLISHER_LIMIT_TYPE_ID as i32, registration_id);
    /// // or, typed
    /// let limit_counter = counters_reader
    ///     .find_by_counter_type_and_registration_id(AeronCounterTypeId::PublisherLimit, registration_id);
    /// ```
    pub fn find_by_type_and_registration_id(&self, type_id: i32, registration_id: i64) -> Option<i32> {
        self.find_by_type_id(type_id, |key| {
            key.len() >= 8 && i64::from_le_bytes(key[..8].try_into().unwrap()) == registration_id
        })
    }

    /// [`Self::find_by_type_id`] with a typed counter type
    #[inline]
    pub fn find_by_counter_type(
        &self,
        counter_type: AeronCounterTypeId,
        predicate: impl FnMut(&[u8]) -> bool,
    ) -> Option<i32> {
        self.find_by_type_id(counter_type.code(), predicate)
    }

    /// [`Self::find_by_type_and_registration_id`] with a typed counter type, e.g.
    /// `reader.find_by_counter_type_and_registration_id(AeronCounterTypeId::PublisherLimit, registration_id)`
    #[inline]
    pub fn find_by_counter_type_and_registration_id(
        &self,
        counter_type: AeronCounterTypeId,
        registration_id: i64,
    ) -> Option<i32> {
        self.find_by_type_and_registration_id(counter_type.code(), registration_id)
    }

    /// [`Self::counter_type_id`] as an [`AeronCounterTypeId`]. Application-defined
    /// types come back as [`AeronCounterTypeId::Unknown`]
    #[inline]
    pub fn counter_type(&self, counter_id: i32) -> Result<AeronCounterTypeId, AeronCError> {
        self.counter_type_id(counter_id).map(AeronCounterTypeId::from_code)
    }

    /// [`Self::counter_state`] as an [`AeronCounterState`]
    #[inline]
    pub fn counter_record_state(&self, counter_id: i32) -> Result<AeronCounterState, AeronCError> {
        self.counter_state(counter_id).map(AeronCounterState::from_code)
    }

    /// The value of a media driver system counter. The driver refuses to start unless
    /// each system counter's id equals its enum value, so no lookup is needed
    #[inline]
    pub fn system_counter_value(&self, counter: AeronSystemCounterType) -> Result<i64, AeronCError> {
        self.get_counter_value(counter as i32)
    }

    /// [`Self::foreach_counter_fn`] with the type id converted to an [`AeronCounterTypeId`].
    /// The callback receives `(value, counter_id, counter_type, key, label)`
    #[inline]
    pub fn foreach_typed_counter_fn(&self, mut func: impl FnMut(i64, i32, AeronCounterTypeId, &[u8], &str)) {
        self.foreach_counter_fn(|value, id, type_id, key, label| {
            func(value, id, AeronCounterTypeId::from_code(type_id), key, label)
        });
    }

    #[inline]
    #[doc = "Get the label for a counter."]
    #[doc = ""]
    #[doc = " \n**param** counters_reader that contains the counter"]
    #[doc = " \n**param** counter_id to find"]
    #[doc = " \n**param** buffer to store the counter in."]
    #[doc = " \n**param** buffer_length length of the output buffer"]
    #[doc = " \n**return** -1 on failure, number of characters copied to buffer on success."]
    pub fn get_counter_label(&self, counter_id: i32, max_length: usize) -> Result<String, AeronCError> {
        let mut result = String::with_capacity(max_length);
        self.get_counter_label_into(counter_id, &mut result)?;
        Ok(result)
    }

    #[inline]
    #[doc = "Get the label for a counter."]
    pub fn get_counter_label_into(&self, counter_id: i32, dst: &mut String) -> Result<(), AeronCError> {
        let capacity = dst.capacity();
        // SAFETY: nothing reads the String until it is cut to a validated UTF-8 prefix.
        let vec = unsafe { dst.as_mut_vec() };
        vec.clear();
        vec.resize(capacity, 0);
        match self.counter_label(counter_id, &mut vec[..]) {
            Ok(written) => {
                // Labels are raw bytes, so a short buffer can split a multibyte char.
                let written = (written as usize).min(capacity);
                let valid = std::str::from_utf8(&vec[..written]).map_or_else(|e| e.valid_up_to(), str::len);
                vec.truncate(valid);
                Ok(())
            }
            Err(e) => {
                vec.clear();
                Err(e)
            }
        }
    }

    /// Get all [`AeronCountersReader::MAX_KEY_LENGTH`] bytes of a counter's key, zero padded.
    ///
    /// This also succeeds for a freed counter. To check a counter is live, also check
    /// [`Self::counter_state`] and [`Self::counter_type_id`].
    #[inline]
    pub fn get_counter_key(&self, counter_id: i32) -> Result<Vec<u8>, AeronCError> {
        let mut dst = Vec::with_capacity(AeronCountersReader::MAX_KEY_LENGTH);
        self.get_counter_key_into(counter_id, &mut dst)?;
        Ok(dst)
    }

    /// Replace `dst` with the counter's key. See [`Self::get_counter_key`].
    #[inline]
    pub fn get_counter_key_into(&self, counter_id: i32, dst: &mut Vec<u8>) -> Result<(), AeronCError> {
        dst.clear();
        // SAFETY: copied out straight away
        dst.extend_from_slice(unsafe { self.counter_key_bytes(counter_id)? });
        Ok(())
    }

    /// Get the bytes up to the first zero, or all of them if there is none.
    /// Use this for keys set from a string. Binary keys need [`Self::get_counter_key`].
    pub fn get_counter_key_str(&self, counter_id: i32) -> Result<String, AeronCError> {
        // SAFETY: copied into the returned `String` before returning
        let key = unsafe { self.counter_key_bytes(counter_id)? };
        let text = std::ffi::CStr::from_bytes_until_nul(key).map_or(key, std::ffi::CStr::to_bytes);
        std::str::from_utf8(text)
            .map(str::to_owned)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))
    }

    /// Borrow all [`AeronCountersReader::MAX_KEY_LENGTH`] bytes of a counter's key without a copy.
    ///
    /// # Safety
    /// The slice points into driver memory. If the counter is freed while you hold it, the
    /// driver can reuse the slot and change these bytes. Only use it while the counter stays
    /// allocated. Otherwise, use [`Self::get_counter_key`].
    pub unsafe fn counter_key_bytes(&self, counter_id: i32) -> Result<&[u8], AeronCError> {
        let mut key_ptr: *mut u8 = std::ptr::null_mut();
        unsafe {
            let result = bindings::aeron_counters_reader_metadata_key(self.get_inner(), counter_id, &mut key_ptr);
            if result < 0 || key_ptr.is_null() {
                return Err(AeronCError::from_code(result));
            }
            Ok(std::slice::from_raw_parts(key_ptr, AeronCountersReader::MAX_KEY_LENGTH))
        }
    }

    /// The current value of a counter. An error if `counter_id` is not a counter id, for
    /// example `-1` from a lookup that found nothing
    #[inline]
    pub fn get_counter_value(&self, counter_id: i32) -> Result<i64, AeronCError> {
        // C checks only `counter_id < 0`; past `max_counter_id` it would point outside the buffer
        if counter_id < 0 || counter_id > self.max_counter_id() {
            return Err(AeronCError::with_message(
                -1,
                "get_counter_value: counter id is outside the reader's range",
            ));
        }
        let value = unsafe { bindings::aeron_counters_reader_addr(self.get_inner(), counter_id) };
        if value.is_null() {
            return Err(AeronCError::with_message(
                -1,
                "get_counter_value: counter has no address",
            ));
        }
        // the driver writes counters while we read them, so read atomically
        Ok(unsafe { std::sync::atomic::AtomicI64::from_ptr(value) }.load(std::sync::atomic::Ordering::Acquire))
    }
}

impl Aeron {
    pub fn new_blocking(context: &AeronContext, timeout: std::time::Duration) -> Result<Self, AeronCError> {
        if let Ok(aeron) = Aeron::new(&context) {
            return Ok(aeron);
        }
        let time = std::time::Instant::now();
        while time.elapsed() < timeout {
            if let Ok(aeron) = Aeron::new(&context) {
                return Ok(aeron);
            }
            #[cfg(debug_assertions)]
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        log::error!("failed to create aeron client for {:?}", context);
        Err(AeronErrorType::TimedOut.into())
    }
}

impl<S: ResourceStorage<aeron_buffer_claim_t>> AeronBufferClaim<S> {
    /// The claimed bytes, to write into. Empty if the claim holds no buffer, for example one
    /// from `new_zeroed_on_heap` or before a successful `try_claim`
    /// # Safety
    /// The publication and native claim must still be live. No other access may
    /// overlap the returned mutable borrow. Prefer `AeronClaim` for safe access.
    #[inline]
    pub unsafe fn data_mut(&mut self) -> &mut [u8] {
        if (unsafe { self.get_inner_ref() }).data.is_null() {
            return &mut [];
        }
        unsafe {
            std::slice::from_raw_parts_mut(
                (unsafe { self.get_inner_ref() }).data,
                (unsafe { self.get_inner_ref() }).length,
            )
        }
    }

    /// The frame header of the claim, or `None` if the claim holds no buffer
    /// # Safety
    /// The publication and native claim must still be live. No other access may
    /// overlap the returned mutable borrow. Prefer `AeronClaim` for safe access.
    #[inline]
    pub unsafe fn frame_header_mut(&mut self) -> Option<&mut aeron_header_values_frame_t> {
        if (unsafe { self.get_inner_ref() }).frame_header.is_null() {
            return None;
        }
        Some(unsafe {
            &mut *(unsafe { self.get_inner_ref() })
                .frame_header
                .cast::<aeron_header_values_frame_t>()
        })
    }
}

/// Zero-copy claim on a publication's term buffer with a RAII commit-or-abort
/// lifecycle.
///
/// A `AeronClaim` that is dropped without being explicitly committed or aborted
/// is **aborted** in [`Drop`], releasing the term-buffer slot immediately
/// instead of waiting for `AERON_PUBLICATION_UNBLOCK_TIMEOUT_NS` (default 15s).
/// [`AeronClaim::commit`] / [`AeronClaim::abort`] consume `self`, encoding
/// Aeron's one-shot claim contract in the type system.
///
/// Construct via [`AeronPublication::try_claim_owned`] or
/// [`AeronExclusivePublication::try_claim_owned`]; the claim is returned only on
/// a successful `try_claim`, so its inner buffer is always a claimed
/// slot (never a null zero-default).
///
/// The claim borrows the publication handle used to create it until it is
/// committed, aborted, or dropped. Other aliases of that publication must not
/// close the underlying C resource while the claim is live
pub struct AeronClaim<'a> {
    // Only successful native claims become guards, so both pointers are non-null.
    // The native output struct remains separate from this owned Rust state.
    frame_header: std::ptr::NonNull<u8>,
    data: std::ptr::NonNull<u8>,
    length: usize,
    position: i64,
    _publication: std::marker::PhantomData<&'a ()>,
}

impl AeronClaim<'_> {
    /// The writable claimed slice (zero-copy into the publication term buffer).
    #[inline]
    pub fn data(&mut self) -> &mut [u8] {
        // SAFETY: only a successful native try_claim constructs a guard. Its
        // data pointer is non-null even for an empty payload, and finalisation
        // consumes the guard. The mutable borrow prevents overlapping slices
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.length) }
    }

    /// Length of the claimed slice in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Whether the claimed slice is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Stream position Aeron assigned to this claim.
    #[inline]
    pub const fn position(&self) -> i64 {
        self.position
    }

    /// Stamp the frame's reserved value, e.g. a send timestamp, which subscribers
    /// read with [`AeronHeader::reserved_value`]. Call before [`Self::commit`].
    #[inline]
    pub fn set_reserved_value(&mut self, value: i64) {
        // SAFETY: a successful claim provides a valid frame header until
        // commit/abort. This field uses Aeron's packed header layout
        unsafe {
            (*self.frame_header.as_ptr().cast::<aeron_header_values_frame_t>()).reserved_value = value;
        }
    }

    /// Commit the claimed bytes, publishing them to subscribers. Consumes `self`.
    #[inline]
    pub fn commit(self) -> Result<i64, AeronCError> {
        #[cfg(feature = "log-c-bindings")]
        log::info!("aeron_buffer_claim_commit(buffer_claim: *mut aeron_buffer_claim_t)");
        self.finish_frame(false);
        let position = self.position;
        // The frame is finished. Disable Drop before logging, which can panic.
        std::mem::forget(self);
        #[cfg(feature = "log-c-bindings")]
        log::info!("  -> 0");
        Ok(position)
    }

    /// Abort the claim, discarding the slot as padding for subscribers.
    /// Consumes `self`.
    #[inline]
    pub fn abort(self) -> Result<(), AeronCError> {
        #[cfg(feature = "log-c-bindings")]
        log::info!("aeron_buffer_claim_abort(buffer_claim: *mut aeron_buffer_claim_t)");
        self.finish_frame(true);
        // The frame is finished. Disable Drop before logging, which can panic.
        std::mem::forget(self);
        #[cfg(feature = "log-c-bindings")]
        log::info!("  -> 0");
        Ok(())
    }

    #[inline]
    fn finish_frame(&self, abort: bool) {
        // Matches aeron_buffer_claim_commit/abort in aeron_publication.c. Those
        // functions always return 0. Keeping the release store here avoids an
        // FFI call and the claim copies needed to pass C its address.
        // SAFETY: only a successful native claim creates this guard. Its frame
        // is non-null and aligned to Aeron's frame alignment; its length fits
        // the native frame length. This guard alone finishes the frame. The
        // release store publishes all payload/header writes to the subscriber.
        unsafe {
            let header = self.frame_header.as_ptr().cast::<aeron_data_header_t>();
            if abort {
                (*header).frame_header.type_ = AERON_HDR_TYPE_PAD as i16;
            }
            std::sync::atomic::AtomicI32::from_ptr(std::ptr::addr_of_mut!((*header).frame_header.frame_length)).store(
                (self.length + AERON_DATA_HEADER_LENGTH as usize) as i32,
                std::sync::atomic::Ordering::Release,
            );
        }
    }
}

impl Drop for AeronClaim<'_> {
    #[inline]
    fn drop(&mut self) {
        // An unfinished guard releases its slot before the unblock timeout.
        #[cfg(feature = "log-c-bindings")]
        log::info!("aeron_buffer_claim_abort(buffer_claim: *mut aeron_buffer_claim_t)");
        self.finish_frame(true);
        #[cfg(feature = "log-c-bindings")]
        log::info!("  -> 0");
    }
}

#[inline(always)]
fn reserved_callback<H: AeronReservedValueSupplierCallback, P: CallbackPolicy>(
    _: &Handler<H, P>,
) -> aeron_reserved_value_supplier_t {
    Some(aeron_reserved_value_supplier_t_callback_catch::<H, P>)
}

macro_rules! impl_publication_methods {
    // `$($supplier_mut)?` is `mut` when the native offer cannot be re-entered from its reserved
    // value supplier, so the borrow checker rejects a nested offer on the same publication
    ($ty:ident, $native:ty, $offer:ident, $offerv:ident, $try_claim:ident $(, $supplier_mut:tt)?) => {
        impl<S: ResourceStorage<$native>> $ty<S> {
            /// Raw, branch-free variant of [`Self::offer`]: returns the new stream position, or a
            /// negative Aeron sentinel (see [`AeronOfferError::from_position`]).
            ///
            /// Distinct name from the generated `offer` method (different signatures)
            /// to avoid macro-hygiene name conflicts.
            #[inline]
            pub fn offer_raw<H: AeronReservedValueSupplierCallback>(
                &$($supplier_mut)? self,
                buffer: &[u8],
                reserved_value_supplier: Option<&Handler<H, impl CallbackPolicy>>,
            ) -> i64 {
                let position = unsafe {
                    $offer(
                        self.get_inner(),
                        buffer.as_ptr() as *mut _,
                        buffer.len(),
                        {
                            let callback: aeron_reserved_value_supplier_t = if reserved_value_supplier.is_none() {
                                None
                            } else {
                                reserved_callback(reserved_value_supplier.unwrap())
                            };
                            callback
                        },
                        reserved_value_supplier
                            .map(|m| m.as_raw())
                            .unwrap_or_else(|| std::ptr::null_mut()),
                    )
                };
                if reserved_value_supplier.is_some() {
                    resume_caught_panic();
                }
                position
            }

            /// Raw, branch-free variant of [`Self::try_claim`]: position or negative sentinel.
            #[inline]
            pub fn try_claim_raw(
                &self,
                length: usize,
                buffer_claim: &AeronBufferClaim<impl ResourceStorage<aeron_buffer_claim_t>>,
            ) -> i64 {
                unsafe { $try_claim(self.get_inner(), length, buffer_claim.get_inner()) }
            }

            /// [`Self::offer`] with a reserved-value supplier.
            #[inline]
            pub fn offer_with_reserved_value<H: AeronReservedValueSupplierCallback>(
                &$($supplier_mut)? self,
                buffer: &[u8],
                reserved_value_supplier: Option<&Handler<H, impl CallbackPolicy>>,
            ) -> Result<i64, AeronOfferError> {
                AeronOfferError::from_position(self.offer_raw(buffer, reserved_value_supplier))
            }

            /// Zero-copy claim returning a RAII [`AeronClaim`] that is auto-aborted on
            /// drop unless explicitly committed or aborted.
            #[inline]
            pub fn try_claim_owned(&self, length: usize) -> Result<AeronClaim<'_>, AeronOfferError> {
                let mut claim = std::mem::MaybeUninit::<aeron_buffer_claim_t>::uninit();
                // SAFETY: the publication is live and the output pointer is writable
                let position = unsafe { $try_claim(self.get_inner(), length, claim.as_mut_ptr()) };
                let position = AeronOfferError::from_position(position)?;
                // SAFETY: both native publication implementations initialise
                // frame_header, data and length on success. Failed claims return
                // above without reading the output or aborting an unclaimed slot
                let claim = unsafe { claim.assume_init() };
                Ok(AeronClaim {
                    // SAFETY: native success points both fields into the live
                    // term buffer, including when the requested length is zero.
                    frame_header: unsafe { std::ptr::NonNull::new_unchecked(claim.frame_header) },
                    data: unsafe { std::ptr::NonNull::new_unchecked(claim.data) },
                    length: claim.length,
                    position,
                    _publication: std::marker::PhantomData,
                })
            }

            /// Gathering (vectored) publish: offer up to [`MAX_OFFER_PARTS`] buffers as ONE
            /// message without concatenating them first: no allocation, and the parts are
            /// copied straight into the term buffer, as `offer` copies its one buffer.
            ///
            /// This is the header+payload send: instead of building a `Vec` per message
            /// (`vec.extend(header); vec.extend(payload); offer(&vec)`), pass the parts:
            ///
            /// ```ignore
            /// publication.offer_parts(&[&header_bytes, payload])?;
            /// ```
            ///
            /// The `aeron_iovec_t` array is built on the stack. Same typed-error semantics
            /// as [`Self::offer`]. Returns [`AeronOfferError::TooManyParts`] if more than
            /// [`MAX_OFFER_PARTS`] parts are passed (use [`Self::offerv`] with your own
            /// iovec array for larger gathers).
            #[inline]
            pub fn offer_parts(&self, parts: &[&[u8]]) -> Result<i64, AeronOfferError> {
                if parts.len() > MAX_OFFER_PARTS {
                    return Err(AeronOfferError::TooManyParts);
                }
                let mut iov = [aeron_iovec_t {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0,
                }; MAX_OFFER_PARTS];
                for (slot, part) in iov.iter_mut().zip(parts) {
                    slot.iov_base = part.as_ptr() as *mut u8;
                    slot.iov_len = part.len();
                }
                let position = unsafe {
                    $offerv(
                        self.get_inner(),
                        iov.as_mut_ptr(),
                        parts.len(),
                        None,
                        std::ptr::null_mut(),
                    )
                };
                AeronOfferError::from_position(position)
            }

            /// High-level connection state derived from the publication handle.
            #[inline]
            pub fn status(&self) -> AeronStatus {
                if self.is_closed() {
                    AeronStatus::Closed
                } else if self.is_connected() {
                    AeronStatus::Connected
                } else {
                    AeronStatus::Disconnected
                }
            }
        }
    };
}

impl_publication_methods!(
    AeronPublication,
    aeron_publication_t,
    aeron_publication_offer,
    aeron_publication_offerv,
    aeron_publication_try_claim
);
impl_publication_methods!(
    AeronExclusivePublication,
    aeron_exclusive_publication_t,
    aeron_exclusive_publication_offer,
    aeron_exclusive_publication_offerv,
    aeron_exclusive_publication_try_claim,
    mut
);

impl<S: ResourceStorage<aeron_publication_t>> AeronPublication<S> {
    /// Non-blocking publish of `buffer`, returning the new stream position.
    ///
    /// Typed-error wrapper over [`Self::offer_raw`]; see
    /// [`AeronOfferError::is_retryable`] for retry-loop guidance.
    #[inline]
    pub fn offer(&self, buffer: &[u8]) -> Result<i64, AeronOfferError> {
        AeronOfferError::from_position(
            self.offer_raw::<AeronReservedValueSupplierLogger>(
                buffer,
                None::<&Handler<AeronReservedValueSupplierLogger>>,
            ),
        )
    }

    /// Zero-copy claim with typed errors. See [`Self::try_claim_owned`] for RAII.
    #[inline]
    pub fn try_claim(
        &self,
        length: usize,
        buffer_claim: &AeronBufferClaim<impl ResourceStorage<aeron_buffer_claim_t>>,
    ) -> Result<i64, AeronOfferError> {
        AeronOfferError::from_position(self.try_claim_raw(length, buffer_claim))
    }
}

impl AeronExclusivePublication {
    /// Safe version of [`Self::revoke`]: revokes and closes the publication, then notifies
    /// `on_close_complete` like [`Self::close_with_handler`]. Errors without revoking when
    /// another owning clone is alive. Runs once at shutdown, so the `SharedHandler` mutex is
    /// off the hot path
    pub fn revoke_with_handler<AeronNotificationHandlerImpl: AeronNotificationCallback + Send + 'static>(
        self,
        on_close_complete: Option<&SharedHandler<AeronNotificationHandlerImpl>>,
    ) -> Result<(), AeronCError> {
        // Check before setting the flag, or a refused close would still revoke on the last close
        if self.inner.as_owned().is_some_and(|owner| ResourceRef::strong_count(owner) > 1) {
            return Err(AeronCError::with_message(-1, "revoke_with_handler requires the last owning handle"));
        }
        self.revoke_on_close();
        self.close_with_handler(on_close_complete)
    }
}

impl<S: ResourceStorage<aeron_exclusive_publication_t>> AeronExclusivePublication<S> {
    /// Without a supplier there is no callback to re-enter, so this keeps `&self`
    #[inline]
    pub fn offer(&self, buffer: &[u8]) -> Result<i64, AeronOfferError> {
        // SAFETY: the publication is live and C only reads `buffer` during the call
        let position = unsafe {
            aeron_exclusive_publication_offer(
                self.get_inner(),
                buffer.as_ptr() as *mut _,
                buffer.len(),
                None,
                std::ptr::null_mut(),
            )
        };
        AeronOfferError::from_position(position)
    }

    #[inline]
    pub fn try_claim(
        &self,
        length: usize,
        buffer_claim: &AeronBufferClaim<impl ResourceStorage<aeron_buffer_claim_t>>,
    ) -> Result<i64, AeronOfferError> {
        AeronOfferError::from_position(self.try_claim_raw(length, buffer_claim))
    }
}

impl<S: ResourceStorage<aeron_subscription_t>> AeronSubscription<S> {
    /// High-level connection state derived from the subscription handle.
    #[inline]
    pub fn status(&self) -> AeronStatus {
        if self.is_closed() {
            AeronStatus::Closed
        } else if self.is_connected() {
            AeronStatus::Connected
        } else {
            AeronStatus::Disconnected
        }
    }

    /// Accessor for the stream id (no direct C accessor exists for
    /// subscriptions; backed by [`Self::get_constants`]). For a hot loop, call
    /// [`Self::get_constants`] once and reuse the returned
    /// [`AeronSubscriptionConstants`].
    #[inline]
    pub fn stream_id(&self) -> Result<i32, AeronCError> {
        self.get_constants().map(|c| c.stream_id())
    }

    /// Accessor for the channel (see [`Self::stream_id`]).
    #[inline]
    pub fn channel(&self) -> Result<String, AeronCError> {
        self.get_constants().map(|c| c.channel().to_string())
    }

    /// Accessor for the registration id (see [`Self::stream_id`]).
    #[inline]
    pub fn registration_id(&self) -> Result<i64, AeronCError> {
        self.get_constants().map(|c| c.registration_id())
    }
}

/// Production error handler that routes Aeron async errors through the Rust `log`
/// facade at **error** level with a concise format. This is the recommended default for
/// `AeronContext::set_error_handler`.
///
/// Not to be confused with the generated [`AeronErrorHandlerLogger`], which is the
/// codegen's generic callback tracer (info level, verbose per-call format, useful
/// only behind `--features log-c-bindings`-style debugging).
pub struct AeronErrorLogger;
impl AeronErrorHandlerCallback for AeronErrorLogger {
    #[cold]
    fn handle_aeron_error_handler(&mut self, error_code: std::ffi::c_int, msg: &str) -> () {
        log::error!("aeron error {}: {}", error_code, msg);
    }
}

/// Status transition tracker that emits only when the observed [`AeronStatus`]
/// differs from the previously recorded state.
///
/// Use it to drive reconnect logic or UI indicators from connection changes.
///
/// # Example
/// ```ignore
/// // (illustrative, as `publication.status()` needs a live handle; see the
/// //  `aeron_custom_tests` unit tests for runnable pure-Rust assertions)
/// let mut tracker = AeronStatusTracker::new();
/// tracker.observe(publication.status()); // emits `Some(Disconnected)`
/// // ... connection establishes ...
/// tracker.observe(publication.status()); // emits `Some(Connected)`
/// tracker.observe(publication.status()); // emits `None` (no change)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AeronStatusTracker {
    last_status: Option<AeronStatus>,
}

impl AeronStatusTracker {
    /// Create a new tracker with no initial state.
    #[must_use]
    pub const fn new() -> Self {
        Self { last_status: None }
    }

    /// Observe a status, returning `Some(status)` if this is a transition from
    /// the previous state (or the first observation), `None` if the status is
    /// unchanged.
    pub fn observe(&mut self, status: AeronStatus) -> Option<AeronStatus> {
        if self.last_status != Some(status) {
            self.last_status = Some(status);
            Some(status)
        } else {
            None
        }
    }

    /// Reset the tracker, causing the next call to [`observe`](Self::observe)
    /// to emit even if the status matches the previously-observed value.
    pub const fn reset(&mut self) {
        self.last_status = None;
    }

    /// The most recent status observed, if any.
    #[must_use]
    pub const fn last_status(&self) -> Option<AeronStatus> {
        self.last_status
    }
}

impl Default for AeronStatusTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate a UDP channel URI endpoint (host:port) for use with Aeron.
///
/// This validates the shape and character set of endpoint strings passed to
/// [`AeronUriStringBuilder::endpoint`] or used directly in channel URIs like
/// `aeron:udp?endpoint=localhost:40123`. Aeron does not validate input upfront;
/// this function rejects malformed endpoints before they reach the C layer, with
/// clearer errors than the C parse failures.
///
/// # Accepted forms
/// - `hostname:port` with host alphanumeric plus `-` and `.`, port 0-65535
/// - `IPv4:port` with dotted decimal host, port 0-65535
/// - `[IPv6]:port` with bracketed IPv6 literal host, port required
///
/// # Errors
/// Returns `Err(AeronCError)` with code `-1` for any validation failure:
/// - empty string or missing port separator
/// - port not `0..=65535` or not a decimal integer
/// - host contains characters outside the safe allowlist (alphanumeric,
///   `-`, `.`, `:` for IPv4, `[]` for IPv6 bracketing)
/// - URI separator characters (`?`, `=`, `/`) appear in the endpoint
///
/// # Example
/// ```ignore
/// // (illustrative, as `aeron_custom.rs` is `include!`'d into multiple crates, so
/// //  no single `use` path compiles across all of them; see the
/// //  `aeron_custom_tests` module for runnable pure-Rust assertions)
/// assert!(validate_endpoint_for_aeron_udp("localhost:40123").is_ok());
/// assert!(validate_endpoint_for_aeron_udp("[::1]:40123").is_ok());
/// assert!(validate_endpoint_for_aeron_udp("localhost:99999").is_err());
/// assert!(validate_endpoint_for_aeron_udp("localhost?foo:8080").is_err());
/// ```
pub fn validate_endpoint_for_aeron_udp(endpoint: &str) -> Result<(), AeronCError> {
    let invalid = || AeronCError::with_message(-1, "invalid UDP endpoint, expected host:port or [IPv6]:port");
    // The last colon separates the port, so an IPv6 host must be bracketed
    let (host, port) = endpoint.rsplit_once(':').ok_or_else(invalid)?;
    // `u16::from_str` alone would accept a leading `+`
    if !port.bytes().all(|b| b.is_ascii_digit()) || port.parse::<u16>().is_err() {
        return Err(invalid());
    }
    let valid_host = match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        Some(ipv6) => ipv6.parse::<std::net::Ipv6Addr>().is_ok(),
        // Hostname or IPv4. Anything else, such as `?`, `=` or `/`, would break channel parsing
        None => !host.is_empty() && host.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.')),
    };
    if valid_host { Ok(()) } else { Err(invalid()) }
}

#[cfg(test)]
mod aeron_custom_tests {
    use super::*;

    #[test]
    fn context_duration_helpers_preserve_units_and_reject_overflow() -> Result<(), AeronCError> {
        use std::time::Duration;
        let context = AeronContext::new()?;
        let interval = Duration::from_nanos(1_234_567);
        context.set_keepalive_interval(interval)?;
        context.set_resource_linger_duration(interval)?;
        context.set_idle_sleep_duration(interval)?;
        assert_eq!(context.get_keepalive_interval_ns(), 1_234_567);
        assert_eq!(context.get_keepalive_interval(), interval);
        assert_eq!(context.get_resource_linger_duration(), interval);
        assert_eq!(context.get_idle_sleep_duration(), interval);

        for (timeout, expected_ms) in [
            (Duration::ZERO, 0),
            (Duration::from_nanos(1), 1),
            (interval, 2),
            (Duration::from_millis(2), 2),
        ] {
            context.set_driver_timeout(timeout)?;
            assert_eq!(context.get_driver_timeout_ms(), expected_ms);
            assert_eq!(context.get_driver_timeout(), Duration::from_millis(expected_ms));
        }
        assert!(context.set_driver_timeout(Duration::MAX).is_err());
        assert!(context.set_keepalive_interval(Duration::MAX).is_err());
        assert!(context.set_resource_linger_duration(Duration::MAX).is_err());
        assert!(context.set_idle_sleep_duration(Duration::MAX).is_err());
        assert_eq!(context.get_driver_timeout_ms(), 2);
        assert_eq!(context.get_keepalive_interval(), interval);
        assert_eq!(context.get_resource_linger_duration(), interval);
        assert_eq!(context.get_idle_sleep_duration(), interval);
        Ok(())
    }

    #[test]
    fn duration_conversion_checks_signed_clock_limits_and_rounding() {
        use std::time::Duration;
        let nanos_limit = i64::MAX as u64;
        assert_eq!(duration_to_nanos(Duration::from_nanos(nanos_limit)).unwrap(), i64::MAX);
        assert!(duration_to_nanos(Duration::from_nanos(nanos_limit + 1)).is_err());
        let millis_limit = nanos_limit / 1_000_000;
        let max_timeout = Duration::from_millis(millis_limit);
        assert_eq!(duration_to_millis(max_timeout).unwrap(), millis_limit);
        assert!(duration_to_millis(max_timeout + Duration::from_nanos(1)).is_err());
    }

    #[test]
    fn uri_duration_helpers_write_nanoseconds_and_reject_overflow() -> Result<(), AeronCError> {
        use std::time::Duration;
        let builder = AeronUriStringBuilder::udp("127.0.0.1:40123")?;
        builder.linger_timeout_duration(Duration::from_millis(2))?;
        builder.nak_delay_duration(Duration::from_micros(3))?;
        builder.untethered_window_limit_timeout_duration(Duration::from_secs(4))?;
        builder.untethered_resting_timeout_duration(Duration::from_nanos(5))?;
        builder.untethered_linger_timeout_duration(Duration::ZERO)?;
        let channel = builder.build(1024)?;
        for parameter in [
            "linger=2000000",
            "nak-delay=3000",
            "untethered-window-limit-timeout=4000000000",
            "untethered-resting-timeout=5",
            "untethered-linger-timeout=0",
        ] {
            assert!(channel.split(['?', '|']).any(|part| part == parameter), "{channel}");
        }
        assert!(builder.linger_timeout_duration(Duration::MAX).is_err());
        assert!(builder.nak_delay_duration(Duration::MAX).is_err());
        assert!(builder.untethered_window_limit_timeout_duration(Duration::MAX).is_err());
        assert!(builder.untethered_resting_timeout_duration(Duration::MAX).is_err());
        assert!(builder.untethered_linger_timeout_duration(Duration::MAX).is_err());
        assert_eq!(builder.build(1024)?, channel);
        Ok(())
    }

    #[test]
    fn from_position_positive_is_ok_position() {
        assert_eq!(AeronOfferError::from_position(0), Ok(0));
        assert_eq!(AeronOfferError::from_position(12_345), Ok(12_345));
    }

    #[test]
    fn from_position_maps_every_sentinel_distinctly() {
        assert_eq!(AeronOfferError::from_position(-1), Err(AeronOfferError::NotConnected));
        assert_eq!(AeronOfferError::from_position(-2), Err(AeronOfferError::BackPressured));
        assert_eq!(AeronOfferError::from_position(-3), Err(AeronOfferError::AdminAction));
        assert_eq!(AeronOfferError::from_position(-4), Err(AeronOfferError::Closed));
        assert_eq!(
            AeronOfferError::from_position(-5),
            Err(AeronOfferError::MaxPositionExceeded)
        );
    }

    #[test]
    fn retryable_vs_fatal_classification() {
        assert!(AeronOfferError::NotConnected.is_retryable());
        assert!(AeronOfferError::BackPressured.is_retryable());
        assert!(AeronOfferError::AdminAction.is_retryable());
        assert!(AeronOfferError::Closed.is_fatal());
        assert!(AeronOfferError::MaxPositionExceeded.is_fatal());
        assert!(AeronOfferError::Error(AeronCError::from_code(-99)).is_fatal());
    }

    #[test]
    fn from_position_unknown_negative_preserves_code() {
        match AeronOfferError::from_position(-99) {
            Err(AeronOfferError::Error(e)) => assert_eq!(e.code, -99),
            other => panic!("expected Error variant, got {other:?}"),
        }
    }

    #[test]
    fn status_from_error_maps_back_pressure_and_closed() {
        assert_eq!(
            AeronStatus::from_error(&AeronOfferError::BackPressured),
            Some(AeronStatus::BackPressured)
        );
        assert_eq!(
            AeronStatus::from_error(&AeronOfferError::Closed),
            Some(AeronStatus::Closed)
        );
    }

    #[test]
    fn status_from_error_returns_none_for_non_status_errors() {
        // PublicationMaxPositionExceeded (-5) is not a status-like transition.
        assert_eq!(AeronStatus::from_error(&AeronOfferError::MaxPositionExceeded), None);
    }

    // A local frame exercises the native commit/abort writes without a driver.
    // The returned guard borrows both allocations until it is finalised
    fn local_claim<'a>(frame: &'a mut aeron_header_values_frame_t, payload: &'a mut [u8]) -> AeronClaim<'a> {
        frame.type_ = AERON_HDR_TYPE_DATA as i16;
        frame.frame_length = -(payload.len() as i32 + 32);
        let claim = aeron_buffer_claim_t {
            frame_header: (frame as *mut aeron_header_values_frame_t).cast(),
            data: payload.as_mut_ptr(),
            length: payload.len(),
        };
        AeronClaim {
            frame_header: std::ptr::NonNull::from(frame).cast(),
            data: std::ptr::NonNull::new(claim.data).unwrap(),
            length: claim.length,
            position: 64,
            _publication: std::marker::PhantomData,
        }
    }

    #[test]
    fn owned_claim_commit_preserves_data_and_does_not_abort() {
        for length in [0, 7] {
            let mut frame = aeron_header_values_frame_t::default();
            let mut payload = vec![0; length];
            let mut claim = local_claim(&mut frame, &mut payload);
            assert_eq!(claim.len(), length);
            assert_eq!(claim.is_empty(), length == 0);
            claim.data().fill(19);
            claim.set_reserved_value(123);
            assert_eq!(claim.commit().unwrap(), 64);
            let (frame_length, frame_type, reserved) = (frame.frame_length, frame.type_, frame.reserved_value);
            assert_eq!(frame_length, length as i32 + 32);
            assert_eq!(frame_type, AERON_HDR_TYPE_DATA as i16);
            assert_eq!(reserved, 123);
            assert_eq!(payload, vec![19; length]);
        }
    }

    #[test]
    fn owned_claim_explicit_abort_marks_padding() {
        let mut frame = aeron_header_values_frame_t::default();
        let mut payload = [0; 7];
        local_claim(&mut frame, &mut payload).abort().unwrap();
        let (length, frame_type) = (frame.frame_length, frame.type_);
        assert_eq!(length, 39);
        assert_eq!(frame_type, AERON_HDR_TYPE_PAD as i16);
    }

    #[test]
    fn owned_claim_aborts_during_unwind() {
        let mut frame = aeron_header_values_frame_t::default();
        let mut payload = [0; 7];
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut claim = local_claim(&mut frame, &mut payload);
            claim.data().fill(19);
            panic!("abandon active claim");
        }));
        assert!(outcome.is_err());
        let (length, frame_type) = (frame.frame_length, frame.type_);
        assert_eq!(length, 39);
        assert_eq!(frame_type, AERON_HDR_TYPE_PAD as i16);
    }

    // --- AeronStatusTracker tests ---

    #[test]
    fn status_tracker_emits_on_first_observation() {
        let mut tracker = AeronStatusTracker::new();
        assert_eq!(
            tracker.observe(AeronStatus::Disconnected),
            Some(AeronStatus::Disconnected)
        );
    }

    #[test]
    fn status_tracker_emits_on_transition() {
        let mut tracker = AeronStatusTracker::new();
        tracker.observe(AeronStatus::Disconnected);
        assert_eq!(tracker.observe(AeronStatus::Connected), Some(AeronStatus::Connected));
    }

    #[test]
    fn status_tracker_does_not_emit_on_same_status() {
        let mut tracker = AeronStatusTracker::new();
        tracker.observe(AeronStatus::Connected);
        assert_eq!(tracker.observe(AeronStatus::Connected), None);
        assert_eq!(tracker.observe(AeronStatus::Connected), None);
    }

    #[test]
    fn status_tracker_emits_again_after_reset() {
        let mut tracker = AeronStatusTracker::new();
        tracker.observe(AeronStatus::Connected);
        assert_eq!(tracker.observe(AeronStatus::Connected), None);

        tracker.reset();
        assert_eq!(tracker.observe(AeronStatus::Connected), Some(AeronStatus::Connected));
    }

    #[test]
    fn status_tracker_records_closed_transition() {
        let mut tracker = AeronStatusTracker::new();
        tracker.observe(AeronStatus::Connected);
        assert_eq!(tracker.observe(AeronStatus::Closed), Some(AeronStatus::Closed));
        // No further transitions from Closed.
        assert_eq!(tracker.observe(AeronStatus::Closed), None);
    }

    #[test]
    fn status_tracker_tracks_all_states() {
        let mut tracker = AeronStatusTracker::new();

        // Full lifecycle: Disconnected -> Connected -> BackPressured -> Connected -> Closed
        assert_eq!(
            tracker.observe(AeronStatus::Disconnected),
            Some(AeronStatus::Disconnected)
        );
        assert_eq!(tracker.observe(AeronStatus::Connected), Some(AeronStatus::Connected));
        assert_eq!(
            tracker.observe(AeronStatus::BackPressured),
            Some(AeronStatus::BackPressured)
        );
        assert_eq!(tracker.observe(AeronStatus::Connected), Some(AeronStatus::Connected));
        assert_eq!(tracker.observe(AeronStatus::Closed), Some(AeronStatus::Closed));
    }

    // --- validate_endpoint_for_aeron_udp tests ---

    #[test]
    fn validate_endpoint_accepts_hostname_with_port() {
        assert!(validate_endpoint_for_aeron_udp("localhost:40123").is_ok());
        assert!(validate_endpoint_for_aeron_udp("localhost:0").is_ok());
        assert!(validate_endpoint_for_aeron_udp("my-host.example.com:65535").is_ok());
    }

    #[test]
    fn validate_endpoint_accepts_ipv4_with_port() {
        assert!(validate_endpoint_for_aeron_udp("127.0.0.1:8080").is_ok());
        assert!(validate_endpoint_for_aeron_udp("192.168.1.1:0").is_ok());
        assert!(validate_endpoint_for_aeron_udp("10.0.0.1:65535").is_ok());
    }

    #[test]
    fn validate_endpoint_accepts_ipv6_bracketed_with_port() {
        assert!(validate_endpoint_for_aeron_udp("[::1]:8080").is_ok());
        assert!(validate_endpoint_for_aeron_udp("[fe80::1]:9000").is_ok());
        assert!(validate_endpoint_for_aeron_udp("[2001:db8::1]:0").is_ok());
        assert!(validate_endpoint_for_aeron_udp("[2001:db8::1]:65535").is_ok());
    }

    #[test]
    fn validate_endpoint_rejects_empty() {
        assert!(validate_endpoint_for_aeron_udp("").is_err());
    }

    #[test]
    fn validate_endpoint_rejects_missing_port() {
        assert!(validate_endpoint_for_aeron_udp("localhost").is_err());
        assert!(validate_endpoint_for_aeron_udp("127.0.0.1").is_err());
        assert!(validate_endpoint_for_aeron_udp("[::1]").is_err());
        assert!(validate_endpoint_for_aeron_udp("[::1]:").is_err());
        assert!(validate_endpoint_for_aeron_udp(":8080").is_err()); // empty host
    }

    #[test]
    fn validate_endpoint_rejects_port_out_of_range() {
        assert!(validate_endpoint_for_aeron_udp("localhost:65536").is_err());
        assert!(validate_endpoint_for_aeron_udp("localhost:99999").is_err());
        assert!(validate_endpoint_for_aeron_udp("[::1]:65536").is_err());
    }

    #[test]
    fn validate_endpoint_rejects_non_digit_port() {
        assert!(validate_endpoint_for_aeron_udp("localhost:abc").is_err());
        assert!(validate_endpoint_for_aeron_udp("localhost:80a0").is_err());
        assert!(validate_endpoint_for_aeron_udp("localhost:-1").is_err());
        assert!(validate_endpoint_for_aeron_udp("localhost:+8080").is_err());
    }

    #[test]
    fn validate_endpoint_rejects_unsafe_characters() {
        // URI separator characters.
        assert!(validate_endpoint_for_aeron_udp("localhost?foo:8080").is_err());
        assert!(validate_endpoint_for_aeron_udp("localhost=bar:8080").is_err());
        assert!(validate_endpoint_for_aeron_udp("localhost/path:8080").is_err());

        // Characters outside the safe allowlist.
        assert!(validate_endpoint_for_aeron_udp("local_host:8080").is_err()); // underscore
        assert!(validate_endpoint_for_aeron_udp("local host:8080").is_err()); // space
        assert!(validate_endpoint_for_aeron_udp("local$host:8080").is_err()); // $
    }

    #[test]
    fn validate_endpoint_rejects_malformed_ipv6() {
        // Missing closing bracket.
        assert!(validate_endpoint_for_aeron_udp("[::1:8080").is_err());

        // Empty brackets.
        assert!(validate_endpoint_for_aeron_udp("[]:8080").is_err());

        // Port missing after bracket.
        assert!(validate_endpoint_for_aeron_udp("[::1]").is_err());

        // Host after bracket (port must immediately follow `]`).
        assert!(validate_endpoint_for_aeron_udp("[::1]x:8080").is_err());

        // Bracket without colon in the right place.
        assert!(validate_endpoint_for_aeron_udp("[::1]8080").is_err());
    }

    #[test]
    fn validate_endpoint_rejects_invalid_hosts_that_pass_the_character_check() {
        // An unbracketed host keeps every colon but the last, so these are not host:port
        assert!(validate_endpoint_for_aeron_udp("a:b:8080").is_err());
        assert!(validate_endpoint_for_aeron_udp("127.0.0.1:1:8080").is_err());
        // Hex digits and colons alone do not make an IPv6 address
        assert!(validate_endpoint_for_aeron_udp("[:::::]:8080").is_err());
        assert!(validate_endpoint_for_aeron_udp("[1:2:3:4:5:6:7:8:9]:8080").is_err());
        assert!(validate_endpoint_for_aeron_udp("[12345::1]:8080").is_err());
    }

    #[test]
    fn validate_endpoint_rejects_colon_in_wrong_position() {
        // Unbracketed with multiple colons is invalid (IPv4 has at most 3 colons,
        // but we require host:port, so only the last colon is the separator).
        assert!(validate_endpoint_for_aeron_udp("::1:8080").is_err());

        // Colon at start is invalid (empty host).
        assert!(validate_endpoint_for_aeron_udp(":8080").is_err());
    }

    #[test]
    fn validate_endpoint_accepts_hyphenated_hostname() {
        assert!(validate_endpoint_for_aeron_udp("my-host-name:8080").is_ok());
        assert!(validate_endpoint_for_aeron_udp("host-1:40123").is_ok());
    }

    #[test]
    fn validate_endpoint_accepts_dotted_hostname() {
        assert!(validate_endpoint_for_aeron_udp("host.sub.example.com:8080").is_ok());
        assert!(validate_endpoint_for_aeron_udp("a.b.c.d:40123").is_ok());
    }
}

// Retryable / unrecoverable classification for Aeron errors. Lives in hand-written code rather
// than the codegen `common.rs` because the generator drops methods that carry multi-line doc
// comments. GenericError(-1) and Unknown(_) are intentionally neither. `-1` is Aeron's catch-all
// and could mean anything, so the caller must decide (treat as fatal if unsure).
impl AeronErrorType {
    /// Transient. Retry the operation (back off first): back-pressure, admin action, a full
    /// client buffer, or a polling timeout.
    pub fn is_retryable(&self) -> bool {
        self == &AeronErrorType::PublicationBackPressured
            || self == &AeronErrorType::PublicationAdminAction
            || self == &AeronErrorType::ClientErrorBufferFull
            || self == &AeronErrorType::ClientErrorDriverBufferFull
            || self == &AeronErrorType::TimedOut
    }

    /// Definitively terminal, so retrying will not help. The publication is closed / exhausted /
    /// errored, or the driver or client has timed out (effectively dead). Not exhaustive: an
    /// ambiguous code (`GenericError` / `Unknown`) is neither retryable nor unrecoverable.
    pub fn is_unrecoverable(&self) -> bool {
        self == &AeronErrorType::PublicationClosed
            || self == &AeronErrorType::PublicationMaxPositionExceeded
            || self == &AeronErrorType::PublicationError
            || self == &AeronErrorType::ClientErrorDriverTimeout
            || self == &AeronErrorType::ClientErrorClientTimeout
            || self == &AeronErrorType::ClientErrorConductorServiceTimeout
            || self == &AeronErrorType::ClientErrorClientIllegalState
            || self == &AeronErrorType::ClientErrorBroadcastReceiverIllegalState
    }
}

impl AeronCError {
    /// Transient failure. Retry the operation (back off first). See [`AeronErrorType::is_retryable`].
    pub fn is_retryable(&self) -> bool {
        self.kind().is_retryable()
    }

    /// Definitively terminal. Abort the operation. See [`AeronErrorType::is_unrecoverable`].
    /// Not exhaustive: ambiguous codes are neither retryable nor unrecoverable.
    pub fn is_unrecoverable(&self) -> bool {
        self.kind().is_unrecoverable()
    }
}

// Owned assemblers
//
// The four native assemblers share one shape: C keeps a pointer to the boxed callback, and the
// assembler itself is the handler passed to `poll`. The multi-session ones keep a buffer per
// session in a hash map. The image ones keep one buffer for a single image, allocated at create,
// so reassembly never looks up or allocates a session buffer while polling
macro_rules! owned_assembler {
    (
        $(#[$doc:meta])*
        $name:ident, $native:ty, $callback:ident, $handle:ident -> $action:ty,
        $create:ident, $delete:ident, $assemble:ident, $catch:ident,
        $native_handler:ty, poll $target:ident<$target_native:ty>.$poll:ident
    ) => {
        $(#[$doc])*
        pub struct $name<T> {
            native: std::ptr::NonNull<$native>,
            handler: Box<T>,
            _local: std::marker::PhantomData<std::rc::Rc<()>>,
        }

        impl<T: $callback> $name<T> {
            pub fn new(handler: T) -> Result<Self, AeronCError> {
                let mut handler = Box::new(handler);
                let mut native = std::ptr::null_mut();
                // SAFETY: the callback stays at a stable address until after native
                // cleanup. The delegate catches panics before returning through C.
                let result = unsafe { $create(&mut native, Some($catch::<T>), (&mut *handler as *mut T).cast()) };
                if result < 0 {
                    return Err(AeronCError::from_code(result));
                }
                let native = std::ptr::NonNull::new(native).ok_or_else(|| AeronCError::from_code(-1))?;
                Ok(Self { native, handler, _local: std::marker::PhantomData })
            }

            /// Read callback state between polls.
            pub fn handler(&self) -> &T {
                &self.handler
            }

            /// Change callback state between polls.
            pub fn handler_mut(&mut self) -> &mut T {
                &mut self.handler
            }

            /// Poll and call the handler with complete messages.
            #[inline]
            pub fn poll<S: ResourceStorage<$target_native>>(
                &mut self,
                source: &mut $target<S>,
                fragment_limit: usize,
            ) -> Result<i32, AeronCError> {
                source.$poll(self, fragment_limit)
            }
        }

        impl<T> Drop for $name<T> {
            fn drop(&mut self) {
                // SAFETY: this is the only owner. The callback is still alive here.
                unsafe {
                    $delete(self.native.as_ptr());
                }
            }
        }

        impl<T: $callback> $callback for $name<T> {
            const RESUMES_CAUGHT_PANIC: bool = true;

            #[inline]
            fn native_poll_callback(&mut self) -> Option<NativePollCallback<'_, $native_handler>> {
                // SAFETY: C and C-unwind have the same calling convention. The delegate
                // catches panics before returning through the native assembler.
                let callback = unsafe {
                    std::mem::transmute::<_, $native_handler>(Some(
                        $assemble as unsafe extern "C" fn(_, _, _, _) -> _,
                    ))
                };
                Some(NativePollCallback { callback, clientd: self.native.as_ptr().cast(), borrow: std::marker::PhantomData })
            }

            fn $handle(&mut self, buffer: &[u8], header: &AeronHeader) -> $action {
                // SAFETY: exclusive access keeps the native assembler and its delegate
                // valid for this call. The buffer and header are borrowed until it returns.
                unsafe { $assemble(self.native.as_ptr().cast(), buffer.as_ptr(), buffer.len(), header.get_inner()) }
            }
        }
    };
}

owned_assembler!(
    /// A fragment assembler that owns its callback.
    ///
    /// Polling and changing callback state require an exclusive borrow. No reference
    /// count, mutex or runtime callback borrow check is needed while polling.
    /// The callback may borrow local state. This assembler stays on its thread and
    /// cannot be cloned
    ///
    /// Use [`Self::handler`] and [`Self::handler_mut`] to access callback state
    /// between calls to [`Self::poll`]. Use [`AeronImageFragmentAssembler`] when polling one image
    FragmentAssembler, aeron_fragment_assembler_t, AeronFragmentHandlerCallback,
    handle_aeron_fragment_handler -> (),
    aeron_fragment_assembler_create, aeron_fragment_assembler_delete, aeron_fragment_assembler_handler,
    aeron_fragment_handler_t_callback_for_borrowed_handler_catch,
    aeron_fragment_handler_t, poll AeronSubscription<aeron_subscription_t>.poll
);

owned_assembler!(
    /// A controlled fragment assembler that owns its callback.
    ///
    /// Use this when the callback needs to return `CONTINUE`, `ABORT`, `BREAK`, or
    /// `COMMIT`. Like [`FragmentAssembler`], it can borrow local state, requires an
    /// exclusive borrow while polling, and has no lock or reference count on the
    /// polling path. Use [`Self::handler_mut`] to change callback state between polls.
    ControlledFragmentAssembler, aeron_controlled_fragment_assembler_t, AeronControlledFragmentHandlerCallback,
    handle_aeron_controlled_fragment_handler -> aeron_controlled_fragment_handler_action_t,
    aeron_controlled_fragment_assembler_create, aeron_controlled_fragment_assembler_delete,
    aeron_controlled_fragment_assembler_handler,
    aeron_controlled_fragment_handler_t_callback_for_borrowed_handler_catch,
    aeron_controlled_fragment_handler_t, poll AeronSubscription<aeron_subscription_t>.controlled_poll
);

owned_assembler!(
    /// A fragment assembler for a single image, such as one from
    /// [`AeronSubscription::image_at_index`], that owns its callback.
    ///
    /// Unlike [`FragmentAssembler`] it keeps one buffer, allocated in [`Self::new`], so a
    /// fragmented message needs no session lookup or allocation while polling. Feed it fragments
    /// from one session only
    AeronImageFragmentAssembler, aeron_image_fragment_assembler_t, AeronFragmentHandlerCallback,
    handle_aeron_fragment_handler -> (),
    aeron_image_fragment_assembler_create, aeron_image_fragment_assembler_delete,
    aeron_image_fragment_assembler_handler,
    aeron_fragment_handler_t_callback_for_borrowed_handler_catch,
    aeron_fragment_handler_t, poll AeronImage<aeron_image_t>.poll
);

owned_assembler!(
    /// The controlled counterpart of [`AeronImageFragmentAssembler`], for a single image
    AeronImageControlledFragmentAssembler, aeron_image_controlled_fragment_assembler_t,
    AeronControlledFragmentHandlerCallback,
    handle_aeron_controlled_fragment_handler -> aeron_controlled_fragment_handler_action_t,
    aeron_image_controlled_fragment_assembler_create, aeron_image_controlled_fragment_assembler_delete,
    aeron_image_controlled_fragment_assembler_handler,
    aeron_controlled_fragment_handler_t_callback_for_borrowed_handler_catch,
    aeron_controlled_fragment_handler_t, poll AeronImage<aeron_image_t>.controlled_poll
);

impl<T> FragmentAssembler<T> {
    /// Release the partial message buffer for a session, if one exists.
    pub fn delete_session_buffer(&mut self, session_id: i32) -> bool {
        // SAFETY: the native assembler is live and exclusively borrowed.
        unsafe { aeron_fragment_assembler_delete_session_buffer(self.native.as_ptr(), session_id) }
    }
}

impl<T> ControlledFragmentAssembler<T> {
    /// Release the partial message buffer for a session, if one exists.
    pub fn delete_session_buffer(&mut self, session_id: i32) -> bool {
        // SAFETY: the native assembler is live and exclusively borrowed.
        unsafe { aeron_controlled_fragment_assembler_delete_session_buffer(self.native.as_ptr(), session_id) }
    }
}

// The native assembler struct is opaque, so its delegate cannot change per poll. The closure
// assemblers give C one fixed delegate that reads this slot, and every entry point writes the
// closure into the slot before C can call it. A stale pointer from an earlier call is never read
struct ClosureSlot<A> {
    call: unsafe fn(*mut (), &[u8], &AeronHeader<Borrowed<aeron_header_t>>) -> A,
    ctx: *mut (),
}

impl<A> ClosureSlot<A> {
    #[inline(always)]
    fn set<F: FnMut(&[u8], &AeronHeader<Borrowed<aeron_header_t>>) -> A>(&mut self, f: &mut F) {
        unsafe fn shim<A, F: FnMut(&[u8], &AeronHeader<Borrowed<aeron_header_t>>) -> A>(
            ctx: *mut (), buffer: &[u8], header: &AeronHeader<Borrowed<aeron_header_t>>,
        ) -> A {
            // SAFETY: `set` stored a live `&mut F` that stays borrowed for the whole poll
            unsafe { (*ctx.cast::<F>())(buffer, header) }
        }
        self.call = shim::<A, F>;
        self.ctx = (f as *mut F).cast();
    }
}

impl AeronFragmentHandlerCallback for ClosureSlot<()> {
    #[inline(always)]
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: &AeronHeader<Borrowed<aeron_header_t>>) {
        // SAFETY: only reached from a poll that called `set` first
        unsafe { (self.call)(self.ctx, buffer, header) }
    }
}

impl AeronControlledFragmentHandlerCallback for ClosureSlot<aeron_controlled_fragment_handler_action_t> {
    #[inline(always)]
    fn handle_aeron_controlled_fragment_handler(
        &mut self, buffer: &[u8], header: &AeronHeader<Borrowed<aeron_header_t>>,
    ) -> aeron_controlled_fragment_handler_action_t {
        // SAFETY: only reached from a poll that called `set` first
        unsafe { (self.call)(self.ctx, buffer, header) }
    }
}

/// A fragment assembler that takes a new closure on every poll.
///
/// Use [`FragmentAssembler`] when the callback is a fixed struct. This one costs one extra
/// indirect call per complete message and has no lock, reference count or allocation per poll.
/// A persistent subscription already reassembles, so poll it with `poll_fn` instead
///
/// ```ignore
/// let mut assembler = AeronFragmentClosureAssembler::new()?;
/// assembler.poll(&subscription, |msg, header| println!("{} bytes", msg.len()), 10)?;
/// // Or with any poll that takes a handler
/// subscription.poll(&mut assembler.with(|msg, _header| total += msg.len()), 10)?;
/// ```
pub struct AeronFragmentClosureAssembler {
    inner: FragmentAssembler<ClosureSlot<()>>,
}

impl AeronFragmentClosureAssembler {
    pub fn new() -> Result<Self, AeronCError> {
        fn noop(_: *mut (), _: &[u8], _: &AeronHeader<Borrowed<aeron_header_t>>) {}
        Ok(Self { inner: FragmentAssembler::new(ClosureSlot { call: noop, ctx: std::ptr::null_mut() })? })
    }

    /// Wrap `handler` so this assembler can be passed to any poll that takes a fragment handler
    #[inline]
    pub fn with<F: FnMut(&[u8], &AeronHeader<Borrowed<aeron_header_t>>)>(&mut self, handler: F) -> FragmentClosureHandler<'_, F> {
        FragmentClosureHandler { assembler: &mut self.inner, handler }
    }

    /// Poll a subscription and call `handler` with complete messages
    #[inline]
    pub fn poll<S: ResourceStorage<aeron_subscription_t>, F: FnMut(&[u8], &AeronHeader<Borrowed<aeron_header_t>>)>(
        &mut self, subscription: &mut AeronSubscription<S>, handler: F, fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        subscription.poll(&mut self.with(handler), fragment_limit)
    }

    /// Release the partial message buffer for a session, if one exists.
    pub fn delete_session_buffer(&mut self, session_id: i32) -> bool {
        self.inner.delete_session_buffer(session_id)
    }
}

/// A closure bound to a [`AeronFragmentClosureAssembler`] for one poll call
pub struct FragmentClosureHandler<'a, F> {
    assembler: &'a mut FragmentAssembler<ClosureSlot<()>>,
    handler: F,
}

impl<F: FnMut(&[u8], &AeronHeader<Borrowed<aeron_header_t>>)> AeronFragmentHandlerCallback for FragmentClosureHandler<'_, F> {
    const RESUMES_CAUGHT_PANIC: bool = true;

    #[inline]
    fn native_poll_callback(&mut self) -> Option<NativePollCallback<'_, aeron_fragment_handler_t>> {
        // The poll borrows `self` until it returns, so the closure cannot move while C uses it
        self.assembler.handler_mut().set(&mut self.handler);
        self.assembler.native_poll_callback()
    }

    #[inline]
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: &AeronHeader<Borrowed<aeron_header_t>>) {
        self.assembler.handler_mut().set(&mut self.handler);
        self.assembler.handle_aeron_fragment_handler(buffer, header)
    }
}

/// A controlled fragment assembler that takes a new closure on every poll.
///
/// Use [`ControlledFragmentAssembler`] when the callback is a fixed struct. Costs and usage
/// match [`AeronFragmentClosureAssembler`], but the closure returns the poll action.
/// A persistent subscription already reassembles, so poll it with `controlled_poll_fn` instead
pub struct AeronControlledFragmentClosureAssembler {
    inner: ControlledFragmentAssembler<ClosureSlot<aeron_controlled_fragment_handler_action_t>>,
}

impl AeronControlledFragmentClosureAssembler {
    pub fn new() -> Result<Self, AeronCError> {
        fn abort(_: *mut (), _: &[u8], _: &AeronHeader<Borrowed<aeron_header_t>>) -> aeron_controlled_fragment_handler_action_t {
            aeron_controlled_fragment_handler_action_t::AERON_ACTION_ABORT
        }
        Ok(Self { inner: ControlledFragmentAssembler::new(ClosureSlot { call: abort, ctx: std::ptr::null_mut() })? })
    }

    /// Wrap `handler` so this assembler can be passed to any controlled poll
    #[inline]
    pub fn with<F: FnMut(&[u8], &AeronHeader<Borrowed<aeron_header_t>>) -> aeron_controlled_fragment_handler_action_t>(
        &mut self, handler: F,
    ) -> ControlledFragmentClosureHandler<'_, F> {
        ControlledFragmentClosureHandler { assembler: &mut self.inner, handler }
    }

    /// Poll a subscription and call `handler` with complete messages
    #[inline]
    pub fn poll<
        S: ResourceStorage<aeron_subscription_t>,
        F: FnMut(&[u8], &AeronHeader<Borrowed<aeron_header_t>>) -> aeron_controlled_fragment_handler_action_t,
    >(
        &mut self, subscription: &mut AeronSubscription<S>, handler: F, fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        subscription.controlled_poll(&mut self.with(handler), fragment_limit)
    }

    /// Release the partial message buffer for a session, if one exists.
    pub fn delete_session_buffer(&mut self, session_id: i32) -> bool {
        self.inner.delete_session_buffer(session_id)
    }
}

/// A closure bound to a [`AeronControlledFragmentClosureAssembler`] for one poll call
pub struct ControlledFragmentClosureHandler<'a, F> {
    assembler: &'a mut ControlledFragmentAssembler<ClosureSlot<aeron_controlled_fragment_handler_action_t>>,
    handler: F,
}

impl<F: FnMut(&[u8], &AeronHeader<Borrowed<aeron_header_t>>) -> aeron_controlled_fragment_handler_action_t>
    AeronControlledFragmentHandlerCallback for ControlledFragmentClosureHandler<'_, F>
{
    const RESUMES_CAUGHT_PANIC: bool = true;

    #[inline]
    fn native_poll_callback(&mut self) -> Option<NativePollCallback<'_, aeron_controlled_fragment_handler_t>> {
        // The poll borrows `self` until it returns, so the closure cannot move while C uses it
        self.assembler.handler_mut().set(&mut self.handler);
        self.assembler.native_poll_callback()
    }

    #[inline]
    fn handle_aeron_controlled_fragment_handler(
        &mut self, buffer: &[u8], header: &AeronHeader<Borrowed<aeron_header_t>>,
    ) -> aeron_controlled_fragment_handler_action_t {
        self.assembler.handler_mut().set(&mut self.handler);
        self.assembler.handle_aeron_controlled_fragment_handler(buffer, header)
    }
}


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
        let Some(owner) = self.inner.as_owned() else {
            return Err(AeronCError::with_message(
                -1,
                "persistent subscription context requires owning storage",
            ));
        };
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
        let Some(owner) = self.inner.as_owned() else {
            return Err(AeronCError::with_message(
                -1,
                "persistent subscription context requires owning storage",
            ));
        };
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
    /// Borrowed for messages from Rust checks, owned for text copied from native code
    pub message: std::borrow::Cow<'static, str>,
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
            message: Aeron::errmsg_owned().into(),
        }
    }

    /// A Rust check rejected the call before C ran, so there is no native error to read.
    /// Generated Archive methods reach this through their `AeronCError` token substitution
    #[cold]
    #[allow(dead_code)]
    pub(crate) fn with_message(code: i32, message: &'static str) -> Self {
        Self {
            code: AeronArchiveErrorCode::from_native_code(code),
            message: message.into(),
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

    // `get_and_own_persistent_subscription_context` is intentionally omitted: `build`
    // already moves the context into Rust ownership, so a second transfer would double free
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
            message: poller.error_message_owned().into(),
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

