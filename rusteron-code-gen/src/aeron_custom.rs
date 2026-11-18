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
        let mut cnc = unsafe { AeronCnc::from_ptr(cnc) };
        handler(&mut cnc);
        unsafe { aeron_cnc_close(cnc.get_inner()) };
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
            client.inner.as_owned().unwrap().bind_conductor_thread()?;
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
    /// The returned [`AeronImage`] releases the image when its last clone drops. Until then
    /// the image stays valid even once it becomes unavailable: [`AeronImage::is_closed`]
    /// turns true, polling reads nothing more, and its log buffer stays mapped. Drop the
    /// handles before calling [`Self::close`]: after it, a handle must not be used, and its
    /// image may stay mapped until the client closes.
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
        let subscription = self.clone();
        let resource = ManagedCResource::<aeron_image_t>::new(
            move |ctx| {
                unsafe { *ctx = image };
                0
            },
            Some(Box::new(move |ctx| unsafe {
                // skip the release if the subscription was closed first, as the C client
                // reclaimed the image during the subscription close
                if !subscription.get_inner().is_null() {
                    aeron_image_release(*ctx)
                } else {
                    0
                }
            })),
            false,
        )
        .ok()?;
        Some(AeronImage {
            inner: CResource::new(ResourceRef::new(resource)),
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
    /// ```
    pub fn find_by_type_and_registration_id(&self, type_id: i32, registration_id: i64) -> Option<i32> {
        self.find_by_type_id(type_id, |key| {
            key.len() >= 8 && i64::from_le_bytes(key[..8].try_into().unwrap()) == registration_id
        })
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

impl AeronFragmentAssembler {
    /// Deletes the native assembler and releases its callback.
    pub fn delete(self) -> Result<i32, AeronCError> {
        self.inner.close_resource_deferred_if_shared()?;
        Ok(0)
    }
}

impl AeronControlledFragmentAssembler {
    /// Deletes the native assembler and releases its callback.
    pub fn delete(self) -> Result<i32, AeronCError> {
        self.inner.close_resource_deferred_if_shared()?;
        Ok(0)
    }
}

impl AeronImageFragmentAssembler {
    /// Deletes the native assembler and releases its callback.
    pub fn delete(self) -> Result<i32, AeronCError> {
        self.inner.close_resource_deferred_if_shared()?;
        Ok(0)
    }
}

impl AeronImageControlledFragmentAssembler {
    /// Deletes the native assembler and releases its callback.
    pub fn delete(self) -> Result<i32, AeronCError> {
        self.inner.close_resource_deferred_if_shared()?;
        Ok(0)
    }
}

// The delegate's panic is caught so the assembler state stays valid; the poll raises it on return
impl AeronFragmentHandlerCallback for AeronFragmentAssembler {
    const RESUMES_CAUGHT_PANIC: bool = true;

    #[inline]
    fn native_poll_callback(&mut self) -> Option<NativePollCallback<'_, aeron_fragment_handler_t>> {
        // SAFETY: C and C-unwind use the same calling convention. The native
        // assembler calls a delegate that catches panics before returning to C.
        let callback = unsafe {
            std::mem::transmute::<_, aeron_fragment_handler_t>(Some(
                aeron_fragment_assembler_handler as unsafe extern "C" fn(_, _, _, _),
            ))
        };
        Some(NativePollCallback {
            callback,
            clientd: self.get_inner().cast(),
            borrow: std::marker::PhantomData,
        })
    }

    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: &AeronHeader) -> () {
        unsafe {
            aeron_fragment_assembler_handler(
                self.get_inner() as *mut _,
                buffer.as_ptr(),
                buffer.len(),
                header.get_inner(),
            )
        }
    }
}

impl AeronControlledFragmentHandlerCallback for AeronControlledFragmentAssembler {
    const RESUMES_CAUGHT_PANIC: bool = true;

    #[inline]
    fn native_poll_callback(&mut self) -> Option<NativePollCallback<'_, aeron_controlled_fragment_handler_t>> {
        // SAFETY: C and C-unwind use the same calling convention. The native
        // assembler calls a delegate that catches panics before returning to C.
        let callback = unsafe {
            std::mem::transmute::<_, aeron_controlled_fragment_handler_t>(Some(
                aeron_controlled_fragment_assembler_handler as unsafe extern "C" fn(_, _, _, _) -> _,
            ))
        };
        Some(NativePollCallback {
            callback,
            clientd: self.get_inner().cast(),
            borrow: std::marker::PhantomData,
        })
    }

    fn handle_aeron_controlled_fragment_handler(
        &mut self,
        buffer: &[u8],
        header: &AeronHeader,
    ) -> aeron_controlled_fragment_handler_action_t {
        unsafe {
            aeron_controlled_fragment_assembler_handler(
                self.get_inner() as *mut _,
                buffer.as_ptr(),
                buffer.len(),
                header.get_inner(),
            )
        }
    }
}

impl<T: AeronFragmentHandlerCallback + 'static> Handler<T> {
    /// Wrap `handler` in a fragment assembler; both are reference-counted and freed
    /// automatically when the last clones drop (the assembler keeps the delegate alive).
    pub fn with_fragment_assembler(handler: T) -> Result<(Handler<AeronFragmentAssembler>, Handler<T>), AeronCError> {
        let handler = Handler::new(handler);
        Ok((Handler::new(AeronFragmentAssembler::new(Some(&handler))?), handler))
    }
}

impl<T: AeronControlledFragmentHandlerCallback + 'static> Handler<T> {
    /// Wrap `handler` in a controlled fragment assembler; both are reference-counted and
    /// freed automatically when the last clones drop.
    pub fn with_controlled_fragment_assembler(
        handler: T,
    ) -> Result<(Handler<AeronControlledFragmentAssembler>, Handler<T>), AeronCError> {
        let handler = Handler::new(handler);
        Ok((
            Handler::new(AeronControlledFragmentAssembler::new(Some(&handler))?),
            handler,
        ))
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
    ($ty:ident, $native:ty, $offer:ident, $offerv:ident, $try_claim:ident) => {
        impl<S: ResourceStorage<$native>> $ty<S> {
            /// Raw, branch-free variant of [`Self::offer`]: returns the new stream position, or a
            /// negative Aeron sentinel (see [`AeronOfferError::from_position`]).
            ///
            /// Distinct name from the generated `offer` method (different signatures)
            /// to avoid macro-hygiene name conflicts.
            #[inline]
            pub fn offer_raw<H: AeronReservedValueSupplierCallback>(
                &self,
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
                &self,
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

            /// Accessor for the registration id (no direct C accessor exists;
            /// backed by [`Self::get_constants`]). `session_id`, `stream_id` and `channel`
            /// have cheap direct getters, so prefer those; for `registration_id` /
            /// `max_payload_length` in a hot loop, call [`Self::get_constants`] once and
            /// reuse the returned [`AeronPublicationConstants`].
            #[inline]
            pub fn registration_id(&self) -> Result<i64, AeronCError> {
                self.get_constants().map(|c| c.registration_id())
            }

            /// Accessor for the max payload length (see [`Self::registration_id`]).
            #[inline]
            pub fn max_payload_length(&self) -> Result<usize, AeronCError> {
                self.get_constants().map(|c| c.max_payload_length())
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
    aeron_exclusive_publication_try_claim
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

impl<S: ResourceStorage<aeron_exclusive_publication_t>> AeronExclusivePublication<S> {
    #[inline]
    pub fn offer(&self, buffer: &[u8]) -> Result<i64, AeronOfferError> {
        AeronOfferError::from_position(
            self.offer_raw::<AeronReservedValueSupplierLogger>(
                buffer,
                None::<&Handler<AeronReservedValueSupplierLogger>>,
            ),
        )
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

impl<S: ResourceStorage<aeron_image_t>> AeronImage<S> {
    /// Instrumented wrapper around [`AeronImage::poll`] that adds tracing spans
    /// when the `instrument-ops` feature is enabled.
    #[inline]
    pub fn poll_instrumented<AeronFragmentHandlerHandlerImpl: AeronFragmentHandlerCallback>(
        &self,
        handler: &mut AeronFragmentHandlerHandlerImpl,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        self.poll(handler, fragment_limit)
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

/// Runs the closure when dropped, so cleanup also happens when a panic unwinds
struct OnDrop<F: FnMut()>(F);

impl<F: FnMut()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        (self.0)()
    }
}

pub struct FnMutMessageHandler {
    func: fn(*mut (), &[u8], &AeronHeader),
    ctx: *mut (),
}

impl AeronFragmentHandlerCallback for FnMutMessageHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: &AeronHeader) -> () {
        self.call(buffer, header);
    }
}

impl FnMutMessageHandler {
    pub const fn new() -> Self {
        Self {
            func: Self::noop,
            ctx: std::ptr::null_mut(),
        }
    }

    #[inline]
    /// Point this handler at `ctx` / `func` so the next [`call`](Self::call)
    /// dispatches into them.
    ///
    /// # Safety
    /// `ctx` must stay live, unmoved and exclusively available to this handler
    /// until `clear` or the next `set`. The handler stores its address without a lifetime.
    pub unsafe fn set<T>(&mut self, ctx: &mut T, func: fn(&mut T, &[u8], &AeronHeader)) -> &mut Self {
        self.func = Self::wrap::<T>(func);
        self.ctx = ctx as *mut T as *mut ();
        self
    }

    #[inline(always)]
    pub fn call(&mut self, msg: &[u8], header: &AeronHeader) {
        (self.func)(self.ctx, msg, header);
    }

    /// Drop the borrowed `ctx` so a later stray [`call`](Self::call) is a safe no-op
    /// (the `noop` func is restored and the pointer nulled) rather than a use-after-free.
    ///
    /// [`AeronFragmentClosureAssembler::poll`] calls this before returning so the
    /// raw pointer never outlives the borrow it was created from.
    #[inline]
    pub const fn clear(&mut self) {
        self.func = Self::noop;
        self.ctx = std::ptr::null_mut();
    }

    #[inline]
    fn wrap<T>(f: fn(&mut T, &[u8], &AeronHeader)) -> fn(*mut (), &[u8], &AeronHeader) {
        // SAFETY: `fn(&mut T,…)` and `fn(*mut(),…)` have the same ABI/representation
        unsafe { std::mem::transmute(f) }
    }

    fn noop(_: *mut (), _: &[u8], _: &AeronHeader) {
        // default no-op handler
    }
}

/// A poll target whose raw fragments can be reassembled by [`AeronFragmentAssembler`].
/// Implemented for [`AeronSubscription`] here and for archive types in rusteron-archive.
pub trait FragmentAssemblable {
    fn poll_with_assembler(
        &self,
        assembler: Option<&Handler<AeronFragmentAssembler>>,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError>;
}

impl FragmentAssemblable for AeronSubscription {
    #[inline]
    fn poll_with_assembler(
        &self,
        assembler: Option<&Handler<AeronFragmentAssembler>>,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        self.poll_with_handler(assembler, fragment_limit)
    }
}

pub struct AeronFragmentClosureAssembler {
    assembler: Handler<AeronFragmentAssembler>,
    handler: Handler<FnMutMessageHandler>,
}

impl AeronFragmentClosureAssembler {
    pub fn new() -> Result<Self, AeronCError> {
        let handler = Handler::new(FnMutMessageHandler::new());
        Ok(Self {
            assembler: Handler::new(AeronFragmentAssembler::new(Some(&handler))?),
            handler,
        })
    }

    /// Poll `pollable` (a subscription or persistent subscription), dispatching each
    /// reassembled message to `func`. `ctx` is borrowed only for the duration of the call.
    ///
    /// Returns the fragment count from the underlying poll.
    pub fn poll<P: FragmentAssemblable, T>(
        &mut self,
        pollable: &P,
        ctx: &mut T,
        func: fn(&mut T, &[u8], &AeronHeader),
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        unsafe {
            self.handler.get_mut().set(ctx, func);
        }
        // clear `ctx` on return and on unwind, so no dangling pointer is left behind
        let handler = &self.handler;
        let _clear = OnDrop(|| unsafe { handler.get_mut().clear() });
        pollable.poll_with_assembler(Some(&self.assembler), fragment_limit)
    }
}

pub struct FnMutControlledMessageHandler {
    func: fn(*mut (), &[u8], &AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ctx: *mut (),
}

impl FnMutControlledMessageHandler {
    pub const fn new() -> Self {
        Self {
            func: Self::noop,
            ctx: std::ptr::null_mut(),
        }
    }

    #[inline]
    /// Point this handler at `ctx` / `func` so the next [`call`](Self::call)
    /// dispatches into them.
    ///
    /// # Safety
    /// `ctx` must stay live, unmoved and exclusively available to this handler
    /// until `clear` or the next `set`. The handler stores its address without a lifetime.
    pub unsafe fn set<T>(
        &mut self,
        ctx: &mut T,
        func: fn(&mut T, &[u8], &AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ) -> &mut Self {
        self.func = Self::wrap::<T>(func);
        self.ctx = ctx as *mut T as *mut ();
        self
    }

    #[inline(always)]
    pub fn call(&mut self, msg: &[u8], header: &AeronHeader) -> aeron_controlled_fragment_handler_action_t {
        (self.func)(self.ctx, msg, header)
    }

    /// Drop the borrowed `ctx` (cf. [`FnMutMessageHandler::clear`]) so a later stray
    /// [`call`](Self::call) returns `CONTINUE` instead of dereferencing a stale pointer.
    #[inline]
    pub const fn clear(&mut self) {
        self.func = Self::noop;
        self.ctx = std::ptr::null_mut();
    }

    #[inline]
    fn wrap<T>(
        f: fn(&mut T, &[u8], &AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ) -> fn(*mut (), &[u8], &AeronHeader) -> aeron_controlled_fragment_handler_action_t {
        unsafe { std::mem::transmute(f) }
    }

    fn noop(_: *mut (), _: &[u8], _: &AeronHeader) -> aeron_controlled_fragment_handler_action_t {
        bindings::aeron_controlled_fragment_handler_action_en::AERON_ACTION_CONTINUE
    }
}

impl AeronControlledFragmentHandlerCallback for FnMutControlledMessageHandler {
    fn handle_aeron_controlled_fragment_handler(
        &mut self,
        buffer: &[u8],
        header: &AeronHeader,
    ) -> aeron_controlled_fragment_handler_action_t {
        self.call(buffer, header)
    }
}

/// A poll target whose raw fragments can be reassembled by [`AeronControlledFragmentAssembler`].
pub trait ControlledFragmentAssemblable {
    fn controlled_poll_with_assembler(
        &self,
        assembler: Option<&Handler<AeronControlledFragmentAssembler>>,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError>;
}

impl ControlledFragmentAssemblable for AeronSubscription {
    #[inline]
    fn controlled_poll_with_assembler(
        &self,
        assembler: Option<&Handler<AeronControlledFragmentAssembler>>,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        self.controlled_poll_with_handler(assembler, fragment_limit)
    }
}

pub struct AeronControlledFragmentClosureAssembler {
    assembler: Handler<AeronControlledFragmentAssembler>,
    handler: Handler<FnMutControlledMessageHandler>,
}

impl AeronControlledFragmentClosureAssembler {
    pub fn new() -> Result<Self, AeronCError> {
        let handler = Handler::new(FnMutControlledMessageHandler::new());
        Ok(Self {
            assembler: Handler::new(AeronControlledFragmentAssembler::new(Some(&handler))?),
            handler,
        })
    }

    /// Controlled poll of `pollable`, dispatching each (possibly reassembled) message to
    /// `func`. `ctx` is borrowed only for the duration of the call.
    pub fn poll<P: ControlledFragmentAssemblable, T>(
        &mut self,
        pollable: &P,
        ctx: &mut T,
        func: fn(&mut T, &[u8], &AeronHeader) -> aeron_controlled_fragment_handler_action_t,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        unsafe {
            self.handler.get_mut().set(ctx, func);
        }
        // clear `ctx` on return and on unwind, so no dangling pointer is left behind
        let handler = &self.handler;
        let _clear = OnDrop(|| unsafe { handler.get_mut().clear() });
        pollable.controlled_poll_with_assembler(Some(&self.assembler), fragment_limit)
    }
}

/// Status transition tracker that emits only when the observed [`AeronStatus`]
/// differs from the previously recorded state.
///
/// Mirrors the reactive side-channel pattern in wingfoil's `AeronStatusStream`,
/// useful for driving application-level state machines (e.g. reconnect logic,
/// UI indicators) that need to react to connection changes without polling.
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
/// this function catches malformed endpoints before they reach the C layer,
/// providing clearer error messages than the generic C parse failures.
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
    if endpoint.is_empty() {
        return Err(AeronCError::from_code(-1));
    }

    // Reject URI separator characters that would break channel string parsing.
    // These are safe in a full URI (e.g. `aeron:udp?endpoint=...`) but not in
    // the endpoint value itself.
    if endpoint.contains('?') || endpoint.contains('=') || endpoint.contains('/') {
        return Err(AeronCError::from_code(-1));
    }

    // IPv6 addresses are bracketed: `[IPv6]:port`. Extract the host part.
    if endpoint.starts_with('[') {
        let end_bracket = endpoint.find(']').ok_or_else(|| AeronCError::from_code(-1))?;
        if end_bracket == 1 {
            // Empty `[]` is invalid.
            return Err(AeronCError::from_code(-1));
        }
        let host_part = &endpoint[1..end_bracket];
        // Find the port separator colon AFTER the closing bracket.
        let after_bracket = &endpoint[end_bracket..];
        let colon_offset = after_bracket.find(':').ok_or_else(|| AeronCError::from_code(-1))?;
        let colon_pos = end_bracket + colon_offset;
        if colon_offset != 1 || colon_pos + 1 >= endpoint.len() {
            // Port must immediately follow `]` and be non-empty.
            return Err(AeronCError::from_code(-1));
        }
        let port_str = &endpoint[colon_pos + 1..];
        validate_ipv6(host_part)?;
        validate_port(port_str)?;
        return Ok(());
    }

    // Unbracketed form: `host:port`. Split on the last `:` (IPv4 has at most
    // 3 colons for dotted decimals; we only want the port separator).
    let colon_pos = endpoint.rfind(':').ok_or_else(|| AeronCError::from_code(-1))?;
    if colon_pos == 0 || colon_pos + 1 >= endpoint.len() {
        // Require non-empty host and port.
        return Err(AeronCError::from_code(-1));
    }
    let host = &endpoint[..colon_pos];
    let port_str = &endpoint[colon_pos + 1..];

    validate_host(host)?;
    validate_port(port_str)?;
    Ok(())
}

/// Validate an IPv6 address (without brackets). Returns `Err` if the string
/// is not a valid IPv6 literal.
fn validate_ipv6(addr: &str) -> Result<(), AeronCError> {
    if addr.is_empty() {
        return Err(AeronCError::from_code(-1));
    }

    // IPv6 allows: hexadecimal digits, `:` (single or `::` for compression).
    // Require valid characters and at least one colon.
    let has_colon = addr.bytes().any(|b| b == b':');
    if !has_colon {
        return Err(AeronCError::from_code(-1));
    }

    for ch in addr.bytes() {
        let is_hex = ch.is_ascii_hexdigit();
        let is_colon = ch == b':';
        if !(is_hex || is_colon) {
            return Err(AeronCError::from_code(-1));
        }
    }

    Ok(())
}

/// Validate an Aeron UDP host identifier (hostname or IPv4). Returns `Err` if
/// the host contains characters outside the safe allowlist, is empty, or looks
/// like an unbracketed IPv6 address.
fn validate_host(host: &str) -> Result<(), AeronCError> {
    if host.is_empty() {
        return Err(AeronCError::from_code(-1));
    }

    // Reject unbracketed IPv6 addresses (they must use `[IPv6]:port` form).
    // IPv6 addresses have multiple consecutive colons or colons in positions
    // that don't match IPv4 dotted decimal.
    let colon_count = host.bytes().filter(|&b| b == b':').count();
    if colon_count > 1 {
        // Multiple colons means this is likely an IPv6 address without brackets.
        return Err(AeronCError::from_code(-1));
    }
    if colon_count == 1 {
        // Single colon: could be IPv4 (ok) or a short IPv6 form like `::1` (reject).
        // If there's a colon adjacent to another colon or at the start, it's IPv6.
        if host.contains("::") || host.starts_with(':') || host.ends_with(':') {
            return Err(AeronCError::from_code(-1));
        }
    }

    // Safe character set: alphanumeric, `-`, `.`, `:` (IPv4 has a single colon
    // between octets; IPv6 brackets are handled by the caller).
    for ch in host.bytes() {
        let is_alnum = ch.is_ascii_alphanumeric();
        let is_safe = matches!(ch, b'-' | b'.' | b':');
        if !(is_alnum || is_safe) {
            return Err(AeronCError::from_code(-1));
        }
    }

    Ok(())
}

/// Validate a TCP/UDP port number. Returns `Err` if the port is not a
/// decimal integer in `0..=65535`.
fn validate_port(port: &str) -> Result<(), AeronCError> {
    if port.is_empty() {
        return Err(AeronCError::from_code(-1));
    }

    // Port must be all digits (no leading `-` or `+`).
    if !port.bytes().all(|b| b.is_ascii_digit()) {
        return Err(AeronCError::from_code(-1));
    }

    // Parse as u16; reject values that overflow 65535.
    let port_num = port.parse::<u32>().map_err(|_| AeronCError::from_code(-1))?;
    if port_num > 65535 {
        return Err(AeronCError::from_code(-1));
    }

    Ok(())
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

/// A fragment assembler that owns its callback.
///
/// Polling and changing callback state require an exclusive borrow. No reference
/// count, mutex or runtime callback borrow check is needed while polling.
/// The callback may borrow local state. This assembler stays on its thread and
/// cannot be cloned; use `Handler` when several owners need the same callback.
///
/// Use [`Self::handler`] and [`Self::handler_mut`] to access callback state
/// between calls to [`Self::poll`].
pub struct FragmentAssembler<T> {
    native: std::ptr::NonNull<aeron_fragment_assembler_t>,
    handler: Box<T>,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl<T: AeronFragmentHandlerCallback> FragmentAssembler<T> {
    pub fn new(handler: T) -> Result<Self, AeronCError> {
        let mut handler = Box::new(handler);
        let mut native = std::ptr::null_mut();
        // SAFETY: the callback stays at a stable address until after native
        // cleanup. The delegate catches panics before returning through C.
        let result = unsafe {
            aeron_fragment_assembler_create(
                &mut native,
                Some(aeron_fragment_handler_t_callback_for_borrowed_handler_catch::<T>),
                (&mut *handler as *mut T).cast(),
            )
        };
        if result < 0 {
            return Err(AeronCError::from_code(result));
        }
        let native = std::ptr::NonNull::new(native).ok_or_else(|| AeronCError::from_code(-1))?;
        Ok(Self {
            native,
            handler,
            _local: std::marker::PhantomData,
        })
    }

    /// Read callback state between polls.
    pub fn handler(&self) -> &T {
        &self.handler
    }

    /// Change callback state between polls.
    pub fn handler_mut(&mut self) -> &mut T {
        &mut self.handler
    }

    /// Release the partial message buffer for a session, if one exists.
    pub fn delete_session_buffer(&mut self, session_id: i32) -> bool {
        // SAFETY: the native assembler is live and exclusively borrowed.
        unsafe { aeron_fragment_assembler_delete_session_buffer(self.native.as_ptr(), session_id) }
    }

    #[inline]
    pub fn poll(&mut self, subscription: &AeronSubscription, fragment_limit: usize) -> Result<i32, AeronCError> {
        subscription.poll(self, fragment_limit)
    }
}

impl<T> Drop for FragmentAssembler<T> {
    fn drop(&mut self) {
        // SAFETY: this is the only owner. The callback is still alive here.
        unsafe {
            aeron_fragment_assembler_delete(self.native.as_ptr());
        }
    }
}

impl<T: AeronFragmentHandlerCallback> AeronFragmentHandlerCallback for FragmentAssembler<T> {
    const RESUMES_CAUGHT_PANIC: bool = true;

    #[inline]
    fn native_poll_callback(&mut self) -> Option<NativePollCallback<'_, aeron_fragment_handler_t>> {
        // SAFETY: C and C-unwind have the same calling convention. The delegate
        // catches panics before returning through the native assembler.
        let callback = unsafe {
            std::mem::transmute::<_, aeron_fragment_handler_t>(Some(
                aeron_fragment_assembler_handler as unsafe extern "C" fn(_, _, _, _),
            ))
        };
        Some(NativePollCallback {
            callback,
            clientd: self.native.as_ptr().cast(),
            borrow: std::marker::PhantomData,
        })
    }

    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: &AeronHeader) {
        // SAFETY: exclusive access keeps the native assembler and its delegate
        // valid for this call. The buffer and header are borrowed until it returns.
        unsafe {
            aeron_fragment_assembler_handler(
                self.native.as_ptr().cast(), buffer.as_ptr(), buffer.len(), header.get_inner(),
            );
        }
    }
}

/// A controlled fragment assembler that owns its callback.
///
/// Use this when the callback needs to return `CONTINUE`, `ABORT`, `BREAK`, or
/// `COMMIT`. Like [`FragmentAssembler`], it can borrow local state, requires an
/// exclusive borrow while polling, and has no lock or reference count on the
/// polling path. Use [`Self::handler_mut`] to change callback state between polls.
pub struct ControlledFragmentAssembler<T> {
    native: std::ptr::NonNull<aeron_controlled_fragment_assembler_t>,
    handler: Box<T>,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl<T: AeronControlledFragmentHandlerCallback> ControlledFragmentAssembler<T> {
    pub fn new(handler: T) -> Result<Self, AeronCError> {
        let mut handler = Box::new(handler);
        let mut native = std::ptr::null_mut();
        // SAFETY: the callback has a stable address until after native cleanup.
        // Panics are caught before the delegate returns to C.
        let result = unsafe {
            aeron_controlled_fragment_assembler_create(
                &mut native,
                Some(aeron_controlled_fragment_handler_t_callback_for_borrowed_handler_catch::<T>),
                (&mut *handler as *mut T).cast(),
            )
        };
        if result < 0 {
            return Err(AeronCError::from_code(result));
        }
        let native = std::ptr::NonNull::new(native).ok_or_else(|| AeronCError::from_code(-1))?;
        Ok(Self {
            native,
            handler,
            _local: std::marker::PhantomData,
        })
    }

    /// Read callback state between polls.
    pub fn handler(&self) -> &T {
        &self.handler
    }

    /// Change callback state between polls.
    pub fn handler_mut(&mut self) -> &mut T {
        &mut self.handler
    }

    /// Release the partial message buffer for a session, if one exists.
    pub fn delete_session_buffer(&mut self, session_id: i32) -> bool {
        // SAFETY: the native assembler is live and exclusively borrowed.
        unsafe { aeron_controlled_fragment_assembler_delete_session_buffer(self.native.as_ptr(), session_id) }
    }

    /// Poll a subscription and call the handler with complete messages.
    #[inline]
    pub fn poll(&mut self, subscription: &AeronSubscription, fragment_limit: usize) -> Result<i32, AeronCError> {
        subscription.controlled_poll(self, fragment_limit)
    }
}

impl<T> Drop for ControlledFragmentAssembler<T> {
    fn drop(&mut self) {
        // SAFETY: this is the only owner. The callback is still alive here.
        unsafe {
            aeron_controlled_fragment_assembler_delete(self.native.as_ptr());
        }
    }
}

impl<T: AeronControlledFragmentHandlerCallback> AeronControlledFragmentHandlerCallback for ControlledFragmentAssembler<T> {
    const RESUMES_CAUGHT_PANIC: bool = true;

    #[inline]
    fn native_poll_callback(&mut self) -> Option<NativePollCallback<'_, aeron_controlled_fragment_handler_t>> {
        // SAFETY: both ABIs have the same calling convention. The delegate
        // catches panics before returning through the native assembler.
        let callback = unsafe {
            std::mem::transmute::<_, aeron_controlled_fragment_handler_t>(Some(
                aeron_controlled_fragment_assembler_handler as unsafe extern "C" fn(_, _, _, _) -> _,
            ))
        };
        Some(NativePollCallback {
            callback,
            clientd: self.native.as_ptr().cast(),
            borrow: std::marker::PhantomData,
        })
    }

    fn handle_aeron_controlled_fragment_handler(
        &mut self, buffer: &[u8], header: &AeronHeader,
    ) -> aeron_controlled_fragment_handler_action_t {
        // SAFETY: the assembler is exclusively borrowed and owns its delegate.
        unsafe {
            aeron_controlled_fragment_assembler_handler(
                self.native.as_ptr().cast(), buffer.as_ptr(), buffer.len(), header.get_inner(),
            )
        }
    }
}
