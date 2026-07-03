// code here is included in all modules and extends generated classes
pub static AERON_IPC_STREAM: &std::ffi::CStr =
    unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(b"aeron:ipc\0") };

// SAFETY (accepted unsoundness — latency trade-off):
// These handle types wrap `Rc` (via `CResource::OwnedOnHeap`), so in principle
// they are `!Send + !Sync`. We deliberately keep `Rc` (non-atomic refcount) for
// latency — `Arc` is banned in this library. The idiomatic Aeron usage is to
// MOVE a handle to a single owning thread and use it exclusively there, never
// sharing `&Handle` across threads, so we retain `Send` to support that pattern.
//
// `Sync` is intentionally NOT implemented: sharing `&Handle` across threads
// would let two threads `Rc::clone` the inner `Rc` concurrently → refcount data
// race → UB. Nothing in this codebase shares `&Handle` across threads.
//
// Retaining `Send` over `Rc` is technically unsound (a non-atomic refcount
// touched from >1 thread races), but per project policy "rather be unsound than
// slow" this is documented and accepted. Do NOT clone these handles across
// threads; do NOT use one handle from multiple threads concurrently.
unsafe impl Send for AeronCountersReader {}
unsafe impl Send for AeronSubscription {}
unsafe impl Send for AeronPublication {}
unsafe impl Send for AeronCounter {}

/// High-level connection state of a publication or subscription.
///
/// [`AeronPublication::status`] / [`AeronSubscription::status`] derive
/// `Disconnected` / `Connected` / `Closed` from the handle's `is_closed` /
/// `is_connected` flags. `BackPressured` is only observable at `offer` /
/// `try_claim` time (via the returned error) and is surfaced by
/// [`AeronStatus::from_error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub fn from_error(error: &AeronOfferError) -> Option<Self> {
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
/// For [`AeronIdleStrategyKind::Sleeping`], the sleep period is configured separately via
/// `set_idle_strategy_init_args` (nanoseconds as a string).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AeronIdleStrategyKind {
    /// Sleep when idle (`"sleeping"`); period from the init args.
    Sleeping,
    /// Yield the OS thread when idle (`"yield"`).
    Yielding,
    /// Busy-spin (`"spin"`) — lowest latency, pins a core.
    BusySpin,
    /// Never back off (`"noop"`).
    NoOp,
    /// Spin, then yield, then park with exponential backoff (`"backoff"`) — the default.
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
}

impl AeronIdleStrategyKind {
    /// Default init args accepted by this strategy's loader (the C client validates the
    /// currently-set init args when the strategy is set, so they must be coherent):
    /// sleeping takes a period in ns; backoff takes `maxSpins-maxYields-minParkNs-maxParkNs`
    /// (hyphen-separated); the rest ignore init args.
    pub const fn default_init_args(&self) -> &'static str {
        match self {
            AeronIdleStrategyKind::Sleeping => "1000000", // 1ms
            AeronIdleStrategyKind::Backoff => "10-20-1000-1000000", // aeron defaults
            _ => "",
        }
    }
}

impl Aeron {
    /// Connect to a media driver in one call: context, client, and conductor start.
    ///
    /// `dir` is the media driver directory (`None` uses the aeron default). For tuned
    /// setups — error handlers, driver timeout, idle strategy — build the
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

impl AeronContext {
    /// Typed variant of [`Self::set_idle_strategy`]: configures the conductor's idle
    /// strategy without stringly-typed names, setting coherent default init args
    /// (override afterwards with [`Self::set_idle_strategy_init_args`] if needed).
    pub fn set_idle_strategy_kind(&self, kind: AeronIdleStrategyKind) -> Result<i32, AeronCError> {
        self.set_idle_strategy_init_args(&kind.default_init_args().into_c_string())?;
        self.set_idle_strategy(&kind.name().into_c_string())
    }
}

impl AeronCnc {
    #[inline]
    pub fn read_on_partial_stack(
        aeron_dir: &std::ffi::CString,
        mut handler: impl FnMut(&mut AeronCnc),
    ) -> Result<(), AeronCError> {
        let cnc = ManagedCResource::initialise(move |cnc| unsafe {
            aeron_cnc_init(cnc, aeron_dir.as_ptr(), 0)
        })?;
        let mut cnc = Self {
            inner: CResource::Borrowed(cnc),
        };
        handler(&mut cnc);
        unsafe { aeron_cnc_close(cnc.get_inner()) };
        Ok(())
    }

    /// **Deprecated**: allocate on the heap. Use `new_on_heap` instead.
    #[deprecated(since = "0.1.122", note = "Use `new_on_heap` instead")]
    #[inline]
    pub fn new(aeron_dir: &str) -> Result<AeronCnc, AeronCError> {
        Self::new_on_heap(aeron_dir)
    }

    /// Note this allocates on the heap, cannot be stored this on stack. As Aeron will do the allocation.
    /// Try to use `read_on_partial_stack` which performs less allocations
    #[inline]
    pub fn new_on_heap(aeron_dir: &str) -> Result<AeronCnc, AeronCError> {
        let c_string = std::ffi::CString::new(aeron_dir).map_err(|_| AeronCError::from_code(-1))?;
        let resource = ManagedCResource::new(
            move |cnc| unsafe { aeron_cnc_init(cnc, c_string.as_ptr(), 0) },
            Some(Box::new(move |cnc| unsafe {
                aeron_cnc_close(*cnc);
                0
            })),
            false,
        )?;

        let result = Self {
            inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource)),
        };
        Ok(result)
    }

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

impl AeronHeader {
    /// returns AeronImage, **must** be called in poll method
    pub fn image(&self) -> Option<AeronImage> {
        let ptr = self.context();
        if ptr.is_null() {
            None
        } else {
            Some(AeronImage::from(ptr as *mut aeron_image_t))
        }
    }

    /// Session id of this fragment, or `None` if the underlying values lookup
    /// failed. Collapses the `get_values().frame().session_id()` hop and never
    /// panics on the fast path.
    #[inline]
    pub fn session_id(&self) -> Option<i32> {
        self.get_values().ok().map(|v| v.frame().session_id())
    }

    /// Stream id of this fragment, or `None` if the underlying values lookup
    /// failed. Collapses the `get_values().frame().stream_id()` hop and never
    /// panics on the fast path.
    #[inline]
    pub fn stream_id(&self) -> Option<i32> {
        self.get_values().ok().map(|v| v.frame().stream_id())
    }

    /// Reserved value of this fragment, or `None` if the underlying values
    /// lookup failed. A sender can stamp a timestamp here (see
    /// [`AeronPublication::offer_timestamped`]) so the receiver can measure
    /// end-to-end latency.
    #[inline]
    pub fn reserved_value(&self) -> Option<i64> {
        self.get_values().ok().map(|v| v.frame().reserved_value())
    }

    /// Term id of this fragment, or `None` if the underlying values lookup
    /// failed.
    #[inline]
    pub fn term_id(&self) -> Option<i32> {
        self.get_values().ok().map(|v| v.frame().term_id())
    }

    /// Term offset of this fragment, or `None` if the underlying values lookup
    /// failed.
    #[inline]
    pub fn term_offset(&self) -> Option<i32> {
        self.get_values().ok().map(|v| v.frame().term_offset())
    }
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum AeronSystemCounterType {
    /// Running total of bytes sent for data over UDP, excluding IP headers.
    BytesSent = 0,
    /// Running total of bytes received for data over UDP, excluding IP headers.
    BytesReceived = 1,
    /// Failed offers to the receiver proxy suggesting back-pressure.
    ReceiverProxyFails = 2,
    /// Failed offers to the sender proxy suggesting back-pressure.
    SenderProxyFails = 3,
    /// Failed offers to the driver conductor proxy suggesting back-pressure.
    ConductorProxyFails = 4,
    /// Count of NAKs sent back to senders requesting re-transmits.
    NakMessagesSent = 5,
    /// Count of NAKs received from receivers requesting re-transmits.
    NakMessagesReceived = 6,
    /// Count of status messages sent back to senders for flow control.
    StatusMessagesSent = 7,
    /// Count of status messages received from receivers for flow control.
    StatusMessagesReceived = 8,
    /// Count of heartbeat data frames sent to indicate liveness in the absence of data to send.
    HeartbeatsSent = 9,
    /// Count of heartbeat data frames received to indicate liveness in the absence of data to send.
    HeartbeatsReceived = 10,
    /// Count of data packets re-transmitted as a result of NAKs.
    RetransmitsSent = 11,
    /// Count of packets received which under-run the current flow control window for images.
    FlowControlUnderRuns = 12,
    /// Count of packets received which over-run the current flow control window for images.
    FlowControlOverRuns = 13,
    /// Count of invalid packets received.
    InvalidPackets = 14,
    /// Count of errors observed by the driver and an indication to read the distinct error log.
    Errors = 15,
    /// Count of socket send operations which resulted in less than the packet length being sent.
    ShortSends = 16,
    /// Count of attempts to free log buffers no longer required by the driver that are still held by clients.
    FreeFails = 17,
    /// Count of the times a sender has entered the state of being back-pressured when it could have sent faster.
    SenderFlowControlLimits = 18,
    /// Count of the times a publication has been unblocked after a client failed to complete an offer within a timeout.
    UnblockedPublications = 19,
    /// Count of the times a command has been unblocked after a client failed to complete an offer within a timeout.
    UnblockedCommands = 20,
    /// Count of the times the channel endpoint detected a possible TTL asymmetry between its config and a new connection.
    PossibleTtlAsymmetry = 21,
    /// Current status of the ControllableIdleStrategy if configured.
    ControllableIdleStrategy = 22,
    /// Count of the times a loss gap has been filled when NAKs have been disabled.
    LossGapFills = 23,
    /// Count of the Aeron clients that have timed out without a graceful close.
    ClientTimeouts = 24,
    /// Count of the times a connection endpoint has been re-resolved resulting in a change.
    ResolutionChanges = 25,
    /// The maximum time spent by the conductor between work cycles.
    ConductorMaxCycleTime = 26,
    /// Count of the number of times the cycle time threshold has been exceeded by the conductor in its work cycle.
    ConductorCycleTimeThresholdExceeded = 27,
    /// The maximum time spent by the sender between work cycles.
    SenderMaxCycleTime = 28,
    /// Count of the number of times the cycle time threshold has been exceeded by the sender in its work cycle.
    SenderCycleTimeThresholdExceeded = 29,
    /// The maximum time spent by the receiver between work cycles.
    ReceiverMaxCycleTime = 30,
    /// Count of the number of times the cycle time threshold has been exceeded by the receiver in its work cycle.
    ReceiverCycleTimeThresholdExceeded = 31,
    /// The maximum time spent by the NameResolver in one of its operations.
    NameResolverMaxTime = 32,
    /// Count of the number of times the time threshold has been exceeded by the NameResolver.
    NameResolverTimeThresholdExceeded = 33,
    /// The version of the media driver.
    AeronVersion = 34,
    /// The total number of bytes currently mapped in log buffers, the CnC file, and the loss report.
    BytesCurrentlyMapped = 35,
    /// A minimum bound on the number of bytes re-transmitted as a result of NAKs.\n///\n/// MDC retransmits are only counted once; therefore, this is a minimum bound rather than the actual number\n/// of retransmitted bytes. Note that retransmitted bytes are not included in the `BytesSent` counter value.
    RetransmittedBytes = 36,
    /// A count of the number of times that the retransmit pool has been overflowed.
    RetransmitOverflow = 37,
    /// A count of the number of error frames received by this driver.
    ErrorFramesReceived = 38,
    /// A count of the number of error frames sent by this driver.
    ErrorFramesSent = 39,
    DummyLast = 40,
}

impl std::convert::TryFrom<i32> for AeronSystemCounterType {
    type Error = AeronCError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < 0 {
            return Err(AeronCError::from_code(value));
        }
        match value as u32 {
            0 => Ok(AeronSystemCounterType::BytesSent),
            1 => Ok(AeronSystemCounterType::BytesReceived),
            2 => Ok(AeronSystemCounterType::ReceiverProxyFails),
            3 => Ok(AeronSystemCounterType::SenderProxyFails),
            4 => Ok(AeronSystemCounterType::ConductorProxyFails),
            5 => Ok(AeronSystemCounterType::NakMessagesSent),
            6 => Ok(AeronSystemCounterType::NakMessagesReceived),
            7 => Ok(AeronSystemCounterType::StatusMessagesSent),
            8 => Ok(AeronSystemCounterType::StatusMessagesReceived),
            9 => Ok(AeronSystemCounterType::HeartbeatsSent),
            10 => Ok(AeronSystemCounterType::HeartbeatsReceived),
            11 => Ok(AeronSystemCounterType::RetransmitsSent),
            12 => Ok(AeronSystemCounterType::FlowControlUnderRuns),
            13 => Ok(AeronSystemCounterType::FlowControlOverRuns),
            14 => Ok(AeronSystemCounterType::InvalidPackets),
            15 => Ok(AeronSystemCounterType::Errors),
            16 => Ok(AeronSystemCounterType::ShortSends),
            17 => Ok(AeronSystemCounterType::FreeFails),
            18 => Ok(AeronSystemCounterType::SenderFlowControlLimits),
            19 => Ok(AeronSystemCounterType::UnblockedPublications),
            20 => Ok(AeronSystemCounterType::UnblockedCommands),
            21 => Ok(AeronSystemCounterType::PossibleTtlAsymmetry),
            22 => Ok(AeronSystemCounterType::ControllableIdleStrategy),
            23 => Ok(AeronSystemCounterType::LossGapFills),
            24 => Ok(AeronSystemCounterType::ClientTimeouts),
            25 => Ok(AeronSystemCounterType::ResolutionChanges),
            26 => Ok(AeronSystemCounterType::ConductorMaxCycleTime),
            27 => Ok(AeronSystemCounterType::ConductorCycleTimeThresholdExceeded),
            28 => Ok(AeronSystemCounterType::SenderMaxCycleTime),
            29 => Ok(AeronSystemCounterType::SenderCycleTimeThresholdExceeded),
            30 => Ok(AeronSystemCounterType::ReceiverMaxCycleTime),
            31 => Ok(AeronSystemCounterType::ReceiverCycleTimeThresholdExceeded),
            32 => Ok(AeronSystemCounterType::NameResolverMaxTime),
            33 => Ok(AeronSystemCounterType::NameResolverTimeThresholdExceeded),
            34 => Ok(AeronSystemCounterType::AeronVersion),
            35 => Ok(AeronSystemCounterType::BytesCurrentlyMapped),
            36 => Ok(AeronSystemCounterType::RetransmittedBytes),
            37 => Ok(AeronSystemCounterType::RetransmitOverflow),
            38 => Ok(AeronSystemCounterType::ErrorFramesReceived),
            39 => Ok(AeronSystemCounterType::ErrorFramesSent),
            40 => Ok(AeronSystemCounterType::DummyLast),
            _ => Err(AeronCError::from_code(-1)),
        }
    }
}

impl AeronCncMetadata {
    #[inline]
    /// allocates on heap
    pub fn load_from_file(aeron_dir: &str) -> Result<Self, AeronCError> {
        let aeron_dir =
            std::ffi::CString::new(aeron_dir).map_err(|_| AeronCError::from_code(-1))?;
        let mapped_file = std::rc::Rc::new(std::cell::RefCell::new(aeron_mapped_file_t {
            addr: std::ptr::null_mut(),
            length: 0,
        }));
        let mapped_file2 = std::rc::Rc::clone(&mapped_file);
        let resource = ManagedCResource::new(
            move |ctx| {
                let result = unsafe {
                    aeron_cnc_map_file_and_load_metadata(
                        aeron_dir.as_ptr(),
                        mapped_file.borrow_mut().deref_mut() as *mut aeron_mapped_file_t,
                        ctx,
                    )
                };
                if result == aeron_cnc_load_result_t::AERON_CNC_LOAD_SUCCESS {
                    1
                } else {
                    -1
                }
            },
            Some(Box::new(move |ctx| unsafe {
                aeron_unmap(mapped_file2.borrow_mut().deref_mut() as *mut aeron_mapped_file_t)
            })),
            false,
        )?;

        let result = Self {
            inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource)),
        };
        Ok(result)
    }

    #[inline]
    /// allocates on stack
    pub fn read_from_file(
        aeron_dir: &std::ffi::CString,
        mut handler: impl FnMut(Self),
    ) -> Result<(), AeronCError> {
        let mut mapped_file = aeron_mapped_file_t {
            addr: std::ptr::null_mut(),
            length: 0,
        };
        let ctx = ManagedCResource::initialise(move |ctx| {
            let result = unsafe {
                aeron_cnc_map_file_and_load_metadata(
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

        let result = Self {
            inner: CResource::Borrowed(ctx),
        };

        handler(result);
        unsafe { aeron_unmap(&mut mapped_file as *mut aeron_mapped_file_t) };
        Ok(())
    }
}

unsafe extern "C" fn rusteron_image_visitor<F: FnMut(&AeronImage)>(
    image: *mut aeron_image_t,
    clientd: *mut ::std::os::raw::c_void,
) {
    let f = &mut *(clientd as *mut F);
    let image = AeronImage {
        inner: CResource::Borrowed(image),
    };
    f(&image);
}

impl AeronSubscription {
    /// A retained image handle for `index`, or `None` when no such image exists.
    ///
    /// The C client retains the image on this call; the returned [`AeronImage`] releases it
    /// automatically when the last clone drops (no manual `aeron_image_release`). If the
    /// subscription is closed first, the release is skipped — the C client has already
    /// reclaimed the image.
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
        let resource = ManagedCResource::new(
            move |ctx| {
                unsafe { *ctx = image };
                0
            },
            Some(Box::new(move |ctx| unsafe {
                // skip the release if the subscription was closed first — the C client
                // reclaimed the image during the subscription close
                if !subscription.get_inner().is_null() {
                    aeron_subscription_image_release(subscription.get_inner(), *ctx)
                } else {
                    0
                }
            })),
            false,
        )
        .ok()?;
        Some(AeronImage {
            inner: CResource::OwnedOnHeap(std::rc::Rc::new(resource)),
        })
    }

    /// Borrow-scoped iteration over the current images — zero retain/release bookkeeping.
    /// The borrowed [`AeronImage`] is only valid inside the closure; call
    /// [`Self::image_at_index`] / [`Self::image_by_session_id`] for a handle that outlives it.
    pub fn for_each_image<F: FnMut(&AeronImage)>(&self, mut f: F) {
        unsafe {
            aeron_subscription_for_each_image(
                self.get_inner(),
                Some(rusteron_image_visitor::<F>),
                &mut f as *mut _ as *mut ::std::os::raw::c_void,
            )
        }
    }

    pub fn async_add_destination(
        &self,
        client: &Aeron,
        destination: &std::ffi::CStr,
    ) -> Result<AeronAsyncDestination, AeronCError> {
        AeronAsyncDestination::aeron_subscription_async_add_destination(client, self, destination)
    }

    /// Add `destination`, polling until the driver acknowledges or `timeout` elapses.
    /// The owning [`Aeron`] client is retrieved automatically from the subscription's
    /// dependency graph — pass it explicitly via [`Self::async_add_destination`] only
    /// when you hold it already.
    pub fn add_destination(
        &self,
        destination: &std::ffi::CStr,
        timeout: std::time::Duration,
    ) -> Result<(), AeronCError> {
        let client = self
            .inner
            .get_dependency::<Aeron>()
            .ok_or_else(|| AeronCError::with_message(-1, "subscription has no owning Aeron client"))?;
        let result = self.async_add_destination(&client, destination)?;
        if result
            .aeron_subscription_async_destination_poll()
            .unwrap_or_default()
            > 0
        {
            return Ok(());
        }
        let time = std::time::Instant::now();
        while time.elapsed() < timeout {
            if result
                .aeron_subscription_async_destination_poll()
                .unwrap_or_default()
                > 0
            {
                return Ok(());
            }
            #[cfg(debug_assertions)]
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        log::error!("failed async poll for {:?} {:?}", destination, self);
        Err(AeronErrorType::TimedOut.into())
    }

    // ===== Spin-poll ergonomics =====

    /// Poll delivering each fragment to `handler`, borrowing the closure only for
    /// this call (zero allocation on the hot path).
    ///
    /// This is the recommended poll for non-fragmented messages. For messages that
    /// may exceed the MTU, use [`AeronFragmentClosureAssembler::poll`] (or an
    /// [`AeronFragmentAssembler`]) to reassemble fragments before delivery.
    ///
    /// Returns the number of fragments delivered (0 if nothing was available).
    #[inline]
    pub fn poll_fn<H: FnMut(&[u8], AeronHeader)>(
        &self,
        mut handler: H,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        let result = unsafe {
            aeron_subscription_poll(
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

    /// Deprecated alias for [`Self::poll_fn`]. The `_once` suffix was widely read
    /// as "poll one fragment"; [`Self::poll_fn`] makes the closure semantics
    /// explicit and aligns with the retained-handler [`Self::poll`].
    #[deprecated(since = "0.1.169", note = "use `poll_fn` instead")]
    #[inline]
    pub fn poll_once<H: FnMut(&[u8], AeronHeader)>(
        &self,
        handler: H,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        self.poll_fn(handler, fragment_limit)
    }
}

impl AeronExclusivePublication {
    pub fn async_add_destination(
        &self,
        client: &Aeron,
        destination: &std::ffi::CStr,
    ) -> Result<AeronAsyncDestination, AeronCError> {
        AeronAsyncDestination::aeron_exclusive_publication_async_add_destination(
            client,
            self,
            destination,
        )
    }

    /// Add `destination` (see [`AeronSubscription::add_destination`]); the owning
    /// [`Aeron`] client is retrieved from the publication's dependency graph.
    pub fn add_destination(
        &self,
        destination: &std::ffi::CStr,
        timeout: std::time::Duration,
    ) -> Result<(), AeronCError> {
        let client = self
            .inner
            .get_dependency::<Aeron>()
            .ok_or_else(|| AeronCError::with_message(-1, "publication has no owning Aeron client"))?;
        let result = self.async_add_destination(&client, destination)?;
        if result
            .aeron_subscription_async_destination_poll()
            .unwrap_or_default()
            > 0
        {
            return Ok(());
        }
        let time = std::time::Instant::now();
        while time.elapsed() < timeout {
            if result
                .aeron_subscription_async_destination_poll()
                .unwrap_or_default()
                > 0
            {
                return Ok(());
            }
            #[cfg(debug_assertions)]
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        log::error!("failed async poll for {:?} {:?}", destination, self);
        Err(AeronErrorType::TimedOut.into())
    }
}

impl AeronPublication {
    pub fn async_add_destination(
        &self,
        client: &Aeron,
        destination: &std::ffi::CStr,
    ) -> Result<AeronAsyncDestination, AeronCError> {
        AeronAsyncDestination::aeron_publication_async_add_destination(client, self, destination)
    }

    /// Add `destination` (see [`AeronSubscription::add_destination`]); the owning
    /// [`Aeron`] client is retrieved from the publication's dependency graph.
    pub fn add_destination(
        &self,
        destination: &std::ffi::CStr,
        timeout: std::time::Duration,
    ) -> Result<(), AeronCError> {
        let client = self
            .inner
            .get_dependency::<Aeron>()
            .ok_or_else(|| AeronCError::with_message(-1, "publication has no owning Aeron client"))?;
        let result = self.async_add_destination(&client, destination)?;
        if result
            .aeron_subscription_async_destination_poll()
            .unwrap_or_default()
            > 0
        {
            return Ok(());
        }
        let time = std::time::Instant::now();
        while time.elapsed() < timeout {
            if result
                .aeron_subscription_async_destination_poll()
                .unwrap_or_default()
                > 0
            {
                return Ok(());
            }
            #[cfg(debug_assertions)]
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        log::error!("failed async poll for {:?} {:?}", destination, self);
        Err(AeronErrorType::TimedOut.into())
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
        let r_constructor = ManagedCResource::new(
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
            inner: CResource::OwnedOnHeap(std::rc::Rc::new(r_constructor)),
        }
    }
}

impl AeronCError {
    pub fn get_last_err_message(&self) -> &str {
        self.message().unwrap_or_else(|| Aeron::errmsg())
    }

    /// Construct from a live C error code returned by an Aeron FFI call, eagerly
    /// snapshotting the current `aeron_errmsg()` text for non-retryable codes.
    ///
    /// Unlike reading `aeron_errmsg()` (via [`Self::get_last_err_message`]) at
    /// `Display` time — by which point another error on the same thread may have
    /// overwritten the buffer — the snapshot is taken at the error site and is
    /// stable for the lifetime of this `AeronCError`.
    ///
    /// Retryable codes (`AeronErrorType::is_retryable`: back-pressure, admin
    /// action, buffer-full, timeout) skip the snapshot so retry loops stay
    /// allocation-free. They can reconstruct the message later via
    /// [`Self::get_last_err_message`] if needed.
    ///
    /// This is what generated wrappers call; user code constructing an error from
    /// a known code should use [`Self::from_code`]
    /// (no message capture) or [`Self::with_message`].
    pub fn from_c_code(code: i32) -> Self {
        let mut err = Self::from_code(code);
        if code < 0 && !err.kind().is_retryable() {
            err.msg = Some(Aeron::errmsg().into());
        }
        err
    }
}

impl std::fmt::Debug for AeronCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AeronCError")
            .field("code", &self.code)
            .field("kind", &self.kind())
            .field("lastError", &self.get_last_err_message())
            .finish()
    }
}

impl std::fmt::Display for AeronCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Aeron error {}: {:?} [lastError={}]",
            self.code,
            self.kind(),
            self.get_last_err_message()
        )
    }
}

impl std::error::Error for AeronCError {}

const PARSE_CSTR_ERROR_CODE: i32 = -132131;

impl AeronUriStringBuilder {
    /// Fresh builder for an `aeron:ipc` channel.
    pub fn ipc() -> Result<Self, AeronCError> {
        let builder = AeronUriStringBuilder::new_zeroed_on_heap();
        builder.init_new()?;
        builder.media(Media::Ipc)?;
        Ok(builder)
    }

    /// Fresh builder for an `aeron:udp` channel with the given `endpoint` (`host:port`).
    pub fn udp(endpoint: &str) -> Result<Self, AeronCError> {
        let builder = AeronUriStringBuilder::new_zeroed_on_heap();
        builder.init_new()?;
        builder.media(Media::Udp)?.endpoint(endpoint)?;
        Ok(builder)
    }

    /// Fresh builder for an `aeron:udp` multi-destination channel with the given
    /// `control` endpoint and [`ControlMode`] (`Dynamic` for MDC publications,
    /// `Manual` for MDS subscriptions/publications, `Response` for response channels).
    pub fn udp_control(control: &str, mode: ControlMode) -> Result<Self, AeronCError> {
        let builder = AeronUriStringBuilder::new_zeroed_on_heap();
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
            if !inner.close_already_called.get() {
                unsafe { aeron_uri_string_builder_close(inner.get()); }
                inner.close_already_called.set(true);
            }
        }
        #[cfg(feature = "log-c-bindings")]
        log::info!("{}", log_msg);
        let result = unsafe { f(self.get_inner()) };
        if result < 0 {
            Err(AeronCError::from_code(result))
        } else {
            if let Some(inner) = self.inner.as_owned() {
                inner.close_already_called.set(false);
            }
            Ok(result)
        }
    }

    #[inline]
    #[doc = "Initialize a new AeronUriStringBuilder. If already initialized, it will close the previous builder to prevent memory leaks."]
    pub fn init_new(&self) -> Result<i32, AeronCError> {
        self.reinit_run(
            "aeron_uri_string_builder_init_new(self.get_inner())",
            |ptr| unsafe { aeron_uri_string_builder_init_new(ptr) },
        )
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

    pub fn put_string(&self, key: &std::ffi::CStr, value: &str) -> Result<&Self, AeronCError> {
        let value = std::ffi::CString::new(value)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put(&key, &value)?;
        Ok(self)
    }

    pub fn put_strings(&self, key: &str, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CString::new(key)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        let value = std::ffi::CString::new(value)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put(&key, &value)?;
        Ok(self)
    }

    pub fn media(&self, value: Media) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_STRING_BUILDER_MEDIA_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value.as_str())?;
        Ok(self)
    }

    pub fn control_mode(&self, value: ControlMode) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_CONTROL_MODE_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value.as_str())?;
        Ok(self)
    }

    pub fn prefix(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_STRING_BUILDER_PREFIX_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }

    pub fn initial_term_id(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_INITIAL_TERM_ID_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn term_id(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TERM_ID_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn term_offset(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TERM_OFFSET_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn alias(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_ALIAS_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn term_length(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TERM_LENGTH_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn linger_timeout(&self, value: i64) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_LINGER_TIMEOUT_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int64(key, value)?;
        Ok(self)
    }
    pub fn mtu_length(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_MTU_LENGTH_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn ttl(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_TTL_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn sparse_term(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_SPARSE_TERM_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn reliable(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_RELIABLE_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn eos(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_EOS_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn tether(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TETHER_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn tags(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TAGS_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn endpoint(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_ENDPOINT_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn interface(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_INTERFACE_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn control(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_UDP_CHANNEL_CONTROL_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn session_id(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_SESSION_ID_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn group(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_GROUP_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn rejoin(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_REJOIN_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn fc(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_FC_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn gtag(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_GTAG_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn cc(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_CC_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn spies_simulate_connection(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_SPIES_SIMULATE_CONNECTION_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn ats(&self, value: bool) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_ATS_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, if value { "true" } else { "false" })?;
        Ok(self)
    }
    pub fn socket_sndbuf(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_SOCKET_SNDBUF_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn socket_rcvbuf(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_SOCKET_RCVBUF_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn receiver_window(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_RECEIVER_WINDOW_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn media_rcv_timestamp_offset(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_MEDIA_RCV_TIMESTAMP_OFFSET_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn channel_rcv_timestamp_offset(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_CHANNEL_RCV_TIMESTAMP_OFFSET_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn channel_snd_timestamp_offset(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_CHANNEL_SND_TIMESTAMP_OFFSET_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn timestamp_offset_reserved(&self, value: &str) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_TIMESTAMP_OFFSET_RESERVED)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_string(key, value)?;
        Ok(self)
    }
    pub fn response_correlation_id(&self, value: i64) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_RESPONSE_CORRELATION_ID_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int64(key, value)?;
        Ok(self)
    }
    pub fn nak_delay(&self, value: i64) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_NAK_DELAY_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int64(key, value)?;
        Ok(self)
    }
    pub fn untethered_window_limit_timeout(&self, value: i64) -> Result<&Self, AeronCError> {
        let key =
            std::ffi::CStr::from_bytes_until_nul(AERON_URI_UNTETHERED_WINDOW_LIMIT_TIMEOUT_KEY)
                .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int64(key, value)?;
        Ok(self)
    }
    pub fn untethered_resting_timeout(&self, value: i64) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_UNTETHERED_RESTING_TIMEOUT_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int64(key, value)?;
        Ok(self)
    }
    pub fn max_resend(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_MAX_RESEND_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn stream_id(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_STREAM_ID_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
        Ok(self)
    }
    pub fn publication_window(&self, value: i32) -> Result<&Self, AeronCError> {
        let key = std::ffi::CStr::from_bytes_until_nul(AERON_URI_PUBLICATION_WINDOW_KEY)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.put_int32(key, value)?;
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
    /// let builder = AeronUriStringBuilder::default();
    /// builder.remove(std::ffi::CStr::from_bytes_until_nul(b"tags\0").unwrap())?;
    /// ```
    #[inline]
    pub fn remove(&self, key: &std::ffi::CStr) -> Result<i32, AeronCError> {
        unsafe {
            let result =
                aeron_uri_string_builder_put(self.get_inner(), key.as_ptr(), std::ptr::null());
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
    /// let builder = AeronUriStringBuilder::default();
    /// builder.remove_str("tags")?;
    /// ```
    #[inline]
    pub fn remove_str(&self, key: &str) -> Result<i32, AeronCError> {
        let key = std::ffi::CString::new(key)
            .map_err(|_| AeronCError::from_code(PARSE_CSTR_ERROR_CODE))?;
        self.remove(&key)
    }
}

impl AeronCountersReader {
    /// Find the first counter of `type_id` whose key matches `predicate`.
    ///
    /// The type ids are the bindgen'd `AERON_COUNTER_*_TYPE_ID` constants; the key layout
    /// is type-specific (see `aeron_counters.h`). Mirrors Java's `CountersReader` lookups.
    pub fn find_by_type_id(&self, type_id: i32, mut predicate: impl FnMut(&[u8]) -> bool) -> Option<i32> {
        let mut found = None;
        self.foreach_counter_once(|_value, id, counter_type_id, key, _label| {
            if found.is_none() && counter_type_id == type_id && predicate(key) {
                found = Some(id);
            }
        });
        found
    }

    /// Find the first counter of `type_id` keyed by `registration_id` — the layout used by
    /// aeron's per-stream counters (publisher limit/position, sender/receiver positions,
    /// subscriber position: the key starts with the registration id as a little-endian
    /// `i64`). Mirrors Java's `findCounterIdByRegistration`-style helpers, e.g.:
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
    pub fn get_counter_label(
        &self,
        counter_id: i32,
        max_length: usize,
    ) -> Result<String, AeronCError> {
        let mut result = String::with_capacity(max_length);
        self.get_counter_label_into(counter_id, &mut result)?;
        Ok(result)
    }

    #[inline]
    #[doc = "Get the label for a counter."]
    pub fn get_counter_label_into(
        &self,
        counter_id: i32,
        dst: &mut String,
    ) -> Result<(), AeronCError> {
        unsafe {
            let capacity = dst.capacity();
            let vec = dst.as_mut_vec();
            vec.set_len(capacity);
            let written =
                self.counter_label(counter_id, vec.as_mut_ptr() as *mut _, capacity)? as usize;
            vec.set_len(std::cmp::min(written, capacity));
        }
        Ok(())
    }

    #[inline]
    #[doc = "Get the key for a counter."]
    pub fn get_counter_key(&self, counter_id: i32) -> Result<Vec<u8>, AeronCError> {
        let mut dst = Vec::new();
        self.get_counter_key_into(counter_id, &mut dst)?;
        Ok(dst)
    }

    #[inline]
    #[doc = "Get the key for a counter."]
    pub fn get_counter_key_into(
        &self,
        counter_id: i32,
        dst: &mut Vec<u8>,
    ) -> Result<(), AeronCError> {
        let mut key_ptr: *mut u8 = std::ptr::null_mut();
        unsafe {
            let result = bindings::aeron_counters_reader_metadata_key(
                self.get_inner(),
                counter_id,
                &mut key_ptr,
            );
            if result < 0 || key_ptr.is_null() {
                return Err(AeronCError::from_code(result));
            }

            loop {
                let val = *key_ptr.add(dst.len());
                if val == 0 {
                    break;
                } else {
                    dst.push(val);
                }
            }
            Ok(())
        }
    }

    #[inline]
    pub fn get_counter_value(&self, counter_id: i32) -> i64 {
        unsafe { *self.addr(counter_id) }
    }
}

impl Aeron {
    pub fn new_blocking(
        context: &AeronContext,
        timeout: std::time::Duration,
    ) -> Result<Self, AeronCError> {
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

impl AeronFragmentHandlerCallback for AeronFragmentAssembler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) -> () {
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
    fn handle_aeron_controlled_fragment_handler(
        &mut self,
        buffer: &[u8],
        header: AeronHeader,
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
    pub fn with_fragment_assembler(
        handler: T,
    ) -> Result<(Handler<AeronFragmentAssembler>, Handler<T>), AeronCError> {
        let handler = Handler::new(handler);
        Ok((
            Handler::new(AeronFragmentAssembler::new(Some(&handler))?),
            handler,
        ))
    }

    #[deprecated(note = "handlers no longer leak; use Handler::with_fragment_assembler")]
    pub fn leak_with_fragment_assembler(
        handler: T,
    ) -> Result<(Handler<AeronFragmentAssembler>, Handler<T>), AeronCError> {
        Self::with_fragment_assembler(handler)
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

    #[deprecated(note = "handlers no longer leak; use Handler::with_controlled_fragment_assembler")]
    pub fn leak_with_controlled_fragment_assembler(
        handler: T,
    ) -> Result<(Handler<AeronControlledFragmentAssembler>, Handler<T>), AeronCError> {
        Self::with_controlled_fragment_assembler(handler)
    }
}

impl AeronBufferClaim {
    /// Writable view over the claimed term-buffer region.
    ///
    /// # Safety contract ( upheld by construction )
    /// Only sound on a **genuinely claimed** buffer (produced by a successful
    /// `try_claim`). `length` and `data` come straight from the Aeron C claim
    /// and are trusted; a default-constructed/null claim would be unsound to
    /// dereference — the `debug_assert` guards that in debug builds only (no
    /// release-build cost on this hot path).
    #[inline]
    pub fn data_mut(&self) -> &mut [u8] {
        debug_assert!(!self.data.is_null());
        unsafe { std::slice::from_raw_parts_mut(self.data, self.length) }
    }

    #[inline]
    pub fn frame_header_mut(&self) -> &mut aeron_header_values_frame_t {
        debug_assert!(!self.frame_header.is_null());
        unsafe { &mut *self.frame_header.cast::<aeron_header_values_frame_t>() }
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
/// a successful `try_claim`, so its inner buffer is always a genuinely claimed
/// slot (never a null zero-default).
pub struct AeronClaim {
    claim: AeronBufferClaim,
    position: i64,
    finalised: bool,
}

impl AeronClaim {
    /// The writable claimed slice (zero-copy into the publication term buffer).
    #[inline]
    pub fn data(&mut self) -> &mut [u8] {
        self.claim.data()
    }

    /// Length of the claimed slice in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.claim.length()
    }

    /// Whether the claimed slice is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.claim.length() == 0
    }

    /// Stream position Aeron assigned to this claim.
    #[inline]
    pub fn position(&self) -> i64 {
        self.position
    }

    /// Commit the claimed bytes, publishing them to subscribers. Consumes `self`.
    pub fn commit(mut self) -> Result<i64, AeronCError> {
        self.finalised = true;
        self.claim.commit()?;
        Ok(self.position)
    }

    /// Abort the claim, discarding the slot as padding for subscribers.
    /// Consumes `self`.
    pub fn abort(mut self) -> Result<(), AeronCError> {
        self.finalised = true;
        self.claim.abort()?;
        Ok(())
    }
}

impl Drop for AeronClaim {
    fn drop(&mut self) {
        if !self.finalised {
            // Defensive backstop: abort so the term slot is released now rather
            // than after the publication unblock timeout. The claim is always a
            // genuine claimed slot (constructed only on a successful try_claim),
            // so calling abort here is safe.
            let _ = self.claim.abort();
        }
    }
}

impl AeronPublication {
    /// Publish `buffer`, returning the **raw** `i64` from `aeron_publication_offer` with
    /// no error mapping: a non-negative value is the new stream log position; a negative
    /// value is an Aeron sentinel (`-1` not-connected, `-2` back-pressured, `-3`
    /// admin-action, `-4` closed, `-5` max-position-exceeded, `-6`/other error).
    ///
    /// This is the **branch-free** variant of [`Self::offer`] / [`Self::offer_with_reserved_value`]:
    /// it skips the `AeronOfferError::from_position` match and `Result` construction that
    /// the typed variants do on every call. Use it in a tight publish loop where you check
    /// `position < 0` yourself anyway — the typed `Result` is redundant overhead there (the
    /// `offer_claim_poll` bench measures raw offer ~0.2 ns cheaper per call than typed).
    ///
    /// For everything else prefer [`Self::offer`] (typed, with `is_retryable()` /
    /// `is_fatal()`). To decode a raw sentinel into a typed error, use
    /// [`AeronOfferError::from_position`].
    #[inline]
    pub fn offer_raw<H: AeronReservedValueSupplierCallback>(
        &self,
        buffer: &[u8],
        reserved_value_supplier: Option<&Handler<H>>,
    ) -> i64 {
        unsafe {
            aeron_publication_offer(
                self.get_inner(),
                buffer.as_ptr() as *mut _,
                buffer.len(),
                {
                    let callback: aeron_reserved_value_supplier_t = if reserved_value_supplier.is_none() {
                        None
                    } else {
                        Some(aeron_reserved_value_supplier_t_callback::<H>)
                    };
                    callback
                },
                reserved_value_supplier
                    .map(|m| m.as_raw())
                    .unwrap_or_else(|| std::ptr::null_mut()),
            )
        }
    }

    /// Non-blocking publish of `buffer`, returning the new stream position.
    ///
    /// This is the common case (no reserved-value supplier). Errors are typed:
    /// check [`AeronOfferError::is_retryable`] to drive retry loops (back pressure
    /// / admin action / not connected) vs fatal conditions (closed, max position
    /// exceeded). Use [`Self::offer_raw`] for the raw sentinel, or
    /// [`Self::offer_with_reserved_value`] to supply a reserved value.
    #[inline]
    pub fn offer(&self, buffer: &[u8]) -> Result<i64, AeronOfferError> {
        AeronOfferError::from_position(self.offer_raw::<AeronReservedValueSupplierLogger>(buffer, None))
    }

    /// [`Self::offer`] with a reserved-value supplier.
    #[inline]
    pub fn offer_with_reserved_value<H: AeronReservedValueSupplierCallback>(
        &self,
        buffer: &[u8],
        reserved_value_supplier: Option<&Handler<H>>,
    ) -> Result<i64, AeronOfferError> {
        AeronOfferError::from_position(self.offer_raw(buffer, reserved_value_supplier))
    }

    /// Raw, branch-free variant of [`Self::try_claim`]: position or negative sentinel.
    #[inline]
    pub fn try_claim_raw(&self, length: usize, buffer_claim: &AeronBufferClaim) -> i64 {
        unsafe { aeron_publication_try_claim(self.get_inner(), length, buffer_claim.get_inner()) }
    }

    /// Zero-copy claim of `length` bytes; write into the claim then `commit()`.
    ///
    /// Same typed-error semantics as [`Self::offer`]. Prefer
    /// [`Self::try_claim_owned`] for RAII abort-on-drop.
    #[inline]
    pub fn try_claim(&self, length: usize, buffer_claim: &AeronBufferClaim) -> Result<i64, AeronOfferError> {
        AeronOfferError::from_position(self.try_claim_raw(length, buffer_claim))
    }

    /// Zero-copy claim returning a RAII [`AeronClaim`] that is auto-aborted on
    /// drop unless explicitly committed or aborted.
    pub fn try_claim_owned(&self, length: usize) -> Result<AeronClaim, AeronOfferError> {
        let claim = AeronBufferClaim::new_zeroed_on_stack();
        let position = self.try_claim(length, &claim)?;
        Ok(AeronClaim {
            claim,
            position,
            finalised: false,
        })
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

    /// Convenience accessor for the registration id (no direct C accessor exists;
    /// backed by [`Self::get_constants`]). `session_id`, `stream_id` and `channel`
    /// have cheap direct getters — prefer those; for `registration_id` /
    /// `max_payload_length` in a hot loop, call [`Self::get_constants`] once and
    /// reuse the returned [`AeronPublicationConstants`].
    #[inline]
    pub fn registration_id(&self) -> Result<i64, AeronCError> {
        self.get_constants().map(|c| c.registration_id())
    }

    /// Convenience accessor for the max payload length (see [`Self::registration_id`]).
    #[inline]
    pub fn max_payload_length(&self) -> Result<usize, AeronCError> {
        self.get_constants().map(|c| c.max_payload_length())
    }
}

impl AeronExclusivePublication {
    /// Publish `buffer`, returning the **raw** `i64` from `aeron_publication_offer` with
    /// no error mapping: a non-negative value is the new stream log position; a negative
    /// value is an Aeron sentinel (`-1` not-connected, `-2` back-pressured, `-3`
    /// admin-action, `-4` closed, `-5` max-position-exceeded, `-6`/other error).
    ///
    /// This is the **branch-free** variant of [`Self::offer`] / [`Self::offer_with_reserved_value`]:
    /// it skips the `AeronOfferError::from_position` match and `Result` construction that
    /// the typed variants do on every call. Use it in a tight publish loop where you check
    /// `position < 0` yourself anyway — the typed `Result` is redundant overhead there (the
    /// `offer_claim_poll` bench measures raw offer ~0.2 ns cheaper per call than typed).
    ///
    /// For everything else prefer [`Self::offer`] (typed, with `is_retryable()` /
    /// `is_fatal()`). To decode a raw sentinel into a typed error, use
    /// [`AeronOfferError::from_position`].
    #[inline]
    pub fn offer_raw<H: AeronReservedValueSupplierCallback>(
        &self,
        buffer: &[u8],
        reserved_value_supplier: Option<&Handler<H>>,
    ) -> i64 {
        unsafe {
            aeron_exclusive_publication_offer(
                self.get_inner(),
                buffer.as_ptr() as *mut _,
                buffer.len(),
                {
                    let callback: aeron_reserved_value_supplier_t = if reserved_value_supplier.is_none() {
                        None
                    } else {
                        Some(aeron_reserved_value_supplier_t_callback::<H>)
                    };
                    callback
                },
                reserved_value_supplier
                    .map(|m| m.as_raw())
                    .unwrap_or_else(|| std::ptr::null_mut()),
            )
        }
    }

    /// Non-blocking publish of `buffer`, returning the new stream position.
    ///
    /// This is the common case (no reserved-value supplier). Errors are typed:
    /// check [`AeronOfferError::is_retryable`] to drive retry loops (back pressure
    /// / admin action / not connected) vs fatal conditions (closed, max position
    /// exceeded). Use [`Self::offer_raw`] for the raw sentinel, or
    /// [`Self::offer_with_reserved_value`] to supply a reserved value.
    #[inline]
    pub fn offer(&self, buffer: &[u8]) -> Result<i64, AeronOfferError> {
        AeronOfferError::from_position(self.offer_raw::<AeronReservedValueSupplierLogger>(buffer, None))
    }

    /// [`Self::offer`] with a reserved-value supplier.
    #[inline]
    pub fn offer_with_reserved_value<H: AeronReservedValueSupplierCallback>(
        &self,
        buffer: &[u8],
        reserved_value_supplier: Option<&Handler<H>>,
    ) -> Result<i64, AeronOfferError> {
        AeronOfferError::from_position(self.offer_raw(buffer, reserved_value_supplier))
    }

    /// Raw, branch-free variant of [`Self::try_claim`]: position or negative sentinel.
    #[inline]
    pub fn try_claim_raw(&self, length: usize, buffer_claim: &AeronBufferClaim) -> i64 {
        unsafe { aeron_exclusive_publication_try_claim(self.get_inner(), length, buffer_claim.get_inner()) }
    }

    /// Zero-copy claim of `length` bytes; write into the claim then `commit()`.
    ///
    /// Same typed-error semantics as [`Self::offer`]. Prefer
    /// [`Self::try_claim_owned`] for RAII abort-on-drop.
    #[inline]
    pub fn try_claim(&self, length: usize, buffer_claim: &AeronBufferClaim) -> Result<i64, AeronOfferError> {
        AeronOfferError::from_position(self.try_claim_raw(length, buffer_claim))
    }

    /// Zero-copy claim returning a RAII [`AeronClaim`] that is auto-aborted on
    /// drop unless explicitly committed or aborted.
    pub fn try_claim_owned(&self, length: usize) -> Result<AeronClaim, AeronOfferError> {
        let claim = AeronBufferClaim::new_zeroed_on_stack();
        let position = self.try_claim(length, &claim)?;
        Ok(AeronClaim {
            claim,
            position,
            finalised: false,
        })
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

    /// Convenience accessor for the registration id (no direct C accessor exists;
    /// backed by [`Self::get_constants`]). See [`AeronPublication::registration_id`]
    /// for the hot-loop caching note.
    #[inline]
    pub fn registration_id(&self) -> Result<i64, AeronCError> {
        self.get_constants().map(|c| c.registration_id())
    }

    /// Convenience accessor for the max payload length (see [`AeronPublication::registration_id`]).
    #[inline]
    pub fn max_payload_length(&self) -> Result<usize, AeronCError> {
        self.get_constants().map(|c| c.max_payload_length())
    }
}

impl AeronSubscription {
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

    /// Convenience accessor for the stream id (no direct C accessor exists for
    /// subscriptions; backed by [`Self::get_constants`]). For a hot loop, call
    /// [`Self::get_constants`] once and reuse the returned
    /// [`AeronSubscriptionConstants`].
    #[inline]
    pub fn stream_id(&self) -> Result<i32, AeronCError> {
        self.get_constants().map(|c| c.stream_id())
    }

    /// Convenience accessor for the channel (see [`Self::stream_id`]).
    #[inline]
    pub fn channel(&self) -> Result<String, AeronCError> {
        self.get_constants().map(|c| c.channel().to_string())
    }

    /// Convenience accessor for the registration id (see [`Self::stream_id`]).
    #[inline]
    pub fn registration_id(&self) -> Result<i64, AeronCError> {
        self.get_constants().map(|c| c.registration_id())
    }
}

impl AeronImage {
    /// Poll delivering each fragment of this image to `handler`, borrowing the
    /// closure only for this call (zero allocation). Use for image-scoped receive
    /// loops; use a fragment assembler for messages that may exceed the MTU.
    ///
    /// Returns the number of fragments delivered (0 if nothing was available).
    #[inline]
    pub fn poll_fn<H: FnMut(&[u8], AeronHeader)>(
        &self,
        mut handler: H,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        let result = unsafe {
            aeron_image_poll(
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

    /// Deprecated alias for [`Self::poll_fn`].
    #[deprecated(since = "0.1.169", note = "use `poll_fn` instead")]
    #[inline]
    pub fn poll_once<H: FnMut(&[u8], AeronHeader)>(
        &self,
        handler: H,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        self.poll_fn(handler, fragment_limit)
    }

    /// Instrumented wrapper around [`AeronImage::poll`] that adds tracing spans
    /// when the `instrument-ops` feature is enabled.
    #[inline]
    pub fn poll_instrumented<AeronFragmentHandlerHandlerImpl: AeronFragmentHandlerCallback>(
        &self,
        handler: Option<&Handler<AeronFragmentHandlerHandlerImpl>>,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        self.poll(handler, fragment_limit)
    }

    /// Instrumented wrapper around [`AeronImage::poll_fn`] that adds tracing spans
    /// when the `instrument-ops` feature is enabled.
    #[inline]
    pub fn poll_once_instrumented<
        AeronFragmentHandlerHandlerImpl: FnMut(&[u8], AeronHeader) -> (),
    >(
        &self,
        handler: AeronFragmentHandlerHandlerImpl,
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        self.poll_fn(handler, fragment_limit)
    }
}

/// Production error handler that routes Aeron async errors through the Rust `log`
/// facade at **error** level with a concise format — the recommended default for
/// `AeronContext::set_error_handler`.
///
/// Not to be confused with the generated [`AeronErrorHandlerLogger`], which is the
/// codegen's generic callback tracer (info level, verbose per-call format, useful
/// only behind `--features log-c-bindings`-style debugging).
pub struct AeronErrorLogger;
impl AeronErrorHandlerCallback for AeronErrorLogger {
    fn handle_aeron_error_handler(&mut self, error_code: std::ffi::c_int, msg: &str) -> () {
        log::error!("aeron error {}: {}", error_code, msg);
    }
}
unsafe impl Send for AeronErrorLogger {}
unsafe impl Sync for AeronErrorLogger {}

pub struct FnMutMessageHandler {
    func: fn(*mut (), &[u8], AeronHeader),
    ctx: *mut (),
}

impl AeronFragmentHandlerCallback for FnMutMessageHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) -> () {
        self.call(buffer, header);
    }
}

impl FnMutMessageHandler {
    pub fn new() -> Self {
        Self {
            func: Self::noop,
            ctx: std::ptr::null_mut(),
        }
    }

    #[inline]
    /// Point this handler at `ctx` / `func` so the next [`call`](Self::call)
    /// dispatches into them.
    ///
    /// # Lifetime contract (caller must uphold)
    /// `ctx` is stored as a raw `*mut ()` that **escapes the borrow** — the
    /// borrow checker does NOT keep it alive. The caller MUST ensure `ctx`
    /// outlives every subsequent `call` (i.e. `ctx` is dropped only after this
    /// handler is retired or re-`set`). Calling `call` after `ctx` is dropped is
    /// use-after-free. `ctx` must also not be moved while borrowed.
    pub fn set<T>(&mut self, ctx: &mut T, func: fn(&mut T, &[u8], AeronHeader)) -> &mut Self {
        self.func = Self::wrap::<T>(func);
        self.ctx = ctx as *mut T as *mut ();
        self
    }

    #[inline(always)]
    pub fn call(&mut self, msg: &[u8], header: AeronHeader) {
        (self.func)(self.ctx, msg, header);
    }

    /// Drop the borrowed `ctx` so a later stray [`call`](Self::call) is a safe no-op
    /// (the `noop` func is restored and the pointer nulled) rather than a use-after-free.
    ///
    /// [`AeronFragmentClosureAssembler::poll`] calls this before returning so the
    /// raw pointer never outlives the borrow it was created from.
    #[inline]
    pub fn clear(&mut self) {
        self.func = Self::noop;
        self.ctx = std::ptr::null_mut();
    }

    #[inline]
    fn wrap<T>(f: fn(&mut T, &[u8], AeronHeader)) -> fn(*mut (), &[u8], AeronHeader) {
        // SAFETY: `fn(&mut T,…)` and `fn(*mut(),…)` have the same ABI/representation
        unsafe { std::mem::transmute(f) }
    }

    fn noop(_: *mut (), _: &[u8], _: AeronHeader) {
        // default no-op handler
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

    /// Poll `subscription`, dispatching each reassembled message to `func` with
    /// `ctx`, borrowing `ctx` only for the duration of this call.
    ///
    /// Unlike the deprecated [`Self::process`], the raw `ctx` pointer is cleared
    /// before returning, so it can never be dereferenced after `ctx` goes out of
    /// scope — the borrow checker's lifetime on `ctx: &mut T` is now the real
    /// safety contract, not a comment the caller must uphold.
    ///
    /// Returns the fragment count from the underlying poll.
    pub fn poll<T>(
        &mut self,
        subscription: &AeronSubscription,
        ctx: &mut T,
        func: fn(&mut T, &[u8], AeronHeader),
        fragment_limit: usize,
    ) -> Result<i32, AeronCError> {
        self.handler.set(ctx, func);
        let result = subscription.poll(Some(&self.assembler), fragment_limit);
        self.handler.clear();
        result
    }

    /// Deprecated two-phase pattern: point the assembler at `ctx`/`func` and return
    /// the assembler handler for a later `subscription.poll(...)`.
    ///
    /// **Unsafe by construction**: `ctx` is stored as a raw pointer that escapes
    /// the borrow — calling `subscription.poll(...)` after `ctx` is dropped is
    /// use-after-free. Prefer [`Self::poll`], which scopes the borrow.
    #[deprecated(since = "0.1.169", note = "use `poll(subscription, ctx, func, limit)` instead — it clears the ctx pointer on return")]
    pub fn process<T>(
        &mut self,
        ctx: &mut T,
        func: fn(&mut T, &[u8], AeronHeader),
    ) -> Option<&Handler<AeronFragmentAssembler>> {
        self.handler.set(ctx, func);
        Some(&self.assembler)
    }
}

pub struct FnMutControlledMessageHandler {
    func: fn(*mut (), &[u8], AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ctx: *mut (),
}

impl FnMutControlledMessageHandler {
    pub fn new() -> Self {
        Self {
            func: Self::noop,
            ctx: std::ptr::null_mut(),
        }
    }

    #[inline]
    /// Point this handler at `ctx` / `func` so the next [`call`](Self::call)
    /// dispatches into them.
    ///
    /// # Lifetime contract (caller must uphold)
    /// Same as [`FnMutMessageHandler::set`](super::FnMutMessageHandler::set):
    /// `ctx` is stored as a raw pointer that escapes the borrow. The caller MUST
    /// keep `ctx` alive (and unmoved) until this handler is retired or re-`set`;
    /// calling `call` after `ctx` is dropped is use-after-free.
    pub fn set<T>(
        &mut self,
        ctx: &mut T,
        func: fn(&mut T, &[u8], AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ) -> &mut Self {
        self.func = Self::wrap::<T>(func);
        self.ctx = ctx as *mut T as *mut ();
        self
    }

    #[inline(always)]
    pub fn call(
        &mut self,
        msg: &[u8],
        header: AeronHeader,
    ) -> aeron_controlled_fragment_handler_action_t {
        (self.func)(self.ctx, msg, header)
    }

    /// Drop the borrowed `ctx` (cf. [`FnMutMessageHandler::clear`]) so a later stray
    /// [`call`](Self::call) returns `CONTINUE` instead of dereferencing a stale pointer.
    #[inline]
    pub fn clear(&mut self) {
        self.func = Self::noop;
        self.ctx = std::ptr::null_mut();
    }

    #[inline]
    fn wrap<T>(
        f: fn(&mut T, &[u8], AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ) -> fn(*mut (), &[u8], AeronHeader) -> aeron_controlled_fragment_handler_action_t {
        unsafe { std::mem::transmute(f) }
    }

    fn noop(_: *mut (), _: &[u8], _: AeronHeader) -> aeron_controlled_fragment_handler_action_t {
        bindings::aeron_controlled_fragment_handler_action_en::AERON_ACTION_CONTINUE
    }
}

impl AeronControlledFragmentHandlerCallback for FnMutControlledMessageHandler {
    fn handle_aeron_controlled_fragment_handler(
        &mut self,
        buffer: &[u8],
        header: AeronHeader,
    ) -> aeron_controlled_fragment_handler_action_t {
        self.call(buffer, header)
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

    pub fn process<T>(
        &mut self,
        ctx: &mut T,
        func: fn(&mut T, &[u8], AeronHeader) -> aeron_controlled_fragment_handler_action_t,
    ) -> Option<&Handler<AeronControlledFragmentAssembler>> {
        self.handler.set(ctx, func);
        Some(&self.assembler)
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
/// // (illustrative — `publication.status()` needs a live handle; see the
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
    pub fn reset(&mut self) {
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
/// - `hostname:port` — host is alphanumeric plus `-` and `.`, port 0-65535
/// - `IPv4:port` — dotted decimal, port 0-65535
/// - `[IPv6]:port` — bracketed IPv6 literal, port required
///
/// # Errors
/// Returns `Err(AeronCError)` with code `-1` for any validation failure:
/// - empty string or missing port separator
/// - port not `0..=65535` or not a decimal integer
/// - host contains characters outside the safe allowlist (alphanumeric,
///   `-`, `.`, `:` for IPv4, `[]` for IPv6 bracketing)
/// - URI separator characters (`?`, `=`, `:`, `/`) appear unescaped
///
/// # Example
/// ```ignore
/// // (illustrative — `aeron_custom.rs` is `include!`'d into multiple crates, so
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
        let end_bracket = endpoint
            .find(']')
            .ok_or_else(|| AeronCError::from_code(-1))?;
        if end_bracket == 1 {
            // Empty `[]` is invalid.
            return Err(AeronCError::from_code(-1));
        }
        let host_part = &endpoint[1..end_bracket];
        // Find the port separator colon AFTER the closing bracket.
        let after_bracket = &endpoint[end_bracket..];
        let colon_offset = after_bracket
            .find(':')
            .ok_or_else(|| AeronCError::from_code(-1))?;
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
    let colon_pos = endpoint
        .rfind(':')
        .ok_or_else(|| AeronCError::from_code(-1))?;
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
    // Minimal validation: ensure only valid characters and at least one colon.
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
    let port_num = port
        .parse::<u32>()
        .map_err(|_| AeronCError::from_code(-1))?;
    if port_num > 65535 {
        return Err(AeronCError::from_code(-1));
    }

    Ok(())
}

#[cfg(test)]
mod aeron_custom_tests {
    use super::*;

    #[test]
    fn from_position_positive_is_ok_position() {
        assert_eq!(AeronOfferError::from_position(0), Ok(0));
        assert_eq!(AeronOfferError::from_position(12_345), Ok(12_345));
    }

    #[test]
    fn from_position_maps_every_sentinel_distinctly() {
        assert_eq!(
            AeronOfferError::from_position(-1),
            Err(AeronOfferError::NotConnected)
        );
        assert_eq!(
            AeronOfferError::from_position(-2),
            Err(AeronOfferError::BackPressured)
        );
        assert_eq!(
            AeronOfferError::from_position(-3),
            Err(AeronOfferError::AdminAction)
        );
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
        assert_eq!(
            AeronStatus::from_error(&AeronOfferError::MaxPositionExceeded),
            None
        );
    }

    #[test]
    fn defused_claim_drops_without_invoking_ffi() {
        // A zero-default `AeronBufferClaim` has a null inner pointer; calling
        // abort/commit FFI on it would be unsound. With `finalised = true` the
        // `Drop` backstop must skip the abort. Constructing directly is allowed
        // because the test sits in the same module as the private fields. The
        // mere fact that this drops without segfaulting is the assertion
        // (mirrors wingfoil's defused-claim test). `position()` reads only the
        // stored i64 field, so it is also FFI-free.
        let claim = AeronClaim {
            claim: AeronBufferClaim::default(),
            position: 7,
            finalised: true,
        };
        assert_eq!(claim.position(), 7);
        drop(claim);
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
        assert_eq!(
            tracker.observe(AeronStatus::Connected),
            Some(AeronStatus::Connected)
        );
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
        assert_eq!(
            tracker.observe(AeronStatus::Connected),
            Some(AeronStatus::Connected)
        );
    }

    #[test]
    fn status_tracker_records_closed_transition() {
        let mut tracker = AeronStatusTracker::new();
        tracker.observe(AeronStatus::Connected);
        assert_eq!(
            tracker.observe(AeronStatus::Closed),
            Some(AeronStatus::Closed)
        );
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
        assert_eq!(
            tracker.observe(AeronStatus::Connected),
            Some(AeronStatus::Connected)
        );
        assert_eq!(
            tracker.observe(AeronStatus::BackPressured),
            Some(AeronStatus::BackPressured)
        );
        assert_eq!(
            tracker.observe(AeronStatus::Connected),
            Some(AeronStatus::Connected)
        );
        assert_eq!(
            tracker.observe(AeronStatus::Closed),
            Some(AeronStatus::Closed)
        );
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
