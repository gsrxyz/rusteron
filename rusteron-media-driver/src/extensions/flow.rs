use super::*;

/// A nonnegative sender limit. Construction saturates negative values to zero.
/// Aeron subtracts a nonnegative sender position from this value in signed C arithmetic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Position(i64);
impl Position {
    #[inline]
    pub fn new(value: i64) -> Self {
        Self(value.max(0))
    }
    #[inline]
    pub fn get(self) -> i64 {
        self.0
    }
}

/// Initialization arguments borrowed only for `FlowControl::new`.
pub struct FlowControlArgs<'a> {
    pub channel: ChannelRef<'a>,
    pub context: StrategyContext,
    pub stream_id: i32,
    pub session_id: i32,
    pub registration_id: i64,
    pub initial_term_id: i32,
    pub term_length: usize,
}

impl FlowControlArgs<'_> {
    pub fn retransmit_window_multiple(&self) -> usize {
        let multi = unsafe {
            matches!(
                (*self.channel.raw).control_mode,
                aeron_udp_channel_control_mode::AERON_UDP_CHANNEL_CONTROL_MODE_DYNAMIC
                    | aeron_udp_channel_control_mode::AERON_UDP_CHANNEL_CONTROL_MODE_MANUAL
            )
        };
        if self.channel.is_multicast() || multi {
            self.context.multicast_retransmit_window_multiple
        } else {
            self.context.unicast_retransmit_window_multiple
        }
    }
}

/// Sender-owned flow control with separate state for concurrent receiver checks.
///
/// Aeron calls `has_required_receivers` on both the sender and conductor.
/// The other callbacks have exclusive access to `Self` on the sender.
/// Panics in any method, including `new` and `Drop`, abort the process.
pub trait FlowControl: Sized + Send + 'static {
    type Shared: Send + Sync + 'static;
    /// Allocate a driver counter for transitions reported through `metrics`.
    const COUNTER_LABEL: Option<&'static CStr> = None;
    fn new(args: &FlowControlArgs<'_>) -> Result<(Self, Self::Shared), AeronCError>;
    fn on_idle(
        &mut self,
        shared: &Self::Shared,
        now_ns: i64,
        snd_lmt: i64,
        snd_pos: i64,
        is_eos: bool,
        metrics: StrategyMetrics<'_>,
    ) -> Position {
        Position::new(snd_lmt)
    }
    fn on_status_message(
        &mut self,
        shared: &Self::Shared,
        sm: StatusMessage<'_>,
        source: SockAddrRef<'_>,
        snd_lmt: i64,
        initial_term_id: i32,
        position_bits_to_shift: usize,
        now_ns: i64,
        metrics: StrategyMetrics<'_>,
    ) -> Position;
    fn on_setup(
        &mut self,
        shared: &Self::Shared,
        setup: SetupMessage<'_>,
        now_ns: i64,
        snd_lmt: i64,
        position_bits_to_shift: usize,
        snd_pos: i64,
    ) {
    }
    fn on_error(&mut self, shared: &Self::Shared, error: ErrorMessage<'_>, source: SockAddrRef<'_>, now_ns: i64) {}
    fn on_trigger_send_setup(
        &mut self,
        shared: &Self::Shared,
        sm: StatusMessage<'_>,
        source: SockAddrRef<'_>,
        now_ns: i64,
    ) {
    }
    /// Return the requested retransmission length. The adapter bounds it to the
    /// request and the remaining term before returning it to C.
    fn max_retransmission_length(
        &mut self,
        shared: &Self::Shared,
        term_offset: usize,
        resend_length: usize,
        term_length: usize,
        mtu: usize,
        receiver_window_multiple: usize,
    ) -> usize {
        unsafe {
            aeron_flow_control_calculate_retransmission_length(
                resend_length,
                term_length,
                term_offset,
                receiver_window_multiple,
            )
        }
    }
    fn has_required_receivers(shared: &Self::Shared) -> bool {
        true
    }
}

#[repr(C)]
struct FlowBox<T: FlowControl> {
    c: aeron_flow_control_strategy_t,
    shared: T::Shared,
    sender: UnsafeCell<T>,
    counter: DriverCounter,
    retransmit_window_multiple: usize,
}
impl<T: FlowControl> FlowBox<T> {
    const TABLE: aeron_flow_control_strategy_t = aeron_flow_control_strategy_t {
        on_status_message: Some(status::<T>),
        on_idle: Some(idle::<T>),
        on_setup: Some(setup::<T>),
        on_error: Some(error::<T>),
        fini: Some(fini::<T>),
        has_required_receivers: Some(required::<T>),
        on_trigger_send_setup: Some(trigger::<T>),
        max_retransmission_length: Some(retransmit::<T>),
        state: std::ptr::null_mut(),
    };
}

// Project each field through raw pointers. A reference to the entire allocation
// would overlap the sender's mutable borrow during conductor receiver checks.
#[inline]
unsafe fn parts<'a, T: FlowControl>(raw: *mut c_void) -> (&'a mut T, &'a T::Shared) {
    let state = raw.cast::<FlowBox<T>>();
    unsafe {
        (
            &mut *(*std::ptr::addr_of!((*state).sender)).get(),
            &*std::ptr::addr_of!((*state).shared),
        )
    }
}
unsafe extern "C" fn idle<T: FlowControl>(raw: *mut c_void, now: i64, limit: i64, position: i64, eos: bool) -> i64 {
    let (sender, shared) = unsafe { parts::<T>(raw) };
    sender
        .on_idle(shared, now, limit, position, eos, unsafe { metrics::<T>(raw) })
        .get()
}
unsafe extern "C" fn status<T: FlowControl>(
    raw: *mut c_void,
    sm: *const u8,
    length: usize,
    source: *mut sockaddr_storage,
    limit: i64,
    term: i32,
    shift: usize,
    now: i64,
) -> i64 {
    let (sender, shared) = unsafe { parts::<T>(raw) };
    sender
        .on_status_message(
            shared,
            unsafe { StatusMessage::from_raw(sm, length) },
            unsafe { SockAddrRef::from_raw(source) },
            limit,
            term,
            shift,
            now,
            unsafe { metrics::<T>(raw) },
        )
        .get()
}
unsafe extern "C" fn setup<T: FlowControl>(
    raw: *mut c_void,
    data: *const u8,
    length: usize,
    now: i64,
    limit: i64,
    shift: usize,
    position: i64,
) -> i64 {
    let (sender, shared) = unsafe { parts::<T>(raw) };
    sender.on_setup(
        shared,
        unsafe { SetupMessage::from_raw(data, length) },
        now,
        limit,
        shift,
        position,
    );
    limit
}
unsafe extern "C" fn error<T: FlowControl>(
    raw: *mut c_void,
    data: *const u8,
    length: usize,
    source: *mut sockaddr_storage,
    now: i64,
) {
    let (sender, shared) = unsafe { parts::<T>(raw) };
    sender.on_error(
        shared,
        unsafe { ErrorMessage::from_raw(data, length) },
        unsafe { SockAddrRef::from_raw(source) },
        now,
    );
}
unsafe extern "C" fn trigger<T: FlowControl>(
    raw: *mut c_void,
    data: *const u8,
    length: usize,
    source: *mut sockaddr_storage,
    now: i64,
) {
    let (sender, shared) = unsafe { parts::<T>(raw) };
    sender.on_trigger_send_setup(
        shared,
        unsafe { StatusMessage::from_raw(data, length) },
        unsafe { SockAddrRef::from_raw(source) },
        now,
    );
}
unsafe extern "C" fn retransmit<T: FlowControl>(
    raw: *mut c_void,
    offset: usize,
    length: usize,
    term: usize,
    mtu: usize,
) -> usize {
    let (sender, shared) = unsafe { parts::<T>(raw) };
    sender
        .max_retransmission_length(shared, offset, length, term, mtu, unsafe {
            (*raw.cast::<FlowBox<T>>()).retransmit_window_multiple
        })
        .min(length)
        .min(term.saturating_sub(offset))
}
unsafe extern "C" fn required<T: FlowControl>(strategy: *mut aeron_flow_control_strategy_t) -> bool {
    T::has_required_receivers(unsafe { &*std::ptr::addr_of!((*strategy.cast::<FlowBox<T>>()).shared) })
}
unsafe extern "C" fn fini<T: FlowControl>(strategy: *mut aeron_flow_control_strategy_t) -> i32 {
    let state = strategy.cast::<FlowBox<T>>();
    unsafe {
        (*state).counter.close();
        drop(Box::from_raw(state))
    };
    0
}
unsafe extern "C" fn supplier<T: FlowControl>(
    output: *mut *mut aeron_flow_control_strategy_t,
    context: *mut aeron_driver_context_t,
    counters: *mut aeron_counters_manager_t,
    channel: *const aeron_udp_channel_t,
    stream_id: i32,
    session_id: i32,
    registration_id: i64,
    initial_term_id: i32,
    term_length: usize,
) -> i32 {
    unsafe { *output = std::ptr::null_mut() };
    let args = FlowControlArgs {
        channel: unsafe { ChannelRef::from_raw(channel) },
        context: unsafe { StrategyContext::from_raw(context) },
        stream_id,
        session_id,
        registration_id,
        initial_term_id,
        term_length,
    };
    match T::new(&args) {
        Ok((sender, shared)) => {
            let counter = match unsafe { DriverCounter::new(counters, T::COUNTER_LABEL, registration_id) } {
                Ok(counter) => counter,
                Err(error) => return unsafe { supplier_error(error) },
            };
            let state = Box::into_raw(Box::new(FlowBox::<T> {
                c: FlowBox::<T>::TABLE,
                shared,
                sender: UnsafeCell::new(sender),
                counter,
                retransmit_window_multiple: args.retransmit_window_multiple(),
            }));
            unsafe {
                (*state).c.state = state.cast();
                *output = state.cast()
            };
            0
        }
        Err(error) => unsafe { supplier_error(error) },
    }
}

impl AeronDriverContext {
    /// Install a type-selected supplier. Copy runtime configuration from the channel
    /// or use constants on `T`. Aeron's `fc=` option overrides multicast suppliers.
    pub fn set_unicast_flow_control<T: FlowControl>(&self) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        check(unsafe { aeron_driver_context_set_unicast_flowcontrol_supplier(self.get_inner(), Some(supplier::<T>)) })
    }
    /// Install a multicast supplier. Omit `fc=` from channels that use this supplier.
    pub fn set_multicast_flow_control<T: FlowControl>(&self) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        check(unsafe { aeron_driver_context_set_multicast_flowcontrol_supplier(self.get_inner(), Some(supplier::<T>)) })
    }
}

/// Aeron's max flow-control policy, with the context's retransmission window multiple.
pub struct MaxFlowControl {
    retransmit_window_multiple: usize,
}
impl FlowControl for MaxFlowControl {
    type Shared = ();
    fn new(args: &FlowControlArgs<'_>) -> Result<(Self, ()), AeronCError> {
        Ok((
            Self {
                retransmit_window_multiple: args.retransmit_window_multiple(),
            },
            (),
        ))
    }
    #[inline]
    fn on_status_message(
        &mut self,
        _: &(),
        sm: StatusMessage<'_>,
        _: SockAddrRef<'_>,
        limit: i64,
        term: i32,
        shift: usize,
        _: i64,
        _: StrategyMetrics<'_>,
    ) -> Position {
        Position::new(limit.max(sm.position(term, shift).saturating_add(i64::from(sm.receiver_window()))))
    }
    fn max_retransmission_length(
        &mut self,
        _: &(),
        offset: usize,
        length: usize,
        term: usize,
        _: usize,
        _: usize,
    ) -> usize {
        // This helper reads Aeron's default initial window, as the native max policy does.
        unsafe {
            aeron_flow_control_calculate_retransmission_length(length, term, offset, self.retransmit_window_multiple)
        }
    }
}

/// A callback-scoped driver counter. Calling `increment` records a policy transition.
/// No counter exists unless the strategy declares `COUNTER_LABEL`. Only the
/// sender writes this counter. Native observers can read it concurrently.
pub struct StrategyMetrics<'a> {
    value: *mut i64,
    _life: PhantomData<&'a ()>,
}
impl StrategyMetrics<'_> {
    #[inline]
    pub fn increment(&self) {
        if !self.value.is_null() {
            // The sender is the only writer. Match Aeron's single-writer
            // increment_release without a locked read-modify-write operation.
            // Keep both accesses atomic for concurrent native observers.
            unsafe {
                let value = std::sync::atomic::AtomicI64::from_ptr(self.value);
                let next = value.load(std::sync::atomic::Ordering::Relaxed).wrapping_add(1);
                value.store(next, std::sync::atomic::Ordering::Release);
            }
        }
    }
}
struct DriverCounter {
    manager: *mut aeron_counters_manager_t,
    id: i32,
    value: *mut i64,
}
impl DriverCounter {
    unsafe fn new(
        manager: *mut aeron_counters_manager_t,
        label: Option<&CStr>,
        registration: i64,
    ) -> Result<Self, AeronCError> {
        let mut counter = Self {
            manager,
            id: -1,
            value: std::ptr::null_mut(),
        };
        if let Some(label) = label {
            let label = format!("{}: registration={registration}", label.to_string_lossy());
            counter.id = unsafe {
                aeron_counters_manager_allocate(manager, 20001, std::ptr::null(), 0, label.as_ptr().cast(), label.len())
            };
            check(counter.id)?;
            unsafe {
                aeron_counters_manager_counter_registration_id(manager, counter.id, registration);
            }
            counter.value = unsafe {
                (*manager)
                    .values
                    .add(counter.id as usize * AERON_COUNTER_VALUE_LENGTH as usize)
                    .cast()
            };
        }
        Ok(counter)
    }
    unsafe fn close(&mut self) {
        if self.id >= 0 {
            unsafe {
                aeron_counters_manager_free(self.manager, self.id);
            }
            self.id = -1;
        }
    }
}
#[inline]
unsafe fn metrics<'a, T: FlowControl>(raw: *mut c_void) -> StrategyMetrics<'a> {
    // Compile away the counter lookup for strategies that do not request a counter.
    let value = if T::COUNTER_LABEL.is_some() {
        unsafe { (*raw.cast::<FlowBox<T>>()).counter.value }
    } else {
        std::ptr::null_mut()
    };
    StrategyMetrics {
        value,
        _life: PhantomData,
    }
}

#[cfg(test)]
pub(super) unsafe fn test_metrics_at(value: *mut i64) -> StrategyMetrics<'static> {
    StrategyMetrics {
        value,
        _life: PhantomData,
    }
}
