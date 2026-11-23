use super::*;

/// A positive receiver window. The adapter also bounds it to half the image term.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct ReceiverWindow(i32);
impl ReceiverWindow {
    #[inline]
    pub fn new(value: i32) -> Self {
        Self(value.max(1))
    }
    pub fn get(self) -> i32 {
        self.0
    }
}

pub struct CongestionControlArgs<'a> {
    pub channel: ChannelRef<'a>,
    pub context: StrategyContext,
    pub stream_id: i32,
    pub session_id: i32,
    pub registration_id: i64,
    pub term_length: i32,
    pub sender_mtu_length: i32,
    pub control_address: SockAddrRef<'a>,
    pub source_address: SockAddrRef<'a>,
}

#[derive(Clone, Copy, Debug)]
pub struct TrackRebuild {
    pub now_ns: i64,
    pub new_consumption_position: i64,
    pub last_sm_position: i64,
    pub hwm_position: i64,
    pub starting_rebuild_position: i64,
    pub ending_rebuild_position: i64,
    pub loss_occurred: bool,
}

/// Receiver and conductor callbacks with disjoint mutable state.
/// RTT values are signed and can be negative. Shared measurements need atomics
/// or another synchronisation mechanism chosen by the strategy.
pub trait CongestionControl: Sized + 'static {
    type Receiver: Send + 'static;
    type Conductor: Send + 'static;
    type Shared: Send + Sync + 'static;
    fn new(args: &CongestionControlArgs<'_>) -> Result<(Self::Receiver, Self::Conductor, Self::Shared), AeronCError>;
    fn initial_window_length(c: &mut Self::Conductor, s: &Self::Shared) -> ReceiverWindow;
    fn max_window_length(c: &mut Self::Conductor, s: &Self::Shared) -> ReceiverWindow;
    fn on_track_rebuild(
        c: &mut Self::Conductor,
        s: &Self::Shared,
        rebuild: TrackRebuild,
        should_force_sm: &mut bool,
    ) -> ReceiverWindow;
    fn should_measure_rtt(r: &mut Self::Receiver, s: &Self::Shared, now_ns: i64) -> bool {
        false
    }
    fn on_rttm_sent(r: &mut Self::Receiver, s: &Self::Shared, now_ns: i64) {}
    fn on_rttm(r: &mut Self::Receiver, s: &Self::Shared, now_ns: i64, rtt_ns: i64, source: SockAddrRef<'_>) {}
}

#[repr(C)]
struct CongestionBox<T: CongestionControl> {
    c: aeron_congestion_control_strategy_t,
    shared: T::Shared,
    receiver: UnsafeCell<T::Receiver>,
    conductor: UnsafeCell<T::Conductor>,
    window_limit: i32,
}
impl<T: CongestionControl> CongestionBox<T> {
    const TABLE: aeron_congestion_control_strategy_t = aeron_congestion_control_strategy_t {
        should_measure_rtt: Some(measure::<T>),
        on_rttm_sent: Some(sent::<T>),
        on_rttm: Some(rtt::<T>),
        on_track_rebuild: Some(rebuild::<T>),
        initial_window_length: Some(initial::<T>),
        max_window_length: Some(maximum::<T>),
        fini: Some(fini::<T>),
        state: std::ptr::null_mut(),
    };
}
unsafe fn receiver<'a, T: CongestionControl>(raw: *mut c_void) -> (&'a mut T::Receiver, &'a T::Shared) {
    let state = raw.cast::<CongestionBox<T>>();
    unsafe {
        (
            &mut *(*std::ptr::addr_of!((*state).receiver)).get(),
            &*std::ptr::addr_of!((*state).shared),
        )
    }
}
unsafe fn conductor<'a, T: CongestionControl>(raw: *mut c_void) -> (&'a mut T::Conductor, &'a T::Shared, i32) {
    let state = raw.cast::<CongestionBox<T>>();
    unsafe {
        (
            &mut *(*std::ptr::addr_of!((*state).conductor)).get(),
            &*std::ptr::addr_of!((*state).shared),
            (*state).window_limit,
        )
    }
}
unsafe extern "C" fn measure<T: CongestionControl>(raw: *mut c_void, now: i64) -> bool {
    let (r, s) = unsafe { receiver::<T>(raw) };
    T::should_measure_rtt(r, s, now)
}
unsafe extern "C" fn sent<T: CongestionControl>(raw: *mut c_void, now: i64) {
    let (r, s) = unsafe { receiver::<T>(raw) };
    T::on_rttm_sent(r, s, now);
}
unsafe extern "C" fn rtt<T: CongestionControl>(raw: *mut c_void, now: i64, ns: i64, addr: *mut sockaddr_storage) {
    let (r, s) = unsafe { receiver::<T>(raw) };
    T::on_rttm(r, s, now, ns, unsafe { SockAddrRef::from_raw(addr) });
}
unsafe extern "C" fn initial<T: CongestionControl>(raw: *mut c_void) -> i32 {
    let (c, s, limit) = unsafe { conductor::<T>(raw) };
    T::initial_window_length(c, s).get().min(limit)
}
unsafe extern "C" fn maximum<T: CongestionControl>(raw: *mut c_void) -> i32 {
    let (c, s, limit) = unsafe { conductor::<T>(raw) };
    T::max_window_length(c, s).get().min(limit)
}
unsafe extern "C" fn rebuild<T: CongestionControl>(
    raw: *mut c_void,
    force: *mut bool,
    now_ns: i64,
    new_consumption_position: i64,
    last_sm_position: i64,
    hwm_position: i64,
    starting_rebuild_position: i64,
    ending_rebuild_position: i64,
    loss_occurred: bool,
) -> i32 {
    let (c, s, limit) = unsafe { conductor::<T>(raw) };
    T::on_track_rebuild(
        c,
        s,
        TrackRebuild {
            now_ns,
            new_consumption_position,
            last_sm_position,
            hwm_position,
            starting_rebuild_position,
            ending_rebuild_position,
            loss_occurred,
        },
        unsafe { &mut *force },
    )
    .get()
    .min(limit)
}
unsafe extern "C" fn fini<T: CongestionControl>(raw: *mut aeron_congestion_control_strategy_t) -> i32 {
    unsafe { drop(Box::from_raw(raw.cast::<CongestionBox<T>>())) };
    0
}
unsafe extern "C" fn supplier<T: CongestionControl>(
    out: *mut *mut aeron_congestion_control_strategy_t,
    channel: *mut aeron_udp_channel_t,
    stream_id: i32,
    session_id: i32,
    registration_id: i64,
    term_length: i32,
    sender_mtu_length: i32,
    control: *mut sockaddr_storage,
    source: *mut sockaddr_storage,
    context: *mut aeron_driver_context_t,
    counters: *mut aeron_counters_manager_t,
) -> i32 {
    unsafe { *out = std::ptr::null_mut() };
    let args = CongestionControlArgs {
        channel: unsafe { ChannelRef::from_raw(channel) },
        context: unsafe { StrategyContext::from_raw(context) },
        stream_id,
        session_id,
        registration_id,
        term_length,
        sender_mtu_length,
        control_address: unsafe { SockAddrRef::from_raw(control) },
        source_address: unsafe { SockAddrRef::from_raw(source) },
    };
    match T::new(&args) {
        Ok((receiver, conductor, shared)) => {
            let state = Box::into_raw(Box::new(CongestionBox::<T> {
                c: CongestionBox::<T>::TABLE,
                shared,
                receiver: UnsafeCell::new(receiver),
                conductor: UnsafeCell::new(conductor),
                window_limit: (term_length / 2).max(1),
            }));
            unsafe {
                (*state).c.state = state.cast();
                *out = state.cast()
            };
            0
        }
        Err(error) => unsafe { supplier_error(error) },
    }
}
impl AeronDriverContext {
    /// Replace native congestion-control selection. A custom supplier owns `cc=` parsing.
    pub fn set_congestion_control<T: CongestionControl>(&self) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        check(unsafe { aeron_driver_context_set_congestioncontrol_supplier(self.get_inner(), Some(supplier::<T>)) })
    }
}
