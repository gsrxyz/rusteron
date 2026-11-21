use super::*;

/// The native measurement to replace. Each role owns a separate tracker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DutyCycleRole {
    Conductor,
    Sender,
    Receiver,
    /// Measures resolver lookup and resolve calls on the native resource agent.
    NameResolver,
}

/// Observe native monotonic timestamps in nanoseconds.
///
/// `update` sets a baseline. `measure_and_update` measures elapsed time and sets
/// the next baseline. Calls for one instance never overlap. Keep callbacks
/// bounded because they run on the measured agent. A panic aborts the process.
pub trait DutyCycleTracker: Send + 'static {
    fn update(&mut self, now_ns: i64);
    fn measure_and_update(&mut self, now_ns: i64);
}

/// Measure elapsed nanoseconds with a closure and a separate baseline per instance.
///
/// The first timestamp sets the baseline without calling the closure. An
/// explicit `update` replaces the baseline without reporting a measurement.
/// Backward timestamps report zero. Differences saturate at `i64::MAX`.
/// The closure runs on the measured agent and must keep its work bounded.
pub struct DutyCycleTimer<F> {
    last_ns: Option<i64>,
    record: F,
}

impl<F: FnMut(i64) + Send + 'static> DutyCycleTimer<F> {
    pub fn new(record: F) -> Self {
        Self { last_ns: None, record }
    }
}

impl<F: FnMut(i64) + Send + 'static> DutyCycleTracker for DutyCycleTimer<F> {
    #[inline]
    fn update(&mut self, now_ns: i64) {
        self.last_ns = Some(now_ns);
    }

    #[inline]
    fn measure_and_update(&mut self, now_ns: i64) {
        if let Some(last) = self.last_ns {
            (self.record)(now_ns.saturating_sub(last).max(0));
        }
        self.last_ns = Some(now_ns);
    }
}

struct Tracker<T> {
    table: aeron_duty_cycle_tracker_t,
    value: UnsafeCell<T>,
}

// The public context is local. Native callbacks access value on one agent, and
// the retained owner drops only after native shutdown has joined that agent.
unsafe impl<T: DutyCycleTracker> Send for Tracker<T> {}
unsafe impl<T: DutyCycleTracker> Sync for Tracker<T> {}

unsafe extern "C" fn update<T: DutyCycleTracker>(state: *mut c_void, now: i64) {
    unsafe { &mut *state.cast::<T>() }.update(now);
}

unsafe extern "C" fn measure<T: DutyCycleTracker>(state: *mut c_void, now: i64) {
    unsafe { &mut *state.cast::<T>() }.measure_and_update(now);
}

impl AeronDriverContext {
    /// Replace a native stall tracker before driver initialization.
    ///
    /// The context retains this instance until native context shutdown, even if
    /// another setter replaces it. This replaces the role's built-in stall
    /// counter updates. Other roles keep their trackers.
    pub fn set_duty_cycle_tracker<T: DutyCycleTracker>(
        &self,
        role: DutyCycleRole,
        tracker: T,
    ) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        let mut owner = Box::new(Tracker {
            table: aeron_duty_cycle_tracker_t {
                update: Some(update::<T>),
                measure_and_update: Some(measure::<T>),
                state: std::ptr::null_mut(),
            },
            value: UnsafeCell::new(tracker),
        });
        owner.table.state = owner.value.get().cast();
        unsafe {
            let context = self.get_inner();
            let slot = match role {
                DutyCycleRole::Conductor => &mut (*context).conductor_duty_cycle_tracker,
                DutyCycleRole::Sender => &mut (*context).sender_duty_cycle_tracker,
                DutyCycleRole::Receiver => &mut (*context).receiver_duty_cycle_tracker,
                DutyCycleRole::NameResolver => &mut (*context).name_resolver_time_tracker,
            };
            *slot = &mut owner.table;
        }
        // C stores this pointer without freeing it. Resource dependencies drop
        // after native close, which follows the join of all driver agents.
        self.inner.add_dependency(owner);
        Ok(())
    }
}
