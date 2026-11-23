use super::*;

/// A native measurement role. Each role owns a separate tracker.
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

struct ForwardingTracker<T> {
    table: aeron_duty_cycle_tracker_t,
    previous: *mut aeron_duty_cycle_tracker_t,
    value: UnsafeCell<T>,
}

// Each tracker and its predecessor are invoked by the same agent, and the
// context retains both until native shutdown has joined the agents.
unsafe impl<T: DutyCycleTracker> Send for ForwardingTracker<T> {}
unsafe impl<T: DutyCycleTracker> Sync for ForwardingTracker<T> {}

unsafe extern "C" fn update_forwarding<T: DutyCycleTracker>(state: *mut c_void, now: i64) {
    // SAFETY: registration retains this owner and its predecessor in the same
    // context. Registration checks both predecessor callbacks. Only their
    // selected agent invokes these callbacks, in order.
    unsafe {
        let owner = state.cast::<ForwardingTracker<T>>();
        let previous = (*owner).previous;
        ((*previous).update.unwrap_unchecked())((*previous).state, now);
        (&mut *(*owner).value.get()).update(now);
    }
}

unsafe extern "C" fn measure_forwarding<T: DutyCycleTracker>(state: *mut c_void, now: i64) {
    // SAFETY: the predecessor and value remain alive through native shutdown.
    // Registration checks both predecessor callbacks. Both callbacks run
    // synchronously on the same measured agent.
    unsafe {
        let owner = state.cast::<ForwardingTracker<T>>();
        let previous = (*owner).previous;
        ((*previous).measure_and_update.unwrap_unchecked())((*previous).state, now);
        (&mut *(*owner).value.get()).measure_and_update(now);
    }
}

unsafe fn tracker_slot(
    context: *mut aeron_driver_context_t,
    role: DutyCycleRole,
) -> *mut *mut aeron_duty_cycle_tracker_t {
    unsafe {
        match role {
            DutyCycleRole::Conductor => &mut (*context).conductor_duty_cycle_tracker,
            DutyCycleRole::Sender => &mut (*context).sender_duty_cycle_tracker,
            DutyCycleRole::Receiver => &mut (*context).receiver_duty_cycle_tracker,
            DutyCycleRole::NameResolver => &mut (*context).name_resolver_time_tracker,
        }
    }
}

impl AeronDriverContext {
    /// Replace a native stall tracker before driver initialisation.
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
        let owner = NativeOwned::new(Tracker {
            table: aeron_duty_cycle_tracker_t {
                update: Some(update::<T>),
                measure_and_update: Some(measure::<T>),
                state: std::ptr::null_mut(),
            },
            value: UnsafeCell::new(tracker),
        });
        unsafe {
            let raw = owner.as_ptr();
            (*raw).table.state = (*raw).value.get().cast();
            *tracker_slot(self.get_inner(), role) = &raw mut (*raw).table;
        }
        // C stores this pointer without freeing it. Resource dependencies drop
        // after native close, which follows the join of all driver agents.
        self.inner.add_dependency(owner);
        Ok(())
    }

    /// Add a tracker after the role's current tracker before driver initialisation.
    ///
    /// Both receive each timestamp, with the existing tracker called first.
    /// When the existing tracker is Aeron's default, its max-cycle and stall
    /// counters continue to update. Repeated additions run in installation order;
    /// `set_duty_cycle_tracker` replaces the entire chain.
    /// Chaining adds one native indirect call per event. It adds no heap
    /// allocation, lock, or atomic read-modify-write to that event.
    pub fn add_duty_cycle_tracker<T: DutyCycleTracker>(
        &self,
        role: DutyCycleRole,
        tracker: T,
    ) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        unsafe {
            let slot = tracker_slot(self.get_inner(), role);
            let previous = *slot;
            if previous.is_null() || (*previous).update.is_none() || (*previous).measure_and_update.is_none() {
                return Err(AeronCError::with_message(
                    -1,
                    "current duty cycle tracker is incomplete",
                ));
            }
            let owner = NativeOwned::new(ForwardingTracker {
                table: aeron_duty_cycle_tracker_t {
                    update: Some(update_forwarding::<T>),
                    measure_and_update: Some(measure_forwarding::<T>),
                    state: std::ptr::null_mut(),
                },
                previous,
                value: UnsafeCell::new(tracker),
            });
            let raw = owner.as_ptr();
            (*raw).table.state = raw.cast();
            *slot = &raw mut (*raw).table;
            self.inner.add_dependency(owner);
        }
        Ok(())
    }
}
