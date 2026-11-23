use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

// AERON_STATUS_MESSAGE_HEADER_EOS_FLAG in protocol/aeron_udp_protocol.h
const STATUS_MESSAGE_EOS_FLAG: u8 = 0x40;

#[derive(Clone, Copy, Default)]
struct Receiver {
    id: i64,
    limit: i64,
    last_ns: i64,
    eos: bool,
}

/// Tagged receiver flow control with a bounded wait before the minimum becomes zero.
///
/// Set the group tag and minimum through the context and omit `fc=` from channels.
/// `WAIT_NS` must be nonnegative. `CAPACITY` receiver slots are reserved at setup.
/// When all are used, unknown receivers are ignored until a slot expires.
/// Each degradation increments counter type 20001. A full group restores the minimum.
pub struct BoundedTaggedFlowControl<const WAIT_NS: i64, const CAPACITY: usize = 64> {
    receivers: Box<[Receiver; CAPACITY]>,
    count: usize,
    timeout: i64,
    tag: i64,
    minimum: usize,
    waiting_since: Option<i64>,
    degraded: bool,
    latest_limit: i64,
    setup_limit: Option<(i64, i64)>,
    tagged_setup: bool,
    retransmit_multiple: usize,
    capacity_warned: bool,
}

/// The conductor reads only this flag. Receiver bookkeeping stays on the sender.
/// Its 64-byte alignment separates the flag and sender state on CPUs with
/// 64-byte cache lines.
#[repr(align(64))]
pub struct TaggedReceiverState {
    required: AtomicBool,
}

impl<const WAIT_NS: i64, const CAPACITY: usize> BoundedTaggedFlowControl<WAIT_NS, CAPACITY> {
    // Logs once per publication so a full table cannot flood the sender thread
    #[cold]
    fn warn_capacity(&mut self, receiver_id: i64) {
        self.capacity_warned = true;
        log::warn!("tagged flow control ignores receiver {receiver_id}, all {CAPACITY} slots are in use");
    }
    fn setup_limit(&mut self, now: i64) -> i64 {
        match self.setup_limit {
            Some((at, _)) if now.wrapping_sub(at) > self.timeout => {
                self.setup_limit = None;
                i64::MAX
            }
            Some((_, limit)) => limit,
            None => i64::MAX,
        }
    }
    fn update_minimum(
        &mut self,
        shared: &TaggedReceiverState,
        now: i64,
        metrics: StrategyMetrics<'_>,
        old_count: usize,
    ) {
        let was_required = self.degraded || old_count >= self.minimum;
        if self.count >= self.minimum {
            self.waiting_since = None;
            self.degraded = false;
        } else if !self.degraded {
            let since = *self.waiting_since.get_or_insert(now);
            if now.wrapping_sub(since) >= WAIT_NS {
                self.degraded = true;
                metrics.increment();
            }
        }
        let required = self.degraded || self.count >= self.minimum;
        if required != was_required {
            shared.required.store(required, Ordering::Release);
        }
    }
    fn limit(&self, receiver_limit: i64, old_limit: i64, idle: bool) -> Position {
        if !self.degraded && self.count < self.minimum {
            return Position::new(old_limit);
        }
        if self.count == 0 {
            return Position::new(if self.degraded {
                old_limit.max(self.latest_limit)
            } else {
                old_limit
            });
        }
        Position::new(if idle {
            receiver_limit
        } else {
            old_limit.max(receiver_limit)
        })
    }
}
impl<const WAIT_NS: i64, const CAPACITY: usize> FlowControl for BoundedTaggedFlowControl<WAIT_NS, CAPACITY> {
    type Shared = TaggedReceiverState;
    const COUNTER_LABEL: Option<&'static CStr> = Some(c"bounded tagged flow control degradations");
    fn new(args: &FlowControlArgs<'_>) -> Result<(Self, TaggedReceiverState), AeronCError> {
        if WAIT_NS < 0
            || CAPACITY == 0
            || args.context.group_min_size < 0
            || args.context.group_min_size as usize > CAPACITY
        {
            return Err(AeronCError::with_message(
                -1,
                "invalid tagged flow-control wait, minimum or capacity",
            ));
        }
        let minimum = args.context.group_min_size as usize;
        // Allocate on the heap directly. Box::new would build the array on the stack first
        let receivers = vec![Receiver::default(); CAPACITY]
            .into_boxed_slice()
            .try_into()
            .map_err(|_| AeronCError::with_message(-1, "tagged flow-control slots have the wrong length"))?;
        Ok((
            Self {
                receivers,
                count: 0,
                timeout: args.context.receiver_timeout_ns.min(i64::MAX as u64) as i64,
                tag: args.context.group_tag,
                minimum,
                waiting_since: None,
                degraded: false,
                latest_limit: 0,
                setup_limit: None,
                tagged_setup: false,
                retransmit_multiple: args.context.multicast_retransmit_window_multiple,
                capacity_warned: false,
            },
            TaggedReceiverState {
                required: AtomicBool::new(minimum == 0),
            },
        ))
    }
    fn on_idle(
        &mut self,
        shared: &TaggedReceiverState,
        now: i64,
        limit: i64,
        _: i64,
        _: bool,
        metrics: StrategyMetrics<'_>,
    ) -> Position {
        if self.count == 0 && self.degraded {
            return Position::new(limit.max(self.latest_limit));
        }
        let old_count = self.count;
        let mut min = i64::MAX;
        let mut count = old_count;
        let receivers = &mut self.receivers[..old_count];
        for index in (0..old_count).rev() {
            let receiver = &receivers[index];
            if receiver.eos || now.wrapping_sub(receiver.last_ns) > self.timeout {
                count -= 1;
                receivers[index] = receivers[count];
            } else {
                min = min.min(receiver.limit);
            }
        }
        self.count = count;
        if count != old_count || (!self.degraded && count < self.minimum) {
            self.update_minimum(shared, now, metrics, old_count);
        }
        if count > 0 {
            min = min.min(self.setup_limit(now));
        }
        self.limit(min, limit, true)
    }
    fn on_status_message(
        &mut self,
        shared: &TaggedReceiverState,
        sm: StatusMessage<'_>,
        _: SockAddrRef<'_>,
        limit: i64,
        initial: i32,
        shift: usize,
        now: i64,
        metrics: StrategyMetrics<'_>,
    ) -> Position {
        let position = sm.position(initial, shift);
        let window = i64::from(sm.receiver_window());
        let received_limit = position.saturating_add(window);
        self.latest_limit = self.latest_limit.max(received_limit);
        let matches = sm.group_tag() == Some(self.tag);
        let eos = sm.flags() & STATUS_MESSAGE_EOS_FLAG != 0;
        let mut existing = false;
        let old_count = self.count;
        let mut setup_limit = self.setup_limit(now);
        let mut min = i64::MAX;
        for receiver in &mut self.receivers[..self.count] {
            if matches && receiver.id == sm.receiver_id() {
                receiver.limit = received_limit;
                receiver.last_ns = now;
                receiver.eos = eos;
                existing = true;
            }
            min = min.min(receiver.limit);
        }
        if !existing
            && !eos
            && matches
            && self.count < CAPACITY
            && (self.count == 0 || received_limit >= min.min(setup_limit).saturating_sub(window))
        {
            self.receivers[self.count] = Receiver {
                id: sm.receiver_id(),
                limit: received_limit,
                last_ns: now,
                eos: false,
            };
            self.count += 1;
            self.setup_limit = None;
            setup_limit = i64::MAX;
            min = min.min(received_limit);
        } else if !existing && matches && self.count == CAPACITY && !self.capacity_warned {
            self.warn_capacity(sm.receiver_id());
        }
        if self.count != old_count || (!self.degraded && self.count < self.minimum) {
            self.update_minimum(shared, now, metrics, old_count);
        }
        if self.minimum == 0 && self.count == 0 {
            return Position::new(limit.max(received_limit));
        }
        self.limit(min.min(setup_limit), limit, false)
    }
    fn has_required_receivers(shared: &TaggedReceiverState) -> bool {
        shared.required.load(Ordering::Acquire)
    }
    fn on_setup(&mut self, _: &TaggedReceiverState, _: SetupMessage<'_>, now: i64, limit: i64, _: usize, _: i64) {
        if self.tagged_setup && self.count > 0 {
            self.setup_limit = Some((now, limit));
        }
        self.tagged_setup = false;
    }
    fn on_trigger_send_setup(&mut self, _: &TaggedReceiverState, sm: StatusMessage<'_>, _: SockAddrRef<'_>, _: i64) {
        self.tagged_setup |= sm.group_tag() == Some(self.tag);
    }
    fn on_error(&mut self, _: &TaggedReceiverState, error: ErrorMessage<'_>, _: SockAddrRef<'_>, _: i64) {
        if error.group_tag() == self.tag {
            for receiver in &mut self.receivers[..self.count] {
                if receiver.id == error.receiver_id() {
                    receiver.eos = true;
                }
            }
        }
    }
    fn max_retransmission_length(
        &mut self,
        _: &TaggedReceiverState,
        offset: usize,
        length: usize,
        term: usize,
        _: usize,
        _: usize,
    ) -> usize {
        unsafe { aeron_flow_control_calculate_retransmission_length(length, term, offset, self.retransmit_multiple) }
    }
}
