use super::*;

pub use crate::bindings::aeron_udp_channel_transport_affinity_t as TransportAffinity;

/// A synchronous outgoing batch. It can only be forwarded once.
/// Borrowed datagrams are read-only because C can pass aliased payload buffers.
pub struct OutgoingPacket<'a> {
    transport: *mut aeron_udp_channel_transport_t,
    address: *mut sockaddr_storage,
    iov: *mut iovec,
    length: usize,
    bytes_sent: *mut i64,
    _life: PhantomData<&'a mut ()>,
}
impl OutgoingPacket<'_> {
    pub fn address(&self) -> SockAddrRef<'_> {
        unsafe { SockAddrRef::from_raw(self.address) }
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    #[inline]
    pub fn datagram(&self, index: usize) -> Option<&[u8]> {
        if index >= self.length {
            return None;
        }
        unsafe {
            let iov = &*self.iov.add(index);
            Some(if iov.iov_len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(iov.iov_base.cast(), iov.iov_len)
            })
        }
    }
    /// Borrow every datagram in send order without allocating or copying payloads.
    /// Empty datagrams remain in the iterator.
    ///
    /// Forwarding consumes the packet, so a live iterator cannot survive it.
    ///
    /// ```compile_fail
    /// use rusteron_media_driver::extensions::{OutgoingNext, OutgoingPacket};
    /// fn forward<'a>(next: OutgoingNext<'a>, packet: OutgoingPacket<'a>) {
    ///     let mut datagrams = packet.datagrams();
    ///     next.send(packet);
    ///     let _ = datagrams.next();
    /// }
    /// ```
    #[inline]
    pub fn datagrams(&self) -> impl DoubleEndedIterator<Item = &[u8]> + ExactSizeIterator + '_ {
        (0..self.length).map(|index| self.datagram(index).unwrap())
    }
    /// Report a deliberate drop as a successful send, so Aeron can retransmit it.
    pub fn drop_as_sent(self) -> i32 {
        let mut bytes = 0i64;
        for i in 0..self.length {
            bytes = bytes.saturating_add(unsafe { (*self.iov.add(i)).iov_len } as i64);
        }
        unsafe { *self.bytes_sent = (*self.bytes_sent).saturating_add(bytes) };
        self.length as i32
    }
}

/// The next outgoing callback. Consuming both values prevents repeated delegation
/// and prevents forwarding while a datagram borrow is live.
pub struct OutgoingNext<'a> {
    raw: *mut aeron_udp_channel_outgoing_interceptor_t,
    _life: PhantomData<&'a mut ()>,
}
impl<'a> OutgoingNext<'a> {
    #[inline]
    pub fn send(self, packet: OutgoingPacket<'a>) -> i32 {
        unsafe {
            ((*self.raw).outgoing_send_func.unwrap_unchecked())(
                (*self.raw).interceptor_state,
                (*self.raw).next_interceptor,
                packet.transport,
                packet.address,
                packet.iov,
                packet.length,
                packet.bytes_sent,
            )
        }
    }
}

pub struct IncomingPacket<'a> {
    transport: *mut aeron_udp_channel_transport_t,
    receiver: *mut c_void,
    endpoint: *mut c_void,
    destination: *mut c_void,
    buffer: *mut u8,
    length: usize,
    address: *mut sockaddr_storage,
    timestamp: *mut timespec,
    _life: PhantomData<&'a mut ()>,
}
impl IncomingPacket<'_> {
    pub fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buffer, self.length) }
    }
    pub fn address(&self) -> SockAddrRef<'_> {
        unsafe { SockAddrRef::from_raw(self.address) }
    }
    /// Kernel receive time when Aeron received a timestamp control message.
    pub fn media_timestamp(&self) -> Option<(i64, i64)> {
        unsafe {
            self.timestamp
                .as_ref()
                .map(|time| (time.tv_sec as i64, time.tv_nsec as i64))
        }
    }
}
pub struct IncomingNext<'a> {
    raw: *mut aeron_udp_channel_incoming_interceptor_t,
    _life: PhantomData<&'a mut ()>,
}
impl<'a> IncomingNext<'a> {
    #[inline]
    pub fn receive(self, packet: IncomingPacket<'a>) {
        unsafe {
            ((*self.raw).incoming_func.unwrap_unchecked())(
                (*self.raw).interceptor_state,
                (*self.raw).next_interceptor,
                packet.transport,
                packet.receiver,
                packet.endpoint,
                packet.destination,
                packet.buffer,
                packet.length,
                packet.address,
                packet.timestamp,
            )
        }
    }
}

/// Per-agent outgoing interception. State moves from initialization to its agent
/// and returns to the closing thread after all agents have joined.
/// Implement `Default` for construction without context, or `InterceptorFactory`
/// when construction needs the frozen context or agent affinity.
/// Implement `inspect` to observe a batch and forward it unchanged. Override
/// `send` to decide whether and when to forward within this callback.
pub trait OutgoingInterceptor: Sized + Send + 'static {
    const NOTIFICATIONS: bool = false;
    fn on_notification(&mut self, notification: InterceptorNotification<'_>) -> i32 {
        0
    }

    /// Observe a batch before the default `send` forwards it. An override of
    /// `send` controls whether this method runs.
    #[inline]
    fn inspect(&mut self, packet: &OutgoingPacket<'_>) {}

    #[inline]
    fn send<'a>(&mut self, next: OutgoingNext<'a>, packet: OutgoingPacket<'a>) -> i32 {
        self.inspect(&packet);
        next.send(packet)
    }
}
/// Per-agent incoming interception. Construction follows `OutgoingInterceptor`.
/// Implement `inspect` to observe a packet and forward it unchanged. Override
/// `on_datagram` to control forwarding.
pub trait IncomingInterceptor: Sized + Send + 'static {
    const NOTIFICATIONS: bool = false;
    fn on_notification(&mut self, notification: InterceptorNotification<'_>) -> i32 {
        0
    }

    /// Observe a packet before the default `on_datagram` forwards it. An override
    /// of `on_datagram` controls whether this method runs.
    #[inline]
    fn inspect(&mut self, packet: &IncomingPacket<'_>) {}

    #[inline]
    fn on_datagram<'a>(&mut self, next: IncomingNext<'a>, packet: IncomingPacket<'a>) {
        self.inspect(&packet);
        next.receive(packet);
    }
}

/// Construct an interceptor once for each native agent that uses it.
///
/// Types that implement `Default` receive this implementation automatically.
/// Implement this trait instead of `Default` for fallible or context-dependent
/// construction. The constructor does not run during packet processing.
pub trait InterceptorFactory: Sized {
    fn new(context: StrategyContext, affinity: TransportAffinity) -> Result<Self, AeronCError>;
}

impl<T: Default> InterceptorFactory for T {
    fn new(_: StrategyContext, _: TransportAffinity) -> Result<Self, AeronCError> {
        Ok(Self::default())
    }
}

unsafe extern "C" fn outgoing_init<T: OutgoingInterceptor + InterceptorFactory>(
    out: *mut *mut c_void,
    ctx: *mut aeron_driver_context_t,
    affinity: TransportAffinity,
) -> i32 {
    unsafe { *out = std::ptr::null_mut() };
    match T::new(unsafe { StrategyContext::from_raw(ctx) }, affinity) {
        Ok(state) => {
            unsafe { *out = Box::into_raw(Box::new(state)).cast() };
            0
        }
        Err(error) => unsafe { supplier_error(error) },
    }
}
unsafe extern "C" fn incoming_init<T: IncomingInterceptor + InterceptorFactory>(
    out: *mut *mut c_void,
    ctx: *mut aeron_driver_context_t,
    affinity: TransportAffinity,
) -> i32 {
    unsafe { *out = std::ptr::null_mut() };
    match T::new(unsafe { StrategyContext::from_raw(ctx) }, affinity) {
        Ok(state) => {
            unsafe { *out = Box::into_raw(Box::new(state)).cast() };
            0
        }
        Err(error) => unsafe { supplier_error(error) },
    }
}
unsafe extern "C" fn close<T>(raw: *mut c_void) -> i32 {
    unsafe { drop(Box::from_raw(raw.cast::<T>())) };
    0
}
unsafe extern "C" fn send<T: OutgoingInterceptor>(
    raw: *mut c_void,
    next: *mut aeron_udp_channel_outgoing_interceptor_t,
    transport: *mut aeron_udp_channel_transport_t,
    address: *mut sockaddr_storage,
    iov: *mut iovec,
    length: usize,
    bytes_sent: *mut i64,
) -> i32 {
    unsafe {
        (&mut *raw.cast::<T>()).send(
            OutgoingNext {
                raw: next,
                _life: PhantomData,
            },
            OutgoingPacket {
                transport,
                address,
                iov,
                length,
                bytes_sent,
                _life: PhantomData,
            },
        )
    }
}
unsafe extern "C" fn receive<T: IncomingInterceptor>(
    raw: *mut c_void,
    next: *mut aeron_udp_channel_incoming_interceptor_t,
    transport: *mut aeron_udp_channel_transport_t,
    receiver: *mut c_void,
    endpoint: *mut c_void,
    destination: *mut c_void,
    buffer: *mut u8,
    length: usize,
    address: *mut sockaddr_storage,
    timestamp: *mut timespec,
) {
    unsafe {
        (&mut *raw.cast::<T>()).on_datagram(
            IncomingNext {
                raw: next,
                _life: PhantomData,
            },
            IncomingPacket {
                transport,
                receiver,
                endpoint,
                destination,
                buffer,
                length,
                address,
                timestamp,
                _life: PhantomData,
            },
        )
    }
}

impl AeronDriverContext {
    /// Prepend a plaintext interceptor to the existing outgoing chain. The latest
    /// addition runs first. Existing environment-loaded bindings keep their order.
    pub fn add_outgoing_interceptor<T: OutgoingInterceptor + InterceptorFactory>(&self) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        let mut table = aeron_udp_channel_interceptor_bindings_t::default();
        table.outgoing_init_func = Some(outgoing_init::<T>);
        table.outgoing_send_func = Some(send::<T>);
        // Even a type without Drop owns a Box that the close callback must free.
        table.outgoing_close_func = Some(close::<T>);
        if T::NOTIFICATIONS {
            table.outgoing_transport_notification_func = Some(out_transport::<T>);
            table.outgoing_publication_notification_func = Some(out_publication::<T>);
            table.outgoing_image_notification_func = Some(out_image::<T>);
        }
        unsafe { self.install(table, true) }
    }
    /// Prepend an interceptor to the existing incoming chain.
    pub fn add_incoming_interceptor<T: IncomingInterceptor + InterceptorFactory>(&self) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        let mut table = aeron_udp_channel_interceptor_bindings_t::default();
        table.incoming_init_func = Some(incoming_init::<T>);
        table.incoming_func = Some(receive::<T>);
        table.incoming_close_func = Some(close::<T>);
        if T::NOTIFICATIONS {
            table.incoming_transport_notification_func = Some(in_transport::<T>);
            table.incoming_publication_notification_func = Some(in_publication::<T>);
            table.incoming_image_notification_func = Some(in_image::<T>);
        }
        unsafe { self.install(table, false) }
    }
    unsafe fn install(
        &self,
        mut table: aeron_udp_channel_interceptor_bindings_t,
        outgoing: bool,
    ) -> Result<(), AeronCError> {
        let ctx = self.get_inner();
        table.meta_info.name = c"rusteron".as_ptr();
        table.meta_info.type_ = c"interceptor".as_ptr();
        table.meta_info.next_interceptor_bindings = unsafe {
            if outgoing {
                aeron_driver_context_get_udp_channel_outgoing_interceptors(ctx)
            } else {
                aeron_driver_context_get_udp_channel_incoming_interceptors(ctx)
            }
        };
        let mut raw: *mut c_void = std::ptr::null_mut();
        check(unsafe { aeron_alloc(&mut raw, std::mem::size_of_val(&table)) })?;
        unsafe { raw.cast::<aeron_udp_channel_interceptor_bindings_t>().write(table) };
        let result = unsafe {
            if outgoing {
                aeron_driver_context_set_udp_channel_outgoing_interceptors(ctx, raw.cast())
            } else {
                aeron_driver_context_set_udp_channel_incoming_interceptors(ctx, raw.cast())
            }
        };
        if result < 0 {
            unsafe { aeron_free(raw) };
        }
        check(result)
    }
}

/// Repeatable packet loss for tests. Every Nth batch is dropped after a seeded
/// offset. Each agent has its own counter. N=0 disables loss.
pub struct SeededLoss<const N: u64, const SEED: u64 = 1> {
    count: u64,
}
impl<const N: u64, const SEED: u64> Default for SeededLoss<N, SEED> {
    fn default() -> Self {
        Self { count: SEED }
    }
}
impl<const N: u64, const SEED: u64> OutgoingInterceptor for SeededLoss<N, SEED> {
    fn send<'a>(&mut self, next: OutgoingNext<'a>, packet: OutgoingPacket<'a>) -> i32 {
        self.count = self.count.wrapping_add(1);
        if N != 0 && self.count.is_multiple_of(N) {
            packet.drop_as_sent()
        } else {
            next.send(packet)
        }
    }
}

/// A transport identity valid during a notification. It grants no socket mutation.
#[derive(Clone, Copy)]
pub struct TransportRef<'a> {
    raw: *mut aeron_udp_channel_transport_t,
    _life: PhantomData<&'a ()>,
}
impl TransportRef<'_> {
    /// Compare identities while their native transports remain alive. C can reuse an address after close.
    pub fn identity(self) -> usize {
        self.raw as usize
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotificationChange {
    Added,
    Removed,
}
impl NotificationChange {
    fn from_native(change: aeron_udp_channel_interceptor_notification_type_t) -> Self {
        match change {
            aeron_udp_channel_interceptor_notification_type_t::AERON_UDP_CHANNEL_INTERCEPTOR_ADD_NOTIFICATION => {
                Self::Added
            }
            aeron_udp_channel_interceptor_notification_type_t::AERON_UDP_CHANNEL_INTERCEPTOR_REMOVE_NOTIFICATION => {
                Self::Removed
            }
        }
    }
}
pub enum NotificationResource<'a> {
    Transport(ChannelRef<'a>),
    Publication { session_id: i32, stream_id: i32 },
    Image { session_id: i32, stream_id: i32 },
}
pub struct InterceptorNotification<'a> {
    pub transport: TransportRef<'a>,
    pub change: NotificationChange,
    pub resource: NotificationResource<'a>,
}
macro_rules! notifications {
    ($trait:ident, $transport:ident, $publication:ident, $image:ident) => {
        unsafe extern "C" fn $transport<T: $trait>(
            state: *mut c_void,
            transport: *mut aeron_udp_channel_transport_t,
            channel: *const aeron_udp_channel_t,
            dispatcher: *mut aeron_data_packet_dispatcher_t,
            change: aeron_udp_channel_interceptor_notification_type_t,
        ) -> i32 {
            unsafe {
                (&mut *state.cast::<T>()).on_notification(InterceptorNotification {
                    transport: TransportRef {
                        raw: transport,
                        _life: PhantomData,
                    },
                    change: NotificationChange::from_native(change),
                    resource: NotificationResource::Transport(ChannelRef::from_raw(channel)),
                })
            }
        }
        unsafe extern "C" fn $publication<T: $trait>(
            state: *mut c_void,
            transport: *mut aeron_udp_channel_transport_t,
            publication: *mut aeron_network_publication_t,
            change: aeron_udp_channel_interceptor_notification_type_t,
        ) -> i32 {
            unsafe {
                (&mut *state.cast::<T>()).on_notification(InterceptorNotification {
                    transport: TransportRef {
                        raw: transport,
                        _life: PhantomData,
                    },
                    change: NotificationChange::from_native(change),
                    resource: NotificationResource::Publication {
                        session_id: (*publication).session_id,
                        stream_id: (*publication).stream_id,
                    },
                })
            }
        }
        unsafe extern "C" fn $image<T: $trait>(
            state: *mut c_void,
            transport: *mut aeron_udp_channel_transport_t,
            image: *mut aeron_publication_image_t,
            change: aeron_udp_channel_interceptor_notification_type_t,
        ) -> i32 {
            unsafe {
                (&mut *state.cast::<T>()).on_notification(InterceptorNotification {
                    transport: TransportRef {
                        raw: transport,
                        _life: PhantomData,
                    },
                    change: NotificationChange::from_native(change),
                    resource: NotificationResource::Image {
                        session_id: (*image).session_id,
                        stream_id: (*image).stream_id,
                    },
                })
            }
        }
    };
}
notifications!(OutgoingInterceptor, out_transport, out_publication, out_image);
notifications!(IncomingInterceptor, in_transport, in_publication, in_image);

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Sent {
        calls: usize,
        datagrams: Vec<Vec<u8>>,
    }

    unsafe extern "C" fn record_send(
        state: *mut c_void,
        _: *mut aeron_udp_channel_outgoing_interceptor_t,
        _: *mut aeron_udp_channel_transport_t,
        _: *mut sockaddr_storage,
        vectors: *mut iovec,
        length: usize,
        bytes_sent: *mut i64,
    ) -> i32 {
        let state = unsafe { &mut *state.cast::<Sent>() };
        state.calls += 1;
        for vector in unsafe { std::slice::from_raw_parts(vectors, length) } {
            let data = if vector.iov_len == 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(vector.iov_base.cast::<u8>(), vector.iov_len) }
            };
            state.datagrams.push(data.to_vec());
            unsafe { *bytes_sent += vector.iov_len as i64 };
        }
        length as i32
    }

    fn packet<'a>(vectors: &'a mut [iovec], bytes_sent: &'a mut i64) -> OutgoingPacket<'a> {
        OutgoingPacket {
            transport: std::ptr::null_mut(),
            address: std::ptr::null_mut(),
            iov: vectors.as_mut_ptr(),
            length: vectors.len(),
            bytes_sent,
            _life: PhantomData,
        }
    }

    #[derive(Default)]
    struct BatchObserver {
        calls: usize,
        bytes: usize,
    }
    impl OutgoingInterceptor for BatchObserver {
        fn inspect(&mut self, packet: &OutgoingPacket<'_>) {
            self.calls += 1;
            self.bytes += packet.datagrams().map(<[u8]>::len).sum::<usize>();
        }
    }

    #[test]
    fn datagram_iteration_preserves_empty_entries_and_borrows_payloads_without_allocation() {
        let mut vectors = [
            iovec {
                iov_base: c"abc".as_ptr().cast_mut().cast(),
                iov_len: 3,
            },
            iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            },
            iovec {
                iov_base: c"defgh".as_ptr().cast_mut().cast(),
                iov_len: 5,
            },
        ];
        let first_pointer = vectors[0].iov_base.cast::<u8>();
        let mut bytes = 0;
        let batch = packet(&mut vectors, &mut bytes);
        crate::test_alloc::assert_no_heap_allocation(|| {
            let mut datagrams = batch.datagrams();
            assert_eq!(datagrams.len(), 3);
            let first = datagrams.next().unwrap();
            assert_eq!(first, b"abc");
            assert_eq!(first.as_ptr(), first_pointer);
            assert_eq!(datagrams.next_back(), Some(b"defgh".as_slice()));
            assert_eq!(datagrams.len(), 1);
            assert_eq!(datagrams.next(), Some(b"".as_slice()));
            assert_eq!(datagrams.len(), 0);
            assert_eq!(datagrams.next(), None);
            assert_eq!(datagrams.next_back(), None);
        });
        let empty = packet(&mut [], &mut bytes);
        assert_eq!(empty.datagrams().len(), 0);
        assert_eq!(empty.datagrams().next(), None);
    }

    #[test]
    fn outgoing_inspection_forwards_native_failures_without_allocating() {
        unsafe extern "C" fn fail(
            calls: *mut c_void,
            _: *mut aeron_udp_channel_outgoing_interceptor_t,
            _: *mut aeron_udp_channel_transport_t,
            _: *mut sockaddr_storage,
            _: *mut iovec,
            length: usize,
            bytes_sent: *mut i64,
        ) -> i32 {
            unsafe { *calls.cast::<usize>() += 1 };
            assert_eq!(length, 1);
            assert_eq!(unsafe { *bytes_sent }, 7);
            -7
        }
        let mut calls = 0usize;
        let mut next = aeron_udp_channel_outgoing_interceptor_t {
            interceptor_state: (&raw mut calls).cast(),
            outgoing_send_func: Some(fail),
            ..Default::default()
        };
        let mut vectors = [iovec {
            iov_base: c"data".as_ptr().cast_mut().cast(),
            iov_len: 4,
        }];
        let mut bytes = 7;
        let mut observer = BatchObserver::default();
        crate::test_alloc::assert_no_heap_allocation(|| {
            assert_eq!(
                observer.send(
                    OutgoingNext {
                        raw: &mut next,
                        _life: PhantomData
                    },
                    packet(&mut vectors, &mut bytes),
                ),
                -7
            );
        });
        assert_eq!(observer.calls, 1);
        assert_eq!(observer.bytes, 4);
        assert_eq!(calls, 1);
        assert_eq!(bytes, 7);
    }

    #[test]
    fn outgoing_delegation_keeps_datagrams_and_byte_accounting() {
        let mut vectors = [
            iovec {
                iov_base: c"abc".as_ptr().cast_mut().cast(),
                iov_len: 3,
            },
            iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            },
            iovec {
                iov_base: c"defgh".as_ptr().cast_mut().cast(),
                iov_len: 5,
            },
        ];
        let mut sent = Sent::default();
        let mut next = aeron_udp_channel_outgoing_interceptor_t {
            interceptor_state: (&raw mut sent).cast(),
            outgoing_send_func: Some(record_send),
            ..Default::default()
        };
        let mut bytes = 7;
        let packet = packet(&mut vectors, &mut bytes);
        assert_eq!(packet.len(), 3);
        assert!(!packet.is_empty());
        assert_eq!(packet.datagram(0), Some(b"abc".as_slice()));
        assert_eq!(packet.datagram(1), Some(b"".as_slice()));
        assert_eq!(packet.datagram(2), Some(b"defgh".as_slice()));
        assert_eq!(packet.datagram(3), None);
        let mut observer = BatchObserver::default();
        assert_eq!(
            observer.send(
                OutgoingNext {
                    raw: &mut next,
                    _life: PhantomData
                },
                packet
            ),
            3
        );
        assert_eq!(observer.calls, 1);
        assert_eq!(observer.bytes, 8);
        assert_eq!(sent.calls, 1);
        assert_eq!(sent.datagrams, [b"abc".to_vec(), vec![], b"defgh".to_vec()]);
        assert_eq!(bytes, 15);
    }

    #[test]
    fn dropped_batches_report_all_bytes_and_saturate_the_total() {
        let mut vectors = [iovec {
            iov_base: c"data".as_ptr().cast_mut().cast(),
            iov_len: 4,
        }];
        let mut bytes = i64::MAX - 2;
        assert_eq!(packet(&mut vectors, &mut bytes).drop_as_sent(), 1);
        assert_eq!(bytes, i64::MAX);
        let mut bytes = 9;
        assert_eq!(packet(&mut [], &mut bytes).drop_as_sent(), 0);
        assert_eq!(bytes, 9);
    }

    #[test]
    fn seeded_loss_drops_exact_batches_and_zero_disables_loss() {
        let mut vectors = [iovec {
            iov_base: c"data".as_ptr().cast_mut().cast(),
            iov_len: 4,
        }];
        let mut sent = Sent::default();
        let mut next = aeron_udp_channel_outgoing_interceptor_t {
            interceptor_state: (&raw mut sent).cast(),
            outgoing_send_func: Some(record_send),
            ..Default::default()
        };
        let mut loss = SeededLoss::<3, 1>::default();
        for expected_calls in [1, 1, 2, 3, 3, 4] {
            let mut bytes = 0;
            let result = loss.send(
                OutgoingNext {
                    raw: &mut next,
                    _life: PhantomData,
                },
                packet(&mut vectors, &mut bytes),
            );
            assert_eq!(result, 1);
            assert_eq!(bytes, 4);
            assert_eq!(sent.calls, expected_calls);
        }
        let mut no_loss = SeededLoss::<0, { u64::MAX }>::default();
        for expected_calls in 5..8 {
            let mut bytes = 0;
            assert_eq!(
                no_loss.send(
                    OutgoingNext {
                        raw: &mut next,
                        _life: PhantomData
                    },
                    packet(&mut vectors, &mut bytes)
                ),
                1
            );
            assert_eq!(sent.calls, expected_calls);
        }
    }

    #[test]
    fn incoming_delegation_preserves_bytes_and_timestamp() {
        #[derive(Default)]
        struct Observer {
            calls: usize,
        }
        impl IncomingInterceptor for Observer {
            fn inspect(&mut self, packet: &IncomingPacket<'_>) {
                self.calls += 1;
                assert_eq!(packet.bytes(), b"packet");
                assert_eq!(packet.media_timestamp(), Some((17, 23)));
            }
        }
        unsafe extern "C" fn record(
            state: *mut c_void,
            _: *mut aeron_udp_channel_incoming_interceptor_t,
            _: *mut aeron_udp_channel_transport_t,
            _: *mut c_void,
            _: *mut c_void,
            _: *mut c_void,
            buffer: *mut u8,
            length: usize,
            _: *mut sockaddr_storage,
            timestamp: *mut timespec,
        ) {
            let calls = unsafe { &mut *state.cast::<usize>() };
            *calls += 1;
            assert_eq!(unsafe { std::slice::from_raw_parts(buffer, length) }, b"packet");
            assert_eq!(unsafe { (*timestamp).tv_sec }, 17);
            assert_eq!(unsafe { (*timestamp).tv_nsec }, 23);
        }
        let mut calls = 0usize;
        let mut next = aeron_udp_channel_incoming_interceptor_t {
            interceptor_state: (&raw mut calls).cast(),
            incoming_func: Some(record),
            ..Default::default()
        };
        let mut buffer = *b"packet";
        let mut timestamp = timespec {
            tv_sec: 17,
            tv_nsec: 23,
        };
        let mut packet = IncomingPacket {
            transport: std::ptr::null_mut(),
            receiver: std::ptr::null_mut(),
            endpoint: std::ptr::null_mut(),
            destination: std::ptr::null_mut(),
            buffer: buffer.as_mut_ptr(),
            length: buffer.len(),
            address: std::ptr::null_mut(),
            timestamp: &mut timestamp,
            _life: PhantomData,
        };
        assert_eq!(packet.bytes(), b"packet");
        assert_eq!(packet.media_timestamp(), Some((17, 23)));
        packet.timestamp = std::ptr::null_mut();
        assert_eq!(packet.media_timestamp(), None);
        packet.timestamp = &mut timestamp;
        let mut observer = Observer::default();
        crate::test_alloc::assert_no_heap_allocation(|| {
            observer.on_datagram(
                IncomingNext {
                    raw: &mut next,
                    _life: PhantomData,
                },
                packet,
            );
        });
        assert_eq!(observer.calls, 1);
        assert_eq!(calls, 1);
        assert_eq!(&buffer, b"packet");
    }
}
