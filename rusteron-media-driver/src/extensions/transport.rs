use super::*;

/// Native transport tables are separate for data traffic and resolver traffic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportRole {
    /// Both sender and receiver data paths.
    Data,
    /// The native driver name resolver's UDP transport.
    Conductor,
}

/// An owned native transport table and the Rust resources its callbacks use.
///
/// Full transport replacement exposes sockets, pollers, and raw message buffers.
/// Use packet interceptors for safe packet inspection. Start a transport
/// replacement with [`Self::udp`] and delegate unchanged operations to Aeron.
pub struct NativeTransportBindings {
    table: Box<aeron_udp_channel_transport_bindings_t>,
    _keepalive: Box<dyn std::any::Any + Send + Sync>,
}

// The table is immutable after construction. udp uses native callbacks and
// static metadata. from_callbacks requires the caller to establish the same
// pointer lifetime and concurrency contracts for custom callbacks.
unsafe impl Send for NativeTransportBindings {}
unsafe impl Sync for NativeTransportBindings {}

impl NativeTransportBindings {
    /// Aeron's standard UDP transport. This does not load or mutate global symbols.
    pub fn udp() -> Self {
        Self {
            table: Box::new(aeron_udp_channel_transport_bindings_t {
                init_func: Some(aeron_udp_channel_transport_init),
                reconnect_func: Some(aeron_udp_channel_transport_reconnect),
                close_func: Some(aeron_udp_channel_transport_close),
                recvmmsg_func: Some(aeron_udp_channel_transport_recvmmsg),
                send_func: Some(aeron_udp_channel_transport_send),
                get_so_rcvbuf_func: Some(aeron_udp_channel_transport_get_so_rcvbuf),
                bind_addr_and_port_func: Some(aeron_udp_channel_transport_bind_addr_and_port),
                poller_init_func: Some(aeron_udp_transport_poller_init),
                poller_close_func: Some(aeron_udp_transport_poller_close),
                poller_add_func: Some(aeron_udp_transport_poller_add),
                poller_remove_func: Some(aeron_udp_transport_poller_remove),
                poller_poll_func: Some(aeron_udp_transport_poller_poll),
                meta_info: aeron_udp_channel_transport_bindings_stct_meta_info_fields {
                    name: c"rusteron-udp".as_ptr(),
                    type_: c"media".as_ptr(),
                    source_symbol: std::ptr::null(),
                },
            }),
            _keepalive: Box::new(()),
        }
    }

    /// Copy the table so selected callbacks can be replaced. The copy does not
    /// retain metadata or state. Keep their owners alive when reusing the copy.
    pub fn callbacks(&self) -> aeron_udp_channel_transport_bindings_t {
        *self.table
    }

    /// Own a complete native table and retain its resources until context close.
    ///
    /// # Safety
    /// Every callback must obey the vendored Aeron transport and poller ABI.
    /// Callbacks must respect buffer lengths, initialize all native outputs,
    /// preserve socket ownership, and release resources on failed setup.
    /// Sender and receiver callbacks can run concurrently. Mutable state must
    /// belong to one agent or have synchronization. All pointers and callback
    /// code must remain valid through context shutdown. Retain owned resources
    /// in `keepalive`. Metadata strings must be valid C strings for that period.
    /// Receive operations must deliver through the supplied receive callback
    /// so Aeron's incoming interceptors and endpoint processing run. Native UDP
    /// pollers own `poller.bindings_clientd`; preserve that storage when
    /// delegating to their callbacks.
    /// Callbacks must not unwind across C.
    pub unsafe fn from_callbacks<S: Send + Sync + 'static>(
        table: aeron_udp_channel_transport_bindings_t,
        keepalive: S,
    ) -> Result<Self, AeronCError> {
        if table.init_func.is_none()
            || table.reconnect_func.is_none()
            || table.close_func.is_none()
            || table.recvmmsg_func.is_none()
            || table.send_func.is_none()
            || table.get_so_rcvbuf_func.is_none()
            || table.bind_addr_and_port_func.is_none()
            || table.poller_init_func.is_none()
            || table.poller_close_func.is_none()
            || table.poller_add_func.is_none()
            || table.poller_remove_func.is_none()
            || table.poller_poll_func.is_none()
            || table.meta_info.name.is_null()
            || table.meta_info.type_.is_null()
        {
            return Err(AeronCError::with_message(
                -1,
                "native transport bindings require every callback and metadata name and type",
            ));
        }
        Ok(Self {
            table: Box::new(table),
            _keepalive: Box::new(keepalive),
        })
    }
}

impl AeronDriverContext {
    /// Install an owned table before driver initialization.
    ///
    /// The context retains replaced tables until shutdown. Unchanged callbacks
    /// remain direct native calls without an extra Rust dispatch.
    /// Data interceptors wrap the installed table regardless of setter order.
    /// Flow control still runs in Aeron's publication logic. Cloned contexts
    /// can retain the table and its keepalive after the driver has joined.
    pub fn set_native_transport_bindings(
        &self,
        role: TransportRole,
        mut bindings: NativeTransportBindings,
    ) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        unsafe {
            let context = self.get_inner();
            match role {
                TransportRole::Data => (*context).udp_channel_transport_bindings = &mut *bindings.table,
                TransportRole::Conductor => (*context).conductor_udp_channel_transport_bindings = &mut *bindings.table,
            }
        }
        self.inner.add_dependency(bindings);
        Ok(())
    }
}
