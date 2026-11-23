use super::*;
use std::net::SocketAddr;

/// Select the manager for publication sockets or subscription sockets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortManagerRole {
    Sender,
    Receiver,
}

/// Choose UDP bind ports and release reservations after sockets close.
///
/// Returning zero lets the operating system choose an ephemeral port. Return
/// the requested port to preserve an explicit channel port. The adapter keeps
/// the requested IP address, IPv6 scope, and flow information unchanged.
///
/// Allocation runs on the conductor. Shutdown can release a port on the closing thread.
pub trait PortManager: Send + 'static {
    fn get_managed_port(&mut self, channel: ChannelRef<'_>, requested: SocketAddr) -> Result<u16, AeronCError>;
    /// Aeron passes the managed bind address, which can still have port zero.
    /// A failed socket setup also releases a successful reservation.
    /// The default does nothing for policies that hold no port reservations.
    fn free_managed_port(&mut self, address: SocketAddr) {}
}

/// A closure can select ports when it holds no reservations that need release.
/// Implement `PortManager` on a named type when ports need explicit release.
impl<F> PortManager for F
where
    F: FnMut(ChannelRef<'_>, SocketAddr) -> Result<u16, AeronCError> + Send + 'static,
{
    #[inline]
    fn get_managed_port(&mut self, channel: ChannelRef<'_>, requested: SocketAddr) -> Result<u16, AeronCError> {
        self(channel, requested)
    }
}

struct Manager<T> {
    table: aeron_port_manager_t,
    value: UnsafeCell<T>,
}

// The local context prevents concurrent registration. Native allocation and
// release callbacks are serialised, including release during driver shutdown.
unsafe impl<T: PortManager> Send for Manager<T> {}
unsafe impl<T: PortManager> Sync for Manager<T> {}

unsafe extern "C" fn get<T: PortManager>(
    state: *mut c_void,
    output: *mut sockaddr_storage,
    channel: *mut aeron_udp_channel_t,
    address: *mut sockaddr_storage,
) -> i32 {
    let Some(requested) = (unsafe { SockAddrRef::from_raw(address) }).to_socket_addr() else {
        return unsafe { supplier_error(AeronCError::with_message(-1, "unsupported bind address family")) };
    };
    match unsafe { &mut *state.cast::<T>() }.get_managed_port(unsafe { ChannelRef::from_raw(channel) }, requested) {
        Ok(port) => {
            unsafe {
                // Copy the native address without reading its padding as values.
                std::ptr::copy(address, output, 1);
                output.cast::<u8>().add(2).cast::<u16>().write_unaligned(port.to_be());
            }
            0
        }
        Err(error) => unsafe { supplier_error(error) },
    }
}

unsafe extern "C" fn free<T: PortManager>(state: *mut c_void, address: *mut sockaddr_storage) {
    if let Some(address) = (unsafe { SockAddrRef::from_raw(address) }).to_socket_addr() {
        unsafe { &mut *state.cast::<T>() }.free_managed_port(address);
    }
}

impl AeronDriverContext {
    /// Replace a native port manager before driver initialisation.
    ///
    /// The context retains the manager until native shutdown. This replaces
    /// the role's native wildcard port range policy.
    pub fn set_port_manager<T: PortManager>(&self, role: PortManagerRole, manager: T) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        let owner = NativeOwned::new(Manager {
            table: aeron_port_manager_t {
                get_managed_port: Some(get::<T>),
                free_managed_port: Some(free::<T>),
                state: std::ptr::null_mut(),
            },
            value: UnsafeCell::new(manager),
        });
        unsafe {
            let raw = owner.as_ptr();
            (*raw).table.state = (*raw).value.get().cast();
            let context = self.get_inner();
            match role {
                PortManagerRole::Sender => (*context).sender_port_manager = &raw mut (*raw).table,
                PortManagerRole::Receiver => (*context).receiver_port_manager = &raw mut (*raw).table,
            }
        }
        self.inner.add_dependency(owner);
        Ok(())
    }
}
