use super::*;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};

/// A borrowed native socket address. Conversion is optional and copies the fields.
#[derive(Clone, Copy)]
pub struct SockAddrRef<'a> {
    pub(super) raw: *const sockaddr_storage,
    pub(super) _life: PhantomData<&'a sockaddr_storage>,
}
// The bindings do not export the socket families, and AF_INET6 differs by platform
pub(super) const AF_INET: u32 = 2;
pub(super) const AF_INET6: u32 = if cfg!(windows) {
    23
} else if cfg!(target_vendor = "apple") {
    30
} else {
    10
};

impl SockAddrRef<'_> {
    /// # Safety
    /// `raw` must point to an initialised address that outlives the returned view
    pub(super) unsafe fn from_raw(raw: *const sockaddr_storage) -> Self {
        Self {
            raw,
            _life: PhantomData,
        }
    }
    pub fn to_socket_addr(self) -> Option<SocketAddr> {
        // Only initialised address fields are read. Native padding is never read.
        unsafe {
            let family = (*self.raw).ss_family as u32;
            let raw = self.raw.cast::<u8>();
            if family == AF_INET {
                let port = u16::from_be(raw.add(2).cast::<u16>().read_unaligned());
                let ip = raw.add(4).cast::<[u8; 4]>().read_unaligned();
                Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(ip)), port))
            } else if family == AF_INET6 {
                let port = u16::from_be(raw.add(2).cast::<u16>().read_unaligned());
                let ip = raw.add(8).cast::<[u8; 16]>().read_unaligned();
                Some(SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(ip),
                    port,
                    u32::from_be(raw.add(4).cast::<u32>().read_unaligned()),
                    raw.add(24).cast::<u32>().read_unaligned(),
                )))
            } else {
                None
            }
        }
    }
}

macro_rules! frame_view {
    ($name:ident, $size:expr, $session:expr) => {
        #[derive(Clone, Copy)]
        pub struct $name<'a> {
            bytes: &'a [u8],
        }
        impl<'a> $name<'a> {
            /// Read a packet whose fixed header is present. The declared frame length
            /// never determines the size of the borrowed memory.
            pub fn from_bytes(bytes: &'a [u8]) -> Option<Self> {
                (bytes.len() >= $size).then_some(Self { bytes })
            }
            pub(super) unsafe fn from_raw(raw: *const u8, length: usize) -> Self {
                debug_assert!(length >= $size);
                Self {
                    bytes: unsafe { std::slice::from_raw_parts(raw, length) },
                }
            }
            pub fn bytes(self) -> &'a [u8] {
                self.bytes
            }
            pub fn frame_length(self) -> i32 {
                self.i32(0)
            }
            pub fn flags(self) -> u8 {
                // Both constructors require the fixed header, which includes byte 5.
                unsafe { *self.bytes.get_unchecked(5) }
            }
            pub fn session_id(self) -> i32 {
                self.i32($session)
            }
            pub fn stream_id(self) -> i32 {
                self.i32($session + 4)
            }
            #[inline]
            fn i32(self, offset: usize) -> i32 {
                // Every caller supplies a fixed offset inside the checked header.
                unsafe { i32::from_le(self.bytes.as_ptr().add(offset).cast::<i32>().read_unaligned()) }
            }
        }
    };
}
frame_view!(StatusMessage, 36, 8);
frame_view!(SetupMessage, 40, 12);
frame_view!(ErrorMessage, 40, 8);
impl StatusMessage<'_> {
    pub fn consumption_term_id(self) -> i32 {
        self.i32(16)
    }
    pub fn consumption_term_offset(self) -> i32 {
        self.i32(20)
    }
    pub fn receiver_window(self) -> i32 {
        self.i32(24)
    }
    pub fn receiver_id(self) -> i64 {
        unsafe { i64::from_le(self.bytes.as_ptr().add(28).cast::<i64>().read_unaligned()) }
    }
    pub fn group_tag(self) -> Option<i64> {
        if self.frame_length() == 44 && self.bytes.len() >= 44 {
            Some(unsafe { i64::from_le(self.bytes.as_ptr().add(36).cast::<i64>().read_unaligned()) })
        } else {
            None
        }
    }
    /// Compute a position with wrapping term-id arithmetic, as the wire protocol requires.
    pub fn position(self, initial_term_id: i32, shift: usize) -> i64 {
        (i64::from(self.consumption_term_id().wrapping_sub(initial_term_id)))
            .wrapping_shl(shift as u32)
            .wrapping_add(i64::from(self.consumption_term_offset()))
    }
}
impl SetupMessage<'_> {
    pub fn term_offset(self) -> i32 {
        self.i32(8)
    }
    pub fn initial_term_id(self) -> i32 {
        self.i32(20)
    }
    pub fn active_term_id(self) -> i32 {
        self.i32(24)
    }
    pub fn term_length(self) -> i32 {
        self.i32(28)
    }
    pub fn mtu(self) -> i32 {
        self.i32(32)
    }
}
impl<'a> ErrorMessage<'a> {
    pub fn receiver_id(self) -> i64 {
        unsafe { i64::from_le(self.bytes.as_ptr().add(16).cast::<i64>().read_unaligned()) }
    }
    pub fn group_tag(self) -> i64 {
        unsafe { i64::from_le(self.bytes.as_ptr().add(24).cast::<i64>().read_unaligned()) }
    }
    pub fn error_code(self) -> i32 {
        self.i32(32)
    }
    pub fn message(self) -> &'a [u8] {
        let len = (self.i32(36).max(0) as usize).min(self.bytes.len() - 40);
        &self.bytes[40..40 + len]
    }
}
