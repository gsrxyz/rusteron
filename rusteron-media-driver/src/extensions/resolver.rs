use super::*;
use std::net::IpAddr;

/// A resolver used by Aeron's native resource agent. Keep callbacks bounded if
/// that agent shares a thread with packet processing.
/// Implement callback behaviour here. Pass an existing value to
/// `set_name_resolver_instance`, or use `Default` or `NameResolverFactory`
/// with `set_name_resolver` for construction during native initialisation.
pub trait NameResolver: Send + Sized + 'static {
    /// Reserve replacement storage during initialisation. Include the trailing
    /// NUL byte. This is a capacity hint, not a length limit. Larger replacements
    /// grow the buffer. Resolve-only implementations need no lookup storage.
    const LOOKUP_CAPACITY: usize = 0;

    fn resolve(&mut self, name: &CStr, uri_parameter: &CStr, is_re_resolution: bool) -> Result<IpAddr, AeronCError>;
    /// Return a host:port replacement. The adapter copies it before any subsequent
    /// mutable callback, so `resolve` cannot invalidate the string C still uses.
    /// The copy reuses its allocation. A replacement longer than the buffer's
    /// capacity allocates before returning to C.
    fn lookup(&self, name: &CStr, uri_parameter: &CStr, is_re_lookup: bool) -> Option<&CStr> {
        None
    }
    fn on_start(&mut self) -> Result<(), AeronCError> {
        Ok(())
    }
    fn do_work(&mut self, now_ms: i64) -> Result<i32, AeronCError> {
        Ok(0)
    }
}

/// Construct a resolver from native initialisation arguments.
///
/// Types that implement `Default` receive this implementation automatically.
/// Use `set_name_resolver_instance` to pass an existing value or a closure.
pub trait NameResolverFactory: NameResolver {
    fn new(args: &CStr) -> Result<Self, AeronCError>;
}

impl<T: NameResolver + Default> NameResolverFactory for T {
    fn new(_: &CStr) -> Result<Self, AeronCError> {
        Ok(Self::default())
    }
}

impl<F> NameResolver for F
where
    F: FnMut(&CStr, &CStr, bool) -> Result<IpAddr, AeronCError> + Send + 'static,
{
    #[inline]
    fn resolve(&mut self, name: &CStr, uri_parameter: &CStr, is_re_resolution: bool) -> Result<IpAddr, AeronCError> {
        self(name, uri_parameter, is_re_resolution)
    }
}

struct ResolverBox<T> {
    resolver: T,
    lookup: Vec<u8>,
}
unsafe extern "C" fn supplier<T: NameResolverFactory>(
    out: *mut aeron_name_resolver_t,
    args: *const std::os::raw::c_char,
    context: *mut aeron_driver_context_t,
) -> i32 {
    let args = if args.is_null() {
        c""
    } else {
        unsafe { CStr::from_ptr(args) }
    };
    match T::new(args) {
        Ok(resolver) => {
            unsafe { install(out, resolver) };
            0
        }
        Err(error) => unsafe { supplier_error(error) },
    }
}

unsafe fn install<T: NameResolver>(out: *mut aeron_name_resolver_t, resolver: T) {
    unsafe {
        out.write(aeron_name_resolver_t {
            name: c"rusteron".as_ptr(),
            lookup_func: Some(lookup::<T>),
            resolve_func: Some(resolve::<T>),
            start_func: Some(start::<T>),
            do_work_func: Some(work::<T>),
            close_func: Some(close::<T>),
            state: Box::into_raw(Box::new(ResolverBox {
                resolver,
                lookup: Vec::with_capacity(T::LOOKUP_CAPACITY),
            }))
            .cast(),
        });
    }
}

const INSTANCE_KEY: &CStr = c"rusteron.name_resolver.instance";

struct PendingResolver<T>(UnsafeCell<Option<T>>);

// Context configuration and driver initialisation stay on the context's thread.
// Only initialisation accesses this cell. The value then moves into ResolverBox,
// whose callbacks have exclusive access on the native resource agent.
unsafe impl<T: NameResolver> Sync for PendingResolver<T> {}

unsafe extern "C" fn instance_supplier<T: NameResolver>(
    out: *mut aeron_name_resolver_t,
    _: *const std::os::raw::c_char,
    context: *mut aeron_driver_context_t,
) -> i32 {
    // Registration reserves this entry before publishing this supplier. Native
    // initialisation runs on the context's thread and consumes the value once.
    let index = unsafe { aeron_driver_context_bindings_clientd_find(context, INSTANCE_KEY.as_ptr()) };
    if index < 0 {
        return unsafe { supplier_error(AeronCError::with_message(-1, "the resolver instance entry is missing")) };
    }
    let entry = unsafe { &*(*context).bindings_clientd_entries.add(index as usize) };
    let value = unsafe { &mut *(*entry.clientd.cast::<PendingResolver<T>>()).0.get() };
    match value.take() {
        Some(resolver) => {
            unsafe { install(out, resolver) };
            0
        }
        None => unsafe {
            supplier_error(AeronCError::with_message(
                -1,
                "the resolver instance was already used; install a new instance before retrying initialisation",
            ))
        },
    }
}

unsafe extern "C" fn lookup<T: NameResolver>(
    resolver: *mut aeron_name_resolver_t,
    name: *const std::os::raw::c_char,
    parameter: *const std::os::raw::c_char,
    again: bool,
    output: *mut *const std::os::raw::c_char,
) -> i32 {
    let state = unsafe { &mut *(*resolver).state.cast::<ResolverBox<T>>() };
    let replacement = state.resolver.lookup(
        unsafe { CStr::from_ptr(name) },
        unsafe { CStr::from_ptr(parameter) },
        again,
    );
    match replacement {
        Some(replacement) => {
            state.lookup.clear();
            state.lookup.extend_from_slice(replacement.to_bytes_with_nul());
            unsafe { *output = state.lookup.as_ptr().cast() };
            1
        }
        None => {
            unsafe { *output = name };
            0
        }
    }
}
unsafe extern "C" fn resolve<T: NameResolver>(
    resolver: *mut aeron_name_resolver_t,
    name: *const std::os::raw::c_char,
    parameter: *const std::os::raw::c_char,
    again: bool,
    output: *mut sockaddr_storage,
) -> i32 {
    let state = unsafe { &mut *std::ptr::addr_of_mut!((*(*resolver).state.cast::<ResolverBox<T>>()).resolver) };
    match state.resolve(
        unsafe { CStr::from_ptr(name) },
        unsafe { CStr::from_ptr(parameter) },
        again,
    ) {
        Ok(ip) => {
            unsafe { write_ip(output, ip) };
            0
        }
        Err(error) => unsafe { supplier_error(error) },
    }
}
unsafe fn write_ip(output: *mut sockaddr_storage, ip: IpAddr) {
    unsafe {
        std::ptr::write_bytes(output, 0, 1);
        #[cfg(target_os = "macos")]
        {
            (*output).ss_len = if ip.is_ipv4() { 16 } else { 28 };
        }
        let raw = output.cast::<u8>();
        match ip {
            IpAddr::V4(ip) => {
                (*output).ss_family = AF_INET as _;
                std::ptr::copy_nonoverlapping(ip.octets().as_ptr(), raw.add(4), 4);
            }
            IpAddr::V6(ip) => {
                (*output).ss_family = AF_INET6 as _;
                std::ptr::copy_nonoverlapping(ip.octets().as_ptr(), raw.add(8), 16);
            }
        }
    }
}
unsafe extern "C" fn start<T: NameResolver>(resolver: *mut aeron_name_resolver_t) -> i32 {
    let state = unsafe { &mut *std::ptr::addr_of_mut!((*(*resolver).state.cast::<ResolverBox<T>>()).resolver) };
    match state.on_start() {
        Ok(()) => 0,
        Err(error) => unsafe { supplier_error(error) },
    }
}
unsafe extern "C" fn work<T: NameResolver>(resolver: *mut aeron_name_resolver_t, now: i64) -> i32 {
    let state = unsafe { &mut *std::ptr::addr_of_mut!((*(*resolver).state.cast::<ResolverBox<T>>()).resolver) };
    match state.do_work(now) {
        Ok(work) => work.max(0),
        Err(error) => unsafe { supplier_error(error) },
    }
}
unsafe extern "C" fn close<T: NameResolver>(resolver: *mut aeron_name_resolver_t) -> i32 {
    unsafe {
        drop(Box::from_raw((*resolver).state.cast::<ResolverBox<T>>()));
        (*resolver).state = std::ptr::null_mut();
    }
    0
}
impl AeronDriverContext {
    /// Install a type-selected resolver. Configure it with `set_name_resolver_init_args`
    /// before driver initialisation. The context copies the arguments.
    pub fn set_name_resolver<T: NameResolverFactory>(&self) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        check(unsafe { aeron_driver_context_set_name_resolver_supplier(self.get_inner(), Some(supplier::<T>)) })
    }

    /// Install a resolver closure with inferred argument types.
    /// The closure follows the ownership rules of `set_name_resolver_instance`.
    pub fn set_name_resolver_fn<F>(&self, resolve: F) -> Result<(), AeronCError>
    where
        F: FnMut(&CStr, &CStr, bool) -> Result<IpAddr, AeronCError> + Send + 'static,
    {
        self.set_name_resolver_instance(resolve)
    }

    /// Move a resolver or closure into the driver during initialisation.
    ///
    /// The native resolver owns the value after initialisation and drops it at shutdown.
    /// Initialisation consumes it once, so install a new value before retrying a failed start.
    pub fn set_name_resolver_instance<T: NameResolver>(&self, resolver: T) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        let owner = NativeOwned::new(PendingResolver(UnsafeCell::new(Some(resolver))));
        unsafe {
            let context = self.get_inner();
            let entry =
                aeron_driver_context_bindings_clientd_get_or_find_first_free_entry(context, INSTANCE_KEY.as_ptr());
            if entry.is_null() {
                return Err(AeronCError::with_message(
                    -1,
                    "no native context slot is available for the resolver instance",
                ));
            }
            // The native context is valid, so this setter cannot fail. Publish
            // the pointer only after it succeeds and retain its allocation.
            check(aeron_driver_context_set_name_resolver_supplier(
                context,
                Some(instance_supplier::<T>),
            ))?;
            (*entry).clientd = owner.as_ptr().cast();
        }
        self.inner.add_dependency(owner);
        Ok(())
    }
}
#[cfg(test)]
pub(super) unsafe fn write_ip_for_test(output: *mut sockaddr_storage, ip: IpAddr) {
    unsafe { write_ip(output, ip) }
}
