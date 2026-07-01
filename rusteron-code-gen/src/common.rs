use crate::AeronErrorType::Unknown;
#[cfg(feature = "backtrace")]
use std::backtrace::Backtrace;
use std::cell::UnsafeCell;
use std::fmt::Formatter;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
pub enum CResource<T> {
    OwnedOnHeap(std::rc::Rc<ManagedCResource<T>>),
    /// Always initialised by construction (zeroed or `new(v)`). Never store
    /// `uninit()` — `Clone` and `get()` assume it's valid.
    OwnedOnStack(std::mem::MaybeUninit<T>),
    Borrowed(*mut T),
}

impl<T: Clone> Clone for CResource<T> {
    fn clone(&self) -> Self {
        // SAFETY: each branch only dereferences pointers/references that are
        // valid by construction. `OwnedOnStack` upholds the initialised-by-
        // construction invariant documented on the variant, so `assume_init_ref`
        // is sound.
        unsafe {
            match self {
                CResource::OwnedOnHeap(r) => CResource::OwnedOnHeap(r.clone()),
                CResource::OwnedOnStack(r) => CResource::OwnedOnStack(MaybeUninit::new(r.assume_init_ref().clone())),
                CResource::Borrowed(r) => CResource::Borrowed(r.clone()),
            }
        }
    }
}

impl<T> CResource<T> {
    #[inline]
    pub fn get(&self) -> *mut T {
        match self {
            CResource::OwnedOnHeap(r) => r.get(),
            CResource::OwnedOnStack(r) => r.as_ptr() as *mut T,
            CResource::Borrowed(r) => *r,
        }
    }

    #[inline]
    // to prevent the dependencies from being dropped as you have a copy here
    pub fn add_dependency<D: std::any::Any>(&self, dep: D) {
        match self {
            CResource::OwnedOnHeap(r) => r.add_dependency(dep),
            CResource::OwnedOnStack(_) | CResource::Borrowed(_) => {
                unreachable!("only owned on heap")
            }
        }
    }
    #[inline]
    pub fn get_dependency<V: Clone + 'static>(&self) -> Option<V> {
        match self {
            CResource::OwnedOnHeap(r) => r.get_dependency(),
            CResource::OwnedOnStack(_) | CResource::Borrowed(_) => None,
        }
    }

    #[inline]
    pub fn as_owned(&self) -> Option<&std::rc::Rc<ManagedCResource<T>>> {
        match self {
            CResource::OwnedOnHeap(r) => Some(r),
            CResource::OwnedOnStack(_) | CResource::Borrowed(_) => None,
        }
    }

    /// Run the clean-up / close on the resource via its shared state.
    ///
    /// For `OwnedOnHeap` resources this calls `close_shared` on the
    /// `ManagedCResource` — the FFI close fires exactly once across all
    /// clones.  Stack and borrowed resources are no-ops (they don't own a
    /// cleanup closure).
    #[inline]
    pub fn close_resource(&self) -> Result<(), AeronCError> {
        match self {
            CResource::OwnedOnHeap(r) => r.close_shared(),
            CResource::OwnedOnStack(_) | CResource::Borrowed(_) => Ok(()),
        }
    }

    /// Run a custom close function through the same shared close gate.
    ///
    /// This is used for close methods that take extra parameters, such as
    /// Aeron's close-complete notification callback.  The custom close still
    /// consumes the wrapper handle and still closes exactly once across clones.
    #[inline]
    pub fn close_resource_with(&self, cleanup: impl FnMut(*mut *mut T) -> i32) -> Result<(), AeronCError> {
        match self {
            CResource::OwnedOnHeap(r) => r.close_shared_with(cleanup),
            CResource::OwnedOnStack(_) | CResource::Borrowed(_) => Ok(()),
        }
    }

    /// Close an owner resource only when this is the last shared reference.
    ///
    /// This is for owner/client handles (e.g. Aeron/AeronArchive) whose C close
    /// frees child resources.  If child handles still hold dependency clones, we
    /// must defer to natural Rc teardown to preserve child-before-parent order.
    #[inline]
    pub fn close_resource_deferred_if_shared(&self) -> Result<(), AeronCError> {
        match self {
            CResource::OwnedOnHeap(r) => {
                let refs = std::rc::Rc::strong_count(r);
                if refs > 1 {
                    log::info!(
                        "close deferred for {} because {} references are still alive",
                        std::any::type_name::<T>(),
                        refs
                    );
                    Ok(())
                } else {
                    r.close_shared()
                }
            }
            CResource::OwnedOnStack(_) | CResource::Borrowed(_) => Ok(()),
        }
    }
}

impl<T> std::fmt::Debug for CResource<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = std::any::type_name::<T>();

        match self {
            CResource::OwnedOnHeap(r) => {
                write!(f, "{name} heap({:?})", r)
            }
            CResource::OwnedOnStack(r) => {
                write!(f, "{name} stack({:?})", *r)
            }
            CResource::Borrowed(r) => {
                write!(f, "{name} borrowed ({:?})", r)
            }
        }
    }
}

/// A custom struct for managing C resources with automatic cleanup.
///
/// It handles initialisation and clean-up of the resource and ensures that resources
/// are properly released when they go out of scope. All teardown goes through the
/// single `cleanup` closure (if set), which is the FFI close function (e.g.
/// `aeron_close`). The Rc dependency graph ensures parents outlive children
/// structurally — you cannot race `aeron_close` ahead of a live child handle.
#[allow(dead_code)]
pub struct ManagedCResource<T> {
    resource: std::cell::Cell<*mut T>,
    /// Interior mutability so the cleanup can be invoked through a shared
    /// `&self` reference when the resource is shared via `Rc`.  The
    /// `close_already_called` gate ensures single-execution: only the first
    /// call to `close()` or `close_shared()` takes and runs the closure.
    cleanup: UnsafeCell<Option<Box<dyn FnMut(*mut *mut T) -> i32>>>,
    cleanup_struct: bool,
    /// Set when close() has been called (gate against double-cleanup).
    close_already_called: std::cell::Cell<bool>,
    /// indicates if the underlying resource has already been handed off and should not be re-polled
    resource_released: std::cell::Cell<bool>,
    /// Keeps deps alive (e.g. the Aeron client while a pub/sub exists).
    /// Mutated only at construction from the owning thread — no locking,
    /// same Send-over-Rc unsoundness stance. Empty vec doesn't allocate.
    dependencies: UnsafeCell<Vec<std::rc::Rc<dyn std::any::Any>>>,
}

impl<T> std::fmt::Debug for ManagedCResource<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedCResource")
            .field(
                "resource",
                if self.close_already_called.get() {
                    &"<closed>"
                } else {
                    &self.resource
                },
            )
            .field("type", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T> ManagedCResource<T> {
    /// Creates a new ManagedCResource with a given initializer and cleanup function.
    ///
    /// The initializer is a closure that attempts to initialize the resource.
    /// If initialization fails, the initializer should return an error code.
    /// The cleanup function is used to release the resource when it's no longer needed.
    /// `cleanup_struct` where it should clean up the struct in rust
    pub fn new(
        init: impl FnOnce(*mut *mut T) -> i32,
        cleanup: Option<Box<dyn FnMut(*mut *mut T) -> i32>>,
        cleanup_struct: bool,
    ) -> Result<Self, AeronCError> {
        let resource = Self::initialise(init)?;

        let result = Self {
            resource: std::cell::Cell::new(resource),
            cleanup: UnsafeCell::new(cleanup),
            cleanup_struct,
            close_already_called: std::cell::Cell::new(false),
            resource_released: std::cell::Cell::new(false),
            dependencies: UnsafeCell::new(vec![]),
        };
        #[cfg(feature = "extra-logging")]
        log::info!("created c resource: {:?}", result);
        Ok(result)
    }

    pub fn initialise(init: impl FnOnce(*mut *mut T) -> i32 + Sized) -> Result<*mut T, AeronCError> {
        let mut resource: *mut T = std::ptr::null_mut();
        let result = init(&mut resource);
        if result < 0 || resource.is_null() {
            return Err(AeronCError::from_code(result));
        }
        Ok(resource)
    }

    /// Returns `true` if the resource has been closed (via close() or is
    /// already null).
    pub fn is_closed_already_called(&self) -> bool {
        self.close_already_called.get() || self.resource.get().is_null()
    }

    /// Gets a raw pointer to the resource.
    #[inline(always)]
    pub fn get(&self) -> *mut T {
        self.resource.get()
    }

    #[inline(always)]
    pub fn get_mut(&self) -> &mut T {
        unsafe { &mut *self.resource.get() }
    }

    #[inline]
    // to prevent the dependencies from being dropped as you have a copy here
    pub fn add_dependency<D: std::any::Any>(&self, dep: D) {
        if let Some(dep) = (&dep as &dyn std::any::Any).downcast_ref::<std::rc::Rc<dyn std::any::Any>>() {
            unsafe {
                (*self.dependencies.get()).push(dep.clone());
            }
        } else {
            unsafe {
                (*self.dependencies.get()).push(std::rc::Rc::new(dep));
            }
        }
    }

    #[inline]
    pub fn get_dependency<V: Clone + 'static>(&self) -> Option<V> {
        unsafe {
            (*self.dependencies.get())
                .iter()
                .filter_map(|x| x.as_ref().downcast_ref::<V>().cloned())
                .next()
        }
    }

    #[inline]
    pub fn is_resource_released(&self) -> bool {
        self.resource_released.get()
    }

    #[inline]
    pub fn mark_resource_released(&self) {
        self.resource_released.set(true);
    }

    /// Closes the resource through a shared reference.
    ///
    /// Like `close(&mut self)` but works with `&self`, enabling explicit close
    /// on handles that share the resource via `Rc`.  The cleanup closure is
    /// accessed through `UnsafeCell` interior mutability; the
    /// `close_already_called` gate ensures it is only taken once regardless of
    /// how many clones call `close_shared()`.
    ///
    /// This is the method called by the generated `close(self)` method on
    /// wrapper types.
    pub fn close_shared(&self) -> Result<(), AeronCError> {
        if self.close_already_called.get() {
            return Ok(());
        }

        // SAFETY: this library deliberately uses Rc/Cell/UnsafeCell for
        // single-threaded low-latency handles. close_shared() is not Sync; the
        // first caller takes the cleanup closure and either completes close or
        // restores the closure on failure so a later call/drop can retry.
        let cleanup = unsafe { (*self.cleanup.get()).take() };
        if let Some(mut cleanup) = cleanup {
            let mut resource = self.resource.get();
            if !resource.is_null() {
                let result = cleanup(&mut resource);
                if result < 0 {
                    unsafe {
                        *self.cleanup.get() = Some(cleanup);
                    }
                    return Err(AeronCError::from_code(result));
                }
            }

            self.close_already_called.set(true);
            if !self.cleanup_struct {
                // C-owned resources have been freed by the close function.
                // Null the shared pointer so clones cannot keep using a
                // dangling pointer after explicit close.
                self.resource.set(std::ptr::null_mut());
            }
        } else {
            self.close_already_called.set(true);
        }

        Ok(())
    }

    /// Closes the resource with a caller-supplied C close function.
    ///
    /// The stored default cleanup is taken first so Drop cannot later run it a
    /// second time.  If the custom close fails, the default cleanup is restored
    /// and the resource remains retryable.
    pub fn close_shared_with(&self, mut custom_cleanup: impl FnMut(*mut *mut T) -> i32) -> Result<(), AeronCError> {
        if self.close_already_called.get() {
            return Ok(());
        }

        let stored_cleanup = unsafe { (*self.cleanup.get()).take() };
        let mut resource = self.resource.get();
        if !resource.is_null() {
            let result = custom_cleanup(&mut resource);
            if result < 0 {
                unsafe {
                    *self.cleanup.get() = stored_cleanup;
                }
                return Err(AeronCError::from_code(result));
            }
        }

        self.close_already_called.set(true);
        if !self.cleanup_struct {
            self.resource.set(std::ptr::null_mut());
        }

        Ok(())
    }
}

impl<T> Drop for ManagedCResource<T> {
    fn drop(&mut self) {
        // Delegate to close_shared() which handles single-execution, error
        // logging, and pointer-nulling.  close_already_called prevents
        // double-execution if the resource was already closed through
        // another clone.
        if !self.close_already_called.get() {
            if let Err(e) = self.close_shared() {
                log::warn!(
                    "cleanup failed for {} during Drop with code {}",
                    std::any::type_name::<T>(),
                    e.code,
                );
            }
        }

        if self.cleanup_struct {
            #[cfg(feature = "extra-logging")]
            log::info!("closing rust struct resource: {:?}", self.resource.get());
            let resource = self.resource.get();
            if !resource.is_null() {
                unsafe {
                    let _ = Box::from_raw(resource);
                }
                self.resource.set(std::ptr::null_mut());
            }
        }
    }
}

#[derive(Debug, PartialOrd, Eq, PartialEq, Clone)]
pub enum AeronErrorType {
    GenericError,
    ClientErrorDriverTimeout,
    ClientErrorClientTimeout,
    ClientErrorConductorServiceTimeout,
    ClientErrorBufferFull,
    PublicationBackPressured,
    PublicationAdminAction,
    PublicationClosed,
    PublicationMaxPositionExceeded,
    PublicationError,
    TimedOut,
    Unknown(i32),
}

impl From<AeronErrorType> for AeronCError {
    fn from(value: AeronErrorType) -> Self {
        AeronCError::from_code(value.code())
    }
}

impl AeronErrorType {
    pub fn code(&self) -> i32 {
        match self {
            AeronErrorType::GenericError => -1,
            AeronErrorType::ClientErrorDriverTimeout => -1000,
            AeronErrorType::ClientErrorClientTimeout => -1001,
            AeronErrorType::ClientErrorConductorServiceTimeout => -1002,
            AeronErrorType::ClientErrorBufferFull => -1003,
            AeronErrorType::PublicationBackPressured => -2,
            AeronErrorType::PublicationAdminAction => -3,
            AeronErrorType::PublicationClosed => -4,
            AeronErrorType::PublicationMaxPositionExceeded => -5,
            AeronErrorType::PublicationError => -6,
            AeronErrorType::TimedOut => -234324,
            AeronErrorType::Unknown(code) => *code,
        }
    }

    pub fn is_back_pressured(&self) -> bool {
        self == &AeronErrorType::PublicationBackPressured
    }

    pub fn is_admin_action(&self) -> bool {
        self == &AeronErrorType::PublicationAdminAction
    }

    pub fn is_back_pressured_or_admin_action(&self) -> bool {
        self.is_back_pressured() || self.is_admin_action()
    }

    pub fn from_code(code: i32) -> Self {
        match code {
            -1 => AeronErrorType::GenericError,
            -1000 => AeronErrorType::ClientErrorDriverTimeout,
            -1001 => AeronErrorType::ClientErrorClientTimeout,
            -1002 => AeronErrorType::ClientErrorConductorServiceTimeout,
            -1003 => AeronErrorType::ClientErrorBufferFull,
            -2 => AeronErrorType::PublicationBackPressured,
            -3 => AeronErrorType::PublicationAdminAction,
            -4 => AeronErrorType::PublicationClosed,
            -5 => AeronErrorType::PublicationMaxPositionExceeded,
            -6 => AeronErrorType::PublicationError,
            -234324 => AeronErrorType::TimedOut,
            _ => Unknown(code),
        }
    }

    pub fn to_string(&self) -> &'static str {
        match self {
            AeronErrorType::GenericError => "Generic Error",
            AeronErrorType::ClientErrorDriverTimeout => "Client Error Driver Timeout",
            AeronErrorType::ClientErrorClientTimeout => "Client Error Client Timeout",
            AeronErrorType::ClientErrorConductorServiceTimeout => "Client Error Conductor Service Timeout",
            AeronErrorType::ClientErrorBufferFull => "Client Error Buffer Full",
            AeronErrorType::PublicationBackPressured => "Publication Back Pressured",
            AeronErrorType::PublicationAdminAction => "Publication Admin Action",
            AeronErrorType::PublicationClosed => "Publication Closed",
            AeronErrorType::PublicationMaxPositionExceeded => "Publication Max Position Exceeded",
            AeronErrorType::PublicationError => "Publication Error",
            AeronErrorType::TimedOut => "Timed Out",
            AeronErrorType::Unknown(_) => "Unknown Error",
        }
    }
}

/// Represents an Aeron-specific error with a code and an optional message.
///
/// The error code is derived from Aeron C API calls.
/// Use `get_last_err_message()` to retrieve the last human-readable message, if available.
#[derive(Eq, PartialEq, Clone)]
pub struct AeronCError {
    pub code: i32,
}

impl AeronCError {
    /// Creates an AeronError from the error code returned by Aeron.
    ///
    /// Error codes below zero are considered failure.
    pub fn from_code(code: i32) -> Self {
        #[cfg(feature = "backtrace")]
        {
            if code < 0 {
                let backtrace = Backtrace::capture();
                let backtrace = format!("{:?}", backtrace);

                // Compile the backtrace-parsing regex ONCE, not per error.
                // `from_code` sits on the error path; re-compiling the regex on
                // every Aeron error (including back-pressure retries) is wasteful.
                static BACKTRACE_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
                let re = BACKTRACE_RE
                    .get_or_init(|| regex::Regex::new(r#"fn: "([^"]+)", file: "([^"]+)", line: (\d+)"#).unwrap());
                let mut lines = String::new();
                re.captures_iter(&backtrace).for_each(|cap| {
                    let function = &cap[1];
                    let mut file = cap[2].to_string();
                    let line = &cap[3];
                    if file.starts_with("./") {
                        file = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), &file[2..]);
                    } else if file.starts_with("/rustc/") {
                        file = file.split("/").last().unwrap().to_string();
                    }
                    // log in intellij friendly error format so can hyperlink to source code in stack trace
                    lines.push_str(&format!(" {file}:{line} in {function}\n"));
                });

                log::error!(
                    "Aeron C error code: {}, kind: '{:?}'\n{}",
                    code,
                    AeronErrorType::from_code(code),
                    lines
                );
            }
        }
        AeronCError { code }
    }

    pub fn kind(&self) -> AeronErrorType {
        AeronErrorType::from_code(self.code)
    }

    pub fn is_back_pressured(&self) -> bool {
        self.kind().is_back_pressured()
    }

    pub fn is_admin_action(&self) -> bool {
        self.kind().is_admin_action()
    }

    pub fn is_back_pressured_or_admin_action(&self) -> bool {
        self.kind().is_back_pressured_or_admin_action()
    }
}

/// # Handler
///
/// `Handler` is a struct that wraps a raw pointer and a drop flag.
///
/// Memory is freed automatically when `Handler` goes out of scope (via `Drop`).
/// You must ensure the `Handler` outlives the Aeron session that uses it, since Aeron
/// holds a raw `clientd` pointer to the boxed value and will call callbacks until closed.
///
/// Call `release()` early if you want to free the memory before the `Handler` drops.
///
/// ## Example
///
/// ```no_compile
/// use rusteron_code_gen::Handler;
/// let handler = Handler::leak(your_value);
/// // handler is freed automatically when it goes out of scope
/// ```
pub struct Handler<T> {
    raw_ptr: *mut T,
    should_drop: bool,
}

unsafe impl<T> Send for Handler<T> {}
unsafe impl<T> Sync for Handler<T> {}

/// Utility method for setting empty handlers
pub struct Handlers;

impl<T> Handler<T> {
    pub fn leak(handler: T) -> Self {
        let raw_ptr = Box::into_raw(Box::new(handler)) as *mut _;
        #[cfg(feature = "extra-logging")]
        log::info!("creating handler {:?}", raw_ptr);
        Self {
            raw_ptr,
            should_drop: true,
        }
    }

    pub fn is_none(&self) -> bool {
        self.raw_ptr.is_null()
    }

    pub fn as_raw(&self) -> *mut std::os::raw::c_void {
        self.raw_ptr as *mut std::os::raw::c_void
    }

    pub fn release(&mut self) {
        if self.should_drop && !self.raw_ptr.is_null() {
            unsafe {
                #[cfg(feature = "extra-logging")]
                log::info!("dropping handler {:?}", self.raw_ptr);
                let _ = Box::from_raw(self.raw_ptr as *mut T);
                self.should_drop = false;
                // Null the pointer so a subsequent `Deref`/`DerefMut`/`is_none()`
                // cannot reach freed memory. Without this, `release()` left
                // `raw_ptr` dangling (freed-but-not-nulled) and any later deref
                // would be use-after-free. `Drop` only acts when `should_drop`,
                // which we already cleared, so the null is safe.
                self.raw_ptr = std::ptr::null_mut();
            }
        }
    }

    pub unsafe fn new(raw_ptr: *mut T, should_drop: bool) -> Self {
        Self { raw_ptr, should_drop }
    }
}

impl<T> Drop for Handler<T> {
    fn drop(&mut self) {
        if self.should_drop && !self.raw_ptr.is_null() {
            log::error!(
                "Handler<{}> at {:?} is being dropped but release() was never called — \
                 memory leak: {} bytes. Call release() explicitly when the C side no longer holds the pointer.",
                std::any::type_name::<T>(),
                self.raw_ptr,
                std::mem::size_of::<T>(),
            );
        }
    }
}

impl<T> Deref for Handler<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.raw_ptr as &T }
    }
}

impl<T> DerefMut for Handler<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.raw_ptr as &mut T }
    }
}

pub fn find_unused_udp_port(start_port: u16) -> Option<u16> {
    let end_port = u16::MAX;

    for port in start_port..=end_port {
        if is_udp_port_available(port) {
            return Some(port);
        }
    }

    None
}

pub fn is_udp_port_available(port: u16) -> bool {
    std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok()
}

/// Represents the Aeron URI parser and handler.
pub struct ChannelUri {}

impl ChannelUri {
    pub const AERON_SCHEME: &'static str = "aeron";
    pub const SPY_QUALIFIER: &'static str = "aeron-spy";
    pub const MAX_URI_LENGTH: usize = 4095;
}

pub const DRIVER_TIMEOUT_MS_DEFAULT: u64 = 10_000;
pub const AERON_DIR_PROP_NAME: &str = "aeron.dir";
pub const AERON_IPC_MEDIA: &str = "aeron:ipc";
pub const AERON_UDP_MEDIA: &str = "aeron:udp";
pub const SPY_PREFIX: &str = "aeron-spy:";
pub const TAG_PREFIX: &str = "tag:";

/// Enum for media types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Media {
    Ipc,
    Udp,
}

impl Media {
    pub fn as_str(&self) -> &'static str {
        match self {
            Media::Ipc => "ipc",
            Media::Udp => "udp",
        }
    }
}

/// Enum for control modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlMode {
    Manual,
    Dynamic,
    /// this is a beta feature useful when dealing with docker containers and networking
    Response,
}

impl ControlMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ControlMode::Manual => "manual",
            ControlMode::Dynamic => "dynamic",
            ControlMode::Response => "response",
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod test_alloc {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::env;
    use std::fs::OpenOptions;
    #[allow(unused_imports)]
    use std::os::unix::fs::OpenOptionsExt;
    use std::sync::atomic::{AtomicIsize, Ordering};

    /// A simple global allocator that tracks the net allocation count.
    /// Used mainly for testing memory leaks or unintended allocations.
    pub struct TrackingAllocator {
        allocs: AtomicIsize,
    }

    impl TrackingAllocator {
        pub const fn new() -> Self {
            Self {
                allocs: AtomicIsize::new(0),
            }
        }
        pub fn current(&self) -> isize {
            self.allocs.load(Ordering::SeqCst)
        }
    }

    unsafe impl GlobalAlloc for TrackingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            self.allocs.fetch_add(1, Ordering::SeqCst);
            System.alloc(layout)
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            self.allocs.fetch_sub(1, Ordering::SeqCst);
            System.dealloc(ptr, layout)
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            self.allocs.fetch_add(1, Ordering::SeqCst);
            System.alloc_zeroed(layout)
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            System.realloc(ptr, layout, new_size)
        }
    }

    #[global_allocator]
    static GLOBAL: TrackingAllocator = TrackingAllocator::new();

    /// Returns the current number of net allocations
    pub fn current_allocs() -> isize {
        GLOBAL.current()
    }

    /// Asserts that no allocations occur within the provided closure.
    /// Uses a file lock to ensure exclusive access across threads/tests.
    pub fn assert_no_allocation<F: FnOnce()>(f: F) {
        let tmp = env::temp_dir().join("rusteron_allocation.lck");

        #[cfg(unix)]
        let file = {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .mode(0o600)
                .open(&tmp)
                .expect("Failed to open allocation lock file")
        };
        #[cfg(not(unix))]
        let file = {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&tmp)
                .expect("Failed to open allocation lock file")
        };

        let mut lock = fd_lock::RwLock::new(file);
        let lock = lock.write().expect("Failed to acquire file lock");

        let before = current_allocs();
        f();
        let after = current_allocs();
        let diff = (after - before).abs();
        assert!(
            diff < 50,
            "Expected no allocation leak, but alloc count changed from {} to {} (diff {})",
            before,
            after,
            diff
        );

        drop(lock)
    }
}

pub trait IntoCString {
    fn into_c_string(self) -> std::ffi::CString;
}

impl IntoCString for std::ffi::CString {
    fn into_c_string(self) -> std::ffi::CString {
        self
    }
}

impl IntoCString for &str {
    fn into_c_string(self) -> std::ffi::CString {
        #[cfg(feature = "extra-logging")]
        log::info!("created c string on heap: {:?}", self);

        std::ffi::CString::new(self).expect("failed to create CString")
    }
}

impl IntoCString for String {
    fn into_c_string(self) -> std::ffi::CString {
        #[cfg(feature = "extra-logging")]
        log::info!("created c string on heap: {:?}", self);

        std::ffi::CString::new(self).expect("failed to create CString")
    }
}

#[cfg(test)]
mod handler_tests {
    use super::*;

    #[test]
    fn release_nulls_pointer_so_is_none_is_true() {
        let mut handler = Handler::leak(42u32);
        // Boxed value lives on the heap before release.
        assert!(!handler.is_none(), "freshly leaked handler must be non-null");

        handler.release();

        // After release the inner pointer is nulled, not dangling — so a later
        // Deref/is_none cannot reach freed memory.
        assert!(handler.is_none(), "release() must null raw_ptr (was a UAF)");
    }

    #[test]
    fn release_is_idempotent_no_double_free() {
        let mut handler = Handler::leak(99u64);
        handler.release();
        // A second release must be a no-op: the guard `should_drop && !null`
        // is false, so Box::from_raw is not called again (no double-free).
        handler.release();
        assert!(handler.is_none());
    }

    #[test]
    fn release_then_drop_is_silent() {
        // After release, should_drop is false and raw_ptr is null, so Drop must
        // not log the "memory leak" warning nor touch freed memory.
        let mut handler = Handler::leak(7u16);
        handler.release();
        drop(handler);
    }
}
