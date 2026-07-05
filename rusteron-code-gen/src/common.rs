// ─── Compilation contexts (read before adding code here) ────────────────────────────
// This file compiles in TWO contexts:
//   1. as `mod common` inside rusteron-code-gen itself (unit tests, shared types), and
//   2. verbatim inside every generated crate's `aeron.rs` (via COMMON_CODE include_str).
// Consequence: nothing here may depend on impls that live in `aeron_custom*.rs` — those
// exist only in context 2 (e.g. AeronCError's Debug/Display). That is why AeronOfferError
// hand-rolls its Debug. Crate-specific code belongs in `aeron_custom.rs` (all crates) or
// `aeron_custom_<crate>.rs` (one crate); build-script-only code goes in `build_common.rs`.
// ─────────────────────────────────────────────────────────────────────────────────────

use crate::AeronErrorType::Unknown;
#[cfg(feature = "backtrace")]
use std::backtrace::Backtrace;
use std::cell::UnsafeCell;
use std::fmt::Formatter;
use std::mem::MaybeUninit;
use std::ops::Deref;
#[allow(unused_imports)]
use std::ops::DerefMut;

/// Reference-counting smart pointer: `Rc` by default, `Arc` under the
/// `multi-threaded` feature. Swap is transparent — `RcOrArc::new`, `.clone()`,
/// `strong_count` all work on both.
#[cfg(not(feature = "multi-threaded"))]
pub type RcOrArc<T> = std::rc::Rc<T>;
#[cfg(feature = "multi-threaded")]
pub type RcOrArc<T> = std::sync::Arc<T>;

#[cfg(not(feature = "multi-threaded"))]
pub type RefCellOrMutex<T> = std::cell::RefCell<T>;
#[cfg(feature = "multi-threaded")]
pub type RefCellOrMutex<T> = std::sync::Mutex<T>;

#[cfg(not(feature = "multi-threaded"))]
pub type CleanupBox<T> = Box<dyn FnMut(*mut *mut T) -> i32>;
#[cfg(feature = "multi-threaded")]
pub type CleanupBox<T> = Box<dyn FnMut(*mut *mut T) -> i32 + Send>;

pub enum CResource<T> {
    OwnedOnHeap(RcOrArc<ManagedCResource<T>>),
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
    pub fn as_owned(&self) -> Option<&RcOrArc<ManagedCResource<T>>> {
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
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn close_resource(&self) -> Result<(), AeronCError> {
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
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn close_resource_with(&self, cleanup: impl FnMut(*mut *mut T) -> i32) -> Result<(), AeronCError> {
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
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn close_resource_deferred_if_shared(&self) -> Result<(), AeronCError> {
        match self {
            CResource::OwnedOnHeap(r) => {
                let refs = RcOrArc::strong_count(r);
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
#[allow(dead_code)]
pub struct ManagedCResource<T> {
    #[cfg(not(feature = "multi-threaded"))]
    resource: std::cell::Cell<*mut T>,
    #[cfg(feature = "multi-threaded")]
    resource: std::sync::atomic::AtomicPtr<T>,

    #[cfg(not(feature = "multi-threaded"))]
    cleanup: UnsafeCell<Option<CleanupBox<T>>>,
    #[cfg(feature = "multi-threaded")]
    cleanup: std::sync::Mutex<Option<CleanupBox<T>>>,

    cleanup_struct: bool,

    /// `true` when constructed via `new` with `cleanup: None` AND
    /// `cleanup_struct: false` — an owned resource with no automatic close path.
    /// Drop emits a `log::warn!` if such a resource is dropped without an
    /// explicit `close()`/`close_now()`, since nothing else can free it.
    /// Catches the `Box::leak + None cleanup` bug class (the
    /// `AeronCncMetadata::load_from_file` leak was exactly this). False positives
    /// are impossible: `initialise` (borrowed scope) returns a raw `*mut T` and
    /// never builds a `ManagedCResource`, and generated `new(_, None, true)`
    /// resources are excluded by the `!cleanup_struct` conjunct.
    manual_close_required: bool,

    #[cfg(not(feature = "multi-threaded"))]
    close_already_called: std::cell::Cell<bool>,
    #[cfg(feature = "multi-threaded")]
    close_already_called: std::sync::atomic::AtomicBool,

    #[cfg(not(feature = "multi-threaded"))]
    resource_released: std::cell::Cell<bool>,
    #[cfg(feature = "multi-threaded")]
    resource_released: std::sync::atomic::AtomicBool,

    #[cfg(not(feature = "multi-threaded"))]
    dependencies: UnsafeCell<Vec<RcOrArc<dyn std::any::Any>>>,
    #[cfg(feature = "multi-threaded")]
    dependencies: std::sync::Mutex<Vec<RcOrArc<dyn std::any::Any>>>,
}

impl<T> std::fmt::Debug for ManagedCResource<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("ManagedCResource");
        if self.get_close_already_called() {
            debug.field("resource", &"<closed>");
        } else {
            debug.field("resource", &self.get());
        }
        debug.field("type", &std::any::type_name::<T>()).finish()
    }
}

// Under `multi-threaded` the refcount is `Arc` (atomic), so `Send` is sound.
// `Sync` enables sharing `&Handle` across threads for the C-documented
// thread-safe operations (offer / try_claim / position). The `UnsafeCell`
// fields are only mutated during construction and close (single-threaded),
// never during the shared-read window — same "accepted unsoundness" policy
// as the unconditional `unsafe impl Send` on the handle types.
#[cfg(feature = "multi-threaded")]
unsafe impl<T> Send for ManagedCResource<T> {}
#[cfg(feature = "multi-threaded")]
unsafe impl<T> Sync for ManagedCResource<T> {}

impl<T> ManagedCResource<T> {
    /// Creates a new ManagedCResource with a given initializer and cleanup function.
    ///
    /// The initializer is a closure that attempts to initialize the resource.
    /// If initialization fails, the initializer should return an error code.
    /// The cleanup function is used to release the resource when it's no longer needed.
    /// `cleanup_struct` where it should clean up the struct in rust
    pub fn new(
        init: impl FnOnce(*mut *mut T) -> i32,
        cleanup: Option<CleanupBox<T>>,
        cleanup_struct: bool,
    ) -> Result<Self, AeronCError> {
        let resource = Self::initialise(init)?;
        // Compute before `cleanup` is moved into the struct literal below.
        // `is_none()` borrows `cleanup` immutably; the move happens later.
        let manual_close_required = cleanup.is_none() && !cleanup_struct;

        let result = Self {
            #[cfg(not(feature = "multi-threaded"))]
            resource: std::cell::Cell::new(resource),
            #[cfg(feature = "multi-threaded")]
            resource: std::sync::atomic::AtomicPtr::new(resource),

            #[cfg(not(feature = "multi-threaded"))]
            cleanup: UnsafeCell::new(cleanup),
            #[cfg(feature = "multi-threaded")]
            cleanup: std::sync::Mutex::new(cleanup),

            cleanup_struct,
            manual_close_required,

            #[cfg(not(feature = "multi-threaded"))]
            close_already_called: std::cell::Cell::new(false),
            #[cfg(feature = "multi-threaded")]
            close_already_called: std::sync::atomic::AtomicBool::new(false),

            #[cfg(not(feature = "multi-threaded"))]
            resource_released: std::cell::Cell::new(false),
            #[cfg(feature = "multi-threaded")]
            resource_released: std::sync::atomic::AtomicBool::new(false),

            #[cfg(not(feature = "multi-threaded"))]
            dependencies: UnsafeCell::new(vec![]),
            #[cfg(feature = "multi-threaded")]
            dependencies: std::sync::Mutex::new(vec![]),
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

    /// Gets a raw pointer to the resource.
    #[inline(always)]
    pub fn get(&self) -> *mut T {
        #[cfg(not(feature = "multi-threaded"))]
        {
            self.resource.get()
        }
        #[cfg(feature = "multi-threaded")]
        {
            self.resource.load(std::sync::atomic::Ordering::Acquire)
        }
    }

    #[inline(always)]
    fn set_resource(&self, val: *mut T) {
        #[cfg(not(feature = "multi-threaded"))]
        {
            self.resource.set(val);
        }
        #[cfg(feature = "multi-threaded")]
        {
            self.resource.store(val, std::sync::atomic::Ordering::Release);
        }
    }

    #[inline(always)]
    fn get_close_already_called(&self) -> bool {
        #[cfg(not(feature = "multi-threaded"))]
        {
            self.close_already_called.get()
        }
        #[cfg(feature = "multi-threaded")]
        {
            self.close_already_called.load(std::sync::atomic::Ordering::Acquire)
        }
    }

    #[inline(always)]
    fn set_close_already_called(&self, val: bool) {
        #[cfg(not(feature = "multi-threaded"))]
        {
            self.close_already_called.set(val);
        }
        #[cfg(feature = "multi-threaded")]
        {
            self.close_already_called
                .store(val, std::sync::atomic::Ordering::Release);
        }
    }

    #[inline(always)]
    fn get_resource_released(&self) -> bool {
        #[cfg(not(feature = "multi-threaded"))]
        {
            self.resource_released.get()
        }
        #[cfg(feature = "multi-threaded")]
        {
            self.resource_released.load(std::sync::atomic::Ordering::Acquire)
        }
    }

    #[inline(always)]
    fn set_resource_released(&self, val: bool) {
        #[cfg(not(feature = "multi-threaded"))]
        {
            self.resource_released.set(val);
        }
        #[cfg(feature = "multi-threaded")]
        {
            self.resource_released.store(val, std::sync::atomic::Ordering::Release);
        }
    }

    /// Mutable access to the underlying C struct, minted from `&self`.
    ///
    /// # Safety
    /// No other reference (`&` or `&mut`) to the underlying struct may be
    /// alive while the returned `&mut` is in use.
    #[inline(always)]
    pub unsafe fn get_mut(&self) -> &mut T {
        &mut *self.get()
    }

    #[inline]
    // to prevent the dependencies from being dropped as you have a copy here
    pub fn add_dependency<D: std::any::Any>(&self, dep: D) {
        if let Some(dep) = (&dep as &dyn std::any::Any).downcast_ref::<RcOrArc<dyn std::any::Any>>() {
            #[cfg(not(feature = "multi-threaded"))]
            unsafe {
                (*self.dependencies.get()).push(dep.clone());
            }
            #[cfg(feature = "multi-threaded")]
            {
                self.dependencies.lock().unwrap().push(dep.clone());
            }
        } else {
            #[cfg(not(feature = "multi-threaded"))]
            unsafe {
                (*self.dependencies.get()).push(RcOrArc::new(dep));
            }
            #[cfg(feature = "multi-threaded")]
            {
                self.dependencies.lock().unwrap().push(RcOrArc::new(dep));
            }
        }
    }

    #[inline]
    pub fn get_dependency<V: Clone + 'static>(&self) -> Option<V> {
        #[cfg(not(feature = "multi-threaded"))]
        unsafe {
            (*self.dependencies.get())
                .iter()
                .filter_map(|x| x.as_ref().downcast_ref::<V>().cloned())
                .next()
        }
        #[cfg(feature = "multi-threaded")]
        {
            self.dependencies
                .lock()
                .unwrap()
                .iter()
                .filter_map(|x| x.as_ref().downcast_ref::<V>().cloned())
                .next()
        }
    }

    #[inline]
    pub fn is_resource_released(&self) -> bool {
        self.get_resource_released()
    }

    #[inline]
    pub fn mark_resource_released(&self) {
        self.set_resource_released(true);
        // The C client frees async resources when their poll completes (created,
        // errored, or cancelled). Null the stale pointer so any later use faults
        // deterministically on null instead of reading freed memory.
        self.set_resource(std::ptr::null_mut());
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
    pub(crate) fn close_shared(&self) -> Result<(), AeronCError> {
        if self.get_close_already_called() {
            return Ok(());
        }

        // SAFETY: this library deliberately uses Rc/Cell/UnsafeCell for
        // single-threaded low-latency handles. close_shared() is not Sync; the
        // first caller takes the cleanup closure and either completes close or
        // restores the closure on failure so a later call/drop can retry.
        #[cfg(not(feature = "multi-threaded"))]
        let cleanup = unsafe { (*self.cleanup.get()).take() };
        #[cfg(feature = "multi-threaded")]
        let cleanup = self.cleanup.lock().unwrap().take();

        if let Some(mut cleanup) = cleanup {
            let mut resource = self.get();
            if !resource.is_null() {
                let result = cleanup(&mut resource);
                if result < 0 {
                    #[cfg(not(feature = "multi-threaded"))]
                    unsafe {
                        *self.cleanup.get() = Some(cleanup);
                    }
                    #[cfg(feature = "multi-threaded")]
                    {
                        *self.cleanup.lock().unwrap() = Some(cleanup);
                    }
                    return Err(AeronCError::from_code(result));
                }
            }

            self.set_close_already_called(true);
            if !self.cleanup_struct {
                // C-owned resources have been freed by the close function.
                // Null the shared pointer so clones cannot keep using a
                // dangling pointer after explicit close.
                self.set_resource(std::ptr::null_mut());
            }
        } else {
            self.set_close_already_called(true);
        }

        Ok(())
    }

    /// Closes the resource with a caller-supplied C close function.
    ///
    /// The stored default cleanup is taken first so Drop cannot later run it a
    /// second time.  If the custom close fails, the default cleanup is restored
    /// and the resource remains retryable.
    #[allow(dead_code)]
    pub(crate) fn close_shared_with(
        &self,
        mut custom_cleanup: impl FnMut(*mut *mut T) -> i32,
    ) -> Result<(), AeronCError> {
        if self.get_close_already_called() {
            return Ok(());
        }

        #[cfg(not(feature = "multi-threaded"))]
        let stored_cleanup = unsafe { (*self.cleanup.get()).take() };
        #[cfg(feature = "multi-threaded")]
        let stored_cleanup = self.cleanup.lock().unwrap().take();

        let mut resource = self.get();
        if !resource.is_null() {
            let result = custom_cleanup(&mut resource);
            if result < 0 {
                #[cfg(not(feature = "multi-threaded"))]
                unsafe {
                    *self.cleanup.get() = stored_cleanup;
                }
                #[cfg(feature = "multi-threaded")]
                {
                    *self.cleanup.lock().unwrap() = stored_cleanup;
                }
                return Err(AeronCError::from_code(result));
            }
        }

        self.set_close_already_called(true);
        if !self.cleanup_struct {
            self.set_resource(std::ptr::null_mut());
        }

        Ok(())
    }
}

impl<T> Drop for ManagedCResource<T> {
    fn drop(&mut self) {
        // Capture whether close ran BEFORE Drop — close_shared() below would set
        // the flag even when the cleanup closure is None, hiding the leak signal.
        let close_ran_before_drop = self.get_close_already_called();
        // Delegate to close_shared() which handles single-execution, error
        // logging, and pointer-nulling.  close_already_called prevents
        // double-execution if the resource was already closed through
        // another clone.
        if !close_ran_before_drop {
            if let Err(e) = self.close_shared() {
                log::warn!(
                    "cleanup failed for {} during Drop with code {}",
                    std::any::type_name::<T>(),
                    e.code,
                );
            }
        }

        // Validation: an owned resource built with `cleanup: None` AND
        // `cleanup_struct: false` has NO path that can free it. If it reaches
        // Drop without an explicit `close()`/`close_now()`, the underlying C
        // resource leaks. This is the exact shape of the
        // `AeronCncMetadata::load_from_file` bug. There is no legitimate case.
        if self.manual_close_required && !close_ran_before_drop {
            let resource = self.get();
            if !resource.is_null() {
                log::warn!(
                    "ManagedCResource<{}> dropped without explicit close and no cleanup closure \
                     — resource likely leaked. Call close()/close_now() before drop, or supply a \
                     cleanup closure at construction.",
                    std::any::type_name::<T>()
                );
            }
        }

        if self.cleanup_struct {
            let resource = self.get();
            if !resource.is_null() {
                #[cfg(feature = "extra-logging")]
                log::info!("closing rust struct resource: {:?}", resource);
                unsafe {
                    let _ = Box::from_raw(resource);
                }
                self.set_resource(std::ptr::null_mut());
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

/// Aeron C API error: code + optional message.
///
/// Construction is allocation-free (never reads `aeron_errmsg()`), so retry loops
/// that discard the error stay cheap. Attach the message via `capture_errmsg()`
/// (at the error site) or `with_message()`; `Display` / `get_last_err_message()`
/// otherwise read the live `aeron_errmsg()` buffer.
#[derive(Clone)]
pub struct AeronCError {
    pub code: i32,
    /// Attached via `capture_errmsg()` / `with_message()`; `None` otherwise.
    msg: Option<String>,
}

/// Equality is on `code` only — the attached message is advisory.
impl PartialEq for AeronCError {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
    }
}
impl Eq for AeronCError {}

impl AeronCError {
    /// Construct from an Aeron error code (`< 0` is failure).
    ///
    /// Allocation-free; does not read `aeron_errmsg()`. Use `capture_errmsg()` to
    /// attach the message when the error will be stored or logged later.
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
        AeronCError { code, msg: None }
    }

    /// [`Self::from_code`] with an attached message.
    pub fn with_message(code: i32, msg: impl Into<String>) -> Self {
        let mut err = Self::from_code(code);
        err.msg = Some(msg.into());
        err
    }

    /// Message attached via `capture_errmsg()` / `with_message()`, if any.
    pub fn message(&self) -> Option<&str> {
        self.msg.as_deref()
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

/// Typed error for `offer` / `try_claim` on a publication.
///
/// Aeron returns a negative *sentinel* instead of a stream position. These are not
/// errno-style codes — `-1` here means "not connected", not a generic error — hence
/// this dedicated type. Use [`Self::is_retryable`] to drive retry loops.
#[derive(Clone, PartialEq, Eq)]
pub enum AeronOfferError {
    /// No subscriber is connected (`-1`). Usually transient: a subscriber may
    /// connect later. Retryable.
    NotConnected,
    /// Flow control or a full term buffer is applying back pressure (`-2`).
    /// Retry after idling. Retryable.
    BackPressured,
    /// An administrative action (e.g. term rotation) is in progress (`-3`).
    /// Retry immediately. Retryable.
    AdminAction,
    /// The publication is closed (`-4`). Fatal for this handle.
    Closed,
    /// The maximum stream position was reached (`-5`). Fatal: a new publication
    /// (new session) is required.
    MaxPositionExceeded,
    /// Any other negative value (`-6` / unexpected). Fatal; inspect the inner
    /// [`AeronCError`] and `Aeron::errmsg()` for detail.
    Error(AeronCError),
}

impl AeronOfferError {
    /// Maps a raw offer/try_claim return to `Ok(position)` or a typed error.
    #[inline]
    pub fn from_position(position: i64) -> Result<i64, Self> {
        if position >= 0 {
            return Ok(position);
        }
        Err(match position {
            -1 => AeronOfferError::NotConnected,
            -2 => AeronOfferError::BackPressured,
            -3 => AeronOfferError::AdminAction,
            -4 => AeronOfferError::Closed,
            -5 => AeronOfferError::MaxPositionExceeded,
            _ => AeronOfferError::Error(AeronCError::from_code(position as i32)),
        })
    }

    /// A retry (possibly after idling / waiting for a subscriber) can succeed.
    #[inline]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            AeronOfferError::NotConnected | AeronOfferError::BackPressured | AeronOfferError::AdminAction
        )
    }

    /// The publication will never accept this offer again; recreate or give up.
    #[inline]
    pub fn is_fatal(&self) -> bool {
        !self.is_retryable()
    }
}

impl std::fmt::Display for AeronOfferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AeronOfferError::NotConnected => write!(f, "publication not connected"),
            AeronOfferError::BackPressured => write!(f, "publication back pressured"),
            AeronOfferError::AdminAction => write!(f, "publication admin action in progress"),
            AeronOfferError::Closed => write!(f, "publication closed"),
            AeronOfferError::MaxPositionExceeded => write!(f, "publication max position exceeded"),
            AeronOfferError::Error(e) => write!(f, "publication error (code {})", e.code),
        }
    }
}

impl std::fmt::Debug for AeronOfferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for AeronOfferError {}

/// # Handler
/// **Heap-allocated, reference-counted** callback holder for callbacks the C
/// client **retains** (fires later, possibly many times, from the conductor thread).
///
/// `Handler<T>` wraps `Arc<UnsafeCell<T>>`. The callback value lives on the heap
/// and is freed only when the last clone drops. The raw `clientd` pointer handed
/// to C is `&T` (via [`Handler::as_raw`]); C keeps firing it for as long as it
/// holds the callback, so the `Handler` must outlive that — methods that register
/// a retained callback ([`AeronContext::set_error_handler`], the image lifecycle
/// handlers on `async_add_subscription`, `set_on_available_image`, …) store a
/// clone of the `Handler` inside the registering resource (as a dependency), so
/// the value is guaranteed to outlive the C side's use of it. No manual
/// `release()` is needed.
///
/// # Heap vs stack — when to reach for `Handler` vs a `*_fn` / `*_once` method
///
/// | Callback kind | Where the closure lives | API |
/// |---|---|---|
/// | **Retained** (C stores it; fires later / repeatedly) | **heap** (`Handler`/`Arc`) | `set_error_handler(Some(Handler::new(...)))`, `async_add_subscription(.., Some(&h), ..)`, `poll(Some(&h), limit)` |
/// | **Sync / call-only** (C fires it during the call, then is done) | **stack** (borrowed `FnMut`, zero allocation) | `poll_fn(\|msg, hdr\| ..., limit)`, the generated `*_once` variants |
///
/// Prefer the stack form (`poll_fn`, `*_once`) on the hot path: it borrows the
/// closure for the duration of the call only, so there is no `Arc`, no heap
/// allocation, and the closure may borrow local state. Reach for `Handler`
/// (heap) when the callback must survive past the registering call — image
/// lifecycle handlers, error handlers, counters callbacks, anything the
/// conductor invokes asynchronously.
///
/// The reference count is atomic (`Arc`), so a `Handler` may be moved to another
/// thread; it is deliberately **not `Sync`** — callbacks fire from the conductor
/// thread and must not be shared concurrently.
///
/// ## Example
///
/// ```no_compile
/// use rusteron_code_gen::Handler;
/// let handler = Handler::new(your_value);
/// // the value is freed when the last clone of `handler` goes out of scope
/// ```
pub struct Handler<T> {
    inner: std::sync::Arc<UnsafeCell<T>>,
}

// Arc's refcount is atomic, so moving a Handler (or a clone) to another thread is fine
// as long as T itself is Send. No Sync: the C conductor thread may call into T via the
// raw clientd pointer, so shared &Handler across threads would race on T.
unsafe impl<T: Send> Send for Handler<T> {}

/// Under the `multi-threaded` feature, `Handler` is also `Sync` so callbacks can
/// be registered from one thread and the handle shared across threads. The
/// underlying `Arc<UnsafeCell<T>>` is `!Sync` by construction; this impl follows
/// the same "accepted unsoundness" policy as the handle-type impls — callbacks
/// fire from the conductor thread only, never concurrently.
#[cfg(feature = "multi-threaded")]
unsafe impl<T: Send> Sync for Handler<T> {}

impl<T> Clone for Handler<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Utility method for setting empty handlers
pub struct Handlers;

/// Type-level "no callback" sentinel.
///
/// Pass [`Handlers::NONE`] (which is `None::<&Handler<NoHandler>>`) to any
/// callback-accepting method to leave that callback unset. `NoHandler` implements
/// every generated callback trait, so the method's callback generic is inferred as
/// `NoHandler` without a per-callback helper or turbofish — including methods with
/// several callback parameters (e.g. `async_add_subscription`'s image handlers).
/// Its callback methods are unreachable: the C side is handed a null callback +
/// null clientd, so they can never fire.
pub struct NoHandler;

impl Handlers {
    /// `None` for any callback parameter — pins the callback generic to
    /// [`NoHandler`] so type inference works without a per-callback helper or
    /// turbofish. Replaces `Handlers::no_available_image_handler()` /
    /// `no_unavailable_image_handler()` / `no_reserved_value_supplier_handler()`
    /// / … with one constant. Parallels `Option::None`.
    pub const NONE: Option<&'static Handler<NoHandler>> = None;
}

impl<T> Handler<T> {
    pub fn new(handler: T) -> Self {
        let inner = std::sync::Arc::new(UnsafeCell::new(handler));
        #[cfg(feature = "extra-logging")]
        log::info!("creating handler {:?}", inner.get());
        Self { inner }
    }

    #[inline(always)]
    pub fn as_raw(&self) -> *mut std::os::raw::c_void {
        self.inner.get() as *mut std::os::raw::c_void
    }

    /// Get a mutable reference to the inner value.
    ///
    /// # Safety
    /// Caller must ensure that no other references to the inner value are active.
    #[inline(always)]
    pub unsafe fn get_mut(&self) -> &mut T {
        &mut *self.inner.get()
    }
}

impl<T> Deref for Handler<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.get() }
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

    /// Return `channel` with a `session-id` param added (replacing any existing one).
    ///
    /// Mirrors Java's `ChannelUri.addSessionId` — the standard way to build a channel
    /// that joins a specific session, e.g. when subscribing to an archive replay:
    ///
    /// ```
    /// # use rusteron_code_gen::ChannelUri;
    /// assert_eq!(
    ///     ChannelUri::add_session_id("aeron:ipc", 42),
    ///     "aeron:ipc?session-id=42"
    /// );
    /// assert_eq!(
    ///     ChannelUri::add_session_id("aeron:udp?endpoint=localhost:20121", -123),
    ///     "aeron:udp?endpoint=localhost:20121|session-id=-123"
    /// );
    /// ```
    pub fn add_session_id(channel: &str, session_id: i32) -> String {
        Self::set_param(channel, "session-id", &session_id.to_string())
    }

    /// Return `channel` with URI param `key=value` set, replacing an existing `key`
    /// param if present. Other params keep their relative order; `key` goes last.
    pub fn set_param(channel: &str, key: &str, value: &str) -> String {
        let (base, params) = match channel.split_once('?') {
            None => (channel, ""),
            Some((base, params)) => (base, params),
        };
        let mut out = String::with_capacity(channel.len() + key.len() + value.len() + 2);
        out.push_str(base);
        out.push('?');
        for param in params.split('|') {
            if param.is_empty() || param.split('=').next() == Some(key) {
                continue;
            }
            out.push_str(param);
            out.push('|');
        }
        out.push_str(key);
        out.push('=');
        out.push_str(value);
        out
    }
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

        // Background threads from earlier #[serial] tests (driver/conductor shutdown,
        // captured log buffers) can allocate or free during our window and produce
        // spurious deltas. Take the baseline only once the global count is stable.
        let mut before = current_allocs();
        let settle_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            std::thread::sleep(std::time::Duration::from_millis(5));
            let now = current_allocs();
            if now == before || std::time::Instant::now() > settle_deadline {
                before = now;
                break;
            }
            before = now;
        }

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

/// `format!` for C strings: builds the formatted [`String`] and converts it to a
/// [`CString`](std::ffi::CString) in one visibly-named step.
///
/// This is the recommended way to build **dynamic** channel URIs and other C-string
/// arguments. The `c`-prefix keeps the heap allocation greppable and visible at the
/// call site — important for latency-sensitive code review — while removing the
/// `format!(...).into_c_string()` noise:
///
/// ```
/// # use rusteron_code_gen::cformat;
/// let port = 4040;
/// let uri = cformat!("aeron:udp?endpoint=localhost:{port}");
/// assert_eq!(uri.to_bytes(), b"aeron:udp?endpoint=localhost:4040");
/// ```
///
/// The three-tier pattern for C-string arguments (cheapest first):
/// 1. **`c"aeron:ipc"` literals** — compile-time `&'static CStr`, zero runtime cost.
///    Use for every constant channel/name.
/// 2. **`cformat!(...)`** — one heap allocation (the formatted `String`; `CString::new`
///    reuses its buffer). Use for dynamic URIs built once per stream/reconnect.
/// 3. **Reuse** — build the `CString` once, store it, pass `&it` on every call
///    (zero-copy via `&CString → &CStr` deref). Use for anything on a repeated path.
///
/// Panics if the formatted string contains an interior nul byte.
#[macro_export]
macro_rules! cformat {
    ($($arg:tt)*) => {
        ::std::ffi::CString::new(::std::format!($($arg)*))
            .expect("nul byte in cformat! string")
    };
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
    fn clones_share_the_same_clientd_pointer() {
        let handler = Handler::new(42u32);
        let clone = handler.clone();
        // C receives the same clientd pointer regardless of which clone
        // registered it, so callbacks always see the same value.
        assert_eq!(handler.as_raw(), clone.as_raw());
        assert_eq!(*handler, 42);
    }

    #[test]
    fn value_dropped_exactly_once_when_last_clone_drops() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct Counted;
        impl Drop for Counted {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::SeqCst);
            }
        }

        let handler = Handler::new(Counted);
        let clone = handler.clone();
        drop(handler);
        assert_eq!(DROPS.load(Ordering::SeqCst), 0, "value must outlive remaining clones");
        drop(clone);
        assert_eq!(DROPS.load(Ordering::SeqCst), 1, "value freed exactly once on last drop");
    }
}

#[cfg(test)]
mod managed_c_resource_lifecycle_tests {
    use super::*;

    // These tests pin down `manual_close_required` — the field that powers the
    // Drop-time leak warning. The exact `AeronCncMetadata::load_from_file` bug
    // was a `new(_, None, false)` resource, so the first test asserts that
    // construction shape trips the flag. The resource pointers are dummies
    // (never dereferenced); only the field value is checked.

    #[test]
    fn manual_close_required_true_for_none_cleanup_no_struct() {
        // The bug shape: owned, no cleanup closure, no Rust struct ownership.
        let r: ManagedCResource<u8> = ManagedCResource::new(
            |ctx| {
                unsafe { *ctx = 0x1 as *mut u8 };
                1
            },
            None,
            false,
        )
        .unwrap_or_else(|e| panic!("init failed: code {}", e.code));
        assert!(
            r.manual_close_required,
            "owned + None cleanup + no struct ownership must require manual close"
        );
    }

    #[test]
    fn manual_close_required_false_when_cleanup_closure_present() {
        let r: ManagedCResource<u8> = ManagedCResource::new(
            |ctx| {
                unsafe { *ctx = 0x1 as *mut u8 };
                1
            },
            Some(Box::new(|_ctx| 0)),
            false,
        )
        .unwrap_or_else(|e| panic!("init failed: code {}", e.code));
        assert!(
            !r.manual_close_required,
            "real cleanup closure means Drop frees the resource — no warning needed"
        );
    }

    #[test]
    fn manual_close_required_false_when_struct_owned() {
        // Generated `new(_, None, true)` resources: Rust frees the Box itself
        // via Box::from_raw in the cleanup_struct branch of Drop.
        let r: ManagedCResource<u8> = ManagedCResource::new(
            |ctx| {
                unsafe { *ctx = Box::into_raw(Box::new(0u8)) };
                1
            },
            None,
            true,
        )
        .unwrap_or_else(|e| panic!("init failed: code {}", e.code));
        assert!(
            !r.manual_close_required,
            "cleanup_struct=true means Rust owns and frees the struct — no warning needed"
        );
    }
}
