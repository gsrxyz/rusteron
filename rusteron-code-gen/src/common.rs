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
        // The C client frees async resources when their poll completes (created,
        // errored, or cancelled). Null the stale pointer so any later use faults
        // deterministically on null instead of reading freed memory.
        self.resource.set(std::ptr::null_mut());
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
    #[allow(dead_code)]
    pub(crate) fn close_shared_with(
        &self,
        mut custom_cleanup: impl FnMut(*mut *mut T) -> i32,
    ) -> Result<(), AeronCError> {
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
/// The error code is derived from Aeron C API calls. When the error is created from a
/// live C call (`from_c_code`), the human-readable `aeron_errmsg()` text is snapshotted
/// eagerly for non-retryable codes, so it still describes *this* error when displayed
/// later — see [`Self::message`].
#[derive(Clone)]
pub struct AeronCError {
    pub code: i32,
    /// Message snapshotted from `aeron_errmsg()` at construction time (non-retryable
    /// codes constructed via `from_c_code` only). `None` for sentinel/retryable codes,
    /// where allocating would tax retry loops.
    msg: Option<Box<str>>,
}

/// Equality is on `code` only — the snapshotted message is advisory.
impl PartialEq for AeronCError {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
    }
}
impl Eq for AeronCError {}

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
        AeronCError { code, msg: None }
    }

    /// Like [`Self::from_code`], but attaching a human-readable message captured at
    /// the error site (e.g. a snapshot of `aeron_errmsg()`).
    pub fn with_message(code: i32, msg: impl Into<Box<str>>) -> Self {
        let mut err = Self::from_code(code);
        err.msg = Some(msg.into());
        err
    }

    /// Message snapshotted when this error was created, if any. Unlike reading the
    /// global `aeron_errmsg()` later, this cannot be overwritten by a subsequent
    /// error on the same thread.
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
///
/// `Handler` is a reference-counted callback holder passed to Aeron C callbacks as the
/// `clientd` pointer.
///
/// The callback value is freed automatically when the last `Handler` clone drops. Methods
/// that register a callback the C client retains (e.g. `AeronContext::set_error_handler`,
/// image lifecycle handlers on subscription add) store a clone of the `Handler` inside the
/// registering resource, so the value is guaranteed to outlive the C side's use of it —
/// no manual `release()` is needed.
///
/// The reference count is atomic (`Arc`), so a `Handler` may be moved to another thread;
/// it is deliberately not `Sync` — callbacks may be invoked from the conductor thread and
/// must not be shared concurrently.
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

impl<T> Clone for Handler<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Utility method for setting empty handlers
pub struct Handlers;

impl<T> Handler<T> {
    pub fn new(handler: T) -> Self {
        let inner = std::sync::Arc::new(UnsafeCell::new(handler));
        #[cfg(feature = "extra-logging")]
        log::info!("creating handler {:?}", inner.get());
        Self { inner }
    }

    #[deprecated(note = "handlers no longer leak; use Handler::new — the value is freed when the last clone drops")]
    pub fn leak(handler: T) -> Self {
        Self::new(handler)
    }

    #[deprecated(note = "no longer needed; the handler is freed automatically when the last clone drops")]
    pub fn release(&mut self) {}

    #[inline(always)]
    pub fn as_raw(&self) -> *mut std::os::raw::c_void {
        self.inner.get() as *mut std::os::raw::c_void
    }
}

impl<T> Deref for Handler<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.get() }
    }
}

impl<T> DerefMut for Handler<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.inner.get() }
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
