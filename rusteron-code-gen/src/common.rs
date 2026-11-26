// Compilation contexts (read before adding code here)
// This file compiles in TWO contexts:
//   1. as `mod common` inside rusteron-code-gen itself (unit tests, shared types), and
//   2. verbatim inside every generated crate's `aeron.rs` (via COMMON_CODE include_str).
// Consequence: nothing here may depend on impls that live in `aeron_custom*.rs`. Those
// exist only in context 2 (e.g. AeronCError's Debug/Display). That is why AeronOfferError
// hand-rolls its Debug. Crate-specific code belongs in `aeron_custom.rs` (all crates) or
// `aeron_custom_<crate>.rs` (one crate); build-script-only code goes in `build_common.rs`.
//

use crate::AeronErrorType::Unknown;
#[cfg(feature = "backtrace")]
use std::backtrace::Backtrace;
use std::fmt::Formatter;
// unused here, but the generated code that includes this file uses it
#[allow(unused_imports)]
use std::mem::MaybeUninit;
#[allow(unused_imports)]
use std::ops::DerefMut;
use std::time::Duration;

/// Shared ownership of a native resource. Cloning a handle never copies the C object.
pub type ResourceRef<T> = std::sync::Arc<T>;

/// Storage rules for cleanup and retained dependencies.
pub trait ResourcePolicy: 'static {
    type Erased: ?Sized + std::any::Any;
    type Cleanup<T>: FnMut(*mut *mut T) -> i32;
    fn as_any(value: &Self::Erased) -> &dyn std::any::Any;
}

/// Local resources may retain local callbacks and other local resources.
pub enum LocalResource {}
/// Shared owners retain only values that can be accessed and dropped on any thread.
pub enum SharedResource {}

impl ResourcePolicy for LocalResource {
    type Erased = dyn std::any::Any;
    type Cleanup<T> = Box<dyn FnMut(*mut *mut T) -> i32>;
    fn as_any(value: &Self::Erased) -> &dyn std::any::Any {
        value
    }
}
impl ResourcePolicy for SharedResource {
    type Erased = dyn std::any::Any + Send + Sync;
    type Cleanup<T> = Box<dyn FnMut(*mut *mut T) -> i32 + Send>;
    fn as_any(value: &Self::Erased) -> &dyn std::any::Any {
        value
    }
}

pub trait AcceptDependency<D>: ResourcePolicy {
    fn retain(value: D) -> ResourceRef<Self::Erased>;
}
impl<D: std::any::Any> AcceptDependency<D> for LocalResource {
    fn retain(value: D) -> ResourceRef<Self::Erased> {
        ResourceRef::new(value)
    }
}
impl<D: std::any::Any + Send + Sync> AcceptDependency<D> for SharedResource {
    fn retain(value: D) -> ResourceRef<Self::Erased> {
        ResourceRef::new(value)
    }
}

pub trait CopyDependencies<From: ResourcePolicy>: ResourcePolicy {
    fn copy(value: ResourceRef<From::Erased>) -> ResourceRef<Self::Erased>;
}
impl<P: ResourcePolicy> CopyDependencies<P> for P {
    fn copy(value: ResourceRef<P::Erased>) -> ResourceRef<Self::Erased> {
        value
    }
}
impl CopyDependencies<SharedResource> for LocalResource {
    fn copy(value: ResourceRef<<SharedResource as ResourcePolicy>::Erased>) -> ResourceRef<Self::Erased> {
        value
    }
}

/// Native storage and cleanup policy selected by the generator.
///
/// # Safety
/// A shared policy requires that the native allocation can be retained and closed
/// from any thread. All safe constructors must use this policy's checked dependency
/// and cleanup storage. This does not make concurrent native method calls safe.
/// `CachedPointer` additionally requires that native calls cannot release or
/// replace the allocation while an owning handle remains alive.
pub unsafe trait NativeResource: Sized + 'static {
    type Policy: ResourcePolicy;
    /// How an owning handle reads its native pointer. Cached pointers are only
    /// valid when native calls cannot release or replace the allocation.
    type Pointer: ResourcePointer<Self>;
}

#[cfg(test)]
unsafe impl NativeResource for i32 {
    type Policy = LocalResource;
    type Pointer = ReloadPointer;
}
#[cfg(test)]
unsafe impl NativeResource for u8 {
    type Policy = LocalResource;
    type Pointer = ReloadPointer;
}

pub type CleanupBox<T> = <<T as NativeResource>::Policy as ResourcePolicy>::Cleanup<T>;

/// Access used by generated handles. Implementations are sealed to this crate.
#[doc(hidden)]
pub trait ResourceHandle: resource_sealed::Handle + Sized {
    type Native: NativeResource;
    type Storage: ResourceStorage<Self::Native>;
    type Borrowed: ResourceHandle<Native = Self::Native>;
}
#[allow(dead_code)]
pub(crate) trait ResourceHandleExt: ResourceHandle {
    fn resource(&self) -> &Self::Storage;
    fn from_resource(inner: Self::Storage) -> Self;
    fn from_borrowed(inner: Borrowed<Self::Native>) -> Self::Borrowed;
}
#[doc(hidden)]
pub(crate) trait OwnedResourceHandle: ResourceHandleExt {
    fn from_owned(inner: CResource<Self::Native>) -> Self;
}

mod resource_sealed {
    pub trait Storage {}
    pub trait Handle {}
    pub trait Pointer {}
}

/// Pointer access selected by the generator, independently of ownership policy.
#[doc(hidden)]
pub trait ResourcePointer<T: NativeResource>: resource_sealed::Pointer + Clone {
    fn new(owner: &ManagedCResource<T>) -> Self;
    fn get(&self, owner: &ManagedCResource<T>) -> *mut T;
}

/// Native operations can invalidate this allocation, so read shared state.
#[doc(hidden)]
#[derive(Clone)]
pub struct ReloadPointer;
impl resource_sealed::Pointer for ReloadPointer {}
impl<T: NativeResource> ResourcePointer<T> for ReloadPointer {
    #[inline]
    fn new(_: &ManagedCResource<T>) -> Self {
        Self
    }
    #[inline(always)]
    fn get(&self, owner: &ManagedCResource<T>) -> *mut T {
        owner.get()
    }
}

/// Rust owns the boxed value until the last owner drops; its address is stable.
#[doc(hidden)]
pub struct CachedPointer<T>(std::ptr::NonNull<T>);
impl<T> Clone for CachedPointer<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0)
    }
}
impl<T> resource_sealed::Pointer for CachedPointer<T> {}
impl<T: NativeResource> ResourcePointer<T> for CachedPointer<T> {
    #[inline]
    fn new(owner: &ManagedCResource<T>) -> Self {
        Self(std::ptr::NonNull::new(owner.get()).expect("cached value requires a native allocation"))
    }
    #[inline(always)]
    fn get(&self, _: &ManagedCResource<T>) -> *mut T {
        self.0.as_ptr()
    }
}

/// Concrete storage for a generated native handle. Calls are statically dispatched.
#[doc(hidden)]
pub trait ResourceStorage<T: NativeResource>: resource_sealed::Storage + std::fmt::Debug {
    fn get(&self) -> *mut T;
    #[inline]
    fn as_owned(&self) -> Option<&ResourceRef<ManagedCResource<T>>> {
        None
    }
}
// Cleanup and dependency mutation are internal. Exposing these methods would let
// safe callers close a native allocation while another handle still uses it.
#[allow(dead_code)]
pub(crate) trait ResourceStorageExt<T: NativeResource>: ResourceStorage<T> {
    #[inline]
    fn add_dependency<D: std::any::Any>(&self, dep: D)
    where
        T::Policy: AcceptDependency<D>,
    {
        self.as_owned()
            .expect("dependencies require a heap owner")
            .add_dependency(dep);
    }
    #[cfg(test)]
    fn dependency_len(&self) -> usize {
        self.as_owned().map_or(0, |owner| owner.dependency_len())
    }
    #[inline]
    fn get_dependency<V: Clone + 'static>(&self) -> Option<V> {
        self.as_owned().and_then(|owner| owner.get_dependency())
    }

    fn add_handle_dependency<H: ResourceHandleExt>(&self, handle: &H)
    where
        T::Policy: AcceptDependency<ResourceRef<ManagedCResource<H::Native>>>,
    {
        if let Some(owner) = self.as_owned() {
            owner.add_handle_dependency(handle);
        }
    }

    fn get_handle_dependency<H: OwnedResourceHandle>(&self) -> Option<H> {
        self.as_owned().and_then(|owner| owner.get_handle_dependency())
    }

    fn get_handle_dependency_matching<H: OwnedResourceHandle>(&self, pointer: *mut H::Native) -> Option<H> {
        let owner = self.as_owned()?;
        let dependencies = owner.dependencies.lock().unwrap_or_else(|e| e.into_inner());
        dependencies.iter().find_map(|dependency| {
            let resource = T::Policy::as_any(dependency.value.as_ref())
                .downcast_ref::<ResourceRef<ManagedCResource<H::Native>>>()?;
            (resource.get() == pointer).then(|| H::from_owned(CResource::new(resource.clone())))
        })
    }

    fn copy_dependencies_from<U: NativeResource>(&self, source: &impl ResourceStorage<U>)
    where
        T::Policy: CopyDependencies<U::Policy>,
    {
        if let (Some(target), Some(source)) = (self.as_owned(), source.as_owned()) {
            target.copy_dependencies_from(source);
        }
    }

    /// Transfer dependencies after C has consumed a successful async operation.
    fn take_dependencies_from<U: NativeResource>(&self, source: &impl ResourceStorage<U>)
    where
        T::Policy: CopyDependencies<U::Policy>,
    {
        if let (Some(target), Some(source)) = (self.as_owned(), source.as_owned()) {
            let dependencies = std::mem::take(&mut *source.dependencies.lock().unwrap_or_else(|e| e.into_inner()));
            target
                .dependencies
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend(dependencies.into_iter().map(|dep| ResourceDependency {
                    callback: dep.callback,
                    in_use: dep.in_use,
                    value: <T::Policy as CopyDependencies<U::Policy>>::copy(dep.value),
                }));
        }
    }

    /// Archive contexts are local. Their native setters do not call callbacks,
    /// so this check also prevents a callback from freeing itself during a call.
    fn check_callback_replacement(&self, key: &'static str) -> Result<(), AeronCError> {
        let owner = self
            .as_owned()
            .ok_or_else(|| AeronCError::with_message(-1, "a borrowed wrapper cannot retain a callback"))?;
        let dependencies = owner.dependencies.lock().unwrap_or_else(|e| e.into_inner());
        let active = dependencies.iter().any(|dep| {
            dep.callback == Some(key)
                && dep
                    .in_use
                    .is_some_and(|check| check(T::Policy::as_any(dep.value.as_ref())))
        });
        if active {
            return Err(AeronCError::with_message(
                -1,
                "a running or borrowed callback cannot be replaced",
            ));
        }
        Ok(())
    }

    fn check_configurable(&self) -> Result<(), AeronCError> {
        self.as_owned()
            .ok_or_else(|| AeronCError::with_message(-1, "a borrowed context cannot be changed"))?
            .check_not_frozen()
    }

    #[inline]
    fn close_resource(&self) -> Result<(), AeronCError> {
        self.as_owned().map_or(Ok(()), |owner| owner.close_shared())
    }
    #[inline]
    fn close_resource_with(&self, cleanup: impl FnMut(*mut *mut T) -> i32) -> Result<(), AeronCError> {
        self.as_owned().map_or(Ok(()), |owner| owner.close_shared_with(cleanup))
    }
    #[inline]
    fn close_resource_deferred_if_shared(&self) -> Result<(), AeronCError> {
        match self.as_owned() {
            Some(owner) if ResourceRef::strong_count(owner) == 1 => owner.close_shared(),
            _ => Ok(()),
        }
    }
}

impl<T: NativeResource, S: ResourceStorage<T>> ResourceStorageExt<T> for S {}

/// Shared ownership of a native allocation. Clone retains the allocation; it
/// never copies the native object. Rust-owned values cache one additional pointer;
/// resources that native calls can invalidate retain the shared pointer load.
pub struct CResource<T: NativeResource>(
    ResourceRef<ManagedCResource<T>>,
    T::Pointer,
    std::marker::PhantomData<*mut ()>,
);
impl<T: NativeResource> CResource<T> {
    #[inline]
    pub(crate) fn new(owner: ResourceRef<ManagedCResource<T>>) -> Self {
        let pointer = T::Pointer::new(&owner);
        Self(owner, pointer, std::marker::PhantomData)
    }
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn owned_value(value: T) -> Self {
        let resource = ManagedCResource::new(
            move |ptr| {
                // SAFETY: initialise supplies a writable output pointer.
                unsafe { *ptr = Box::into_raw(Box::new(value)) };
                0
            },
            None,
            true,
        )
        .unwrap_or_else(|_| unreachable!("Box allocation returned null"));
        Self::new(ResourceRef::new(resource))
    }
}
impl<T: NativeResource> Clone for CResource<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone(), std::marker::PhantomData)
    }
}
impl<T: NativeResource> resource_sealed::Storage for CResource<T> {}
impl<T: NativeResource> ResourceStorage<T> for CResource<T> {
    #[inline(always)]
    fn get(&self) -> *mut T {
        self.1.get(&self.0)
    }
    #[inline(always)]
    fn as_owned(&self) -> Option<&ResourceRef<ManagedCResource<T>>> {
        Some(&self.0)
    }
}
impl<T: NativeResource> std::fmt::Debug for CResource<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Owned").field(&self.0).finish()
    }
}

/// A cached pointer for native handles whose cleanup waits for the last owner.
/// Async operations use `CResource` instead because C can release them early.
pub struct LiveCResource<T: NativeResource> {
    ptr: std::ptr::NonNull<T>,
    owner: CResource<T>,
}
impl<T: NativeResource> From<CResource<T>> for LiveCResource<T> {
    #[inline]
    fn from(owner: CResource<T>) -> Self {
        Self {
            ptr: std::ptr::NonNull::new(owner.get()).expect("live handle requires a native allocation"),
            owner,
        }
    }
}
impl<T: NativeResource> LiveCResource<T> {
    /// A live handle always has an owner, unlike `ResourceStorage::as_owned`
    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) fn owner(&self) -> &ResourceRef<ManagedCResource<T>> {
        &self.owner.0
    }
}
impl<T: NativeResource> Clone for LiveCResource<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            owner: self.owner.clone(),
        }
    }
}
impl<T: NativeResource> resource_sealed::Storage for LiveCResource<T> {}
impl<T: NativeResource> ResourceStorage<T> for LiveCResource<T> {
    #[inline(always)]
    fn get(&self) -> *mut T {
        self.ptr.as_ptr()
    }
    #[inline(always)]
    fn as_owned(&self) -> Option<&ResourceRef<ManagedCResource<T>>> {
        self.owner.as_owned()
    }
}
impl<T: NativeResource> std::fmt::Debug for LiveCResource<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.owner.fmt(f)
    }
}

/// An initialised native value stored inline. It cannot be cloned implicitly.
/// Borrow the wrapper while passing its address to C. Use `PinnedInline` when C
/// must retain the address beyond a call.
pub struct Inline<T: NativeResource>(std::cell::UnsafeCell<T>, std::marker::PhantomData<*mut ()>);
impl<T: NativeResource> Inline<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value), std::marker::PhantomData)
    }
}
impl<T: NativeResource> resource_sealed::Storage for Inline<T> {}
impl<T: NativeResource> ResourceStorage<T> for Inline<T> {
    #[inline(always)]
    fn get(&self) -> *mut T {
        self.0.get()
    }
}
impl<T: NativeResource> std::fmt::Debug for Inline<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Inline").field(&self.0.get()).finish()
    }
}

/// An inline native result whose pointer fields borrow the queried handle.
pub struct ScopedInline<'a, T: NativeResource> {
    value: Inline<T>,
    _owner: std::marker::PhantomData<&'a ()>,
}
impl<T: NativeResource> resource_sealed::Storage for ScopedInline<'_, T> {}
impl<T: NativeResource> ResourceStorage<T> for ScopedInline<'_, T> {
    #[inline(always)]
    fn get(&self) -> *mut T {
        self.value.get()
    }
}
impl<T: NativeResource> std::fmt::Debug for ScopedInline<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

/// Inline storage whose address stays fixed once pinned.
/// It does not implement `ResourceStorage`. Getting its pointer requires `Pin`.
pub struct PinnedInline<T: NativeResource> {
    value: std::cell::UnsafeCell<T>,
    _pin: std::marker::PhantomPinned,
}
impl<T: NativeResource> PinnedInline<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: std::cell::UnsafeCell::new(value),
            _pin: std::marker::PhantomPinned,
        }
    }
    #[inline(always)]
    pub fn get(self: std::pin::Pin<&Self>) -> *mut T {
        self.value.get()
    }
}

/// Non-owning pointer storage. Only unsafe constructors or lifetime-bound
/// generated getters can create it. It has no `Clone` implementation.
pub struct Borrowed<T: NativeResource>(std::ptr::NonNull<T>);
impl<T: NativeResource> Borrowed<T> {
    /// # Safety
    /// `ptr` is non-null and points to a live native object.
    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) unsafe fn new_unchecked(ptr: *mut T) -> Self {
        Self(unsafe { std::ptr::NonNull::new_unchecked(ptr) })
    }

    /// Panics if `ptr` is null
    ///
    /// # Safety
    /// `ptr` points to a live native object for as long as this borrow is used
    #[inline]
    pub(crate) unsafe fn new(ptr: *mut T) -> Self {
        Self(std::ptr::NonNull::new(ptr).expect("native getter returned null"))
    }
}
impl<T: NativeResource> resource_sealed::Storage for Borrowed<T> {}
impl<T: NativeResource> ResourceStorage<T> for Borrowed<T> {
    #[inline(always)]
    fn get(&self) -> *mut T {
        self.0.as_ptr()
    }
}
impl<T: NativeResource> std::fmt::Debug for Borrowed<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Borrowed").field(&self.0).finish()
    }
}

/// Storage allowed for audited shared wrappers. Retained dependencies and cleanup
/// must also be safe to use and drop across threads.
#[doc(hidden)]
pub trait SharedStorage<T: NativeResource>: ResourceStorage<T> {}
impl<T: NativeResource> SharedStorage<T> for CResource<T> where ResourceRef<ManagedCResource<T>>: Send + Sync {}
impl<T: NativeResource> SharedStorage<T> for LiveCResource<T> where ResourceRef<ManagedCResource<T>>: Send + Sync {}
impl<T: NativeResource> SharedStorage<T> for Borrowed<T> {}

/// A pointer borrowed from an owner. It cannot escape that borrow or clone the
/// owning handle. No allocation or reference-count operation is needed.
pub struct Ref<'a, T: ResourceHandle> {
    value: T::Borrowed,
    owner: std::marker::PhantomData<&'a T>,
}
impl<T: ResourceHandle> Ref<'_, T> {
    #[inline(always)]
    pub(crate) fn new(value: T::Borrowed) -> Self {
        Self {
            value,
            owner: std::marker::PhantomData,
        }
    }
}
impl<T: ResourceHandle> std::ops::Deref for Ref<'_, T> {
    type Target = T::Borrowed;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl<T: ResourceHandle> std::fmt::Debug for Ref<'_, T>
where
    T::Borrowed: std::fmt::Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

/// A child pointer with an owner that keeps its allocation alive.
/// Cloning it retains that owner. `as_ref()` borrows the child without a clone.
pub struct OwnedWithReference<Owner, T> {
    ptr: std::ptr::NonNull<()>,
    _owner: Owner,
    _child: std::marker::PhantomData<T>,
}
impl<Owner, T> OwnedWithReference<Owner, T> {
    /// # Safety
    /// The child must stay at `ptr` until the final owner drops. Explicit close
    /// must also wait for these retained owners.
    #[allow(dead_code)]
    #[inline]
    pub(crate) unsafe fn new(owner: Owner, ptr: *mut T::Native) -> Self
    where
        T: ResourceHandle,
    {
        Self {
            ptr: std::ptr::NonNull::new(ptr.cast()).expect("retained reference requires a live pointer"),
            _owner: owner,
            _child: std::marker::PhantomData,
        }
    }
    /// Borrow the child while this retained owner is alive.
    #[allow(private_bounds)]
    #[inline(always)]
    pub fn as_ref(&self) -> Ref<'_, T>
    where
        T: ResourceHandleExt,
    {
        self.borrow()
    }
    #[inline(always)]
    pub(crate) fn borrow(&self) -> Ref<'_, T>
    where
        T: ResourceHandleExt,
    {
        // SAFETY: the retained owner keeps this pointer live through the borrow.
        Ref::new(T::from_borrowed(unsafe { Borrowed::new(self.ptr.as_ptr().cast()) }))
    }
}
// SAFETY: the child wrapper's audited thread traits cover native access. The
// owner bound also covers retaining and dropping its dependencies on that thread.
unsafe impl<Owner: Send, T: ResourceHandle + Send> Send for OwnedWithReference<Owner, T> where T::Borrowed: Send {}
unsafe impl<Owner: Sync, T: ResourceHandle + Sync> Sync for OwnedWithReference<Owner, T> where T::Borrowed: Sync {}

impl<Owner: Clone, T> Clone for OwnedWithReference<Owner, T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            _owner: self._owner.clone(),
            _child: std::marker::PhantomData,
        }
    }
}

/// Owns a C resource and calls its cleanup closure when the last handle drops.
///
/// The closure calls the C close function, such as `aeron_close`. Reference-counted
/// dependencies keep parent resources open until their children drop.
// Performance: these fields are off the hot path. Publications, subscriptions,
// images, counters and the client use `LiveCResource`, which caches the pointer,
// so offer and poll never read them. A `CResource` of a Rust-allocated value,
// such as `AeronBufferClaim::default()`, caches the pointer too. Other
// `CResource` handles, such as contexts, pay one Acquire load per `get()`, a
// plain load on x86-64 and `ldapur` on aarch64
// The mutexes, `OnceLock` and flags only run on start, close, dependency changes
// and blocking waits
#[allow(dead_code)]
pub struct ManagedCResource<T: NativeResource> {
    resource: std::sync::atomic::AtomicPtr<T>,
    cleanup: std::sync::Mutex<Option<CleanupBox<T>>>,
    cleanup_struct: bool,
    manual_close_required: bool,
    close_already_called: std::sync::atomic::AtomicBool,
    resource_released: std::sync::atomic::AtomicBool,
    dependencies: std::sync::Mutex<Vec<ResourceDependency<<T::Policy as ResourcePolicy>::Erased>>>,
    frozen: std::sync::atomic::AtomicBool,
    operation: std::sync::atomic::AtomicBool,
    started: std::sync::atomic::AtomicBool,
}

struct ResourceDependency<E: ?Sized> {
    callback: Option<&'static str>,
    in_use: Option<fn(&dyn std::any::Any) -> bool>,
    value: ResourceRef<E>,
}
impl<E: ?Sized> Clone for ResourceDependency<E> {
    fn clone(&self) -> Self {
        Self {
            callback: self.callback,
            in_use: self.in_use,
            value: self.value.clone(),
        }
    }
}

impl<T: NativeResource> std::fmt::Debug for ManagedCResource<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedCResource")
            .field("resource", &self.get())
            .field("type", &std::any::type_name::<T>())
            .finish()
    }
}

#[allow(dead_code)]
impl<T: NativeResource> ManagedCResource<T> {
    pub(crate) fn new(
        init: impl FnOnce(*mut *mut T) -> i32,
        cleanup: Option<CleanupBox<T>>,
        cleanup_struct: bool,
    ) -> Result<Self, AeronCError> {
        let resource = Self::initialise(init)?;
        let manual_close_required = cleanup.is_none() && !cleanup_struct;
        Ok(Self {
            resource: std::sync::atomic::AtomicPtr::new(resource),
            cleanup: std::sync::Mutex::new(cleanup),
            cleanup_struct,
            manual_close_required,
            close_already_called: std::sync::atomic::AtomicBool::new(false),
            resource_released: std::sync::atomic::AtomicBool::new(false),
            dependencies: std::sync::Mutex::new(Vec::new()),
            frozen: std::sync::atomic::AtomicBool::new(false),
            operation: std::sync::atomic::AtomicBool::new(false),
            started: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub(crate) fn initialise(init: impl FnOnce(*mut *mut T) -> i32) -> Result<*mut T, AeronCError> {
        let mut resource = std::ptr::null_mut();
        let result = init(&mut resource);
        if result < 0 || resource.is_null() {
            return Err(AeronCError::from_code(result));
        }
        Ok(resource)
    }

    #[inline(always)]
    pub fn get(&self) -> *mut T {
        self.resource.load(std::sync::atomic::Ordering::Acquire)
    }

    #[inline(always)]
    fn set_resource(&self, value: *mut T) {
        self.resource.store(value, std::sync::atomic::Ordering::Release);
    }

    #[inline(always)]
    fn get_close_already_called(&self) -> bool {
        self.close_already_called.load(std::sync::atomic::Ordering::Acquire)
    }

    #[inline(always)]
    fn set_close_already_called(&self, value: bool) {
        self.close_already_called
            .store(value, std::sync::atomic::Ordering::Release);
    }

    #[inline(always)]
    fn get_resource_released(&self) -> bool {
        self.resource_released.load(std::sync::atomic::Ordering::Acquire)
    }

    #[inline(always)]
    fn set_resource_released(&self, value: bool) {
        self.resource_released
            .store(value, std::sync::atomic::Ordering::Release);
    }

    /// # Safety
    /// No other reference or native operation may access the object while this
    /// mutable reference is in use.
    #[inline(always)]
    pub unsafe fn get_mut(&self) -> &mut T {
        unsafe { &mut *self.get() }
    }

    #[inline]
    pub fn add_dependency<D: std::any::Any>(&self, dep: D)
    where
        T::Policy: AcceptDependency<D>,
    {
        self.dependencies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(ResourceDependency {
                callback: None,
                in_use: None,
                value: <T::Policy as AcceptDependency<D>>::retain(dep),
            });
    }

    /// Replace one native callback registration. Existing connection snapshots
    /// keep their own reference to the previous handler.
    pub(crate) fn replace_callback_dependency<D: std::any::Any>(&self, key: &'static str, callback: Option<D>)
    where
        T::Policy: AcceptDependency<D>,
    {
        self.replace_callback_dependency_checked(key, callback, None);
    }

    pub(crate) fn replace_handler_dependency<D: 'static, P: CallbackPolicy>(
        &self,
        key: &'static str,
        callback: Option<Handler<D, P>>,
    ) where
        T::Policy: AcceptDependency<Handler<D, P>>,
    {
        fn in_use<D: 'static, P: CallbackPolicy>(value: &dyn std::any::Any) -> bool {
            let handler = value.downcast_ref::<Handler<D, P>>().unwrap();
            P::is_in_use(&handler.inner)
        }
        self.replace_callback_dependency_checked(key, callback, Some(in_use::<D, P>));
    }

    fn replace_callback_dependency_checked<D: std::any::Any>(
        &self,
        key: &'static str,
        callback: Option<D>,
        in_use: Option<fn(&dyn std::any::Any) -> bool>,
    ) where
        T::Policy: AcceptDependency<D>,
    {
        let replacement = callback.map(|callback| ResourceDependency {
            callback: Some(key),
            in_use,
            value: <T::Policy as AcceptDependency<D>>::retain(callback),
        });
        let previous = {
            let mut dependencies = self.dependencies.lock().unwrap_or_else(|e| e.into_inner());
            match (
                dependencies.iter().position(|dep| dep.callback == Some(key)),
                replacement,
            ) {
                (Some(index), Some(replacement)) => Some(std::mem::replace(&mut dependencies[index], replacement)),
                (Some(index), None) => Some(dependencies.swap_remove(index)),
                (None, Some(replacement)) => {
                    dependencies.push(replacement);
                    None
                }
                (None, None) => None,
            }
        };
        // Captured values may run user code when dropped. Do that after unlocking.
        drop(previous);
    }

    #[inline]
    pub fn get_dependency<V: Clone + 'static>(&self) -> Option<V> {
        self.dependencies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .find_map(|dep| T::Policy::as_any(dep.value.as_ref()).downcast_ref::<V>().cloned())
    }

    /// Retain the owner's storage, not its public wrapper. A local context can
    /// back a shared client without making its configuration API concurrent.
    pub(crate) fn add_handle_dependency<H: ResourceHandleExt>(&self, handle: &H)
    where
        T::Policy: AcceptDependency<ResourceRef<ManagedCResource<H::Native>>>,
    {
        if let Some(owner) = handle.resource().as_owned() {
            self.add_dependency(owner.clone());
        }
    }

    pub(crate) fn get_handle_dependency<H: OwnedResourceHandle>(&self) -> Option<H> {
        self.get_dependency::<ResourceRef<ManagedCResource<H::Native>>>()
            .map(|owner| H::from_owned(CResource::new(owner)))
    }

    pub(crate) fn copy_dependencies_from<U: NativeResource>(&self, other: &ManagedCResource<U>)
    where
        T::Policy: CopyDependencies<U::Policy>,
    {
        // Release the source lock before taking the destination lock.
        let copies = other.dependencies.lock().unwrap_or_else(|e| e.into_inner()).clone();
        self.dependencies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(copies.into_iter().map(|dep| ResourceDependency {
                callback: dep.callback,
                in_use: dep.in_use,
                value: <T::Policy as CopyDependencies<U::Policy>>::copy(dep.value),
            }));
    }

    #[cfg(test)]
    pub(crate) fn dependency_len(&self) -> usize {
        self.dependencies.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_resource_released(&self) -> bool {
        self.get_resource_released()
    }

    pub(crate) fn mark_resource_released(&self) {
        self.set_resource_released(true);
        self.set_resource(std::ptr::null_mut());
    }

    /// Stop configuration changes before native code starts using a context.
    pub(crate) fn freeze(&self) -> Result<(), AeronCError> {
        if self.frozen.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Err(AeronCError::with_message(-1, "context is already in use"));
        }
        Ok(())
    }

    pub(crate) fn reserve_context(&self) -> Result<ContextReservation<'_, T>, AeronCError> {
        self.freeze()?;
        Ok(ContextReservation {
            owner: self,
            committed: false,
        })
    }

    pub(crate) fn check_not_frozen(&self) -> Result<(), AeronCError> {
        if self.frozen.load(std::sync::atomic::Ordering::Acquire) {
            return Err(AeronCError::with_message(-1, "context cannot be changed after use"));
        }
        Ok(())
    }

    /// Reject overlapping client start calls without spinning or waiting.
    /// Publication, subscription and manual conductor operations do not use this guard.
    pub(crate) fn operation_guard(&self) -> Result<NativeOperation<'_>, AeronCError> {
        self.operation
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .map(|_| NativeOperation(&self.operation))
            .map_err(|_| {
                AeronCError::with_message(
                    -1,
                    "client operation is already running; callbacks must not re-enter the conductor",
                )
            })
    }

    pub(crate) fn close_shared(&self) -> Result<(), AeronCError> {
        let mut stored = self.cleanup.lock().unwrap_or_else(|e| e.into_inner());
        self.close_with_cleanup(&mut stored)
    }

    fn close_with_cleanup(&self, stored: &mut Option<CleanupBox<T>>) -> Result<(), AeronCError> {
        if self.get_close_already_called() {
            return Ok(());
        }
        if let Some(cleanup) = stored.as_mut() {
            let mut resource = self.get();
            if !resource.is_null() {
                let result = cleanup(&mut resource);
                if result < 0 {
                    return Err(AeronCError::from_code(result));
                }
            }
            stored.take();
            if !self.cleanup_struct {
                self.set_resource(std::ptr::null_mut());
            }
        }
        self.set_close_already_called(true);
        Ok(())
    }

    pub(crate) fn close_shared_with(&self, mut cleanup: impl FnMut(*mut *mut T) -> i32) -> Result<(), AeronCError> {
        let mut stored = self.cleanup.lock().unwrap_or_else(|e| e.into_inner());
        if self.get_close_already_called() {
            return Ok(());
        }
        let mut resource = self.get();
        if !resource.is_null() {
            let result = cleanup(&mut resource);
            if result < 0 {
                return Err(AeronCError::from_code(result));
            }
        }
        stored.take();
        self.set_close_already_called(true);
        if !self.cleanup_struct {
            self.set_resource(std::ptr::null_mut());
        }
        Ok(())
    }
}

pub(crate) struct NativeOperation<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for NativeOperation<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// Returns a context to configuration state if native initialization fails.
#[allow(dead_code)]
pub(crate) struct ContextReservation<'a, T: NativeResource> {
    owner: &'a ManagedCResource<T>,
    committed: bool,
}
#[allow(dead_code)]
impl<T: NativeResource> ContextReservation<'_, T> {
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}
impl<T: NativeResource> Drop for ContextReservation<'_, T> {
    fn drop(&mut self) {
        if !self.committed {
            self.owner.frozen.store(false, std::sync::atomic::Ordering::Release);
        }
    }
}

impl<T: NativeResource> Drop for ManagedCResource<T> {
    fn drop(&mut self) {
        let close_ran_before_drop = self.get_close_already_called();
        if !close_ran_before_drop {
            // Drop has exclusive access; no mutex acquisition is needed.
            let mut cleanup = self.cleanup.get_mut().unwrap_or_else(|e| e.into_inner()).take();
            if let Err(e) = self.close_with_cleanup(&mut cleanup) {
                // C may still use these pointers after a failed close. Keep both
                // the cleanup captures and the dependencies alive in that case.
                let dependencies = std::mem::take(self.dependencies.get_mut().unwrap_or_else(|e| e.into_inner()));
                std::mem::forget(dependencies);
                std::mem::forget(cleanup);
                log::warn!(
                    "cleanup failed for {} during Drop with code {}; retaining its dependencies",
                    std::any::type_name::<T>(),
                    e.code
                );
                return;
            }
        }
        if self.manual_close_required
            && !close_ran_before_drop
            && self.dependency_len_for_drop() == 0
            && !self.get().is_null()
        {
            #[cfg(feature = "strict-lifecycle")]
            panic!(
                "ManagedCResource<{}> dropped without explicit close and no cleanup closure — resource leaked. Call close()/close_now() before drop, or supply a cleanup closure at construction.",
                std::any::type_name::<T>()
            );
            #[cfg(not(feature = "strict-lifecycle"))]
            log::warn!(
                "ManagedCResource<{}> dropped without explicit close and no cleanup closure — resource likely leaked. Call close()/close_now() before drop, or supply a cleanup closure at construction.",
                std::any::type_name::<T>()
            );
        }
        if self.cleanup_struct && !self.get().is_null() {
            // SAFETY: this allocation belongs to Rust and is released once, on the last owner.
            unsafe {
                drop(Box::from_raw(self.get()));
            }
            self.set_resource(std::ptr::null_mut());
        }
    }
}

impl<T: NativeResource> ManagedCResource<T> {
    fn dependency_len_for_drop(&mut self) -> usize {
        self.dependencies.get_mut().unwrap_or_else(|e| e.into_inner()).len()
    }
}

#[derive(Debug, PartialOrd, Eq, PartialEq, Clone)]
#[non_exhaustive]
pub enum AeronErrorType {
    GenericError,
    ClientErrorDriverTimeout,
    ClientErrorClientTimeout,
    ClientErrorConductorServiceTimeout,
    ClientErrorBufferFull,
    ClientErrorDriverBufferFull,
    ClientErrorClientIllegalState,
    ClientErrorBroadcastReceiverIllegalState,
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
    pub const fn code(&self) -> i32 {
        match self {
            AeronErrorType::GenericError => -1,
            AeronErrorType::ClientErrorDriverTimeout => -1000,
            AeronErrorType::ClientErrorClientTimeout => -1001,
            AeronErrorType::ClientErrorConductorServiceTimeout => -1002,
            AeronErrorType::ClientErrorBufferFull => -1003,
            AeronErrorType::ClientErrorDriverBufferFull => -1004,
            AeronErrorType::ClientErrorClientIllegalState => -1005,
            AeronErrorType::ClientErrorBroadcastReceiverIllegalState => -1006,
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

    pub const fn from_code(code: i32) -> Self {
        match code {
            -1 => AeronErrorType::GenericError,
            -1000 => AeronErrorType::ClientErrorDriverTimeout,
            -1001 => AeronErrorType::ClientErrorClientTimeout,
            -1002 => AeronErrorType::ClientErrorConductorServiceTimeout,
            -1003 => AeronErrorType::ClientErrorBufferFull,
            -1004 => AeronErrorType::ClientErrorDriverBufferFull,
            -1005 => AeronErrorType::ClientErrorClientIllegalState,
            -1006 => AeronErrorType::ClientErrorBroadcastReceiverIllegalState,
            -2 => AeronErrorType::PublicationBackPressured,
            -3 => AeronErrorType::PublicationAdminAction,
            -4 => AeronErrorType::PublicationClosed,
            -5 => AeronErrorType::PublicationMaxPositionExceeded,
            -6 => AeronErrorType::PublicationError,
            -234324 => AeronErrorType::TimedOut,
            _ => Unknown(code),
        }
    }

    pub const fn to_string(&self) -> &'static str {
        match self {
            AeronErrorType::GenericError => "Generic Error",
            AeronErrorType::ClientErrorDriverTimeout => "Client Error Driver Timeout",
            AeronErrorType::ClientErrorClientTimeout => "Client Error Client Timeout",
            AeronErrorType::ClientErrorConductorServiceTimeout => "Client Error Conductor Service Timeout",
            AeronErrorType::ClientErrorBufferFull => "Client Error Buffer Full",
            AeronErrorType::ClientErrorDriverBufferFull => "Client Error Driver Buffer Full",
            AeronErrorType::ClientErrorClientIllegalState => "Client Error Client Illegal State",
            AeronErrorType::ClientErrorBroadcastReceiverIllegalState => "Client Error Broadcast Receiver Illegal State",
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
/// (at the error site) or `with_message()`. Otherwise `Display` and
/// `get_last_err_message_owned()` read the live `aeron_errmsg()` buffer.
#[derive(Clone)]
pub struct AeronCError {
    pub code: i32,
    /// Attached via `capture_errmsg()` / `with_message()`; `None` otherwise.
    msg: Option<std::borrow::Cow<'static, str>>,
}

/// Equality is on `code` only. The attached message is advisory.
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

                // Compile the regex once: `from_code` runs on every Aeron error, including back-pressure retries
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

    /// Attach a message. String literals are borrowed without allocating.
    /// An owned `String` is moved into the error without another copy.
    #[cold]
    pub fn with_message(code: i32, msg: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        let mut err = Self::from_code(code);
        err.msg = Some(msg.into());
        err
    }

    /// Message attached via `capture_errmsg()` / `with_message()`, if any.
    pub fn message(&self) -> Option<&str> {
        self.msg.as_deref()
    }

    pub const fn kind(&self) -> AeronErrorType {
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
/// errno-style codes. Here `-1` means "not connected", not a generic error, hence
/// this dedicated type. Use [`Self::is_retryable`] to drive retry loops.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
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
    /// More than `MAX_OFFER_PARTS` buffers passed to `offer_parts`. This is a caller
    /// bug, not an Aeron wire error. Fatal (fix the call site).
    TooManyParts,
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
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            AeronOfferError::NotConnected | AeronOfferError::BackPressured | AeronOfferError::AdminAction
        )
    }

    /// The publication will never accept this offer again; recreate or give up.
    #[inline]
    pub const fn is_fatal(&self) -> bool {
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
            AeronOfferError::TooManyParts => write!(f, "too many parts in offer_parts (max 8)"),
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

/// Lets you replace a callback without registering another handler with Aeron.
///
/// Helpers such as `AeronErrorHandlerSlot` keep their callback here. If you replace
/// it while a call is running, that call finishes with the old callback. Later
/// calls use the new one.
///
/// ```
/// # use rusteron_code_gen::CallbackSlot;
/// use std::sync::Arc;
///
/// let slot = CallbackSlot::new();
/// slot.set(Arc::new(str::len));
/// assert_eq!(slot.with(|callback| callback("aeron")), Some(5));
/// slot.clear();
/// assert_eq!(slot.with(|callback| callback("aeron")), None);
/// ```
pub struct CallbackSlot<T: ?Sized> {
    callback: std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<T>>>>,
}

impl<T: ?Sized> CallbackSlot<T> {
    /// Creates an empty slot.
    pub fn new() -> Self {
        Self {
            callback: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Replaces the callback. Drops the old `Arc` after unlocking the mutex.
    pub fn set(&self, callback: std::sync::Arc<T>) {
        let previous = self.callback.lock().unwrap().replace(callback);
        drop(previous);
    }

    /// Removes the callback. Calls already using it can finish.
    pub fn clear(&self) {
        let previous = self.callback.lock().unwrap().take();
        drop(previous);
    }

    /// Calls `f` with the callback after unlocking the mutex.
    /// Returns `None` if empty. A panic in `f` does not poison the mutex.
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> Option<R> {
        let callback = self.callback.lock().unwrap().clone()?;
        Some(f(&callback))
    }
}

impl<T: ?Sized> Default for CallbackSlot<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ?Sized> Clone for CallbackSlot<T> {
    fn clone(&self) -> Self {
        Self {
            callback: self.callback.clone(),
        }
    }
}

/// Callback storage is selected at compile time. There is no trait-object call.
///
/// # Safety
/// Cloning an `Owner<T>` must keep the same `Cell<T>` allocation alive. Its address
/// is passed to C, which may keep it until the last registered handler is removed.
/// `lock` must prevent overlapping mutable access to that cell.
pub unsafe trait CallbackPolicy: 'static {
    type Cell<T>;
    /// Clones keep the same callback allocation alive while C holds its raw pointer.
    type Owner<T>: std::ops::Deref<Target = Self::Cell<T>> + Clone;
    type Guard<'a, T: 'a>: std::ops::DerefMut<Target = T>;
    fn new<T>(value: T) -> Self::Cell<T>;
    fn new_owned<T>(value: T) -> Self::Owner<T>;
    fn lock<T>(cell: &Self::Cell<T>) -> Self::Guard<'_, T>;
    fn is_in_use<T>(cell: &Self::Cell<T>) -> bool;
}
/// Serialized state for callbacks that can run on another thread.
pub enum SharedCallback {}
/// Local state for a callback used only on the current thread.
pub enum LocalCallback {}
// SAFETY: Arc clones keep the Mutex allocation stable and Mutex serializes access.
unsafe impl CallbackPolicy for SharedCallback {
    fn is_in_use<T>(cell: &Self::Cell<T>) -> bool {
        matches!(cell.try_lock(), Err(std::sync::TryLockError::WouldBlock))
    }
    type Cell<T> = std::sync::Mutex<T>;
    type Owner<T> = std::sync::Arc<std::sync::Mutex<T>>;
    type Guard<'a, T: 'a> = std::sync::MutexGuard<'a, T>;
    fn new<T>(value: T) -> Self::Cell<T> {
        std::sync::Mutex::new(value)
    }
    fn new_owned<T>(value: T) -> Self::Owner<T> {
        std::sync::Arc::new(Self::new(value))
    }
    #[inline]
    fn lock<T>(cell: &Self::Cell<T>) -> Self::Guard<'_, T> {
        cell.lock().unwrap_or_else(|e| e.into_inner())
    }
}
// SAFETY: Rc clones keep the RefCell allocation stable and RefCell checks borrows.
unsafe impl CallbackPolicy for LocalCallback {
    fn is_in_use<T>(cell: &Self::Cell<T>) -> bool {
        cell.try_borrow_mut().is_err()
    }
    type Cell<T> = std::cell::RefCell<T>;
    type Owner<T> = std::rc::Rc<std::cell::RefCell<T>>;
    type Guard<'a, T: 'a> = std::cell::RefMut<'a, T>;
    fn new<T>(value: T) -> Self::Cell<T> {
        std::cell::RefCell::new(value)
    }
    fn new_owned<T>(value: T) -> Self::Owner<T> {
        std::rc::Rc::new(Self::new(value))
    }
    #[inline(always)]
    fn lock<T>(cell: &Self::Cell<T>) -> Self::Guard<'_, T> {
        cell.borrow_mut()
    }
}
/// A callback for local polling or an assembler. It uses a borrow check, not a mutex.
pub type LocalHandler<T> = Handler<T>;
/// A callback that may be retained and called by another thread.
pub type SharedHandler<T> = Handler<T, SharedCallback>;

/// A native callback borrowed for one synchronous poll. Only this crate can
/// construct it; the callback and context pointer must describe the same object.
#[doc(hidden)]
#[allow(dead_code)] // Also compiled in the code generator, where no native poll runs.
pub struct NativePollCallback<'a, F> {
    pub(crate) callback: F,
    pub(crate) clientd: *mut std::os::raw::c_void,
    pub(crate) borrow: std::marker::PhantomData<&'a mut ()>,
}

/// A reference-counted callback that C stores and calls later.
///
/// Clones share one heap allocation. Local handlers use `Rc`; shared handlers
/// use `Arc`. C uses the pointer from [`Handler::as_raw`],
/// so the callback must stay alive until C stops using it. Dropping a `Handler`
/// does not unregister it, and the callback is freed when its last clone drops.
///
/// Owned resources retain handler clones until their last owner drops and C cleanup
/// finishes. The client also retains publication, subscription and counter handlers,
/// because the conductor can call them after `close()` returns. An archive connection
/// copies handler pointers when it starts, so replacing a handler on the original
/// context neither updates that copy nor frees the old handler. Borrowed wrappers
/// retain nothing, so keep your `Handler` alive while C can call it.
///
/// Retaining handlers through cleanup prevents C from calling a freed handler, but
/// keeps replaced closures and their captured values in memory too. An `Arc` alone
/// cannot track the raw pointer copies C makes.
///
/// For retained callbacks, keep the `SharedHandler` returned by the setter and change
/// its state with [`Self::lock`]. This waits for an active callback and must not be done
/// from that callback itself.
/// Use an `AeronErrorHandlerSlot` when replacement must not wait for an active call.
/// Its `set` and `clear` methods free old callbacks after their last call.
///
/// Use `*_fn` or `*_once` for callbacks that run during the call. Their closures
/// stay on the stack, can borrow local variables and need no heap allocation.
///
/// `Handler` is only for callbacks C keeps after the call returns, such as error, image,
/// counter and close handlers. None of these run on the poll path. Polls never take a
/// `Handler`: `poll(&mut callback)`, `poll_fn` and the owned assemblers borrow the callback
/// directly with no `RefCell`, mutex or reference count.
///
/// `Handler<T>` stays on its thread and uses `RefCell` to check mutable borrows, with no
/// mutex or atomic operation. [`SharedHandler<T>`] uses a mutex and is `Send + Sync` when
/// `T: Send`. It takes that mutex on each callback and can wait for another thread holding
/// it. Use [`Self::lock`] to inspect or change callback state. Keep poll state in the
/// callback passed to `poll` or in an owned assembler, not in a `Handler`
pub struct Handler<T, P: CallbackPolicy = LocalCallback> {
    inner: P::Owner<T>,
}

impl<T, P: CallbackPolicy> Clone for Handler<T, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Constants for optional callbacks.
pub struct Handlers;

/// A type that represents an unset callback.
///
/// Pass [`Handlers::NONE`] to leave an optional callback unset.
/// The constant is `None::<&SharedHandler<NoHandler>>`.
/// `NoHandler` implements every generated callback trait, so the compiler infers
/// the callback type without a helper or explicit type arguments. This also works
/// for methods with several callbacks, such as `async_add_subscription`.
/// C receives null callback and client-data pointers, so it never calls these methods.
pub struct NoHandler;

impl Handlers {
    /// Leaves an optional callback unset.
    /// The callback type is [`NoHandler`], so no explicit type arguments are needed.
    pub const NONE: Option<&'static SharedHandler<NoHandler>> = None;
}

impl<T> Handler<T> {
    /// Read local callback state. The borrow must end before invoking the callback.
    ///
    /// Acquiring and dropping this guard checks local borrows, with no mutex or
    /// atomic operation. It does not clone the handler or allocate memory.
    pub fn borrow(&self) -> std::cell::Ref<'_, T> {
        self.inner.borrow()
    }

    pub fn new(handler: T) -> Self {
        Self {
            inner: LocalCallback::new_owned(handler),
        }
    }
}
impl<T> SharedHandler<T> {
    pub fn new_shared(handler: T) -> Self {
        Self {
            inner: SharedCallback::new_owned(handler),
        }
    }
}
impl<T, P: CallbackPolicy> Handler<T, P> {
    #[inline(always)]
    pub fn as_raw(&self) -> *mut std::os::raw::c_void {
        (&*self.inner as *const P::Cell<T>).cast_mut().cast()
    }

    /// Mutably access callback state. Release the guard before invoking this handler.
    ///
    /// For a local `Handler`, this performs a `RefCell` borrow check on the current
    /// thread, with no mutex or atomic operation. For a `SharedHandler`, this
    /// acquires a mutex and may wait for a callback or another state guard.
    /// Neither form clones the handler or allocates memory to acquire the guard.
    ///
    /// Polls never call this
    #[inline]
    pub fn lock(&self) -> P::Guard<'_, T> {
        P::lock(&self.inner)
    }

    /// Called by the `Handler` trampolines for callbacks C retains. The poll trampolines
    /// borrow `&mut F` directly and never call this
    ///
    /// # Safety
    /// `raw` must come from a live `Handler<T, P>` and remain valid for the call.
    #[inline]
    pub unsafe fn lock_raw<'a>(raw: *mut std::os::raw::c_void) -> P::Guard<'a, T> {
        P::lock(unsafe { &*raw.cast::<P::Cell<T>>() })
    }

    /// # Safety
    /// No callback or other reference may access the value while this reference
    /// is in use. Prefer [`Self::lock`].
    #[inline]
    pub unsafe fn get_mut(&self) -> &mut T {
        let mut guard = self.lock();
        let ptr: *mut T = &mut *guard;
        drop(guard);
        unsafe { &mut *ptr }
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

thread_local! {
    /// Panic caught by [`catch_panic`], waiting for [`resume_caught_panic`]
    static CAUGHT_PANIC: std::cell::Cell<Option<Box<dyn std::any::Any + Send>>> =
        const { std::cell::Cell::new(None) };
}

/// Runs a callback called from Aeron C that has a Rust caller further up the stack. On panic,
/// stores the panic and returns `fallback`, so C finishes its call normally. The Rust wrapper
/// then raises it again with [`resume_caught_panic`]. If a panic is already stored, the later
/// one is dropped (the panic hook has already printed it)
#[doc(hidden)]
#[inline(always)]
pub fn catch_panic<R>(fallback: R, f: impl FnOnce() -> R) -> R {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            store_caught_panic(payload);
            fallback
        }
    }
}

#[cold]
#[inline(never)]
fn store_caught_panic(payload: Box<dyn std::any::Any + Send>) {
    CAUGHT_PANIC.with(|slot| {
        let first = slot.take().unwrap_or(payload);
        slot.set(Some(first));
    });
}

/// Raises the panic stored by [`catch_panic`] on this thread, if any
#[doc(hidden)]
#[inline]
pub fn resume_caught_panic() {
    if let Some(payload) = CAUGHT_PANIC.take() {
        std::hint::cold_path();
        std::panic::resume_unwind(payload)
    }
}

/// Runs a callback Aeron C fires with no Rust caller to pass a panic to, e.g. on the conductor
/// thread. On panic, logs it and returns `fallback`, so the client keeps running
#[doc(hidden)]
#[inline(always)]
pub fn log_panic<R>(callback: &str, fallback: R, f: impl FnOnce() -> R) -> R {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            report_panic(callback, &*payload);
            fallback
        }
    }
}

#[cold]
#[inline(never)]
fn report_panic(callback: &str, payload: &(dyn std::any::Any + Send)) {
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload");
    log::error!("{callback} panicked inside an Aeron callback, continuing: {message}");
    // not `eprintln!`, which panics if stderr fails, and a panic here would abort
    let _ = std::io::Write::write_fmt(
        &mut std::io::stderr(),
        format_args!("{callback} panicked inside an Aeron callback, continuing: {message}\n"),
    );
}

/// Represents the Aeron URI parser and handler.
pub struct ChannelUri {}

impl ChannelUri {
    pub const AERON_SCHEME: &'static str = "aeron";
    pub const SPY_QUALIFIER: &'static str = "aeron-spy";
    pub const MAX_URI_LENGTH: usize = 4095;

    /// Return `channel` with a `session-id` param added (replacing any existing one).
    ///
    /// Mirrors Java's `ChannelUri.addSessionId`, the standard way to build a channel
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
#[non_exhaustive]
pub enum Media {
    Ipc,
    Udp,
}

impl Media {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Media::Ipc => "ipc",
            Media::Udp => "udp",
        }
    }
}

/// Enum for control modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ControlMode {
    Manual,
    Dynamic,
    /// this is a beta feature useful when dealing with docker containers and networking
    Response,
}

impl ControlMode {
    pub const fn as_str(&self) -> &'static str {
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
    use std::cell::Cell;
    use std::env;
    use std::fs::OpenOptions;
    #[allow(unused_imports)]
    use std::os::unix::fs::OpenOptionsExt;
    use std::sync::atomic::{AtomicIsize, Ordering};

    thread_local! {
        static TRACK_EVENTS: Cell<bool> = const { Cell::new(false) };
        static ALLOCATION_EVENTS: Cell<usize> = const { Cell::new(0) };
    }

    fn record_event() {
        let _ = TRACK_EVENTS.try_with(|enabled| {
            if enabled.get() {
                let _ = ALLOCATION_EVENTS.try_with(|events| events.set(events.get() + 1));
            }
        });
    }

    /// Count allocations and reallocations on this thread, including temporary
    /// allocations freed before the callback returns. Other test threads do not count.
    pub fn assert_no_heap_allocation(callback: impl FnOnce()) {
        struct Restore(bool);
        impl Drop for Restore {
            fn drop(&mut self) {
                TRACK_EVENTS.with(|enabled| enabled.set(self.0));
            }
        }
        let before = ALLOCATION_EVENTS.with(Cell::get);
        let restore = Restore(TRACK_EVENTS.with(|enabled| enabled.replace(true)));
        callback();
        drop(restore);
        assert_eq!(
            ALLOCATION_EVENTS.with(Cell::get) - before,
            0,
            "unexpected heap allocation"
        );
    }

    #[test]
    #[should_panic(expected = "unexpected heap allocation")]
    fn event_counter_detects_an_allocation_freed_inside_the_callback() {
        assert_no_heap_allocation(|| drop(std::hint::black_box(Box::new(42_u64))));
    }

    /// A global allocator that tracks the net allocation count.
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
            record_event();
            self.allocs.fetch_add(1, Ordering::SeqCst);
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            self.allocs.fetch_sub(1, Ordering::SeqCst);
            unsafe { System.dealloc(ptr, layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record_event();
            self.allocs.fetch_add(1, Ordering::SeqCst);
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record_event();
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static GLOBAL: TrackingAllocator = TrackingAllocator::new();

    /// Returns the current number of net allocations
    pub fn current_allocs() -> isize {
        GLOBAL.current()
    }

    /// Asserts that no allocations occur within the provided closure.
    /// A file lock serializes access across threads and tests.
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
/// call site, which matters for latency-sensitive code review, while removing the
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
/// 1. **`c"aeron:ipc"` literals**. Compile-time `&'static CStr`, zero runtime cost.
///    Use for every constant channel/name.
/// 2. **`cformat!(...)`**. One heap allocation (the formatted `String`; `CString::new`
///    reuses its buffer). Use for dynamic URIs built once per stream/reconnect.
/// 3. **Reuse**. Build the `CString` once, store it, pass `&it` on every call
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

/// Back off a poll loop when the last cycle did no work. Mirrors Aeron's `IdleStrategy`:
/// `idle(work_count)` returns immediately when `work_count > 0`, otherwise spins / yields /
/// sleeps depending on the implementation.
pub trait IdleStrategy {
    /// Called with the work/fragment count from the last operation. Implementations return
    /// immediately when `work_count > 0` (work was done) and back off otherwise.
    fn idle(&mut self, work_count: i32);

    /// Reset accumulated backoff state after a productive period. Default: no-op.
    fn reset(&mut self) {}
}

/// Spin with a CPU pause hint. Never sleeps or yields, and does not pin a core
pub struct BusySpinIdleStrategy;
impl BusySpinIdleStrategy {
    pub const fn new() -> Self {
        Self
    }
}
impl Default for BusySpinIdleStrategy {
    fn default() -> Self {
        Self::new()
    }
}
impl IdleStrategy for BusySpinIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: i32) {
        if work_count > 0 {
            return;
        }
        std::hint::spin_loop();
    }
}

/// No-op. Never yields the core. (Aeron `NoOpIdleStrategy`.)
pub struct NoOpIdleStrategy;
impl NoOpIdleStrategy {
    pub const fn new() -> Self {
        Self
    }
}
impl Default for NoOpIdleStrategy {
    fn default() -> Self {
        Self::new()
    }
}
impl IdleStrategy for NoOpIdleStrategy {
    #[inline]
    fn idle(&mut self, _work_count: i32) {}
}

/// Yield the OS thread when idle. Lower CPU than busy-spin, slightly higher latency.
/// (Aeron `YieldingIdleStrategy`.)
pub struct YieldingIdleStrategy;
impl YieldingIdleStrategy {
    pub const fn new() -> Self {
        Self
    }
}
impl Default for YieldingIdleStrategy {
    fn default() -> Self {
        Self::new()
    }
}
impl IdleStrategy for YieldingIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: i32) {
        if work_count > 0 {
            return;
        }
        std::thread::yield_now();
    }
}

/// Sleep for a fixed duration when idle. (Aeron `SleepingIdleStrategy`.)
pub struct SleepingIdleStrategy {
    duration: Duration,
}
impl SleepingIdleStrategy {
    pub const fn new(duration: Duration) -> Self {
        Self { duration }
    }
}
impl IdleStrategy for SleepingIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: i32) {
        if work_count == 0 {
            std::thread::sleep(self.duration);
        }
    }
}

/// Adaptive backoff: spin a few times, then yield a few times, then sleep with an exponentially
/// growing park up to a max. A good general-purpose strategy. (Aeron `BackoffIdleStrategy`.)
///
/// Defaults match Aeron: `max_spins = 10`, `max_yields = 20`, `min_park = 1µs`, `max_park = 1ms`.
pub struct BackoffIdleStrategy {
    max_spins: i64,
    max_yields: i64,
    min_park: Duration,
    max_park: Duration,
    spins: i64,
    yields: i64,
    park: Duration,
    state: u8,
}

const BACKOFF_NOT_IDLE: u8 = 0;
const BACKOFF_SPINNING: u8 = 1;
const BACKOFF_YIELDING: u8 = 2;
const BACKOFF_PARKING: u8 = 3;

impl BackoffIdleStrategy {
    /// Defaults match Aeron: 10 spins, 20 yields, park 1µs..1ms.
    pub const fn new() -> Self {
        Self::with(10, 20, Duration::from_micros(1), Duration::from_millis(1))
    }

    /// Full control over the backoff parameters.
    pub const fn with(max_spins: i64, max_yields: i64, min_park: Duration, max_park: Duration) -> Self {
        Self {
            max_spins,
            max_yields,
            min_park,
            max_park,
            spins: 0,
            yields: 0,
            park: min_park,
            state: BACKOFF_NOT_IDLE,
        }
    }

    #[inline]
    fn idle_one(&mut self) {
        match self.state {
            BACKOFF_NOT_IDLE => {
                self.state = BACKOFF_SPINNING;
                self.spins += 1;
            }
            BACKOFF_SPINNING => {
                std::hint::spin_loop();
                self.spins += 1;
                if self.spins > self.max_spins {
                    self.state = BACKOFF_YIELDING;
                    self.yields = 0;
                }
            }
            BACKOFF_YIELDING => {
                self.yields += 1;
                if self.yields > self.max_yields {
                    self.state = BACKOFF_PARKING;
                    self.park = self.min_park;
                } else {
                    std::thread::yield_now();
                }
            }
            _ => {
                // PARKING. Sleep then double the park period up to the max.
                std::thread::sleep(self.park);
                self.park = std::cmp::min(self.park.saturating_mul(2), self.max_park);
            }
        }
    }
}

impl Default for BackoffIdleStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl IdleStrategy for BackoffIdleStrategy {
    #[inline]
    fn idle(&mut self, work_count: i32) {
        if work_count > 0 {
            self.reset();
        } else {
            self.idle_one();
        }
    }

    fn reset(&mut self) {
        self.spins = 0;
        self.yields = 0;
        self.park = self.min_park;
        self.state = BACKOFF_NOT_IDLE;
    }
}

#[cfg(test)]
mod callback_slot_tests {
    use super::CallbackSlot;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};

    #[test]
    fn callback_slot_clones_share_replacements_and_return_values() {
        let delegate: CallbackSlot<dyn Fn(&str) -> usize + Send + Sync> = CallbackSlot::default();
        let registered = delegate.clone();
        assert_eq!(registered.with(|callback| callback("aeron")), None);

        delegate.set(Arc::new(str::len));
        assert_eq!(registered.with(|callback| callback("aeron")), Some(5));
        delegate.set(Arc::new(|message: &str| message.len() * 2));
        assert_eq!(registered.with(|callback| callback("aeron")), Some(10));

        delegate.clear();
        assert_eq!(registered.with(|callback| callback("aeron")), None);
    }

    #[test]
    fn callback_slot_supports_mutable_callbacks() {
        let delegate: CallbackSlot<Mutex<Box<dyn FnMut(u32) -> u32 + Send>>> = CallbackSlot::new();
        let mut total = 0;
        delegate.set(Arc::new(Mutex::new(Box::new(move |value| {
            total += value;
            total
        }))));
        assert_eq!(delegate.with(|callback| callback.lock().unwrap()(2)), Some(2));
        assert_eq!(delegate.with(|callback| callback.lock().unwrap()(3)), Some(5));
    }

    #[test]
    fn callback_slot_panics_do_not_poison_its_lock() {
        let delegate: CallbackSlot<dyn Fn() -> u32 + Send + Sync> = CallbackSlot::new();
        delegate.set(Arc::new(|| panic!("delegate panic")));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                delegate.with(|callback| callback())
            }))
            .is_err()
        );

        delegate.set(Arc::new(|| 42));
        assert_eq!(delegate.with(|callback| callback()), Some(42));
        delegate.clear();
    }

    #[test]
    fn callback_slot_drops_replaced_and_cleared_values_outside_its_lock() {
        struct CheckDrop {
            slot: Weak<Mutex<Option<Arc<CheckDrop>>>>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for CheckDrop {
            fn drop(&mut self) {
                assert!(self.slot.upgrade().unwrap().try_lock().is_ok());
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }

        let delegate = CallbackSlot::new();
        let drops = Arc::new(AtomicUsize::new(0));
        for expected in 0..2 {
            delegate.set(Arc::new(CheckDrop {
                slot: Arc::downgrade(&delegate.callback),
                drops: drops.clone(),
            }));
            assert_eq!(drops.load(Ordering::SeqCst), expected);
        }
        delegate.clear();
        assert_eq!(drops.load(Ordering::SeqCst), 2);
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
        assert_eq!(*handler.lock(), 42);
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

    // `manual_close_required` drives the Drop-time leak warning. The resource pointers
    // are dummies (never dereferenced); only the field value is checked.

    #[test]
    #[cfg(not(feature = "strict-lifecycle"))] // would panic on drop under strict-lifecycle
    fn manual_close_required_true_for_none_cleanup_no_struct() {
        // Owned, no cleanup closure, no Rust struct ownership.
        let r: ManagedCResource<u8> = ManagedCResource::<u8>::new(
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
        let r: ManagedCResource<u8> = ManagedCResource::<u8>::new(
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
        let r: ManagedCResource<u8> = ManagedCResource::<u8>::new(
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

#[cfg(test)]
mod threading_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};

    struct Native;
    // These tests check owner storage and cleanup without calling C methods.
    unsafe impl NativeResource for Native {
        type Policy = SharedResource;
        type Pointer = ReloadPointer;
    }

    fn owner(cleanup: impl FnMut(*mut *mut Native) -> i32 + Send + 'static) -> Arc<ManagedCResource<Native>> {
        Arc::new(
            ManagedCResource::<Native>::new(
                |ptr| {
                    unsafe {
                        *ptr = Box::into_raw(Box::new(Native));
                    }
                    0
                },
                Some(Box::new(cleanup)),
                false,
            )
            .unwrap_or_else(|e| panic!("init failed: {}", e.code)),
        )
    }

    #[test]
    fn concurrent_close_runs_native_cleanup_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let resource = owner(move |ptr| {
            count.fetch_add(1, Ordering::Relaxed);
            std::thread::yield_now();
            unsafe {
                drop(Box::from_raw(*ptr));
            }
            0
        });
        let start = Arc::new(Barrier::new(8));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let resource = &resource;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    assert!(resource.close_shared().is_ok());
                });
            }
        });
        drop(resource);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn final_drop_retries_cleanup_after_a_panicking_close() {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let dependency = Arc::new(());
        let dependency_weak = Arc::downgrade(&dependency);
        let resource = owner(move |ptr| {
            assert!(dependency_weak.upgrade().is_some());
            if count.fetch_add(1, Ordering::Relaxed) == 0 {
                panic!("cleanup failed before freeing the native allocation");
            }
            unsafe {
                drop(Box::from_raw(*ptr));
            }
            0
        });
        resource.add_dependency(dependency);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| resource.close_shared())).is_err());
        drop(resource);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn conductor_calls_reject_overlap_and_release_the_guard_after_a_panic() {
        let resource = owner(|ptr| {
            unsafe {
                drop(Box::from_raw(*ptr));
            }
            0
        });
        let operation = resource
            .operation_guard()
            .unwrap_or_else(|_| panic!("first operation rejected"));
        std::thread::scope(|scope| {
            let resource = &resource;
            scope
                .spawn(move || assert!(resource.operation_guard().is_err()))
                .join()
                .unwrap();
        });
        drop(operation);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _operation = resource
                    .operation_guard()
                    .unwrap_or_else(|_| panic!("operation rejected"));
                panic!("test");
            }))
            .is_err()
        );
        assert!(resource.operation_guard().is_ok());
    }

    #[test]
    fn a_shared_dependency_is_closed_after_its_child_on_another_thread() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let events = order.clone();
        let parent = owner(move |ptr| {
            events.lock().unwrap().push("parent");
            unsafe {
                drop(Box::from_raw(*ptr));
            }
            0
        });
        let events = order.clone();
        let child = owner(move |ptr| {
            events.lock().unwrap().push("child");
            unsafe {
                drop(Box::from_raw(*ptr));
            }
            0
        });
        child.add_dependency(parent.clone());
        drop(parent);
        std::thread::spawn(move || drop(child)).join().unwrap();
        assert_eq!(*order.lock().unwrap(), ["child", "parent"]);
    }

    #[test]
    fn shared_callback_state_is_serialized_and_survives_poisoning() {
        let state = SharedHandler::new_shared(0usize);
        let state_for_panic = state.clone();
        assert!(
            std::thread::spawn(move || {
                let _guard = state_for_panic.lock();
                panic!("test");
            })
            .join()
            .is_err()
        );
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let state = &state;
                scope.spawn(move || {
                    for _ in 0..1000 {
                        *state.lock() += 1;
                    }
                });
            }
        });
        assert_eq!(*state.lock(), 4000);
    }

    #[test]
    fn local_callbacks_reject_overlapping_mutable_access() {
        let handler = Handler::new(0usize);
        let alias = handler.clone();
        let guard = handler.lock();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| alias.lock())).is_err());
        drop(guard);
        *alias.lock() += 1;
        assert_eq!(*handler.lock(), 1);
    }
}

#[cfg(test)]
mod typed_storage_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Byte<S: ResourceStorage<u8>>(S);
    impl<S: ResourceStorage<u8>> resource_sealed::Handle for Byte<S> {}
    impl<S: ResourceStorage<u8>> ResourceHandle for Byte<S> {
        type Native = u8;
        type Storage = S;
        type Borrowed = Byte<Borrowed<u8>>;
    }
    impl<S: ResourceStorage<u8>> ResourceHandleExt for Byte<S> {
        fn resource(&self) -> &S {
            &self.0
        }
        fn from_resource(inner: S) -> Self {
            Self(inner)
        }
        fn from_borrowed(inner: Borrowed<u8>) -> Self::Borrowed {
            Byte(inner)
        }
    }
    impl<S: ResourceStorage<u8>> Drop for Byte<S> {
        fn drop(&mut self) {
            assert_eq!(unsafe { *self.0.get() }, 31);
        }
    }

    #[test]
    fn storage_has_no_discriminant() {
        assert_eq!(std::mem::size_of::<CResource<u8>>(), std::mem::size_of::<usize>());
        assert_eq!(
            std::mem::size_of::<LiveCResource<u8>>(),
            2 * std::mem::size_of::<usize>()
        );
        assert_eq!(std::mem::size_of::<Borrowed<u8>>(), std::mem::size_of::<usize>());
        assert_eq!(std::mem::size_of::<Inline<u8>>(), 1);
        assert_eq!(
            std::mem::size_of::<Ref<'_, Byte<CResource<u8>>>>(),
            std::mem::size_of::<usize>()
        );
    }

    #[test]
    fn retained_child_survives_owner_and_other_clone() {
        let owner = Arc::new(31_u8);
        let weak = Arc::downgrade(&owner);
        let ptr = Arc::as_ptr(&owner).cast_mut();
        // SAFETY: the retained Arc keeps this immutable byte at a stable address.
        let child: OwnedWithReference<_, Byte<CResource<u8>>> = unsafe { OwnedWithReference::new(owner.clone(), ptr) };
        let second = child.clone();
        drop(owner);
        assert_eq!(child.borrow().resource().get(), second.borrow().resource().get());
        assert_eq!(std::mem::size_of_val(&child), 2 * std::mem::size_of::<usize>());
        drop(child);
        assert!(weak.upgrade().is_some());
        drop(second.borrow()); // Borrowed wrapper's Drop can still read its native value.
        drop(second);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn heap_clone_shares_one_allocation_and_cleans_up_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let capture = drops.clone();
        let resource = ManagedCResource::<u8>::new(
            |out| {
                unsafe { *out = Box::into_raw(Box::new(17_u8)) };
                0
            },
            Some(Box::new(move |ptr: *mut *mut u8| {
                capture.fetch_add(1, Ordering::Relaxed);
                unsafe {
                    drop(Box::from_raw(*ptr));
                    *ptr = std::ptr::null_mut();
                }
                0
            })),
            false,
        )
        .unwrap_or_else(|_| panic!("allocation failed"));
        let first = CResource::new(Arc::new(resource));
        let second = first.clone();
        assert_eq!(first.get(), second.get());
        drop(first);
        assert_eq!(unsafe { *second.get() }, 17);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(second);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn live_handle_keeps_its_pointer_after_moving_and_cloning() {
        let first = LiveCResource::from(CResource::owned_value(31_u8));
        let ptr = first.get();
        let second = first.clone();
        drop(first);
        let moved = Box::new(second);
        assert_eq!(moved.get(), ptr);
        assert_eq!(unsafe { *moved.get() }, 31);
    }

    #[test]
    fn cached_value_keeps_one_allocation_alive_across_moves_and_clones() {
        struct Value(u8);
        unsafe impl NativeResource for Value {
            type Policy = LocalResource;
            type Pointer = CachedPointer<Self>;
        }
        let first = CResource::owned_value(Value(31));
        assert_eq!(std::mem::size_of_val(&first), 2 * std::mem::size_of::<usize>());
        let allocation = Arc::downgrade(&first.0);
        let pointer = first.get();
        let second = Box::new(first.clone());
        drop(first);
        assert!(allocation.upgrade().is_some());
        assert_eq!(pointer, second.get());
        // Native calls can change the value without changing its allocation.
        unsafe { (*pointer).0 = 42 };
        assert_eq!(unsafe { (*second.get()).0 }, 42);
        drop(second);
        assert!(allocation.upgrade().is_none());
    }

    #[test]
    fn checked_owner_observes_native_async_release() {
        let first = CResource::owned_value(31_u8);
        let second = first.clone();
        let ptr = first.get();
        // Simulate native code consuming an async registration allocation.
        unsafe { drop(Box::from_raw(ptr)) };
        first.as_owned().unwrap().mark_resource_released();
        assert!(first.get().is_null());
        assert!(second.get().is_null());
        drop(first);
        drop(second); // Neither clone may free the native allocation again.
    }

    #[test]
    fn pinned_inline_keeps_its_address_when_pin_handle_moves() {
        let pinned = Box::pin(PinnedInline::new(31_u8));
        let ptr = pinned.as_ref().get();
        let moved = pinned;
        assert_eq!(moved.as_ref().get(), ptr);
        unsafe { *ptr = 32 };
        assert_eq!(unsafe { *moved.as_ref().get() }, 32);
    }
}
