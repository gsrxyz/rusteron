use super::*;

/// Select a native agent runner's idle strategy.
///
/// Dedicated mode uses `Conductor`, `Sender`, and `Receiver`. Shared-network
/// mode uses `Conductor` and `SharedNetwork`. Shared and invoker modes use
/// `Shared`. Dedicated and shared-network modes also use `NativeResourceAgent`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdleStrategyRole {
    Conductor,
    Sender,
    Receiver,
    SharedNetwork,
    Shared,
    NativeResourceAgent,
}

/// Idle after one native duty cycle. Positive work should reset any backoff.
///
/// Each installation owns a separate instance and calls never overlap.
/// Closures that implement `FnMut(i32) + Send + 'static` implement this trait.
pub trait IdleStrategy: Send + 'static {
    /// Declare that this strategy never sleeps, yields, or parks.
    ///
    /// The native name is "rusteron-spin" when true and "rusteron" otherwise.
    /// This declaration changes setup metadata only. Closures use the default.
    const BUSY_SPIN: bool = false;

    fn idle(&mut self, work_count: i32);
}

impl<F: FnMut(i32) + Send + 'static> IdleStrategy for F {
    #[inline]
    fn idle(&mut self, work_count: i32) {
        self(work_count);
    }
}

struct IdleOwner<T>(UnsafeCell<T>);
// Only one runner accesses this cell. Context dependency access never exposes
// it, and native shutdown joins the runner before dropping the owner.
unsafe impl<T: IdleStrategy> Sync for IdleOwner<T> {}

const fn inline_state<T>() -> bool {
    !std::mem::needs_drop::<T>() && std::mem::align_of::<T>() <= std::mem::align_of::<*mut T>()
}

unsafe extern "C" fn idle<T: IdleStrategy>(state: *mut c_void, work: i32) {
    // Aeron can free inline values without a destructor. Other values stay in
    // Rust-owned storage. This choice is constant for each callback type.
    let value = if inline_state::<T>() {
        state.cast::<T>()
    } else {
        unsafe { *state.cast::<*mut T>() }
    };
    unsafe { &mut *value }.idle(work);
}

impl AeronDriverContext {
    /// Install Rust state without a dynamic symbol or an exported C function.
    ///
    /// Values without destructors fit directly in native state when their
    /// alignment permits. Other values remain owned by the context until
    /// shutdown, even after replacement. The native name getter returns
    /// "rusteron-spin" for `T::BUSY_SPIN`, or "rusteron" otherwise.
    pub fn set_idle_strategy<T: IdleStrategy>(&self, role: IdleStrategyRole, strategy: T) -> Result<(), AeronCError> {
        self.inner.check_configurable()?;
        let mut state = std::ptr::null_mut();
        let mut name = std::ptr::null_mut();
        let strategy_name = if T::BUSY_SPIN { c"rusteron-spin" } else { c"rusteron" };
        let size = if inline_state::<T>() {
            std::mem::size_of::<T>().max(1)
        } else {
            std::mem::size_of::<*mut T>()
        };
        let owner;
        unsafe {
            check(aeron_alloc(&mut state, size))?;
            if let Err(error) = check(aeron_alloc(&mut name, strategy_name.to_bytes_with_nul().len())) {
                aeron_free(state);
                return Err(error);
            }
            owner = if inline_state::<T>() {
                // aeron_alloc uses malloc, which supports pointer alignment. This branch
                // needs no stronger alignment and no Rust destructor at free.
                state.cast::<T>().write(strategy);
                None
            } else {
                let owner = NativeOwned::new(IdleOwner(UnsafeCell::new(strategy)));
                state.cast::<*mut T>().write((*owner.as_ptr()).0.get());
                Some(owner)
            };
            std::ptr::copy_nonoverlapping(
                strategy_name.as_ptr(),
                name.cast(),
                strategy_name.to_bytes_with_nul().len(),
            );
            let context = self.get_inner();
            let (callback, state_slot, name_slot) = match role {
                IdleStrategyRole::Conductor => (
                    &mut (*context).conductor_idle_strategy_func,
                    &mut (*context).conductor_idle_strategy_state,
                    &mut (*context).conductor_idle_strategy_name,
                ),
                IdleStrategyRole::Sender => (
                    &mut (*context).sender_idle_strategy_func,
                    &mut (*context).sender_idle_strategy_state,
                    &mut (*context).sender_idle_strategy_name,
                ),
                IdleStrategyRole::Receiver => (
                    &mut (*context).receiver_idle_strategy_func,
                    &mut (*context).receiver_idle_strategy_state,
                    &mut (*context).receiver_idle_strategy_name,
                ),
                IdleStrategyRole::SharedNetwork => (
                    &mut (*context).shared_network_idle_strategy_func,
                    &mut (*context).shared_network_idle_strategy_state,
                    &mut (*context).shared_network_idle_strategy_name,
                ),
                IdleStrategyRole::Shared => (
                    &mut (*context).shared_idle_strategy_func,
                    &mut (*context).shared_idle_strategy_state,
                    &mut (*context).shared_idle_strategy_name,
                ),
                IdleStrategyRole::NativeResourceAgent => (
                    &mut (*context).native_resource_agent_idle_strategy_func,
                    &mut (*context).native_resource_agent_idle_strategy_state,
                    &mut (*context).native_resource_agent_idle_strategy_name,
                ),
            };
            aeron_free(*state_slot);
            aeron_free((*name_slot).cast_mut().cast());
            *callback = Some(idle::<T>);
            *state_slot = state;
            *name_slot = name.cast();
        }
        if let Some(owner) = owner {
            self.inner.add_dependency(owner);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Sum(i64);
    impl IdleStrategy for Sum {
        const BUSY_SPIN: bool = true;

        fn idle(&mut self, work: i32) {
            self.0 += i64::from(work);
        }
    }

    #[test]
    fn busy_spin_marker_tracks_the_installed_strategy_for_every_role() {
        let context = AeronDriverContext::new().unwrap();
        for role in [
            IdleStrategyRole::Conductor,
            IdleStrategyRole::Sender,
            IdleStrategyRole::Receiver,
            IdleStrategyRole::SharedNetwork,
            IdleStrategyRole::Shared,
            IdleStrategyRole::NativeResourceAgent,
        ] {
            for spin in [true, false, true] {
                if spin {
                    crate::test_alloc::assert_no_heap_allocation(|| {
                        context.set_idle_strategy(role, Sum(0)).unwrap();
                    });
                } else {
                    context.set_idle_strategy(role, |_| {}).unwrap();
                }
                // SAFETY: this context has no running driver. The name stays
                // allocated until the next setter or context close.
                let name = unsafe {
                    let raw = &*context.get_inner();
                    CStr::from_ptr(match role {
                        IdleStrategyRole::Conductor => raw.conductor_idle_strategy_name,
                        IdleStrategyRole::Sender => raw.sender_idle_strategy_name,
                        IdleStrategyRole::Receiver => raw.receiver_idle_strategy_name,
                        IdleStrategyRole::SharedNetwork => raw.shared_network_idle_strategy_name,
                        IdleStrategyRole::Shared => raw.shared_idle_strategy_name,
                        IdleStrategyRole::NativeResourceAgent => raw.native_resource_agent_idle_strategy_name,
                    })
                };
                assert_eq!(name, if spin { c"rusteron-spin" } else { c"rusteron" });
            }
        }
    }

    #[test]
    fn destructor_free_state_runs_inline_for_every_role_without_rust_allocation() {
        assert!(inline_state::<Sum>());
        let context = AeronDriverContext::new().unwrap();
        for role in [
            IdleStrategyRole::Conductor,
            IdleStrategyRole::Sender,
            IdleStrategyRole::Receiver,
            IdleStrategyRole::SharedNetwork,
            IdleStrategyRole::Shared,
            IdleStrategyRole::NativeResourceAgent,
        ] {
            crate::test_alloc::assert_no_heap_allocation(|| {
                context.set_idle_strategy(role, Sum(7)).unwrap();
            });
        }
        unsafe {
            let raw = &*context.get_inner();
            for (callback, state) in [
                (raw.conductor_idle_strategy_func, raw.conductor_idle_strategy_state),
                (raw.sender_idle_strategy_func, raw.sender_idle_strategy_state),
                (raw.receiver_idle_strategy_func, raw.receiver_idle_strategy_state),
                (
                    raw.shared_network_idle_strategy_func,
                    raw.shared_network_idle_strategy_state,
                ),
                (raw.shared_idle_strategy_func, raw.shared_idle_strategy_state),
                (
                    raw.native_resource_agent_idle_strategy_func,
                    raw.native_resource_agent_idle_strategy_state,
                ),
            ] {
                callback.unwrap()(state, -3);
                callback.unwrap()(state, 0);
                callback.unwrap()(state, i32::MAX);
                assert_eq!((*state.cast::<Sum>()).0, 4 + i64::from(i32::MAX));
            }
        }
    }

    #[test]
    fn over_aligned_state_keeps_its_alignment_through_replacement() {
        #[repr(align(128))]
        struct Aligned(u64);
        impl IdleStrategy for Aligned {
            fn idle(&mut self, work: i32) {
                assert_eq!((self as *const Self as usize) % 128, 0);
                self.0 += work as u64;
                assert_eq!(self.0, 12);
            }
        }
        assert!(!inline_state::<Aligned>());
        let context = AeronDriverContext::new().unwrap();
        context
            .set_idle_strategy(IdleStrategyRole::Conductor, Aligned(7))
            .unwrap();
        unsafe {
            let raw = &*context.get_inner();
            raw.conductor_idle_strategy_func.unwrap()(raw.conductor_idle_strategy_state, 5);
        }
        context.set_idle_strategy(IdleStrategyRole::Conductor, Sum(0)).unwrap();
        context.set_conductor_idle_strategy(c"yield").unwrap();
    }

    #[test]
    fn zero_sized_state_uses_a_native_allocation_that_name_setters_can_free() {
        let context = AeronDriverContext::new().unwrap();
        context
            .set_idle_strategy(IdleStrategyRole::Conductor, |work| assert_eq!(work, -7))
            .unwrap();
        unsafe {
            let raw = &*context.get_inner();
            assert!(!raw.conductor_idle_strategy_state.is_null());
            raw.conductor_idle_strategy_func.unwrap()(raw.conductor_idle_strategy_state, -7);
        }
        context.set_conductor_idle_strategy(c"yield").unwrap();
    }
}
