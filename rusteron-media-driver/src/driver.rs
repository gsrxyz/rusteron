use crate::*;
use std::marker::PhantomData;
use std::rc::Rc;

/// A started driver whose conductor runs on a native thread.
/// Dropping it stops and joins the native agents.
///
/// ```compile_fail
/// use rusteron_media_driver::ThreadedDriver;
/// fn pump(mut driver: ThreadedDriver) { driver.do_work().unwrap(); }
/// ```
pub struct ThreadedDriver {
    _driver: AeronDriver,
}

/// A started driver with exclusive access to the manual conductor loop.
///
/// This value stays on its starting thread. In dedicated and shared-network modes,
/// Aeron still starts the other agents on native threads.
///
/// ```compile_fail
/// use rusteron_media_driver::ManualDriver;
/// fn share(driver: &ManualDriver) { std::thread::scope(|s| { s.spawn(|| driver); }); }
/// ```
///
/// ```compile_fail
/// use rusteron_media_driver::ManualDriver;
/// fn copy(driver: ManualDriver) { let _copy = driver.clone(); }
/// ```
pub struct ManualDriver {
    driver: AeronDriver,
    _thread: PhantomData<Rc<()>>,
}

impl AeronDriver {
    /// Start all agents on native threads and consume the unstarted driver.
    /// INVOKER mode requires [`Self::start_manual`].
    pub fn start(self) -> Result<ThreadedDriver, AeronCError> {
        // SAFETY: this unique driver has not started. Its context is frozen.
        unsafe {
            if (*(*self.get_inner()).context).threading_mode == aeron_threading_mode_t::AERON_THREADING_MODE_INVOKER {
                return Err(AeronCError::with_message(-1, "INVOKER mode requires start_manual"));
            }
            let result = aeron_driver_start(self.get_inner(), false);
            if result < 0 {
                return Err(AeronCError::from_code(result));
            }
        }
        Ok(ThreadedDriver { _driver: self })
    }

    /// Start the driver and return the only handle that can drive its conductor.
    pub fn start_manual(self) -> Result<ManualDriver, AeronCError> {
        // SAFETY: consuming the non-Clone driver prevents a second start or pump.
        let result = unsafe { aeron_driver_start(self.get_inner(), true) };
        if result < 0 {
            return Err(AeronCError::from_code(result));
        }
        Ok(ManualDriver {
            driver: self,
            _thread: PhantomData,
        })
    }
}

impl ManualDriver {
    /// Run one conductor duty cycle. INVOKER mode runs all agents here.
    /// Returns native error codes without copying or attaching the error message.
    /// Call [`AeronCError::capture_errmsg`] before another native call if the
    /// message must survive changes to this thread's native error buffer.
    #[inline]
    pub fn do_work(&mut self) -> Result<i32, AeronCError> {
        // SAFETY: &mut self excludes concurrent calls and safe callback reentry.
        let work = unsafe { aeron_driver_main_do_work(self.driver.get_inner()) };
        if work < 0 {
            Err(AeronCError::from_code(work))
        } else {
            Ok(work)
        }
    }

    /// Run the configured conductor idle strategy after a duty cycle.
    #[inline]
    pub fn idle(&mut self, work: i32) {
        // SAFETY: this handle owns the manual runner on its starting thread.
        unsafe { aeron_driver_main_idle_strategy(self.driver.get_inner(), work) }
    }
}
