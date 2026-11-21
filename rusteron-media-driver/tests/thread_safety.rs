//! Compile-time ownership and thread checks for driver handles.
use rusteron_media_driver::{AeronDriver, AeronDriverContext, ManualDriver, ThreadedDriver};

// Inference is ambiguous if the type implements the forbidden trait.
macro_rules! assert_not_impl {
    ($ty:ty, $bound:ident) => {
        const _: fn() = || {
            trait Ambiguous<A> {
                fn check() {}
            }
            impl<T: ?Sized> Ambiguous<()> for T {}
            struct HasTrait;
            impl<T: ?Sized + $bound> Ambiguous<HasTrait> for T {}
            let _ = <$ty as Ambiguous<_>>::check;
        };
    };
}

assert_not_impl!(AeronDriverContext, Send);
assert_not_impl!(AeronDriverContext, Sync);
assert_not_impl!(AeronDriver, Send);
assert_not_impl!(AeronDriver, Sync);
assert_not_impl!(AeronDriver, Clone);
assert_not_impl!(ManualDriver, Send);
assert_not_impl!(ManualDriver, Sync);
assert_not_impl!(ManualDriver, Clone);
assert_not_impl!(ThreadedDriver, Clone);
