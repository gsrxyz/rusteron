//! Archive operations are local; its transport client supports concurrent use.
use rusteron_archive::*;

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
macro_rules! local {
    ($($ty:ty),+ $(,)?) => { $(assert_not_impl!($ty, Send); assert_not_impl!($ty, Sync);)+ };
}
local!(
    AeronArchive,
    AeronArchiveContext,
    RetainedArchiveContext,
    AeronArchiveAsyncConnect,
    AeronArchiveReplayMerge,
    AeronArchiveControlResponsePoller,
    AeronArchiveRecordingDescriptorPoller,
    AeronArchivePersistentSubscription
);
assert_not_impl!(AeronArchiveContext, Clone);
assert_not_impl!(AeronArchiveRecordingDescriptorPoller, Clone);
assert_not_impl!(AeronArchiveRecordingSubscriptionDescriptorPoller, Clone);

#[test]
fn transport_handles_keep_their_native_threading_contracts_in_the_archive_crate() {
    fn shared<T: Send + Sync>() {}
    shared::<Aeron>();
    shared::<AeronPublication>();
    shared::<AeronCounter>();
    shared::<AeronCountersReader>();
}
