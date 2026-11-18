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

#[test]
fn native_archives_cannot_drive_a_manual_client_from_another_thread() {
    let directory = std::env::temp_dir().join(format!("rusteron-archive-threads-{}", Aeron::nano_clock()));
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(|context| {
        context
            .set_dir(&directory.to_string_lossy().into_c_string())
            .map(|_| ())
    })
    .unwrap();
    let context = AeronContext::new().unwrap();
    context.set_dir(&driver.dir().into_c_string()).unwrap();
    context.set_use_conductor_agent_invoker(true).unwrap();
    let client = Aeron::new(&context).unwrap();
    client.start().unwrap();
    let other = client.clone();
    std::thread::spawn(move || {
        let archive_context = AeronArchiveContext::new().unwrap();
        assert!(archive_context.set_aeron(&other).is_err());
    })
    .join()
    .unwrap();
    AeronArchiveContext::new().unwrap().set_aeron(&client).unwrap();
}
