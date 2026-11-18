//! Native descriptor poller contracts; no Java Archive process is needed.
use super::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::rc::Rc;
use std::time::Duration;

macro_rules! descriptor_poller_contract {
    ($test:ident, $wrapper:ident, $native:ident, $create:ident, $close:ident, $descriptor:ident) => {
        #[test]
        fn $test() {
            let directory = std::env::temp_dir().join(format!(
                "rusteron-descriptor-{}-{}",
                std::process::id(),
                Aeron::nano_clock()
            ));
            let driver = EmbeddedDriver::launch_with(|context| {
                context
                    .set_dir(&directory.to_string_lossy().into_c_string())
                    .map(|_| ())
            })
            .unwrap();
            let client = Aeron::connect_dir(driver.dir()).unwrap();
            let subscription = client
                .add_subscription(
                    c"aeron:ipc",
                    87220,
                    Handlers::NONE,
                    Handlers::NONE,
                    Duration::from_secs(5),
                )
                .unwrap();
            let context = AeronArchiveContext::new().unwrap();
            // SAFETY: the native poller borrows this live context and subscription.
            // Its owner retains both until its close function has finished.
            let resource = ManagedCResource::<$native>::new(
                |out| unsafe { $create(out, context.get_inner(), subscription.get_inner(), 123, 10) },
                Some(Box::new(|pointer| unsafe { $close(*pointer) })),
                false,
            )
            .unwrap();
            let mut poller = $wrapper {
                inner: CResource::new(ResourceRef::new(resource)),
            };
            poller.inner.add_handle_dependency(&context);
            poller.inner.add_handle_dependency(&subscription);
            assert!(poller.poll().is_err(), "a consumer must be installed first");

            let state = Rc::new(());
            let weak = Rc::downgrade(&state);
            let consumer = Handler::new(move |_: &$descriptor| {
                std::hint::black_box(&state);
            });
            poller.reset(321, 1, Some(&consumer)).unwrap();
            assert!(poller.reset::<fn(&$descriptor)>(322, 1, None).is_err());
            assert_eq!(
                poller.correlation_id(),
                321,
                "a rejected reset keeps the previous state"
            );
            drop(consumer);
            assert!(weak.upgrade().is_some(), "the poller retains its consumer");
            assert_eq!(poller.poll().unwrap(), 0);

            // Each request resets the poller, so a reset must replace the retained consumer
            let dependencies = poller.inner.as_owned().unwrap().dependency_len();
            for correlation_id in 323..333 {
                let consumer = Handler::new(|_: &$descriptor| {});
                poller.reset(correlation_id, 1, Some(&consumer)).unwrap();
            }
            assert!(weak.upgrade().is_none(), "a reset releases the previous consumer");
            assert_eq!(
                poller.inner.as_owned().unwrap().dependency_len(),
                dependencies,
                "repeated resets keep one consumer"
            );
            let state = Rc::new(());
            let weak = Rc::downgrade(&state);
            let consumer = Handler::new(move |_: &$descriptor| {
                std::hint::black_box(&state);
            });
            poller.reset(333, 1, Some(&consumer)).unwrap();
            drop(consumer);

            // SAFETY: the owning poller remains alive and calls do not overlap.
            // A native borrow constructed here still cannot poll.
            let mut borrowed = unsafe { $wrapper::from_ptr(poller.get_inner()) };
            assert!(borrowed.poll().is_err());
            drop(borrowed);
            drop(poller);
            assert!(
                weak.upgrade().is_none(),
                "the consumer is released with its owner"
            );
        }
    };
}

descriptor_poller_contract!(
    recording_descriptor_poller_requires_an_owned_consumer,
    AeronArchiveRecordingDescriptorPoller,
    aeron_archive_recording_descriptor_poller_t,
    aeron_archive_recording_descriptor_poller_create,
    aeron_archive_recording_descriptor_poller_close,
    AeronArchiveRecordingDescriptor
);
descriptor_poller_contract!(
    subscription_descriptor_poller_requires_an_owned_consumer,
    AeronArchiveRecordingSubscriptionDescriptorPoller,
    aeron_archive_recording_subscription_descriptor_poller_t,
    aeron_archive_recording_subscription_descriptor_poller_create,
    aeron_archive_recording_subscription_descriptor_poller_close,
    AeronArchiveRecordingSubscriptionDescriptor
);
