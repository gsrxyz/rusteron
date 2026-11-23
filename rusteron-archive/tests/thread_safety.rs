//! Archive operations are local; its transport client supports concurrent use.
use rusteron_archive::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

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
    AeronArchiveRecordingSubscriptionDescriptorPoller,
    AeronArchivePersistentSubscription,
    AeronArchivePersistentSubscriptionContext,
    PersistentSubscriptionBuilder,
    AeronContext,
    AeronExclusivePublication,
    AeronExclusivePublicationRef,
    AeronImage,
    AeronImageRef,
    AeronSubscriptionRef,
    AeronClaim<'static>,
    FragmentAssembler<fn(&[u8], &AeronHeader)>
);
assert_not_impl!(AeronSubscription, Sync);
assert_not_impl!(AeronSubscription, Clone);
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
    fn movable<T: Send>() {}
    movable::<AeronSubscription>();
}

fn driver() -> EmbeddedDriver {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let directory = std::env::temp_dir().join(format!(
        "rusteron-archive-threads-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    EmbeddedDriver::launch_with(|context| {
        context.set_dir(&directory.to_string_lossy().into_c_string())?;
        Ok(())
    })
    .unwrap()
}

#[test]
fn archive_client_registers_regular_and_exclusive_publications_on_other_threads() {
    const MESSAGES: u64 = 500;
    let driver = driver();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let subscription = client
        .add_subscription(
            c"aeron:ipc",
            87301,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_secs(5),
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut workers = Vec::new();
    let start = Arc::new(Barrier::new(3));
    for producer in 0..2 {
        let client = client.clone();
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            // Each exclusive publication stays on the thread that creates it.
            let regular = (producer == 0).then(|| {
                client
                    .add_publication(c"aeron:ipc", 87301, Duration::from_secs(5))
                    .unwrap()
            });
            let exclusive = (producer == 1).then(|| {
                client
                    .add_exclusive_publication(c"aeron:ipc", 87301, Duration::from_secs(5))
                    .unwrap()
            });
            drop(client);
            for sequence in 0..MESSAGES {
                let bytes = (producer * MESSAGES + sequence).to_le_bytes();
                loop {
                    let result = match (&regular, &exclusive) {
                        (Some(publication), _) => publication.offer(&bytes),
                        (_, Some(publication)) => publication.offer(&bytes),
                        _ => unreachable!(),
                    };
                    match result {
                        Ok(_) => break,
                        Err(error) => assert!(error.is_retryable(), "{error}"),
                    }
                    assert!(Instant::now() < deadline, "publisher stalled");
                    std::thread::yield_now();
                }
            }
        }));
    }
    drop(client);
    start.wait();
    // The archive crate's subscription has the same Send-only contract as the client crate's.
    let receiver = std::thread::spawn(move || {
        let mut seen = vec![false; (2 * MESSAGES) as usize];
        let mut received = 0;
        while received < seen.len() {
            subscription
                .poll_fn(
                    |bytes, _| {
                        let id = u64::from_le_bytes(bytes.try_into().unwrap()) as usize;
                        assert!(!seen[id], "duplicate message {id}");
                        seen[id] = true;
                        received += 1;
                    },
                    64,
                )
                .unwrap();
            assert!(Instant::now() < deadline, "receiver stalled at {received}");
        }
        seen
    });
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(receiver.join().unwrap().into_iter().all(|seen| seen));
}

#[test]
fn persistent_subscription_stays_local_while_another_thread_reads_its_counters() {
    struct Listener(Rc<Cell<usize>>);
    impl Drop for Listener {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }
    impl PersistentSubscriptionListener for Listener {}

    let driver = driver();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let archive = AeronArchiveContext::new().unwrap();
    archive.set_aeron(&client).unwrap();
    archive.set_control_request_channel(c"aeron:ipc").unwrap();
    archive.set_control_response_channel(c"aeron:ipc").unwrap();
    let context = AeronArchivePersistentSubscriptionContext::new().unwrap();
    context.set_aeron(&client).unwrap();
    context.set_archive_context(&archive).unwrap();
    context.set_live_channel(c"aeron:ipc").unwrap();
    context.set_live_stream_id(87302).unwrap();
    context.set_replay_channel(c"aeron:ipc").unwrap();
    context.set_replay_stream_id(87303).unwrap();
    context.set_recording_id(0).unwrap();
    let retained = context.clone();
    let drops = Rc::new(Cell::new(0));
    let subscription =
        AeronArchivePersistentSubscription::create(context, Some(Box::new(Listener(drops.clone())))).unwrap();
    let local = subscription.clone();
    drop(subscription);
    let ids = [
        retained
            .get_state_counter()
            .unwrap()
            .get_constants()
            .unwrap()
            .counter_id(),
        retained
            .get_join_difference_counter()
            .unwrap()
            .get_constants()
            .unwrap()
            .counter_id(),
        retained
            .get_live_left_counter()
            .unwrap()
            .get_constants()
            .unwrap()
            .counter_id(),
        retained
            .get_live_joined_counter()
            .unwrap()
            .get_constants()
            .unwrap()
            .counter_id(),
    ];
    let expected = ids.map(|id| client.counters_reader().get_counter_value(id).unwrap());
    let start = Barrier::new(2);
    std::thread::scope(|scope| {
        let observer = scope.spawn(|| {
            let reader = client.counters_reader();
            start.wait();
            let mut samples = 0;
            // No archive server runs in this test, so the subscription waits for a connection.
            // Counter reads run concurrently with its poll and native conductor callbacks.
            for _ in 0..1000 {
                for (id, value) in ids.into_iter().zip(expected) {
                    assert_eq!(reader.get_counter_value(id).unwrap(), value);
                    samples += 1;
                }
            }
            samples
        });
        start.wait();
        for _ in 0..1000 {
            // Aeron returns state-machine work as well as the fragment count.
            local.poll_fn(|_, _| panic!("unexpected fragment"), 16).unwrap();
            assert!(!local.has_failed());
        }
        assert_eq!(observer.join().unwrap(), ids.len() * 1000);
    });
    drop(client);
    drop(archive);
    drop(local);
    assert_eq!(drops.get(), 0);
    // Context aliases retain both the native counters and the local listener through cleanup.
    assert!(retained.get_state_counter().is_some());
    drop(retained);
    assert_eq!(drops.get(), 1);
}
