//! Compile-time trait checks and concurrent use of the native client.
use rusteron_client::*;
use rusteron_media_driver::{AeronDriver, AeronDriverContext, testing::EmbeddedDriver};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

// Ambiguous inference makes this fail to compile if the type gains the trait.
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
    AeronContext,
    AeronSubscription,
    AeronExclusivePublication,
    AeronImage,
    FragmentAssembler<fn(&[u8], &AeronHeader)>,
    ControlledFragmentAssembler<fn(&[u8], &AeronHeader) -> bindings::aeron_controlled_fragment_handler_action_t>,
    AeronAsyncAddPublication,
    AeronAsyncAddSubscription,
    Handler<usize>,
    AeronClaim<'static>,
    AeronDriverContext,
    AeronDriver
);
assert_not_impl!(FragmentAssembler<fn(&[u8], &AeronHeader)>, Clone);
assert_not_impl!(
    ControlledFragmentAssembler<fn(&[u8], &AeronHeader) -> bindings::aeron_controlled_fragment_handler_action_t>,
    Clone
);

assert_not_impl!(AeronPublicationRef, Clone);
assert_not_impl!(AeronSubscriptionRef, Clone);
assert_not_impl!(AeronFrameHeader<Inline<bindings::aeron_frame_header_t>>, Clone);
local!(
    AeronContextRef,
    AeronSubscriptionRef,
    AeronExclusivePublicationRef,
    AeronImageRef
);

#[test]
fn concurrent_handles_and_shared_callbacks_have_the_required_traits() {
    fn shared<T: Send + Sync>() {}
    shared::<Aeron>();
    shared::<AeronPublication>();
    shared::<AeronPublicationRef>();
    shared::<OwnedWithReference<Arc<()>, Aeron>>();
    shared::<AeronCounter>();
    shared::<AeronCountersReader>();
    // Callback state needs Send; the mutex supplies Sync.
    shared::<SharedHandler<Cell<usize>>>();
}

fn driver() -> EmbeddedDriver {
    // Tests run in parallel, so a clock reading alone can repeat and two drivers would share a directory
    static NEXT_DRIVER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let directory = std::env::temp_dir().join(format!(
        "rusteron-threads-{}-{}-{}",
        std::process::id(),
        NEXT_DRIVER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        Aeron::nano_clock()
    ));
    EmbeddedDriver::launch_with(|context| {
        context
            .set_dir(&directory.to_string_lossy().into_c_string())
            .map(|_| ())
    })
    .unwrap()
}

fn connect(driver: &EmbeddedDriver) -> Aeron {
    Aeron::connect_dir(driver.dir()).unwrap()
}

fn pair(client: &Aeron, stream: i32) -> (AeronPublication, AeronSubscription) {
    let publication = client
        .add_publication(AERON_IPC_STREAM, stream, Duration::from_secs(5))
        .unwrap();
    let subscription = client
        .add_subscription(
            AERON_IPC_STREAM,
            stream,
            Handlers::NONE,
            Handlers::NONE,
            Duration::from_secs(5),
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !publication.is_connected() {
        assert!(Instant::now() < deadline, "publication did not connect");
        std::thread::yield_now();
    }
    (publication, subscription)
}

#[test]
fn cloned_publications_publish_from_several_threads_and_keep_the_client_alive() {
    const THREADS: usize = 4;
    const MESSAGES: usize = 1000;
    let driver = driver();
    let client = connect(&driver);
    let (publication, subscription) = pair(&client, 8601);
    let start = Arc::new(Barrier::new(THREADS + 1));
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut workers = Vec::new();
    for producer in 0..THREADS {
        let publication = publication.clone();
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            start.wait();
            for sequence in 0..MESSAGES {
                let id = (producer * MESSAGES + sequence) as u64;
                while publication.offer(&id.to_le_bytes()).is_err() {
                    assert!(Instant::now() < deadline, "publisher stalled");
                    std::thread::yield_now();
                }
            }
        }));
    }
    // Closing these clones must leave the workers' handles usable.
    publication.close().unwrap();
    client.close().unwrap();
    start.wait();
    let mut seen = vec![false; THREADS * MESSAGES];
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
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(seen.into_iter().all(|value| value));
}

#[test]
fn closing_a_publication_clone_does_not_invalidate_an_active_claim() {
    let driver = driver();
    let client = connect(&driver);
    let (publication, subscription) = pair(&client, 8602);
    let other = publication.clone();
    let mut claim = publication.try_claim_owned(8).unwrap();
    other.close().unwrap();
    claim.data().copy_from_slice(b"claimed!");
    claim.commit().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut received = false;
    while !received {
        subscription
            .poll_fn(
                |bytes, _| {
                    assert_eq!(bytes, b"claimed!");
                    received = true;
                },
                10,
            )
            .unwrap();
        assert!(Instant::now() < deadline);
    }
}

#[test]
fn native_assembler_delivers_fragmented_messages_after_a_delegate_panic() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::Rc;

    let driver = driver();
    let client = connect(&driver);
    let (publication, subscription) = pair(&client, 8605);
    let length = publication.max_payload_length().unwrap() + 17;
    let payload = vec![7u8; length];
    let calls = Rc::new(Cell::new(0));
    let callback_calls = calls.clone();
    let mut assembler = FragmentAssembler::new(move |bytes: &[u8], _: &AeronHeader| {
        assert_eq!(bytes.len(), length);
        assert_eq!(bytes.first(), Some(&7));
        assert_eq!(bytes.last(), Some(&7));
        let call = callback_calls.get() + 1;
        callback_calls.set(call);
        assert_ne!(call, 1, "delegate panic for recovery test");
    })
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    for message in 1..=2 {
        while publication.offer(&payload).is_err() {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        while calls.get() < message {
            let result = catch_unwind(AssertUnwindSafe(|| assembler.poll(&subscription, 64).unwrap()));
            assert_eq!(result.is_err(), message == 1 && calls.get() == 1);
            assert!(Instant::now() < deadline);
        }
    }
    assert_eq!(calls.get(), 2);
}

#[test]
fn counter_output_getters_handle_success_and_unwritten_error_outputs() {
    #[cfg(feature = "log-c-bindings")]
    rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
    let driver = driver();
    let client = connect(&driver);
    let counter = client
        .add_counter(86, &[], "output-initialization", Duration::from_secs(5))
        .unwrap();
    let constants = counter.get_constants().unwrap();
    let reader = client.counters_reader();
    let id = constants.counter_id();
    assert_eq!(reader.counter_registration_id(id).unwrap(), constants.registration_id());
    assert_eq!(reader.counter_owner_id(id).unwrap(), client.client_id());
    assert_eq!(reader.counter_reference_id(id).unwrap(), 0);
    assert_eq!(reader.counter_type_id(id).unwrap(), 86);
    assert_eq!(reader.counter_type(id).unwrap(), AeronCounterTypeId::from_code(86));
    assert_eq!(reader.counter_record_state(id).unwrap(), AeronCounterState::Allocated);
    assert_eq!(
        reader.counter_state(id).unwrap(),
        bindings::AERON_COUNTER_RECORD_ALLOCATED as i32
    );
    assert_eq!(
        reader.free_for_reuse_deadline_ms(id).unwrap(),
        bindings::AERON_COUNTER_NOT_FREE_TO_REUSE as i64
    );

    // These native error paths return without writing the output pointer.
    for invalid in [-1, i32::MAX] {
        assert!(reader.counter_registration_id(invalid).is_err());
        assert!(reader.counter_owner_id(invalid).is_err());
        assert!(reader.counter_reference_id(invalid).is_err());
        assert!(reader.counter_type_id(invalid).is_err());
        assert!(reader.counter_state(invalid).is_err());
        assert!(reader.counter_type(invalid).is_err());
        assert!(reader.counter_record_state(invalid).is_err());
        assert!(reader.free_for_reuse_deadline_ms(invalid).is_err());
    }
}

#[test]
fn typed_counter_reader_methods_match_the_raw_ones() {
    let driver = driver();
    let client = connect(&driver);
    let reader = client.counters_reader();

    // The driver allocates system counter ids from the enum values
    let version = reader
        .system_counter_value(AeronSystemCounterType::ControlProtocolVersion)
        .unwrap();
    assert_eq!(
        version,
        reader
            .get_counter_value(AeronSystemCounterType::ControlProtocolVersion as i32)
            .unwrap()
    );
    assert!(version > 0);
    assert_eq!(
        reader.counter_type(AeronSystemCounterType::BytesSent as i32).unwrap(),
        AeronCounterTypeId::SystemCounter
    );

    // The driver adds the client heartbeat counter, keyed by the client id, on the first
    // client command, so it exists once `add_counter` returns
    let counter = client
        .add_counter(1_000_001, &[], "application type", Duration::from_secs(5))
        .unwrap();
    let id = counter.get_constants().unwrap().counter_id();
    let heartbeat = reader
        .find_by_counter_type_and_registration_id(AeronCounterTypeId::ClientHeartbeatTimestamp, client.client_id());
    assert!(heartbeat.is_some());
    assert_eq!(
        heartbeat,
        reader
            .find_by_type_and_registration_id(AeronCounterTypeId::ClientHeartbeatTimestamp.code(), client.client_id())
    );
    assert_eq!(
        reader.find_by_counter_type(AeronCounterTypeId::ClientHeartbeatTimestamp, |_| false),
        None
    );

    let mut raw = Vec::new();
    reader.foreach_counter_fn(|value, id, type_id, _, label| raw.push((value, id, type_id, label.to_owned())));
    let mut typed = Vec::new();
    reader.foreach_typed_counter_fn(|value, id, counter_type, _, label| {
        typed.push((value, id, counter_type.code(), label.to_owned()))
    });
    assert_eq!(raw, typed);
    let mut application = None;
    reader.foreach_typed_counter_fn(|_, counter_id, counter_type, _, _| {
        if counter_id == id {
            application = Some(counter_type);
        }
    });
    assert_eq!(application, Some(AeronCounterTypeId::Unknown(1_000_001)));
    assert_eq!(
        reader.find_by_counter_type(AeronCounterTypeId::Unknown(1_000_001), |_| true),
        Some(id)
    );
}

#[test]
fn shared_counter_updates_are_atomic_after_the_client_handle_is_dropped() {
    let driver = driver();
    let client = connect(&driver);
    let counter = client
        .add_counter(86, &[], "thread-safety", Duration::from_secs(5))
        .unwrap();
    drop(client);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let counter = &counter;
            scope.spawn(move || {
                for _ in 0..10_000 {
                    counter.addr_atomic().fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(counter.addr_atomic().load(Ordering::Relaxed), 40_000);
}

#[test]
fn contexts_cannot_be_reconfigured_after_native_use() {
    let driver = driver();
    assert!(driver.context().set_dir_delete_on_shutdown(false).is_err());
    let context = AeronContext::new().unwrap();
    context.set_dir(&driver.dir().into_c_string()).unwrap();
    let alias = context.clone();
    let client = Aeron::new(&context).unwrap();
    assert!(alias.set_driver_timeout_ms(1000).is_err());
    assert!(Aeron::new(&alias).is_err());
    client.start().unwrap();
    assert!(client.context().set_driver_timeout_ms(1000).is_err());
}

#[test]
fn failed_client_initialization_leaves_the_context_configurable() {
    let context = AeronContext::new().unwrap();
    let directory = std::env::temp_dir().join(format!("rusteron-missing-{}", Aeron::nano_clock()));
    context.set_dir(&directory.to_string_lossy().into_c_string()).unwrap();
    context.set_driver_timeout_ms(1).unwrap();
    assert!(Aeron::new(&context).is_err());
    context.set_driver_timeout_ms(2).unwrap();
}

#[test]
fn borrowed_driver_context_cannot_retain_native_pointers() {
    let context = AeronDriverContext::new().unwrap();
    let context = context.as_ref();
    // SAFETY: the borrowed setter rejects this empty value before passing it to C.
    let tracker = unsafe { rusteron_media_driver::AeronDutyCycleTracker::new_zeroed_on_heap() };
    assert!(context.set_sender_duty_cycle_tracker(&tracker).is_err());
    // SAFETY: the borrowed setter rejects this empty value before passing it to C.
    let manager = unsafe { rusteron_media_driver::AeronPortManager::new_zeroed_on_heap() };
    assert!(context.set_sender_port_manager(&manager).is_err());
}

#[test]
fn a_shared_client_starts_its_native_background_conductor_once() {
    let driver = driver();
    let context = AeronContext::new().unwrap();
    context.set_dir(&driver.dir().into_c_string()).unwrap();
    let client = Aeron::new(&context).unwrap();
    let start = Barrier::new(4);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    start.wait();
                    client.start()
                })
            })
            .collect();
        let mut succeeded = 0;
        for worker in workers {
            match worker.join().unwrap() {
                Ok(_) => succeeded += 1,
                Err(error) => assert!(error.message().unwrap().contains("already running")),
            }
        }
        assert!(succeeded > 0);
    });
    client.start().unwrap();
    assert!(!client.is_closed());
}

#[test]
fn a_shared_callback_can_be_called_by_several_native_conductors() {
    let driver = driver();
    let calls = Arc::new(AtomicUsize::new(0));
    let state = calls.clone();
    // Reuse the same mutable callback on two native conductor threads.
    let handler = SharedHandler::new_shared(move |_: &AeronCountersReaderRef, _: i64, _: i32| {
        state.fetch_add(1, Ordering::Relaxed);
    });
    let mut clients = Vec::new();
    for _ in 0..2 {
        let context = AeronContext::new().unwrap();
        context.set_dir(&driver.dir().into_c_string()).unwrap();
        context.set_on_available_counter(Some(handler.clone())).unwrap();
        let client = Aeron::new(&context).unwrap();
        client.start().unwrap();
        clients.push(client);
    }
    let counter = clients[0]
        .add_counter(86, &[], "callback-safety", Duration::from_secs(5))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while calls.load(Ordering::Relaxed) < 2 {
        assert!(
            Instant::now() < deadline,
            "both conductors did not deliver the callback"
        );
        std::thread::yield_now();
    }
    drop(counter);
    drop(clients);
}

#[test]
fn committed_claims_publish_payload_and_headers_to_another_thread() {
    const MESSAGES: u64 = 2000;
    let driver = driver();
    let client = connect(&driver);
    let (publication, subscription) = pair(&client, 8606);
    let deadline = Instant::now() + Duration::from_secs(15);
    let publisher = std::thread::spawn(move || {
        for sequence in 0..MESSAGES {
            loop {
                if let Ok(mut claim) = publication.try_claim_owned(32) {
                    claim.data().fill(sequence as u8);
                    claim.data()[..8].copy_from_slice(&sequence.to_le_bytes());
                    claim.set_reserved_value(sequence as i64);
                    claim.commit().unwrap();
                    break;
                }
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
        }
    });
    let mut received = 0;
    while received < MESSAGES {
        subscription
            .poll_fn(
                |bytes, header| {
                    assert_eq!(u64::from_le_bytes(bytes[..8].try_into().unwrap()), received);
                    assert!(bytes[8..].iter().all(|&b| b == received as u8));
                    assert_eq!(header.reserved_value(), Some(received as i64));
                    received += 1;
                },
                64,
            )
            .unwrap();
        assert!(Instant::now() < deadline);
    }
    publisher.join().unwrap();
}

#[test]
fn vectored_offers_handle_empty_parts_capacity_and_fragmentation() {
    let driver = driver();
    let client = connect(&driver);
    let (publication, subscription) = pair(&client, 8607);
    let exclusive = client
        .add_exclusive_publication(AERON_IPC_STREAM, 8607, Duration::from_secs(5))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !exclusive.is_connected() {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    let large = vec![9; publication.max_payload_length().unwrap() + 17];
    let cases: Vec<Vec<&[u8]>> = vec![
        vec![],
        vec![b""],
        vec![b"", b"a", b"", b"bc", b""],
        vec![b"a"; MAX_OFFER_PARTS],
        vec![b"", &large[..10], b"", &large[10..], b""],
    ];
    for exclusive_lane in [false, true] {
        for parts in &cases {
            let expected = parts.concat();
            let received = std::rc::Rc::new(Cell::new(false));
            let callback_received = received.clone();
            let mut assembler = FragmentAssembler::new(move |bytes: &[u8], _: &AeronHeader| {
                assert_eq!(bytes, expected);
                assert!(!callback_received.replace(true), "duplicate message");
            })
            .unwrap();
            loop {
                let result = if exclusive_lane {
                    exclusive.offer_parts(parts)
                } else {
                    publication.offer_parts(parts)
                };
                match result {
                    Ok(_) => break,
                    Err(error) => assert!(error.is_retryable(), "{error}"),
                }
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            while !received.get() {
                assembler.poll(&subscription, 64).unwrap();
                assert!(Instant::now() < deadline);
            }
        }
    }
    let too_many = [b"".as_slice(); MAX_OFFER_PARTS + 1];
    assert!(matches!(
        publication.offer_parts(&too_many),
        Err(AeronOfferError::TooManyParts)
    ));
    assert!(matches!(
        exclusive.offer_parts(&too_many),
        Err(AeronOfferError::TooManyParts)
    ));
}

#[test]
fn owned_assembler_borrows_callback_state_and_recovers_after_a_fragmented_panic() {
    struct Capture<'a> {
        seen: &'a mut Vec<Vec<u8>>,
        calls: usize,
        panic_next: bool,
    }
    impl AeronFragmentHandlerCallback for Capture<'_> {
        fn handle_aeron_fragment_handler(&mut self, bytes: &[u8], header: &AeronHeader) {
            assert_eq!(header.stream_id(), Some(8608));
            self.calls += 1;
            if std::mem::take(&mut self.panic_next) {
                panic!("owned assembler recovery");
            }
            self.seen.push(bytes.to_vec());
        }
    }
    let driver = driver();
    let client = connect(&driver);
    let (publication, subscription) = pair(&client, 8608);
    let mut seen = Vec::new();
    let large = vec![7; publication.max_payload_length().unwrap() * 3 + 17];
    let deadline = Instant::now() + Duration::from_secs(15);
    {
        let mut assembler = FragmentAssembler::new(Capture {
            seen: &mut seen,
            calls: 0,
            panic_next: false,
        })
        .unwrap();
        for (index, bytes) in [b"small".as_slice(), large.as_slice(), large.as_slice()]
            .into_iter()
            .enumerate()
        {
            assembler.handler_mut().panic_next = index == 1;
            while publication.offer(bytes).is_err() {
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            let mut panicked = false;
            while assembler.handler().calls <= index {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    assembler.poll(&subscription, 1).unwrap();
                }));
                if let Err(payload) = result {
                    assert_eq!(payload.downcast_ref::<&str>(), Some(&"owned assembler recovery"));
                    assert!(!panicked);
                    panicked = true;
                }
                assert!(Instant::now() < deadline);
            }
            assert_eq!(panicked, index == 1);
        }
        assert_eq!(assembler.handler().seen.len(), 2);
    }
    assert_eq!(seen, [b"small".to_vec(), large]);
}

#[test]
fn owned_assembler_drops_its_callback_once() {
    struct Counted(Arc<AtomicUsize>);
    impl AeronFragmentHandlerCallback for Counted {
        fn handle_aeron_fragment_handler(&mut self, _: &[u8], _: &AeronHeader) {}
    }
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let assembler = FragmentAssembler::new(Counted(drops.clone())).unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(assembler);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn controlled_owned_assembler_retries_a_fragmented_message_after_abort() {
    use bindings::aeron_controlled_fragment_handler_action_t::{AERON_ACTION_ABORT, AERON_ACTION_CONTINUE};

    struct Capture<'a> {
        seen: &'a mut Vec<Vec<u8>>,
        abort_next: bool,
    }
    impl AeronControlledFragmentHandlerCallback for Capture<'_> {
        fn handle_aeron_controlled_fragment_handler(
            &mut self,
            bytes: &[u8],
            _: &AeronHeader,
        ) -> bindings::aeron_controlled_fragment_handler_action_t {
            self.seen.push(bytes.to_vec());
            if std::mem::take(&mut self.abort_next) {
                AERON_ACTION_ABORT
            } else {
                AERON_ACTION_CONTINUE
            }
        }
    }

    let driver = driver();
    let client = connect(&driver);
    let (publication, subscription) = pair(&client, 8610);
    let large = vec![0x5a; publication.max_payload_length().unwrap() * 2 + 17];
    let deadline = Instant::now() + Duration::from_secs(15);
    while publication.offer(&large).is_err() {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }

    let mut seen = Vec::new();
    {
        let mut assembler = ControlledFragmentAssembler::new(Capture {
            seen: &mut seen,
            abort_next: true,
        })
        .unwrap();
        assert!(!assembler.delete_session_buffer(publication.session_id()));
        while assembler.handler().seen.len() < 2 {
            assembler.poll(&subscription, 1).unwrap();
            assert!(
                Instant::now() < deadline,
                "controlled assembler did not retry the final fragment"
            );
        }
        assert!(!assembler.handler().abort_next);
    }
    assert_eq!(seen, [large.clone(), large]);
}

#[test]
fn controlled_owned_assembler_resumes_panic_and_retries() {
    struct Capture<'a> {
        seen: &'a mut Vec<Vec<u8>>,
        panic_next: bool,
    }
    impl AeronControlledFragmentHandlerCallback for Capture<'_> {
        fn handle_aeron_controlled_fragment_handler(
            &mut self,
            bytes: &[u8],
            _: &AeronHeader,
        ) -> bindings::aeron_controlled_fragment_handler_action_t {
            self.seen.push(bytes.to_vec());
            if std::mem::take(&mut self.panic_next) {
                panic!("controlled owned assembler panic");
            }
            bindings::aeron_controlled_fragment_handler_action_t::AERON_ACTION_CONTINUE
        }
    }

    let driver = driver();
    let client = connect(&driver);
    let (publication, subscription) = pair(&client, 8611);
    let large = vec![0x4b; publication.max_payload_length().unwrap() * 2 + 17];
    let deadline = Instant::now() + Duration::from_secs(15);
    while publication.offer(&large).is_err() {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }

    let mut seen = Vec::new();
    {
        let mut assembler = ControlledFragmentAssembler::new(Capture {
            seen: &mut seen,
            panic_next: true,
        })
        .unwrap();
        let panic_payload = loop {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| assembler.poll(&subscription, 1))) {
                Ok(result) => {
                    result.unwrap();
                }
                Err(payload) => break payload,
            }
            assert!(Instant::now() < deadline, "fragmented message was not delivered");
        };
        assert_eq!(
            panic_payload.downcast_ref::<&str>(),
            Some(&"controlled owned assembler panic")
        );
        while assembler.handler().seen.len() < 2 {
            assembler.poll(&subscription, 1).unwrap();
            assert!(Instant::now() < deadline, "panicked message was not retried");
        }
    }
    assert_eq!(seen, [large.clone(), large]);
}

#[test]
fn controlled_owned_assembler_drops_its_callback_once() {
    struct Counted(Arc<AtomicUsize>);
    impl AeronControlledFragmentHandlerCallback for Counted {
        fn handle_aeron_controlled_fragment_handler(
            &mut self,
            _: &[u8],
            _: &AeronHeader,
        ) -> bindings::aeron_controlled_fragment_handler_action_t {
            bindings::aeron_controlled_fragment_handler_action_t::AERON_ACTION_CONTINUE
        }
    }
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let assembler = ControlledFragmentAssembler::new(Counted(drops.clone())).unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(assembler);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn cached_subscription_survives_closing_a_clone() {
    let driver = driver();
    let client = connect(&driver);
    let (publication, subscription) = pair(&client, 8609);
    subscription.clone().close().unwrap();
    let received = std::rc::Rc::new(Cell::new(false));
    let callback_received = received.clone();
    let mut assembler = FragmentAssembler::new(move |bytes: &[u8], _: &AeronHeader| {
        assert_eq!(bytes, b"still alive");
        callback_received.set(true);
    })
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while publication.offer(b"still alive").is_err() {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    while !received.get() {
        assembler.poll(&subscription, 64).unwrap();
        assert!(Instant::now() < deadline);
    }
}

/// The typed `try_claim` wrappers map native sentinels to errors and expose the claimed
/// term buffer, and the fragment header of a polled message points back to its image
#[test]
fn typed_claims_map_errors_and_write_through_the_claimed_buffer() {
    let driver = driver();
    let client = connect(&driver);
    let lonely = client
        .add_exclusive_publication(AERON_IPC_STREAM, 8701, Duration::from_secs(5))
        .unwrap();
    let claim = AeronBufferClaim::default();
    assert_eq!(lonely.try_claim(8, &claim), Err(AeronOfferError::NotConnected));
    assert_eq!(lonely.try_claim_raw(8, &claim), -1);

    let (publication, subscription) = pair(&client, 8702);
    let mut claim = AeronBufferClaim::default();
    assert!(publication.try_claim(4, &claim).unwrap() > 0);
    // SAFETY: the claim succeeded and nothing else touches it before the commit
    unsafe {
        assert_eq!(claim.data_mut().len(), 4);
        claim.data_mut().copy_from_slice(b"ping");
        claim.frame_header_mut().expect("claimed frame header").reserved_value = 77;
        claim.commit().unwrap();
    }

    let mut received = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while received.is_none() {
        assert!(Instant::now() < deadline, "claimed message did not arrive");
        subscription
            .poll_fn(
                |buffer, header| {
                    assert!(header.image().is_some());
                    received = Some((buffer.to_vec(), header.reserved_value()));
                },
                10,
            )
            .unwrap();
    }
    assert_eq!(received, Some((b"ping".to_vec(), Some(77))));
}

#[test]
fn new_blocking_times_out_without_a_driver() {
    let directory = std::env::temp_dir().join(format!("rusteron-no-driver-{}", Aeron::nano_clock()));
    let context = AeronContext::new().unwrap();
    context.set_dir(&directory.to_string_lossy().into_c_string()).unwrap();
    context.set_driver_timeout_ms(50).unwrap();
    let start = Instant::now();
    let error = Aeron::new_blocking(&context, Duration::from_millis(100)).unwrap_err();
    assert_eq!(error.kind(), AeronErrorType::TimedOut);
    assert!(start.elapsed() < Duration::from_secs(5));
}

#[test]
fn addr_atomic_rejects_ids_outside_the_counters_buffer() {
    let driver = driver();
    let client = connect(&driver);
    let reader = client.counters_reader();
    for id in [-1, reader.max_counter_id() + 1] {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reader.addr_atomic(id);
        }));
        assert!(result.is_err(), "counter id {id}");
    }
    reader.addr_atomic(reader.max_counter_id());
}
