//! Behaviour at the edges of the client API: range checks, poll limits, fragmentation,
//! retained images, driver loss and teardown from conductor callbacks
use rusteron_client::bindings::aeron_controlled_fragment_handler_action_t as Action;
use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);

/// `EmbeddedDriver` gives every launch its own directory, so tests can run in parallel
fn driver() -> EmbeddedDriver {
    EmbeddedDriver::launch().unwrap()
}

fn context_for(driver: &EmbeddedDriver) -> AeronContext {
    let context = AeronContext::new().unwrap();
    context.set_dir(&driver.dir().into_c_string()).unwrap();
    context
}

fn client_for(driver: &EmbeddedDriver) -> Aeron {
    let client = Aeron::new(&context_for(driver)).unwrap();
    client.start().unwrap();
    client
}

/// Repeats `attempt` until it returns a value, and fails the test once `TIMEOUT` has passed
fn eventually<T>(mut attempt: impl FnMut() -> Option<T>) -> T {
    let started = Instant::now();
    loop {
        if let Some(value) = attempt() {
            return value;
        }
        assert!(started.elapsed() < TIMEOUT, "no result after {TIMEOUT:?}");
        std::thread::park_timeout(Duration::from_micros(200));
    }
}

/// An IPC publication and a subscription on `stream_id` that sees it
fn connected_pair(client: &Aeron, stream_id: i32) -> (AeronPublication, AeronSubscription) {
    let subscription = client
        .add_subscription(c"aeron:ipc", stream_id, Handlers::NONE, Handlers::NONE, TIMEOUT)
        .unwrap();
    let publication = client.add_publication(c"aeron:ipc", stream_id, TIMEOUT).unwrap();
    eventually(|| subscription.is_connected().then_some(()));
    (publication, subscription)
}

/// Offers `message` until the log takes it and returns the stream position after it. Back
/// pressure is retried, anything else fails the test straight away
fn publish(publication: &AeronPublication, message: &[u8]) -> i64 {
    eventually(|| match publication.offer(message) {
        Ok(position) => Some(position),
        Err(refused) => {
            assert!(refused.is_retryable(), "{refused:?}");
            None
        }
    })
}

/// Polls until `count` fragments arrive, returning their payloads in order
fn take_fragments(subscription: &AeronSubscription, count: usize) -> Vec<Vec<u8>> {
    let mut fragments = Vec::new();
    eventually(|| {
        let limit = count - fragments.len();
        subscription
            .poll_fn(|data, _| fragments.push(data.to_vec()), limit)
            .unwrap();
        (fragments.len() == count).then_some(())
    });
    fragments
}

#[test]
fn counter_lookups_accept_the_last_id_and_reject_the_ones_either_side() {
    let driver = driver();
    let client = client_for(&driver);
    let reader = client.counters_reader();
    let last = reader.max_counter_id();
    assert!(last > 0);
    // C stores the highest valid id, so `last` is inside the values buffer and `last + 1` is not
    assert!(reader.get_counter_value(last).is_ok());
    for outside in [-1, last + 1, i32::MAX, i32::MIN] {
        assert!(reader.get_counter_value(outside).is_err(), "{outside}");
    }
    reader.addr_atomic(last).load(Ordering::Relaxed);
    let past_the_end = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        reader.addr_atomic(last + 1);
    }));
    assert!(past_the_end.is_err());
}

#[test]
fn a_dropped_counter_gives_its_record_back_to_the_driver() {
    let driver = driver();
    let client = client_for(&driver);
    let counter = client.add_counter(9101, b"key", "returned", TIMEOUT).unwrap();
    let id = counter.counter_id().unwrap();
    let reader = client.counters_reader();
    let allocated = bindings::AERON_COUNTER_RECORD_ALLOCATED as i32;
    assert_eq!(reader.counter_state(id).unwrap(), allocated);
    drop(counter);
    eventually(|| (reader.counter_state(id).unwrap() != allocated).then_some(()));
}

#[test]
fn a_zero_fragment_limit_leaves_the_message_for_the_next_poll() {
    let driver = driver();
    let client = client_for(&driver);
    let (publication, subscription) = connected_pair(&client, 9102);
    publish(&publication, b"kept");
    let mut delivered = 0;
    for _ in 0..100 {
        assert_eq!(subscription.poll_fn(|_, _| delivered += 1, 0).unwrap(), 0);
    }
    assert_eq!(delivered, 0);
    assert_eq!(take_fragments(&subscription, 1), [b"kept".to_vec()]);
}

// Each controlled poll is scripted with the actions its handler returns, in order. The image
// position then shows how much of the log each mix of actions consumed. The positions
// returned by `offer` mark where each message ends
#[test]
fn the_image_position_follows_each_controlled_action() {
    use Action::{AERON_ACTION_ABORT as ABORT, AERON_ACTION_BREAK as BREAK};
    use Action::{AERON_ACTION_COMMIT as COMMIT, AERON_ACTION_CONTINUE as CONTINUE};

    let driver = driver();
    let client = client_for(&driver);
    let (publication, subscription) = connected_pair(&client, 9103);
    let image = subscription.image_at_index(0).unwrap();
    let start = image.position();
    let payloads = [vec![0xA1; 24], vec![0xB2; 300], vec![0xC3; 7]];
    let ends: Vec<i64> = payloads.iter().map(|payload| publish(&publication, payload)).collect();

    // Returns the payload length of each fragment the handler was shown
    let scripted_poll = |script: &[Action]| {
        let mut answers = script.iter().copied();
        let mut shown = Vec::new();
        let handler = |data: &[u8], _: &AeronHeader| {
            shown.push(data.len());
            // Past the end of the script, refuse the fragment so it stays in the log
            answers.next().unwrap_or(ABORT)
        };
        subscription.controlled_poll_fn(handler, 10).unwrap();
        shown
    };

    assert_eq!(scripted_poll(&[ABORT]), [24]);
    assert_eq!(image.position(), start);

    // The commit keeps the first message even though the second is then refused
    assert_eq!(scripted_poll(&[COMMIT, ABORT]), [24, 300]);
    assert_eq!(image.position(), ends[0]);

    assert_eq!(scripted_poll(&[BREAK]), [300]);
    assert_eq!(image.position(), ends[1]);

    assert_eq!(scripted_poll(&[CONTINUE]), [7]);
    assert_eq!(image.position(), ends[2]);
    assert!(scripted_poll(&[]).is_empty());
}

#[test]
fn an_offer_with_nobody_listening_is_retryable() {
    let driver = driver();
    let client = client_for(&driver);
    let publication = client.add_publication(c"aeron:ipc", 9104, TIMEOUT).unwrap();
    let error = publication.offer(b"unheard").unwrap_err();
    assert!(matches!(error, AeronOfferError::NotConnected), "{error:?}");
    assert!(error.is_retryable());
}

#[test]
fn the_length_limits_are_exact() {
    let driver = driver();
    let client = client_for(&driver);
    let (publication, subscription) = connected_pair(&client, 9105);
    let max_message = publication.max_message_length().unwrap();
    let max_payload = publication.max_payload_length().unwrap();
    assert!(max_payload < max_message);

    // One byte over the limit is refused outright rather than retried
    let error = publication.offer(&vec![1; max_message + 1]).unwrap_err();
    assert!(!error.is_retryable(), "{error:?}");

    // A payload that fills one frame is not fragmented
    publish(&publication, &vec![2; max_payload]);
    assert_eq!(take_fragments(&subscription, 1)[0].len(), max_payload);

    // One more byte splits into two fragments
    publish(&publication, &vec![3; max_payload + 1]);
    let lengths: Vec<_> = take_fragments(&subscription, 2).iter().map(Vec::len).collect();
    assert_eq!(lengths, [max_payload, 1]);
}

#[test]
fn interleaved_fragmented_messages_from_two_sessions_reassemble_separately() {
    let driver = driver();
    let client = client_for(&driver);
    let subscription = client
        .add_subscription(c"aeron:ipc", 9106, Handlers::NONE, Handlers::NONE, TIMEOUT)
        .unwrap();
    // Each exclusive publication has its own session, where a shared one would be reused
    let first = client.add_exclusive_publication(c"aeron:ipc", 9106, TIMEOUT).unwrap();
    let second = client.add_exclusive_publication(c"aeron:ipc", 9106, TIMEOUT).unwrap();
    eventually(|| (subscription.image_count().unwrap() == 2).then_some(()));
    let length = first.max_payload_length().unwrap() * 3 + 7;
    let messages = [vec![b'a'; length], vec![b'b'; length]];
    eventually(|| first.offer(&messages[0]).ok());
    eventually(|| second.offer(&messages[1]).ok());

    let received = RefCell::new(Vec::new());
    let mut assembler = FragmentAssembler::new(|data: &[u8], _: &AeronHeader| {
        received.borrow_mut().push(data.to_vec());
    })
    .unwrap();
    // One fragment per poll moves round the images, so the two sessions' fragments interleave
    eventually(|| {
        assembler.poll(&subscription, 1).unwrap();
        (received.borrow().len() == 2).then_some(())
    });
    drop(assembler);
    let mut received = received.into_inner();
    received.sort();
    assert_eq!(received, messages);
}

// Every session found by walking the indexes can be looked up again by id. A session id that
// none of them use, and the index just past the end, find nothing
#[test]
fn lookups_find_only_the_images_the_subscription_holds() {
    let driver = driver();
    let client = client_for(&driver);
    let (_publication, subscription) = connected_pair(&client, 9107);
    let count = subscription.image_count().unwrap() as usize;
    let sessions: Vec<i32> = (0..count)
        .map(|index| subscription.image_at_index(index).unwrap().session_id().unwrap())
        .collect();
    for &session in &sessions {
        assert!(subscription.image_by_session_id(session).is_some(), "{session}");
    }
    let unused = (i32::MIN..).find(|id| !sessions.contains(id)).unwrap();
    assert!(subscription.image_by_session_id(unused).is_none());
    assert!(subscription.image_at_index(count).is_none());
}

// Each retained handle is a separate Rust value, but all of them read and move one native
// subscriber position. This is why nesting polls through two handles is not supported
#[test]
fn retained_handles_to_one_image_share_its_position() {
    let driver = driver();
    let client = client_for(&driver);
    let (publication, subscription) = connected_pair(&client, 9108);
    let session_id = publication.get_constants().unwrap().session_id();
    let by_index = subscription.image_at_index(0).unwrap();
    let by_session = subscription.image_by_session_id(session_id).unwrap();
    publish(&publication, b"shared");
    let start = by_session.position();
    eventually(|| (by_index.poll_fn(|_, _| {}, 1).unwrap() == 1).then_some(()));
    assert!(by_session.position() > start);
    assert_eq!(by_session.position(), by_index.position());
}

#[test]
fn a_retained_image_keeps_reading_after_its_subscription_handle_drops() {
    let driver = driver();
    let client = client_for(&driver);
    let (publication, subscription) = connected_pair(&client, 9109);
    let image = subscription.image_at_index(0).unwrap();
    drop(subscription);
    publish(&publication, b"after");
    let mut received = Vec::new();
    eventually(|| {
        image.poll_fn(|data, _| received.push(data.to_vec()), 1).unwrap();
        (!received.is_empty()).then_some(())
    });
    assert_eq!(received, [b"after".to_vec()]);
}

#[test]
fn a_subscription_moved_to_another_thread_resumes_where_it_stopped() {
    let driver = driver();
    let client = client_for(&driver);
    let (publication, subscription) = connected_pair(&client, 9110);
    for message in [b"here".as_slice(), b"there"] {
        publish(&publication, message);
    }
    assert_eq!(take_fragments(&subscription, 1), [b"here".to_vec()]);
    let remaining = std::thread::spawn(move || take_fragments(&subscription, 1))
        .join()
        .unwrap();
    assert_eq!(remaining, [b"there".to_vec()]);
}

#[test]
fn malformed_channels_fail_before_the_timeout() {
    let driver = driver();
    let client = client_for(&driver);
    let started = Instant::now();
    assert!(client.add_publication(c"aeron:carrier-pigeon", 9111, TIMEOUT).is_err());
    let subscription = client.add_subscription(c"not-a-uri", 9111, Handlers::NONE, Handlers::NONE, TIMEOUT);
    assert!(subscription.is_err());
    assert!(started.elapsed() < TIMEOUT / 2);
}

#[test]
fn resources_report_closed_once_the_driver_is_lost() {
    let driver = driver();
    let context = context_for(&driver);
    context.set_driver_timeout_ms(1_000).unwrap();
    // The conductor reports the driver timeout here instead of printing it. The poll loop
    // reads the count with one atomic load, so it never waits on the handler's mutex
    let errors = Arc::new(AtomicUsize::new(0));
    let reported = Arc::clone(&errors);
    context
        .set_error_handler(Some(move |_: i32, _: &str| {
            reported.fetch_add(1, Ordering::Release);
        }))
        .unwrap();
    let client = Aeron::new(&context).unwrap();
    client.start().unwrap();
    let (publication, subscription) = connected_pair(&client, 9112);
    assert_eq!(errors.load(Ordering::Acquire), 0);
    driver.stop();
    // C marks the client closed before it walks the resources and closes each one
    eventually(|| (client.is_closed() && publication.is_closed() && subscription.is_closed()).then_some(()));
    eventually(|| (errors.load(Ordering::Acquire) > 0).then_some(()));
    assert_eq!(publication.position(), i64::from(bindings::AERON_PUBLICATION_CLOSED));
    let error = publication.offer(b"late").unwrap_err();
    assert!(matches!(error, AeronOfferError::Closed), "{error:?}");
    // C refuses to queue the request. The blocking `add_*` helpers retry that refusal until
    // their timeout, so the async form is used to see it directly
    assert!(client.async_add_publication(c"aeron:ipc", 9112).is_err());
}

// A callback runs on the conductor thread, so dropping the last client there makes the
// conductor try to join itself. Aeron C refuses the join and the close fails, so the
// wrapper keeps the native client alive rather than free memory the conductor still uses.
// The drop returns without hanging and the client leaks
#[test]
fn releasing_the_last_client_from_its_own_error_callback_returns() {
    if std::env::var_os("RUSTERON_VALGRIND").is_some() {
        // The deliberate leak would fail the definite-leak check
        return;
    }
    let driver = driver();
    let context = context_for(&driver);
    context.set_driver_timeout_ms(1_000).unwrap();
    let slot: Arc<Mutex<Option<Aeron>>> = Arc::default();
    let released = Arc::new(AtomicBool::new(false));
    let (handler_slot, handler_released) = (slot.clone(), released.clone());
    context
        .set_error_handler(Some(move |_: i32, _: &str| {
            if let Some(client) = handler_slot.lock().unwrap().take() {
                drop(client);
                handler_released.store(true, Ordering::Release);
            }
        }))
        .unwrap();
    let client = Aeron::new(&context).unwrap();
    client.start().unwrap();
    *slot.lock().unwrap() = Some(client);
    drop(context);
    driver.stop();
    eventually(|| released.load(Ordering::Acquire).then_some(()));
}
