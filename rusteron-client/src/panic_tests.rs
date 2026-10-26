//! A panic in a poll handler reaches the caller of `poll`, and the next poll redelivers from
//! the last committed position. All other callbacks abort, checked in a child process

use super::*;
use serial_test::serial;
use std::any::Any;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::rc::Rc;
use std::time::Instant;

const TIMEOUT: Duration = Duration::from_secs(5);

struct Ipc {
    // field order is drop order: client handles before the driver
    publication: AeronPublication,
    subscription: AeronSubscription,
    aeron: Aeron,
    driver: rusteron_media_driver::testing::EmbeddedDriver,
}

fn client(dir: &str) -> Aeron {
    let ctx = AeronContext::new().unwrap();
    ctx.set_dir(&dir.into_c_string()).unwrap();
    let aeron = Aeron::new(&ctx).unwrap();
    aeron.start().unwrap();
    aeron
}

fn ipc(stream_id: i32) -> Ipc {
    rusteron_code_gen::test_logger::init(log::LevelFilter::Info);
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch().unwrap();
    let aeron = client(driver.dir());
    let subscription = aeron
        .add_subscription(AERON_IPC_STREAM, stream_id, Handlers::NONE, Handlers::NONE, TIMEOUT)
        .unwrap();
    let publication = aeron.add_publication(AERON_IPC_STREAM, stream_id, TIMEOUT).unwrap();
    wait(|| subscription.is_connected() && publication.is_connected());
    Ipc {
        publication,
        subscription,
        aeron,
        driver,
    }
}

fn wait(mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(start.elapsed() < TIMEOUT, "timed out");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Offers each value as its own message. Over IPC the next poll sees them all
fn offer(publication: &AeronPublication, values: impl IntoIterator<Item = u32>) {
    for value in values {
        wait(|| publication.offer(&value.to_le_bytes()).is_ok());
    }
}

fn value(buffer: &[u8]) -> u32 {
    u32::from_le_bytes(buffer[..4].try_into().unwrap())
}

/// Repeats `poll` until it panics and returns the payload. A timeout fails the test, it is
/// not mistaken for the expected panic
fn poll_until_panic(mut poll: impl FnMut()) -> Box<dyn Any + Send> {
    let start = Instant::now();
    loop {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(&mut poll)) {
            return payload;
        }
        assert!(start.elapsed() < TIMEOUT, "timed out waiting for the handler to panic");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Repeats `poll` until at least `count` values have arrived
fn collect(count: usize, mut poll: impl FnMut(&mut Vec<u32>)) -> Vec<u32> {
    let mut seen = vec![];
    wait(|| {
        poll(&mut seen);
        seen.len() >= count
    });
    seen
}

fn drain(subscription: &AeronSubscription, count: usize) -> Vec<u32> {
    collect(count, |seen| {
        subscription.poll_fn(|buffer, _| seen.push(value(buffer)), 10).unwrap();
    })
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<non-string payload>")
}

fn boom() -> ! {
    // resume_unwind skips the panic hook, so the tests do not flood stderr
    resume_unwind(Box::new("boom"))
}

#[test]
#[serial]
fn panic_reaches_caller_with_payload_and_batch_is_redelivered() {
    let ipc = ipc(5001);
    offer(&ipc.publication, 1..=5);

    let mut seen = vec![];
    let payload = poll_until_panic(|| {
        ipc.subscription
            .poll_fn(
                |buffer, _| {
                    seen.push(value(buffer));
                    if value(buffer) == 3 {
                        panic!("boom at 3");
                    }
                },
                10,
            )
            .unwrap();
    });
    assert_eq!(panic_message(&*payload), "boom at 3");
    assert_eq!(seen, [1, 2, 3]);

    // nothing was committed, so the whole batch comes back in order
    assert_eq!(drain(&ipc.subscription, 5), [1, 2, 3, 4, 5]);
}

/// With a COMMIT after #2 the next poll starts at #3; without one it starts at #1
#[test]
#[serial]
fn controlled_poll_redelivers_from_last_commit() {
    for (stream_id, commit_at, expected) in [(5002, Some(2), vec![3, 4, 5]), (5003, None, vec![1, 2, 3, 4, 5])] {
        let ipc = ipc(stream_id);
        offer(&ipc.publication, 1..=5);

        let mut seen = vec![];
        let payload = poll_until_panic(|| {
            ipc.subscription
                .controlled_poll_fn(
                    |buffer, _| {
                        let v = value(buffer);
                        seen.push(v);
                        match v {
                            4 => boom(),
                            _ if Some(v) == commit_at => {
                                aeron_controlled_fragment_handler_action_t::AERON_ACTION_COMMIT
                            }
                            _ => aeron_controlled_fragment_handler_action_t::AERON_ACTION_CONTINUE,
                        }
                    },
                    10,
                )
                .unwrap();
        });
        assert_eq!(panic_message(&*payload), "boom");
        assert_eq!(seen, [1, 2, 3, 4], "commit at {commit_at:?}");
        assert_eq!(
            drain(&ipc.subscription, expected.len()),
            expected,
            "commit at {commit_at:?}"
        );
    }
}

/// Subscription and image block poll: the block that panicked is delivered again whole
#[test]
#[serial]
fn block_poll_redelivers_the_block() {
    type BlockPoll = fn(&Ipc, &mut dyn FnMut(&[u8]));
    let variants: [(&str, i32, BlockPoll); 2] = [
        ("subscription", 5004, |ipc, f| {
            ipc.subscription.block_poll_fn(|buffer, _, _| f(buffer), 1 << 16);
        }),
        ("image", 5005, |ipc, f| {
            let image = ipc.subscription.image_at_index(0).expect("connected image");
            image.block_poll_fn(|buffer, _, _| f(buffer), 1 << 16).unwrap();
        }),
    ];
    for (name, stream_id, poll) in variants {
        let ipc = ipc(stream_id);
        offer(&ipc.publication, 1..=3);

        let mut first = 0;
        let payload = poll_until_panic(|| {
            poll(&ipc, &mut |buffer| {
                first = buffer.len();
                boom();
            })
        });
        assert_eq!(panic_message(&*payload), "boom", "{name}");

        let mut again = 0;
        wait(|| {
            poll(&ipc, &mut |buffer| again = buffer.len());
            again > 0
        });
        assert_eq!(again, first, "{name}");
    }
}

/// For each image poll variant: panic on value 2 of 3, then the next poll gets all three
#[test]
#[serial]
fn image_poll_variants_redeliver() {
    type Poll = fn(&AeronImage, &mut dyn FnMut(&[u8]));
    let variants: [(&str, Poll); 4] = [
        ("poll", |image, f| {
            image.poll_fn(|buffer, _| f(buffer), 10).unwrap();
        }),
        ("bounded_poll", |image, f| {
            image.bounded_poll_fn(|buffer, _| f(buffer), i64::MAX, 10).unwrap();
        }),
        ("controlled_poll", |image, f| {
            image
                .controlled_poll_fn(
                    |buffer, _| {
                        f(buffer);
                        aeron_controlled_fragment_handler_action_t::AERON_ACTION_CONTINUE
                    },
                    10,
                )
                .unwrap();
        }),
        ("bounded_controlled_poll", |image, f| {
            image
                .bounded_controlled_poll_fn(
                    |buffer, _| {
                        f(buffer);
                        aeron_controlled_fragment_handler_action_t::AERON_ACTION_CONTINUE
                    },
                    i64::MAX,
                    10,
                )
                .unwrap();
        }),
    ];
    for (i, (name, poll)) in variants.into_iter().enumerate() {
        let ipc = ipc(5100 + i as i32);
        let image = ipc.subscription.image_at_index(0).expect("connected image");
        offer(&ipc.publication, 1..=3);

        let mut seen = vec![];
        let payload = poll_until_panic(|| {
            poll(&image, &mut |buffer| {
                seen.push(value(buffer));
                if value(buffer) == 2 {
                    boom();
                }
            })
        });
        assert_eq!(panic_message(&*payload), "boom", "{name}");
        assert_eq!(seen, [1, 2], "{name}");

        let again = collect(3, |again| poll(&image, &mut |buffer| again.push(value(buffer))));
        assert_eq!(again, [1, 2, 3], "{name}");
    }
}

/// Only the image that panicked redelivers; images polled before it are already committed.
/// The poll can start at either image, so the handler panics on the second image it sees
#[test]
#[serial]
fn only_the_panicking_image_redelivers() {
    let ipc = ipc(5006);
    let first_pub = ipc
        .aeron
        .add_exclusive_publication(AERON_IPC_STREAM, 5006, TIMEOUT)
        .unwrap();
    let second_pub = ipc
        .aeron
        .add_exclusive_publication(AERON_IPC_STREAM, 5006, TIMEOUT)
        .unwrap();
    wait(|| ipc.subscription.image_count().unwrap() >= 3);
    // the image a value came from is value / 100
    for v in 1..=3u32 {
        wait(|| first_pub.offer(&(100 + v).to_le_bytes()).is_ok());
        wait(|| second_pub.offer(&(200 + v).to_le_bytes()).is_ok());
    }
    let image_values = |image: u32| (1..=3).map(|v| image * 100 + v).collect::<Vec<_>>();

    let mut seen: Vec<u32> = vec![];
    let mut panicked_on = None;
    poll_until_panic(|| {
        ipc.subscription
            .poll_fn(
                |buffer, _| {
                    let v = value(buffer);
                    if seen.first().is_some_and(|first| first / 100 != v / 100) {
                        panicked_on = Some(v);
                        boom();
                    }
                    seen.push(v);
                },
                10,
            )
            .unwrap();
    });
    let committed_image = seen[0] / 100;
    let panicking_image = panicked_on.unwrap() / 100;
    assert_eq!(seen, image_values(committed_image));
    assert_eq!(panicked_on, Some(panicking_image * 100 + 1));

    // only the panicking image comes back, from its first value
    assert_eq!(drain(&ipc.subscription, 3), image_values(panicking_image));
}

struct DropCount(Rc<Cell<usize>>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
#[serial]
fn once_closure_captures_drop_exactly_once() {
    let ipc = ipc(5007);
    offer(&ipc.publication, [1]);
    let drops = Rc::new(Cell::new(0));

    let owned = DropCount(drops.clone());
    // the frame is already in the log, so this first poll delivers it
    let result = catch_unwind(AssertUnwindSafe(|| {
        ipc.subscription
            .poll_fn(
                move |_, _| {
                    let _keep = &owned;
                    boom();
                },
                10,
            )
            .unwrap()
    }));
    assert_eq!(panic_message(&*result.unwrap_err()), "boom");
    assert_eq!(drops.get(), 1, "the moved-in capture is dropped once by the unwind");
}

struct Recorder {
    seen: Vec<u32>,
    panic_on: Option<u32>,
    _drops: DropCount,
}

impl AeronFragmentHandlerCallback for Recorder {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        let v = value(buffer);
        self.seen.push(v);
        if self.panic_on == Some(v) {
            boom();
        }
    }
}

#[test]
#[serial]
fn retained_handler_survives_panic_and_drops_once() {
    let ipc = ipc(5008);
    offer(&ipc.publication, 1..=3);
    let drops = Rc::new(Cell::new(0));
    let handler = Handler::new(Recorder {
        seen: vec![],
        panic_on: Some(2),
        _drops: DropCount(drops.clone()),
    });

    poll_until_panic(|| {
        ipc.subscription.poll(Some(&handler), 10).unwrap();
    });
    assert_eq!(handler.seen, [1, 2]);

    // SAFETY: no poll is running, so nothing else borrows the handler
    unsafe { handler.get_mut() }.panic_on = None;
    wait(|| {
        ipc.subscription.poll(Some(&handler), 10).unwrap();
        handler.seen.len() >= 5
    });
    assert_eq!(handler.seen, [1, 2, 1, 2, 3]);

    assert_eq!(drops.get(), 0);
    drop(handler);
    assert_eq!(drops.get(), 1);
}

#[test]
#[serial]
fn many_panics_then_normal_traffic() {
    const WARM_UP: u32 = 100;
    const CYCLES: u32 = 10_000;
    let ipc = ipc(5009);
    let drops = Rc::new(Cell::new(0));

    let mut baseline = 0;
    for i in 0..CYCLES {
        if i == WARM_UP {
            baseline = crate::test_alloc::current_allocs();
        }
        offer(&ipc.publication, [i]);
        let guard = DropCount(drops.clone());
        let payload = poll_until_panic(|| {
            ipc.subscription
                .poll_fn(
                    |_, _| {
                        let _keep = &guard;
                        resume_unwind(Box::new(i));
                    },
                    10,
                )
                .unwrap();
        });
        assert_eq!(*payload.downcast::<u32>().unwrap(), i);
        drop(guard);
        assert_eq!(drain(&ipc.subscription, 1), [i]);
    }
    assert_eq!(drops.get(), CYCLES as usize, "every capture dropped exactly once");
    // a leak per panic would add thousands; 64 allows for stray log lines
    let growth = crate::test_alloc::current_allocs() - baseline;
    assert!(growth < 64, "{growth} allocations still live after the panics");

    let mut received = 0u32;
    for chunk in (0..CYCLES).collect::<Vec<_>>().chunks(100) {
        offer(&ipc.publication, chunk.iter().copied());
        received += drain(&ipc.subscription, chunk.len()).len() as u32;
    }
    assert_eq!(received, CYCLES);
}

#[test]
#[serial]
fn panic_on_one_thread_does_not_affect_another() {
    let ipc = ipc(5010);
    let dir = ipc.driver.dir().to_string();

    let polling = |stream_id: i32, panic_first: bool| {
        let dir = dir.clone();
        std::thread::spawn(move || {
            let aeron = client(&dir);
            let subscription = aeron
                .add_subscription(AERON_IPC_STREAM, stream_id, Handlers::NONE, Handlers::NONE, TIMEOUT)
                .unwrap();
            let publication = aeron.add_publication(AERON_IPC_STREAM, stream_id, TIMEOUT).unwrap();
            wait(|| subscription.is_connected());
            offer(&publication, 1..=100);
            if panic_first {
                poll_until_panic(|| {
                    subscription.poll_fn(|_, _| boom(), 10).unwrap();
                });
            }
            drain(&subscription, 100)
        })
    };
    let panicking = polling(5011, true);
    let normal = polling(5012, false);
    assert_eq!(panicking.join().unwrap(), (1..=100).collect::<Vec<_>>());
    assert_eq!(normal.join().unwrap(), (1..=100).collect::<Vec<_>>());
}

// Abort paths: the parent re-runs the test binary on one child test, which panics in a
// callback, and expects SIGABRT plus the logged callback name

const LOGGED_ABORT: &str = " panicked inside an Aeron callback, aborting";

fn assert_child_aborts(child: &str, callback: &str) {
    use std::os::unix::process::ExitStatusExt;
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([child, "--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let expected = format!("{callback}{LOGGED_ABORT}");
    assert_eq!(output.status.signal(), Some(6), "{child} must abort, stderr:\n{stderr}");
    assert!(stderr.contains(&expected), "missing `{expected}` in stderr:\n{stderr}");
}

#[test]
#[serial]
fn fragment_assembler_delegate_panic_aborts() {
    assert_child_aborts(
        "panic_tests::child_fragment_assembler_delegate_panics",
        "aeron_fragment_handler_t_callback",
    );
}

#[test]
#[ignore = "run by fragment_assembler_delegate_panic_aborts in a child process"]
fn child_fragment_assembler_delegate_panics() {
    let ipc = ipc(5020);
    let (assembler, _delegate) = Handler::with_fragment_assembler(|_: &[u8], _: AeronHeader| panic!("boom")).unwrap();
    offer(&ipc.publication, [1]);
    wait(|| ipc.subscription.poll(Some(&assembler), 10).unwrap() > 0);
}

#[test]
#[serial]
fn image_visitor_panic_aborts() {
    assert_child_aborts("panic_tests::child_image_visitor_panics", "rusteron_image_visitor");
}

#[test]
#[ignore = "run by image_visitor_panic_aborts in a child process"]
fn child_image_visitor_panics() {
    let ipc = ipc(5021);
    ipc.subscription.for_each_image(|_| panic!("boom"));
}

#[test]
#[serial]
fn reserved_value_supplier_panic_aborts() {
    assert_child_aborts(
        "panic_tests::child_reserved_value_supplier_panics",
        "aeron_reserved_value_supplier_t_callback",
    );
}

#[test]
#[ignore = "run by reserved_value_supplier_panic_aborts in a child process"]
fn child_reserved_value_supplier_panics() {
    let ipc = ipc(5022);
    let supplier = Handler::new(|_: &mut [u8]| -> i64 { panic!("boom") });
    let _ = ipc
        .publication
        .offer_with_reserved_value(&[1, 2, 3, 4], Some(&supplier));
}

#[test]
#[serial]
fn conductor_callback_panic_aborts() {
    assert_child_aborts(
        "panic_tests::child_conductor_callback_panics",
        "aeron_on_available_image_t_callback",
    );
}

#[test]
#[ignore = "run by conductor_callback_panic_aborts in a child process"]
fn child_conductor_callback_panics() {
    let ipc = ipc(5023);
    let on_available = Handler::new(|_: AeronSubscription, _: AeronImage| panic!("boom"));
    let subscription = ipc
        .aeron
        .add_subscription(AERON_IPC_STREAM, 5023, Some(&on_available), Handlers::NONE, TIMEOUT)
        .unwrap();
    // the conductor thread aborts the process once it reports the image
    std::thread::sleep(TIMEOUT);
    drop(subscription);
}
