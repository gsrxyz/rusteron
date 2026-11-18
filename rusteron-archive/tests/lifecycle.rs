//! Native lifetime regressions. These also run under Valgrind without a Java archive.
use rusteron_archive::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::rc::Rc;
use std::time::{Duration, Instant};

fn driver() -> EmbeddedDriver {
    EmbeddedDriver::launch_with(|context| {
        let directory = std::env::temp_dir().join(format!(
            "rusteron-lifecycle-{}-{}",
            std::process::id(),
            Aeron::nano_clock()
        ));
        context.set_dir(&directory.to_string_lossy().into_c_string())?;
        Ok(())
    })
    .unwrap()
}

#[test]
fn deleting_proxy_does_not_close_its_separately_owned_publication() {
    let driver = driver();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let publication = client
        .add_exclusive_publication(c"aeron:ipc", 87201, Duration::from_secs(10))
        .unwrap();
    let context = AeronArchiveContext::new().unwrap();
    context.set_aeron(&client).unwrap();
    let proxy = AeronArchiveProxy::new(&context, &publication, 1).unwrap();
    proxy.delete().unwrap();
    // Allow the conductor to reclaim a publication wrongly closed by the proxy.
    std::thread::sleep(Duration::from_millis(100));
    assert!(!publication.is_closed());
    assert!(publication.position() >= 0);
    assert_eq!(publication.get_constants().unwrap().stream_id(), 87201);
    drop(publication);
}

#[test]
fn completed_registration_invalidates_aliases_and_keeps_client_alive() {
    let driver = driver();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let pending = client.async_add_publication(c"aeron:ipc", 87202).unwrap();
    let alias = pending.clone();
    let _registration_id = pending.get_registration_id();
    let publication = pending.poll_blocking(Duration::from_secs(10)).unwrap();
    assert!(alias.poll().unwrap().is_none());
    alias.cancel().unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| alias.get_registration_id())).is_err());
    drop(client);
    drop(alias);
    drop(pending);
    std::thread::sleep(Duration::from_millis(100));
    assert!(!publication.is_closed());
    assert_eq!(publication.get_constants().unwrap().stream_id(), 87202);
}

#[test]
fn cancelled_registration_cannot_be_polled_or_read_again() {
    let driver = driver();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let pending = client.async_add_publication(c"aeron:ipc", 87203).unwrap();
    let alias = pending.clone();
    pending.cancel().unwrap();
    assert!(alias.poll().unwrap().is_none());
    alias.cancel().unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| alias.get_registration_id())).is_err());
}

#[test]
fn terminal_destination_poll_invalidates_all_aliases() {
    let driver = driver();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let publication = client
        .add_publication(c"aeron:udp?control-mode=manual", 87204, Duration::from_secs(10))
        .unwrap();
    let destination = AeronAsyncDestination::aeron_publication_async_add_destination(
        &client,
        &publication,
        c"aeron:udp?endpoint=127.0.0.1:29804",
    )
    .unwrap();
    let alias = destination.clone();
    let deadline = Instant::now() + Duration::from_secs(10);
    while destination.aeron_publication_async_destination_poll().unwrap() == 0 {
        assert!(Instant::now() < deadline, "destination registration timed out");
        std::thread::yield_now();
    }
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| alias.get_registration_id())).is_err());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || alias.aeron_publication_async_destination_poll()
        ))
        .is_err()
    );
}

#[test]
fn image_assemblers_release_their_delegates_once() {
    let capture = Rc::new(());
    let weak = Rc::downgrade(&capture);
    let handler = Handler::new(move |_: &[u8], _: &AeronHeader| {
        std::hint::black_box(&capture);
    });
    let assembler = AeronImageFragmentAssembler::new(Some(&handler)).unwrap();
    drop(handler);
    assert!(weak.upgrade().is_some());
    assembler.delete().unwrap();
    assert!(weak.upgrade().is_none());

    let controlled_handler = Handler::new(|_: &[u8], _: &AeronHeader| {
        bindings::aeron_controlled_fragment_handler_action_t::AERON_ACTION_CONTINUE
    });
    let controlled = AeronImageControlledFragmentAssembler::new(Some(&controlled_handler)).unwrap();
    controlled.delete().unwrap();
}

fn persistent_context(client: &Aeron, archive: &AeronArchiveContext) -> AeronArchivePersistentSubscriptionContext {
    let context = AeronArchivePersistentSubscriptionContext::new().unwrap();
    context.set_aeron(client).unwrap();
    context.set_archive_context(archive).unwrap();
    context.set_live_channel(c"aeron:ipc").unwrap();
    context.set_live_stream_id(87205).unwrap();
    context.set_replay_channel(c"aeron:ipc").unwrap();
    context.set_replay_stream_id(87206).unwrap();
    context.set_recording_id(0).unwrap();
    context
}

#[test]
fn persistent_subscription_retains_context_client_and_listener() {
    struct Listener(Rc<()>);
    impl PersistentSubscriptionListener for Listener {
        fn on_live_left(&self) {
            std::hint::black_box(&self.0);
        }
    }
    let driver = driver();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let archive = AeronArchiveContext::new().unwrap();
    archive.set_aeron(&client).unwrap();
    let context = persistent_context(&client, &archive);
    let alias = context.clone();
    let capture = Rc::new(());
    let weak = Rc::downgrade(&capture);
    let subscription = AeronArchivePersistentSubscription::create(context, Some(Box::new(Listener(capture)))).unwrap();
    assert!(alias.set_live_stream_id(999).is_err());
    assert!(archive.set_message_timeout_ns(100).is_err());
    assert!(archive.set_aeron(&client).is_err());
    assert!(AeronArchivePersistentSubscription::create(alias.clone(), None).is_err());
    drop(client);
    drop(archive);
    subscription.poll_fn(|_, _| {}, 10).unwrap();
    drop(subscription);
    // Native subscription cleanup must leave the context and its counters live.
    assert!(alias.get_state_counter().is_some());
    assert!(weak.upgrade().is_some());
    drop(alias);
    assert!(weak.upgrade().is_none());
}

#[test]
fn closing_persistent_subscription_allows_archive_reconfiguration() {
    let driver = driver();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let archive = AeronArchiveContext::new().unwrap();
    archive.set_aeron(&client).unwrap();
    let context = persistent_context(&client, &archive);
    let subscription = AeronArchivePersistentSubscription::create(context, None).unwrap();
    assert!(archive.set_message_timeout_ns(123).is_err());
    drop(subscription);
    archive.set_message_timeout_ns(123).unwrap();
}

#[test]
fn archive_connect_rejects_missing_client_without_starting_native_connect() {
    let context = AeronArchiveContext::new().unwrap();
    assert!(AeronArchiveAsyncConnect::new(&context).is_err());
    // The rejected call must leave the context configurable.
    context.set_control_request_channel(c"aeron:ipc").unwrap();
}

#[test]
fn persistent_subscription_keeps_parents_after_all_external_owners_drop() {
    let driver = driver();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let archive = AeronArchiveContext::new().unwrap();
    archive.set_aeron(&client).unwrap();
    let capture = Rc::new(());
    let weak = Rc::downgrade(&capture);
    archive
        .set_error_handler(Some(move |_: i32, _: &str| {
            std::hint::black_box(&capture);
        }))
        .unwrap();
    let context = persistent_context(&client, &archive);
    let subscription = AeronArchivePersistentSubscription::create(context, None).unwrap();
    drop(client);
    drop(archive);
    assert!(weak.upgrade().is_some());
    subscription.poll_fn(|_, _| {}, 10).unwrap();
    drop(subscription);
    assert!(weak.upgrade().is_none());
}

#[test]
fn failed_persistent_creation_preserves_listener_and_releases_configuration() {
    struct Listener(Rc<()>);
    impl PersistentSubscriptionListener for Listener {
        fn on_live_left(&self) {
            std::hint::black_box(&self.0);
        }
    }
    let driver = driver();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let archive = AeronArchiveContext::new().unwrap();
    archive.set_aeron(&client).unwrap();
    let context = AeronArchivePersistentSubscriptionContext::new().unwrap();
    context.set_aeron(&client).unwrap();
    context.set_archive_context(&archive).unwrap();
    // Omit the required channel configuration to test initialization failure.
    let alias = context.clone();
    let capture = Rc::new(());
    let weak = Rc::downgrade(&capture);
    assert!(AeronArchivePersistentSubscription::create(context, Some(Box::new(Listener(capture)))).is_err());
    assert!(weak.upgrade().is_some());
    alias.set_live_stream_id(87207).unwrap();
    archive.set_message_timeout_ns(123).unwrap();
    drop(alias);
    assert!(weak.upgrade().is_none());
}

#[test]
fn assemblers_reject_missing_delegate() {
    assert!(AeronFragmentAssembler::new(Handlers::NONE).is_err());
    assert!(AeronControlledFragmentAssembler::new(Handlers::NONE).is_err());
    assert!(AeronImageFragmentAssembler::new(Handlers::NONE).is_err());
    assert!(AeronImageControlledFragmentAssembler::new(Handlers::NONE).is_err());
}
