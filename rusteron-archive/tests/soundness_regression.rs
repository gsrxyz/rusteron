use rusteron_archive::*;
use std::time::Duration;

#[test]
fn retained_archive_context_survives_close_and_final_clone_drop() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let root = std::env::temp_dir().join(format!(
        "rusteron-retained-context-{}-{}",
        std::process::id(),
        Aeron::nano_clock()
    ));
    let request = testing::find_unused_udp_port(19000).ok_or("request port")?;
    let response = testing::find_unused_udp_port(request + 1).ok_or("response port")?;
    let events = testing::find_unused_udp_port(response + 1).ok_or("events port")?;
    let driver = testing::EmbeddedArchiveMediaDriverProcess::start(
        root.join("driver").to_str().unwrap(),
        root.join("archive").to_str().unwrap(),
        &format!("aeron:udp?endpoint=localhost:{request}"),
        &format!("aeron:udp?endpoint=localhost:{response}"),
        &format!("aeron:udp?endpoint=localhost:{events}"),
    )?;
    let (ready, client) = driver.archive_connect()?;
    let context = driver.archive_context(&client)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let weak = Arc::downgrade(&calls);
    let capture = calls.clone();
    context.set_error_handler(Some(move |_, _: &str| {
        capture.fetch_add(1, Ordering::Relaxed);
    }))?;
    let connecting = AeronArchiveAsyncConnect::new_with_aeron(&context, &client)?;
    context.set_error_handler::<fn(i32, &str)>(None)?;
    let archive = connecting.poll_blocking(Duration::from_secs(60))?;
    let retained_client = archive.aeron_owned();
    let retained_client_clone = retained_client.clone();
    assert_eq!(
        std::mem::size_of_val(&retained_client),
        2 * std::mem::size_of::<usize>()
    );
    let retained = archive.retain_archive_context()?;
    let clone = retained.clone();
    assert_eq!(retained.as_ref().get_inner(), clone.as_ref().get_inner());
    assert_eq!(retained.as_ref().get_inner(), archive.get_archive_context().get_inner());
    assert_eq!(std::mem::size_of_val(&retained), 2 * std::mem::size_of::<usize>());
    archive.close()?;
    assert_eq!(retained_client.as_ref().client_id(), client.client_id());
    drop(retained_client);
    assert_eq!(retained_client_clone.as_ref().get_inner(), client.get_inner());
    drop(retained_client_clone);
    drop(context);
    drop(calls);
    assert!(weak.upgrade().is_some());
    let nested_client = retained.as_ref().get_aeron();
    assert_eq!(nested_client.get_inner(), client.get_inner());
    retained.as_ref().invoke_error_handler(1, 0, c"retained context");
    assert_eq!(weak.upgrade().unwrap().load(Ordering::Relaxed), 1);
    drop(retained);
    assert!(weak.upgrade().is_some());
    clone.as_ref().invoke_error_handler(2, 0, c"last retained context");
    assert_eq!(weak.upgrade().unwrap().load(Ordering::Relaxed), 2);
    drop(clone);
    assert!(weak.upgrade().is_none());
    drop(connecting);
    drop(ready);
    drop(client);
    drop(driver);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn owned_channel_getter_keeps_its_snapshot_after_reconfiguration() {
    let context = AeronArchiveContext::new().unwrap();
    context.set_control_request_channel(c"aeron:ipc").unwrap();
    let first = context.get_control_request_channel_owned();
    context
        .set_control_request_channel(c"aeron:udp?endpoint=localhost:12345")
        .unwrap();
    assert_eq!(first, "aeron:ipc");
    assert_eq!(
        context.get_control_request_channel_owned(),
        "aeron:udp?endpoint=localhost:12345"
    );
}

#[test]
fn rust_owned_client_cannot_be_transferred_to_archive_context() {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch().unwrap();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let context = AeronArchiveContext::new().unwrap();
    assert!(context.set_owns_aeron_client(true).is_err());
    context.set_aeron(&client).unwrap();
    assert!(context.set_owns_aeron_client(true).is_err());
    assert!(!context.get_owns_aeron_client());
    drop(context);
    assert!(!client.is_closed());
}

#[test]
fn deleting_one_proxy_clone_keeps_the_other_usable() {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch().unwrap();
    let client = Aeron::connect_dir(driver.dir()).unwrap();
    let publication = client
        .add_exclusive_publication(c"aeron:ipc", 87123, Duration::from_secs(5))
        .unwrap();
    let context = AeronArchiveContext::new().unwrap();
    context.set_aeron(&client).unwrap();
    let proxy = AeronArchiveProxy::new(&context, &publication, 1).unwrap();
    let remaining = proxy.clone();
    proxy.delete().unwrap();
    remaining.set_control_session_id(42).unwrap();
    drop(remaining);
}

#[test]
fn unset_persistent_subscription_counters_are_none() {
    assert_eq!(
        std::mem::size_of::<Option<Ref<'_, AeronCounter>>>(),
        std::mem::size_of::<usize>()
    );
    let context = AeronArchivePersistentSubscriptionContext::new().unwrap();
    assert!(context.get_state_counter().is_none());
    assert!(context.get_join_difference_counter().is_none());
    assert!(context.get_live_left_counter().is_none());
    assert!(context.get_live_joined_counter().is_none());
}
