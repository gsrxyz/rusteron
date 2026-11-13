use crate::{AeronArchiveContext, AeronCError, AeronErrorHandlerSlot};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::time::Duration;

#[test]
fn error_handler_slot_frees_captures_without_more_registrations() -> Result<(), AeronCError> {
    let ctx = AeronArchiveContext::new()?;
    let handler = AeronErrorHandlerSlot::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let dependencies_before = ctx.inner.dependency_len();
    ctx.set_error_handler(Some(handler.clone()))?;
    let mut previous_lifetime: Option<Weak<Vec<i32>>> = None;

    for value in 0..100 {
        let payload = Arc::new(vec![value; 1024]);
        let lifetime = Arc::downgrade(&payload);
        let observed = calls.clone();
        handler.set(move |_, message| {
            assert!(message.contains("test"));
            observed.lock().unwrap().push(payload[0]);
        });
        if let Some(lifetime) = previous_lifetime.replace(lifetime) {
            assert!(lifetime.upgrade().is_none());
        }

        // Call through Aeron's C error handler.
        ctx.invoke_error_handler(1, 0, c"test");
    }

    handler.clear();
    assert!(previous_lifetime.unwrap().upgrade().is_none());
    // C can still call the empty slot.
    ctx.invoke_error_handler(2, 0, c"ignored");

    assert_eq!(*calls.lock().unwrap(), (0..100).collect::<Vec<_>>());
    assert_eq!(ctx.inner.dependency_len(), dependencies_before + 1);
    Ok(())
}

#[test]
fn error_handler_slot_keeps_an_active_call_alive() -> Result<(), Box<dyn std::error::Error>> {
    let handler = AeronErrorHandlerSlot::new();
    let payload = Arc::new(vec![42; 1024]);
    let lifetime = Arc::downgrade(&payload);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let finish_rx = Mutex::new(finish_rx);
    handler.set(move |_, _| {
        entered_tx.send(()).unwrap();
        finish_rx.lock().unwrap().recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(payload[0], 42);
    });

    let current = handler.clone();
    let worker = std::thread::spawn(move || -> Result<(), AeronCError> {
        let ctx = AeronArchiveContext::new()?;
        ctx.set_error_handler(Some(current))?;
        ctx.invoke_error_handler(1, 0, c"first");
        ctx.invoke_error_handler(2, 0, c"second");
        Ok(())
    });

    entered_rx.recv_timeout(Duration::from_secs(10))?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    handler.set(move |_, message| {
        observed.lock().unwrap().push(message.to_owned());
    });
    assert!(lifetime.upgrade().is_some(), "the active call still owns its handler");

    finish_tx.send(())?;
    worker.join().unwrap()?;
    assert!(lifetime.upgrade().is_none(), "the completed call releases its handler");
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].contains("second"));
    Ok(())
}

#[test]
fn error_handler_slot_supports_mutable_callbacks_and_recovers_after_panic() -> Result<(), AeronCError> {
    let ctx = AeronArchiveContext::new()?;
    let handler = AeronErrorHandlerSlot::new();
    ctx.set_error_handler(Some(handler.clone()))?;
    ctx.invoke_error_handler(1, 0, c"empty");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let mut count = 0;
    handler.set(move |_, _| {
        count += 1;
        if count == 1 {
            panic!("test callback panic");
        }
        observed.lock().unwrap().push(count);
    });
    ctx.invoke_error_handler(2, 0, c"panic");
    ctx.invoke_error_handler(3, 0, c"next");
    assert_eq!(*calls.lock().unwrap(), vec![2]);
    Ok(())
}

#[test]
fn returning_callback_slot_uses_the_chosen_empty_result() {
    use crate::bindings::aeron_controlled_fragment_handler_action_t::{AERON_ACTION_ABORT, AERON_ACTION_COMMIT};
    use crate::{AeronControlledFragmentHandlerCallback, AeronControlledFragmentHandlerSlot};
    let mut handler = AeronControlledFragmentHandlerSlot::new(|| AERON_ACTION_ABORT);
    // These callbacks do not read the header.
    let header = crate::AeronHeader::new_zeroed_on_heap();
    assert_eq!(
        handler.handle_aeron_controlled_fragment_handler(&[], &header),
        AERON_ACTION_ABORT
    );
    handler.set(|_, _| AERON_ACTION_COMMIT);
    assert_eq!(
        handler.handle_aeron_controlled_fragment_handler(&[], &header),
        AERON_ACTION_COMMIT
    );
    handler.clear();
    assert_eq!(
        handler.handle_aeron_controlled_fragment_handler(&[], &header),
        AERON_ACTION_ABORT
    );
}

#[test]
fn error_handler_slot_can_replace_and_clear_itself() -> Result<(), AeronCError> {
    let ctx = AeronArchiveContext::new()?;
    let handler = AeronErrorHandlerSlot::new();
    ctx.set_error_handler(Some(handler.clone()))?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let current = handler.clone();
    handler.set(move |_, _| {
        observed.lock().unwrap().push(1);
        let observed = observed.clone();
        let current = current.clone();
        current.clone().set(move |_, _| {
            observed.lock().unwrap().push(2);
            current.clear();
        });
    });
    ctx.invoke_error_handler(1, 0, c"first");
    ctx.invoke_error_handler(2, 0, c"second");
    ctx.invoke_error_handler(3, 0, c"cleared");
    assert_eq!(*calls.lock().unwrap(), vec![1, 2]);
    Ok(())
}
