use rusteron_archive::*;
use std::rc::Rc;
use std::sync::Arc;

#[test]
fn a_running_handler_cannot_unregister_itself() {
    let context = Rc::new(AeronArchiveContext::new().unwrap());
    let weak_context = Rc::downgrade(&context);
    let rejections = Rc::new(std::cell::Cell::new(0));
    let observed = rejections.clone();
    let capture = Arc::new(());
    let lifetime = Arc::downgrade(&capture);
    context
        .set_error_handler(Some(move |_, _: &str| {
            let context = weak_context.upgrade().unwrap();
            if context.set_error_handler::<fn(i32, &str)>(None).is_err() {
                observed.set(observed.get() + 1);
            }
            std::hint::black_box(&capture);
        }))
        .unwrap();
    context.invoke_error_handler(1, 0, c"first invocation");
    context.invoke_error_handler(2, 0, c"second invocation");
    assert_eq!(rejections.get(), 2);
    assert!(lifetime.upgrade().is_some());
    context.set_error_handler::<fn(i32, &str)>(None).unwrap();
    assert!(lifetime.upgrade().is_none());
}

#[test]
fn replacing_or_clearing_an_unused_handler_releases_its_captures() {
    let context = AeronArchiveContext::new().unwrap();
    let capture = Arc::new(());
    let probe = Arc::downgrade(&capture);
    context
        .set_error_handler(Some(move |_, _: &str| {
            std::hint::black_box(&capture);
        }))
        .unwrap();
    assert!(probe.upgrade().is_some());

    // A different closure type must replace the same native registration.
    context.set_error_handler(Some(|_, _: &str| {})).unwrap();
    assert!(probe.upgrade().is_none());

    let capture = Arc::new(());
    let probe = Arc::downgrade(&capture);
    context
        .set_error_handler(Some(move |_, _: &str| {
            std::hint::black_box(&capture);
        }))
        .unwrap();
    context.set_error_handler::<fn(i32, &str)>(None).unwrap();
    assert!(probe.upgrade().is_none());
}

#[test]
fn callers_can_keep_a_replaced_handler_alive() {
    let context = AeronArchiveContext::new().unwrap();
    let capture = Arc::new(());
    let probe = Arc::downgrade(&capture);
    let handler = context
        .set_error_handler(Some(move |_, _: &str| {
            std::hint::black_box(&capture);
        }))
        .unwrap();
    context.set_error_handler::<fn(i32, &str)>(None).unwrap();
    assert!(probe.upgrade().is_some());
    drop(handler);
    assert!(probe.upgrade().is_none());
}

#[test]
fn a_borrowed_context_rejects_handler_registration() {
    let context = AeronArchiveContext::new().unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let capture = calls.clone();
    context
        .set_error_handler(Some(move |_, _: &str| {
            capture.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }))
        .unwrap();
    // SAFETY: context stays alive and all access is on this thread.
    let borrowed = unsafe { AeronArchiveContext::from_ptr(context.get_inner()) };
    assert!(
        borrowed
            .set_error_handler(Some(|_, _: &str| panic!("unregistered")))
            .is_err()
    );
    // Borrowed contexts cannot be passed to the owning connect constructor.
    context.invoke_error_handler(1, 0, c"original callback");
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
}
