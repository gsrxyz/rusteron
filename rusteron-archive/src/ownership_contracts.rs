//! Compile-time ownership and FFI safety contracts.
//! Run with `cargo test --doc ownership_contracts --features static`.

/// ```compile_fail,E0277
/// # use rusteron_archive::*;
/// # fn require_clone<T: Clone>(_: &T) {}
/// fn check(value: &AeronArchiveRecordingDescriptorPoller) { require_clone(value); }
/// ```
fn descriptor_poller_has_no_clone() {}

/// ```compile_fail,E0277
/// # use rusteron_archive::*;
/// # fn require_clone<T: Clone>(_: &T) {}
/// fn check(value: &AeronArchiveRecordingSubscriptionDescriptorPoller) { require_clone(value); }
/// ```
fn subscription_descriptor_poller_has_no_clone() {}

/// ```compile_fail,E0596
/// # use rusteron_archive::*;
/// fn check(archive: &AeronArchive) { archive.recording_descriptor_poller().poll(); }
/// ```
fn borrowed_descriptor_poller_cannot_be_polled() {}

/// ```compile_fail,E0596
/// # use rusteron_archive::*;
/// fn check(archive: &AeronArchive) { archive.recording_subscription_descriptor_poller().poll(); }
/// ```
fn borrowed_subscription_descriptor_poller_cannot_be_polled() {}

/// ```compile_fail,E0596
/// # use rusteron_archive::*;
/// fn check(value: &AeronArchiveRecordingDescriptorPoller) { value.poll(); }
/// ```
fn descriptor_poll_requires_exclusive_access() {}

/// ```compile_fail,E0599
/// # use rusteron_archive::*;
/// fn check(archive: &AeronArchive) { archive.get_and_own_archive_context(); }
/// ```
fn archive_context_transfer_is_not_a_borrowed_getter() {}

/// ```compile_fail,E0599
/// # use rusteron_archive::*;
/// fn check(archive: &AeronArchive) { archive.get_and_own_control_response_subscription(); }
/// ```
fn archive_subscription_transfer_is_not_a_borrowed_getter() {}

/// ```compile_fail,E0599
/// # use rusteron_archive::*;
/// fn check(value: &AeronArchivePersistentSubscription) { unsafe { value.get_and_own_persistent_subscription_context(); } }
/// ```
fn persistent_subscription_context_stays_owned_by_rust() {}

/// A replay merge takes the archive, so the archive cannot be used while the merge runs
///
/// ```compile_fail,E0382
/// # use rusteron_archive::*;
/// fn check(subscription: &AeronSubscription, archive: AeronArchive) -> Result<(), AeronCError> {
///     let _merge = AeronArchiveReplayMerge::new(subscription, archive, c"", c"", c"", 0, 0, 0, 0)?;
///     archive.get_stop_position(0)?;
///     Ok(())
/// }
/// ```
fn replay_merge_takes_the_archive() {}
