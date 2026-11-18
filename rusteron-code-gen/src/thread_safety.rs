//! Threading rules for generated handles and their retained owners.
//!
//! Unknown types stay local. Adding a shared handle requires checking every safe
//! method, its callbacks, its parent resources and its close path against Aeron C.

/// Handles whose safe API supports concurrent use.
pub const SHARED_HANDLES: &[&str] = &[
    "aeron_t",
    "aeron_publication_t",
    "aeron_counter_t",
    "aeron_counters_reader_t",
];

/// Native calls that save their progress only when they return, checked by reading the
/// Aeron C source. A nested call on the same object from a callback would be overwritten,
/// so the wrapper takes `&mut self` and the borrow checker rejects the nested call
pub const NON_REENTRANT_FNS: &[&str] = &[
    "aeron_subscription_poll",
    "aeron_subscription_controlled_poll",
    "aeron_subscription_block_poll",
    "aeron_image_poll",
    "aeron_image_controlled_poll",
    "aeron_image_bounded_poll",
    "aeron_image_bounded_controlled_poll",
    "aeron_image_block_poll",
    "aeron_archive_replay_merge_poll",
    "aeron_archive_persistent_subscription_poll",
    "aeron_archive_persistent_subscription_controlled_poll",
    // The reserved value supplier runs before the term offset advances. The hand-written
    // supplier offers in `aeron_custom.rs` follow the same rule
    "aeron_exclusive_publication_offer",
    "aeron_exclusive_publication_offerv",
];

/// Owners of a `NON_REENTRANT_FNS` call have no `Clone`, so a callback cannot reach the
/// same native object through a second handle
pub fn handle_is_single_owner(name: &str) -> bool {
    matches!(
        name,
        "aeron_subscription_t"
            | "aeron_image_t"
            | "aeron_exclusive_publication_t"
            | "aeron_archive_replay_merge_t"
            | "aeron_archive_persistent_subscription_t"
    )
}

/// Single-owner handles that may move to another thread but not be shared. Their owner
/// retains only shared dependencies, and C allows any one thread at a time to use them
pub fn handle_is_send_only(name: &str) -> bool {
    name == "aeron_subscription_t"
}

/// Owners that a shared handle can retain and drop.
/// Their public wrappers remain local because configuration and async polling
/// do not support concurrent calls.
const SHARED_OWNERS: &[&str] = &[
    "aeron_context_t",
    "aeron_subscription_t",
    "aeron_exclusive_publication_t",
    "aeron_client_registering_resource_t",
    "aeron_driver_context_t",
    "aeron_driver_t",
    "aeron_duty_cycle_tracker_t",
    "aeron_port_manager_t",
    "aeron_async_add_publication_t",
    "aeron_async_add_exclusive_publication_t",
    "aeron_async_add_subscription_t",
    "aeron_async_add_counter_t",
    "aeron_async_destination_t",
    "aeron_async_destination_by_id_t",
    "aeron_async_get_next_available_session_id_t",
    "aeron_on_available_counter_pair_t",
    "aeron_on_unavailable_counter_pair_t",
    "aeron_on_close_client_pair_t",
];

pub fn handle_is_shared(name: &str) -> bool {
    SHARED_HANDLES.contains(&name)
}

/// A terminal native poll lets the conductor reclaim this allocation.
pub fn handle_is_consumed_by_poll(name: &str) -> bool {
    name.starts_with("aeron_async_")
        || name == "aeron_client_registering_resource_t"
        || name == "aeron_archive_async_connect_t"
}

/// A context returned by an archive is a borrow, never an independent owner.
/// Borrowed storage has no Clone or safe raw-pointer constructors.
pub fn handle_is_archive_context(name: &str) -> bool {
    name == "aeron_archive_context_t"
}

/// Native allocations that remain alive until the final owning handle drops.
/// C must not replace or free their pointer while an owner remains alive.
pub fn handle_has_stable_pointer(name: &str) -> bool {
    handle_is_shared(name)
        || matches!(
            name,
            "aeron_subscription_t" | "aeron_exclusive_publication_t" | "aeron_image_t"
        )
}

pub fn owner_is_shared(name: &str) -> bool {
    handle_is_shared(name) || SHARED_OWNERS.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_types_and_single_consumer_handles_stay_local() {
        for name in [
            "aeron_subscription_t",
            "aeron_exclusive_publication_t",
            "aeron_image_t",
            "aeron_archive_t",
            "aeron_driver_t",
            "future_unreviewed_type_t",
        ] {
            assert!(!handle_is_shared(name), "{name}");
            if ![
                "aeron_subscription_t",
                "aeron_exclusive_publication_t",
                "aeron_driver_t",
            ]
            .contains(&name)
            {
                assert!(!owner_is_shared(name), "{name}");
            }
        }
    }

    #[test]
    fn shared_ownership_does_not_grant_concurrent_access() {
        for name in SHARED_OWNERS {
            assert!(owner_is_shared(name));
            assert!(!handle_is_shared(name));
        }
    }

    #[test]
    fn non_reentrant_owners_are_single_owner_and_not_shared() {
        for function in NON_REENTRANT_FNS {
            let owner = SINGLE_OWNERS_BY_PREFIX
                .iter()
                .find(|(prefix, _)| function.starts_with(prefix))
                .map(|(_, owner)| *owner)
                .unwrap_or_else(|| panic!("{function} has no single owner"));
            assert!(handle_is_single_owner(owner), "{owner}");
            assert!(!handle_is_shared(owner), "{owner}");
        }
        assert!(handle_is_single_owner("aeron_subscription_t"));
        assert!(!handle_is_send_only("aeron_image_t"));
        assert!(!handle_is_send_only("aeron_exclusive_publication_t"));
    }

    const SINGLE_OWNERS_BY_PREFIX: &[(&str, &str)] = &[
        (
            "aeron_archive_persistent_subscription_",
            "aeron_archive_persistent_subscription_t",
        ),
        ("aeron_archive_replay_merge_", "aeron_archive_replay_merge_t"),
        ("aeron_exclusive_publication_", "aeron_exclusive_publication_t"),
        ("aeron_subscription_", "aeron_subscription_t"),
        ("aeron_image_", "aeron_image_t"),
    ];

    #[test]
    fn stable_pointers_do_not_grant_concurrent_access() {
        for name in ["aeron_subscription_t", "aeron_exclusive_publication_t"] {
            assert!(handle_has_stable_pointer(name));
            assert!(!handle_is_shared(name));
        }
        // Successful polls free async native registrations before their wrappers drop.
        assert!(!handle_has_stable_pointer("aeron_async_add_publication_t"));
        assert!(!handle_has_stable_pointer("future_unreviewed_type_t"));
    }
}
