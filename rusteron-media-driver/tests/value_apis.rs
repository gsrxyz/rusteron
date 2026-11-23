//! Consumer tests for native records and driver callbacks with no pointer access.
use rusteron_media_driver::*;

#[test]
fn scalar_alias_records_support_safe_value_operations() {
    let address = InAddr::new(0x1234).unwrap();
    let copy = address.clone_struct();
    drop(address);
    assert_eq!(copy.s_addr(), 0x1234);
    assert_eq!(InAddr::new_zeroed_on_stack().s_addr(), 0);
    let native = bindings::in_addr { s_addr: 0x5678 };
    assert_eq!(InAddr::from_native(native).s_addr(), 0x5678);
}

#[test]
fn fixed_counter_labels_can_be_borrowed_safely() {
    let native = bindings::aeron_system_counter_t {
        label: c"counter".as_ptr(),
        id: 7,
    };
    // SAFETY: the label has static storage and this record has no native writer.
    let counter = unsafe { AeronSystemCounter::from_native(native) };
    assert_eq!(counter.label(), "counter");
    assert_eq!(counter.id(), 7);
    assert_eq!(AeronSystemCounter::new_zeroed_on_stack().label(), "");
}

#[test]
fn default_termination_validators_ignore_their_pointer_arguments() {
    type Validator = fn(*mut std::ffi::c_void, &mut [u8]) -> bool;
    let allow: Validator = AeronDriver::termination_validator_default_allow;
    let deny: Validator = AeronDriver::termination_validator_default_deny;
    for state in [std::ptr::null_mut(), std::ptr::without_provenance_mut(1)] {
        assert!(allow(state, &mut []));
        assert!(!deny(state, &mut []));
        assert!(allow(state, &mut [1, 2, 3]));
        assert!(!deny(state, &mut [1, 2, 3]));
    }
}

#[test]
fn scalar_pointer_getters_return_the_address_without_borrowing_it() {
    let mut value = 7_i64;
    let address = &raw mut value;
    let native = bindings::aeron_position_t {
        counter_id: 0,
        value_addr: address,
    };
    // SAFETY: the pointer stays valid until after the position's final use.
    let position = unsafe { AeronPosition::from_native(native) };
    let getter: fn(&AeronPosition<Inline<bindings::aeron_position_t>>) -> *mut i64 = AeronPosition::value_addr;
    let first = getter(&position);
    let second = getter(&position);
    assert_eq!(first, address);
    assert_eq!(second, address);
    // SAFETY: this test owns the value and no reference or native access overlaps.
    unsafe { first.write(9) };
    assert_eq!(value, 9);
}
