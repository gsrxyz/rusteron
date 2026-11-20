/// ```compile_fail,E0133
/// # use rusteron_client::*;
/// fn check(params: bindings::aeron_uri_stct__bindgen_ty_1) {
///     AeronUri::new([0; 4096], bindings::aeron_uri_type_enum::AERON_URI_UDP, params);
/// }
/// ```
fn union_constructor_requires_unsafe() {}

/// ```compile_fail,E0133
/// # use rusteron_client::*;
/// fn check(uri: &AeronUri) { uri.clone_struct(); }
/// ```
fn union_struct_copy_requires_unsafe() {}

/// ```compile_fail,E0133
/// # use rusteron_client::*;
/// fn check(value: &AeronUriParam) { value.clone_struct(); }
/// ```
fn pointer_struct_copy_requires_unsafe() {}

/// ```no_run
/// # use rusteron_client::*;
/// fn check(value: &AeronFrameHeader) { value.clone_struct(); }
/// ```
fn plain_struct_copy_remains_safe() {}

/// ```compile_fail,E0133
/// # use rusteron_client::*;
/// fn check(ptr: *mut std::ffi::c_char) { AeronUri::parse_params_fn(ptr, |_, _| 0); }
/// ```
fn raw_pointer_method_requires_unsafe() {}

/// A reader that needs a callback takes it without `Option`
///
/// ```compile_fail,E0308
/// # use rusteron_client::*;
/// fn check(reader: &AeronCountersReader) {
///     reader.foreach_counter::<fn(i64, i32, i32, &[u8], &str), LocalCallback>(None);
/// }
/// ```
fn required_reader_callback_cannot_be_none() {}

/// ```no_run
/// # use rusteron_client::*;
/// fn check(reader: &AeronCountersReader, visitor: &Handler<fn(i64, i32, i32, &[u8], &str)>) {
///     reader.foreach_counter(visitor);
/// }
/// ```
fn required_reader_callback_takes_a_handler() {}

/// ```compile_fail,E0624
/// # use rusteron_client::*;
/// fn check() { ManagedCResource::<bindings::aeron_frame_header_t>::new(|_| 0, None, true); }
/// ```
fn resource_constructor_is_internal() {}

/// ```no_run
/// # use rusteron_client::*;
/// fn client_id<S: ResourceStorage<bindings::aeron_t>>(client: &Aeron<S>) -> i64 {
///     client.client_id()
/// }
/// fn duplicate<S: ResourceStorage<bindings::aeron_t> + Clone>(client: &Aeron<S>) -> Aeron<S> {
///     client.clone()
/// }
/// fn check(client: &Aeron) {
///     let id = client_id(client);
///     let borrowed = client.as_ref();
///     assert_eq!(id, client_id(&borrowed));
///     let _copy = duplicate(client);
/// }
/// ```
fn generic_methods_accept_owning_and_borrowed_storage() {}

/// ```no_run
/// # use rusteron_client::*;
/// const _: () = {
///     let word = std::mem::size_of::<usize>();
///     assert!(std::mem::size_of::<Aeron>() == 2 * word);
///     assert!(std::mem::size_of::<AeronPublication>() == 2 * word);
///     assert!(std::mem::size_of::<Ref<'_, Aeron>>() == word);
///     assert!(std::mem::size_of::<Ref<'_, AeronPublication>>() == word);
/// };
/// ```
fn typed_handle_layouts() {}

/// ```compile_fail,E0505
/// # use rusteron_client::*;
/// fn check(owner: AeronCountersReader) {
///     let buffers = owner.get_buffers().unwrap(); drop(owner); std::hint::black_box(buffers.values());
/// }
/// ```
fn counter_buffers_cannot_outlive_reader() {}

/// ```no_run
/// # use rusteron_client::*;
/// # fn require_clone<T: Clone>(_: &T) {}
/// fn shared<T: Send + Sync>() {}
/// fn check(owner: &AeronPublication) {
///     require_clone(owner);
///     shared::<Aeron>(); shared::<AeronPublication>();
///     shared::<AeronRef>(); shared::<AeronPublicationRef>();
///     let view = owner.as_ref();
///     std::hint::black_box(view.get_inner());
/// }
/// ```
fn heap_clone_and_borrowed_shared_traits() {}

/// ```no_run
/// # use rusteron_client::*;
/// fn check(header: &AeronHeader) -> AeronHeaderValues<Inline<bindings::aeron_header_values_t>> { header.get_values().unwrap() }
/// ```
fn pointer_free_header_snapshot_can_escape_callback() {}

/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// # fn require_clone<T: Clone>(_: &T) {}
/// fn check(value: &AeronPublicationRef) { require_clone(value); }
/// ```
fn borrowed_handle_has_no_clone() {}

/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// # fn require_clone<T: Clone>(_: &T) {}
/// fn check(value: &Ref<'_, Aeron>) { require_clone(value); }
/// ```
fn borrowed_ref_has_no_clone() {}

/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// # fn require_clone<T: Clone>(_: &T) {}
/// fn check(value: &AeronFrameHeader<Inline<bindings::aeron_frame_header_t>>) { require_clone(value); }
/// ```
fn inline_has_no_clone() {}

/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// # fn require_clone<T: Clone>(_: &T) {}
/// fn check(value: &AeronPublicationConstants<ScopedInline<'_, bindings::aeron_publication_constants_t>>) { require_clone(value); }
/// ```
fn scoped_output_has_no_clone() {}

/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// # fn require_clone<T: Clone>(_: &T) {}
/// fn check(value: &AeronClaim<'_>) { require_clone(value); }
/// ```
fn claim_guard_has_no_clone() {}

/// ```compile_fail,E0133
/// # use rusteron_client::*;
/// fn check(pointer: *mut std::ffi::c_void) { Aeron::free(pointer); }
/// ```
fn native_free_requires_unsafe() {}

/// ```compile_fail,E0133
/// # use rusteron_client::*;
/// fn check(value: &AeronMappedFile) { value.aeron_unmap(); }
/// ```
fn native_unmap_requires_unsafe() {}

/// ```compile_fail,E0133
/// # use rusteron_client::*;
/// fn check(value: &AeronLossReporter, bytes: &mut [u8]) { value.init(bytes); }
/// ```
fn native_buffer_initializer_requires_unsafe() {}

/// ```compile_fail,E0624
/// # use rusteron_client::*;
/// fn check(value: &AeronAsyncAddPublication) { AeronPublication::new(value); }
/// ```
fn async_constructor_cannot_bypass_poll() {}

/// ```compile_fail,E0599
/// # use rusteron_client::*;
/// fn check(value: &AeronAsyncAddPublication) { value.as_ref(); }
/// ```
fn async_handle_has_no_cached_borrow() {}

/// ```compile_fail,E0505
/// # use rusteron_client::*;
/// fn check(owner: Aeron) {
///     let view = owner.as_ref(); drop(owner); std::hint::black_box(view.get_inner());
/// }
/// ```
fn borrow_cannot_outlive_owner() {}

/// ```compile_fail,E0515
/// # use rusteron_client::*;
/// fn check(owner: Aeron) -> Ref<'static, AeronContext> { owner.context() }
/// ```
fn child_cannot_escape_owner() {}

/// ```compile_fail,E0515
/// # use rusteron_client::*;
/// fn check(owner: Aeron) -> Ref<'static, AeronContext> { owner.as_ref().context() }
/// ```
fn nested_child_cannot_escape_owner() {}

/// ```compile_fail,E0505
/// # use rusteron_client::*;
/// fn check(owner: AeronPublication) {
///     let constants = owner.get_constants().unwrap(); drop(owner); std::hint::black_box(constants.channel());
/// }
/// ```
fn native_constants_cannot_outlive_publication() {}

/// ```compile_fail,E0505
/// # use rusteron_client::*;
/// fn check(owner: OwnedWithReference<std::sync::Arc<()>, Aeron>) {
///     let view = owner.as_ref(); drop(owner); std::hint::black_box(view.get_inner());
/// }
/// ```
fn view_cannot_outlive_retained_owner() {}

/// ```compile_fail,E0133
/// # use rusteron_client::*;
/// fn check(value: &AeronPublication) { value.get_inner_ref(); }
/// ```
fn native_struct_borrow_requires_unsafe() {}

/// ```compile_fail,E0599
/// # use rusteron_client::*;
/// fn check(owner: &AeronRef) {
///     owner.add_publication(c"aeron:ipc", 1, std::time::Duration::from_secs(1));
/// }
/// ```
fn borrowed_client_cannot_create_owned_children() {}

/// ```compile_fail,E0599
/// # use rusteron_client::*;
/// fn check(value: AeronRef) { value.close(); }
/// ```
fn borrowed_handle_has_no_owning_close() {}

/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// # fn require_send<T: Send>() {}
/// fn check() { require_send::<AeronSubscriptionRef>(); }
/// ```
fn borrowed_subscription_stays_local() {}

/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// # fn require_sync<T: Sync>() {}
/// fn check() { require_sync::<Inline<bindings::aeron_frame_header_t>>(); }
/// ```
fn inline_cannot_be_shared() {}

/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// # fn require_send<T: Send>() {}
/// fn check() { require_send::<AeronContext<Inline<bindings::aeron_context_t>>>(); }
/// ```
fn inline_native_context_stays_local() {}

/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// # fn require_send<T: Send>() {}
/// fn check() { require_send::<OwnedWithReference<std::rc::Rc<()>, Aeron>>(); }
/// ```
fn retained_local_owner_stays_local() {}

/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// fn check(value: std::pin::Pin<&mut PinnedInline<bindings::aeron_frame_header_t>>) { value.get_mut(); }
/// ```
fn pinned_inline_cannot_move() {}

/// ```compile_fail,E0133
/// # use rusteron_client::*;
/// fn check() { AeronHeader::new_zeroed_on_heap(); }
/// ```
fn native_zeroed_handle_requires_unsafe() {}

/// ```compile_fail,E0308
/// # use rusteron_client::*;
/// fn check(value: &AeronFrameHeader) { let _: &bindings::aeron_frame_header_t = value; }
/// ```
fn native_reference_is_not_an_implicit_deref() {}

/// ```compile_fail,E0133
/// # use rusteron_client::*;
/// fn check(ptr: *mut bindings::aeron_publication_t) -> AeronPublicationRef { AeronPublication::from_ptr(ptr) }
/// ```
fn raw_pointer_constructor_requires_unsafe() {}

/// ```compile_fail,E0599
/// # use rusteron_client::*;
/// fn check(value: bindings::aeron_buffer_claim_t) { AeronBufferClaim::from_resource(Inline::new(value)); }
/// ```
fn internal_factory_cannot_bypass_unsafe_constructor() {}

/// Polling a callback requires exclusive access to its state.
///
/// ```compile_fail,E0502
/// # use rusteron_client::*;
/// fn receive(subscription: &AeronSubscription) {
///     let mut received = 0;
///     let state = &received;
///     let mut callback = |_: &[u8], _: &AeronHeader| received += 1;
///     subscription.poll(&mut callback, 32).unwrap();
///     println!("{state}");
/// }
/// ```
fn direct_poll_exclusively_borrows_callback_state() {}

/// A shared Handler must use the explicit checked API.
///
/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// fn receive(subscription: &mut AeronSubscription) {
///     let mut handler = Handler::new(|_: &[u8], _: &AeronHeader| {});
///     subscription.poll(&mut handler, 32).unwrap();
/// }
/// ```
fn direct_poll_does_not_hide_shared_callback_storage() {}

/// A shared callback cannot add borrow checks inside an owned assembler.
///
/// ```compile_fail,E0277
/// # use rusteron_client::*;
/// let handler = Handler::new(|_: &[u8], _: &AeronHeader| {});
/// let assembler = FragmentAssembler::new(handler).unwrap();
/// ```
fn owned_assembler_does_not_hide_shared_callback_storage() {}

/// ```no_run
/// # use rusteron_client::*;
/// fn send(publication: &AeronExclusivePublication) {
///     let supplier = Handler::new(|_: &mut [u8]| 7);
///     publication.offer_raw(b"outer", Some(&supplier));
/// }
/// ```
fn exclusive_supplier_offer_accepts_a_local_supplier() {}
