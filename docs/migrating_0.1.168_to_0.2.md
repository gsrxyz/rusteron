# Migrating from 0.1.168 to 0.2

The table maps every renamed or changed API from old to new. See also [What's new in 0.2](https://github.com/gsrxyz/rusteron/blob/main/docs/whats_new_0.2.md).

| 0.1.168 | 0.2 | Notes |
|---|---|---|
| `Handler::leak(h)` with manual `handler.release()` | `Handler::new(h)` | The handler drops with its last clone. Registering resources keep clones. |
| `ctx.set_error_handler(Some(&handler))` (borrowed) | `ctx.set_error_handler(Some(handler_or_closure))` | Retained setters take the value or a closure and return the `Handler`. |
| `aeron.close()`, which freed children immediately | deferred close | The C client closes when the last reference drops. `unsafe close_now()` closes it immediately. |
| `Handler` was `Sync` | `Send` only | The conductor thread invokes callbacks. Sharing `&Handler` across threads caused a data race. |
| `AeronPublication` / `AeronSubscription` / … were `Send` and `Sync` | Neither by default; `Send` only under `multi-threaded`, and `Sync` only there and only for types Aeron C documents as thread-safe | Handles use `Rc` (single-thread ownership), and a handle shares its client's reference count, so moving one to another thread raced it. Give each thread its own client, or enable the `multi-threaded` feature, which replaces `Rc` with `Arc`. The handles become `Send`, and `AeronPublication`/`AeronCounter`/`Aeron` also `Sync` (share `&Handle` across threads for `offer` / `try_claim` / `position` / `is_connected`). `AeronSubscription` and `AeronExclusivePublication` are documented by Aeron as not safe to share and stay `Send`-only (never `Sync`). |
| `publication.offer(buf, supplier)` returns a raw `i64` | `publication.offer_raw(buf, supplier)` | Same branch-free sentinel return, renamed to make "raw" explicit. |
| `publication.offer_result(buf, supplier)` returns `Result<_, AeronCError>` | `publication.offer_with_reserved_value(buf, supplier)` returns `Result<_, AeronOfferError>` | Typed offer errors with `is_retryable()`. |
| `publication.offer_result_simple(buf)` | `publication.offer(buf)` | The common no-supplier case is now the main name. |
| `try_claim_result(len, claim)` and `try_claim_owned` use `AeronCError` | `try_claim(len, claim)` and `try_claim_owned` use `AeronOfferError` | The RAII `AeronClaim` is unchanged. Errors have a publication-specific type. |
| `subscription.poll_once(f, limit)` | `subscription.poll_fn(f, limit)` | Renamed (`_once` read as "one fragment"); same on `AeronImage` / `AeronArchiveReplayMerge`. |
| `subscription.for_each_fragment(limit, f)` | `subscription.poll_fn(f, limit)` | Removed (alias with the arguments in the opposite order). |
| `sub.poll(assembler.process(&mut ctx, f), limit)` | `assembler.poll(&sub, &mut ctx, f, limit)` | `process()` leaked a raw context pointer past the borrow (UAF hazard); the new form scopes it. |
| `Handlers::no_available_image_handler()`, `no_unavailable_image_handler()`, … | `Handlers::NONE` | Use this constant for any callback parameter. Rust infers its type. The old helpers are removed. |
| `pub.add_destination(&aeron, dest, timeout)` (`&mut self`) | `pub.add_destination(dest, timeout)` (`&self`) | Owning `Aeron` comes from the handle's dependency graph. Same for subscriptions / exclusive publications. |
| `AeronCnc::new(dir)` | `AeronCnc::open(&CStr)` or `AeronCnc::read(&CStr, \|cnc\| { … })` | `new_on_heap`/`read_on_partial_stack` renamed; now accept `&CStr` (not `&str`/`&CString`). `read` borrows the CnC data for one call without allocating. `open` returns an owned handle for repeated polling. |
| `&"aeron:ipc".into_c_string()` (allocates at runtime) | `c"aeron:ipc"` | See [C strings without hidden allocations](https://github.com/gsrxyz/rusteron/blob/main/README.md#c-strings-without-hidden-allocations); `cformat!` for dynamic URIs. |
| `wrapper.get_inner_mut()` / `ManagedCResource::get_mut()` (safe) | `unsafe …()` | `&mut` from `&self`; the caller must now promise exclusive access. |
| `poll_fn(\|msg, header: AeronHeader\| …)`, `fn handle_aeron_fragment_handler(&mut self, msg: &[u8], header: AeronHeader)`, `list_recordings_fn(…, \|d: AeronArchiveRecordingDescriptor\| …)` | `header: &AeronHeader`, `d: &AeronArchiveRecordingDescriptor`, `image: &AeronImage`, … | Callbacks borrow every wrapper argument for the call, because C frees or reuses the struct when the callback returns. Untyped closures that only read the argument need no change. Closures that keep it no longer compile. Functions you pass the header on to take `&AeronHeader` too. |
| `header.clone()`, `AeronHeader::from(ptr)`, `descriptor.clone()` | copy the fields out (`d.recording_id`, `d.original_channel().to_owned()`), or `unsafe { d.clone_reference() }` / `unsafe { AeronHeader::from_ptr(ptr) }` inside the callback | `AeronHeader`, `AeronArchiveRecordingDescriptor`, `AeronArchiveRecordingSubscriptionDescriptor`, `AeronArchiveRecordingSignal` and `AeronPublicationErrorValues` only exist inside callbacks. They lose `Clone` and the safe `From` impls, because each one let safe code keep the struct after the callback. |
| `descriptor.clone_struct()` (safe) | `unsafe { descriptor.clone_struct() }` | `clone_struct` is `unsafe` on structs with pointer fields, because the copy's strings still point at the source's memory. Read only number fields from the copy, or null its pointers before storing it. It stays safe on structs without pointers. |
| `AeronUriStringBuilder::put_string(&CStr, &str)` / `put_strings(&str, &str)` | `AeronUriStringBuilder::put_str(&CStr, &str)` | Use `&CStr` keys, such as `c"media"` or a value from `CStr::from_bytes_until_nul`; `put_strings` removed (no external callers). |
| `archive.start_replay(...)`, `start_recording(...)` and other control operations return `Result<_, AeronCError>` | `Result<_, AeronArchiveError>` | Control operations on `AeronArchive` return the typed error with a code and message. Constructors, async-connect, and context setters still return `AeronCError`; `From<AeronArchiveError> for AeronCError` keeps `?` working across the boundary. |
| `async_add_exclusive_publication.poll(...).get_registration_id()` on the deprecated `exclusive_exclusive` alias | only `aeron_async_add_exclusive_publication_get_registration_id` is exposed | The deprecated `aeron_async_add_exclusive_exclusive_publication_get_registration_id` C alias is dropped (it collided with the canonical name); use the canonical `get_registration_id()`. |
| `DarwinPthread*`, `OpaquePthread*` wrapper structs in the generated API | removed | Bindgen pthread internals are no longer emitted as wrapper types; socket types the driver wrappers reference (`sockaddr_storage`, `iovec`, …) are retained. |
| `recording_descriptor_poller().reset(...)` and `recording_subscription_descriptor_poller().reset(...)` return `()` | `Result<(), AeronCError>` | C keeps a pointer to the consumer, so the poller must keep it alive. Pollers returned by `AeronArchive` are borrowed and cannot, so `reset` returns an error before C is changed. Use `list_recordings` / `list_recording_subscriptions` instead. |

Behavioural notes:

- Handler panics: see [Panics in callbacks](https://github.com/gsrxyz/rusteron/blob/main/README.md#panics-in-callbacks). Poll handlers unwind
  with redelivery, other callbacks are caught and raised, or logged when C fires them later.
- `AeronCError` construction never reads or copies `aeron_errmsg()`, so retry loops
  (e.g. polling that keeps returning `-1`) stay allocation-free. `Display` /
  `get_last_err_message()` read the live buffer; call `err.capture_errmsg()` at the
  error site to store the text in the error when you store it or log it later.

For recording, replay and persistent subscriptions (replay history, then join a
live stream), see [`rusteron-archive`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-archive/README.md#persistent-subscriptions).
