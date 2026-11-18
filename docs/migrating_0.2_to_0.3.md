# Migrating from 0.2 to 0.3

0.3 changes resource storage, ownership and thread safety. Send and poll methods
still call the native functions directly. Most changes affect setup, callbacks,
explicit type annotations and low-level FFI access.

## Upgrade checklist

| Check existing code for | Change for 0.3 |
| --- | --- |
| `multi-threaded` dependency feature | Remove it. Thread traits are set per native type |
| `Handler::new(...)` passed to a retained conductor callback | Pass an owned callback, or use `SharedHandler::new_shared(...)` |
| Direct access through a handler's `Deref` | Use checked state access. See [accessing callback state](#accessing-callback-state) |
| Cloning a getter's `Ref` | Borrow again with `as_ref()`, or use an explicit retained-child getter |
| Function arguments annotated as the default owning wrapper | Add a storage parameter if the function should also accept borrowed views |
| Cloning an Archive context or native assembler | Keep one owner. Borrow it, or share a newly created context through `Rc` during setup |
| Cloning a recording descriptor poller | Keep one mutable owner. `poll()` takes `&mut self` and needs a consumer installed by `reset()` |
| `close()` or `delete()` followed by reuse | These consume the owning handle. Use another owning clone if supported |
| `From<*mut T>`, `From<native_struct>` or implicit native struct `Deref` | Use the documented unsafe constructors or explicit field getters |
| Mutable configuration strings borrowed as `&str` | Borrowing needs `unsafe`. Use `_owned()` for a copy |
| Raw counter addresses used as references | Use `addr_atomic()` |
| `timestamp_offset_reserved("...")` | Use `media_rcv_timestamp_offset("reserved")`, `channel_rcv_timestamp_offset("reserved")` or `channel_snd_timestamp_offset("reserved")`. Aeron defines `reserved` as a value, not a key |
| Exhaustive matches on public Rust enums | Add a fallback arm. The enums are `#[non_exhaustive]` |
| Raw manual conductor calls | Coordinate all native callers and use the documented unsafe entry point |

Update the crate versions together:

```toml
[dependencies]
rusteron-client = { version = "0.3", features = ["static"] }
```

Use `rusteron-archive` instead if you need both transport and Archive APIs.

## Borrowed strings

```rust,no_run
# use rusteron_archive::*;
# fn example() -> Result<(), AeronCError> {
let context = AeronArchiveContext::new()?;
context.set_control_response_channel(c"aeron:ipc")?;

// No allocation. This local context stays unchanged while channel is borrowed.
// SAFETY: the text is used at once, before any setter can change it
let channel = unsafe { context.get_control_response_channel() };
assert_eq!(channel, "aeron:ipc");

// One explicit copy. The snapshot stays valid after the context changes.
let saved = context.get_control_response_channel_owned();
context.set_control_response_channel(c"aeron:udp?endpoint=localhost:8010")?;
assert_eq!(saved, "aeron:ipc");
# Ok(()) }
```

The safe `&str` getter in 0.2 let a setter free or overwrite text that was still
borrowed. In 0.3 the borrowing getter is `unsafe` and `_owned` returns a copy.

Publishing, claiming, committing, direct polling and reading recording descriptor
fields do not allocate on the Rust heap. Owned fragment assemblers allocate once
when created. Retained callbacks and parent dependencies allocate when registered.
Replication builders retain their channel strings.

`AeronCError::with_message` borrows string literals and takes ownership of a
`String`. Copy a non-static borrowed message with `.to_owned()`. `capture_errmsg()`
copies the native message once.

## Owning and borrowed handles

Generated handles have a sealed storage parameter. `Aeron` and `AeronPublication`
still mean their owning forms, so most code does not name it.

| Form | Purpose | Clone |
| --- | --- | --- |
| `Aeron`, `AeronPublication`, other supported owning handles | Own native cleanup and retain their dependencies | Where supported |
| `Ref<'a, T>` | Borrow a child or an owning handle | No |
| `AeronRef`, `AeronPublicationRef`, other `*Ref` aliases | The wrapper type inside a borrowed view | No |
| `OwnedWithReference<Owner, T>` | Retain a parent while accessing a stable child | When `Owner: Clone` |
| `Handle<Inline<Native>>` | Store an initialized native value directly | No |
| `Handle<ScopedInline<'a, Native>>` | Hold query results whose pointers borrow a parent | No |

The `CResource` enum and its variants are removed. Storage is chosen at compile
time, with no runtime tag check. Shared heap owners use `Arc`, so cloning or
dropping an owner updates a reference count.

### Accept any storage type

Use a generic parameter when a function needs the same methods on owning and
borrowed handles. `_` is not allowed in a named function's parameter type.

```rust
use rusteron_archive::{Aeron, ResourceStorage, bindings::aeron_t};

fn client_id<S: ResourceStorage<aeron_t>>(client: &Aeron<S>) -> i64 {
    client.client_id()
}

# fn check(client: &Aeron) {
let owning_id = client_id(client);
let borrowed = client.as_ref();
assert_eq!(owning_id, client_id(&borrowed));
# }
```

Add `S: Clone` if the function clones its handle. Borrowed and inline storage
cannot meet that bound. Constructors and cleanup methods stay on the owning type.

### Borrow a child or retain its parent

Getters such as `client.context()`, `archive.aeron()` and
`archive.get_archive_context()` return views tied to their parent. Do not turn
them into owning handles by cloning or copying native structs.

To keep a child beyond a short borrow, use a retained getter:

```rust,no_run
use rusteron_archive::*;

# fn retain(archive: AeronArchive) -> Result<(), AeronCError> {
let client = archive.aeron_owned();
let context = archive.retain_archive_context()?;
archive.close()?;

// These retained handles keep the archive and its children alive.
let id = client.as_ref().client_id();
let view = context.as_ref();
// SAFETY: context stays alive and unchanged while request_channel is borrowed
let request_channel = unsafe { view.get_control_request_channel() };
# let _ = (id, request_channel);
# Ok(()) }
```

Create publications and subscriptions from the original owning client. A borrowed
client view cannot create owning children.

`AeronArchiveContext` and native fragment assemblers are not cloneable. To share a
newly created context during setup, use `Rc<AeronArchiveContext>`. Its
configuration methods still stop working after initialization.

## Close and delete

`close(self)` and `delete(self)` release that owning handle. Other owning clones
keep the allocation alive. Native cleanup runs after the last owner releases it.

Code that expected closing one clone to invalidate every other clone must now
release the other owners.

Borrowed views have no cleanup method. Unsafe forced cleanup and native removal
calls bypass these rules. Follow their safety contracts.

## Thread safety

Remove `multi-threaded` from Cargo features. It no longer exists.

Default owning handle types:

| Handle | Send | Sync |
| --- | --- | --- |
| `Aeron`, `AeronPublication`, `AeronCounter`, `AeronCountersReader` | Yes | Yes |
| `AeronSubscription`, `AeronExclusivePublication`, `AeronImage` | No | No |
| Contexts, async registrations, native assemblers, Archive and driver handles | No | No |
| Local `Handler<T>` | No | No |
| `SharedHandler<T>` with `T: Send` | Yes | Yes |

Create local resources on the thread that uses them. A regular publication can be
shared among producer threads. Borrowed and retained views follow the thread
traits of their storage and parent.

Subscriptions and exclusive publications are cloneable and their poll and send
methods take `&self`, so they stay local to stop two clones using one native
object at once. The grouped `No` entries are conservative. See
[thread safety](thread-safety.md#native-contracts-and-wrapper-traits).

Configure contexts before native initialization. Later setters return an error,
or panic if the signature has no error result. Manual conductor methods such as
`main_do_work()` and `main_idle_strategy()` require `unsafe`. The caller must
prevent concurrent and recursive conductor calls, including calls from native
Archive operations. A background conductor avoids this for shared clients.

To share a client or regular publication, put it in an `Arc` or clone it. See
[sharing across threads](client-guide.md#sharing-across-threads) for an example
and [thread safety](thread-safety.md) for the audited types.

## Callbacks and fragment assembly

Fragment assembly has three forms:

- `FragmentAssembler` and `ControlledFragmentAssembler` own a fixed callback.
  They replace the native `Aeron*FragmentAssembler` wrappers,
  `FnMutMessageHandler`, `FnMutControlledMessageHandler` and
  `Handler::with_*_fragment_assembler`. Read callback state with `handler()` or
  `handler_mut()`.
- `AeronImageFragmentAssembler` and `AeronImageControlledFragmentAssembler` keep
  their names. `new` takes the callback by value instead of a `Handler`, and
  `poll(&image, limit)` polls with it. They poll one image with one buffer
  allocated at creation.
- `AeronFragmentClosureAssembler` and `AeronControlledFragmentClosureAssembler`
  keep their names but take a closure on each poll:
  `poll(&subscription, closure, limit)`. The closure can borrow local state, so
  the `ctx` argument and `fn` pointer are gone.

Poll a subscription with `assembler.poll(&subscription, ...)` and an image with
`image_assembler.poll(&image, ...)`. For any other poll target, pass the
assembler itself, or `&mut assembler.with(closure)` for the closure forms.

A persistent subscription reassembles messages itself. If 0.2 code polled one
through a closure assembler, use `ps.poll_fn(closure, limit)`, or
`ps.controlled_poll_fn(closure, limit)` for the controlled form. Passing an
assembler to `ps.poll` or `ps.controlled_poll` still works but adds a call per
message.

Choose callback storage by how C invokes it:

| Callback | API |
| --- | --- |
| Reusable fragment callback | `subscription.poll(&mut callback, limit)` |
| Synchronous closure | `subscription.poll_fn(closure, limit)` |
| State kept in a `Handler` | `subscription.poll(&mut *handler.lock(), limit)` |
| Retained callback that may run on a conductor | Owned setter, or `SharedHandler::new_shared(callback)` |
| Fixed complete-message callback | `FragmentAssembler::new(callback)` |
| New complete-message closure per poll | `AeronFragmentClosureAssembler::new()` with `poll(&subscription, closure, limit)` |
| Controlled complete-message callback | `ControlledFragmentAssembler::new(callback)` or `AeronControlledFragmentClosureAssembler::new()` |

An owned setter picks the shared callback storage for you:

```rust,no_run
use rusteron_archive::*;

# fn configure(context: &AeronContext) -> Result<(), AeronCError> {
context.set_error_handler(Some(|code: i32, message: &str| {
    eprintln!("Aeron {code}: {message}");
}))?;
# Ok(()) }
```

`poll`, `controlled_poll`, bounded polls and block polls take `&mut callback`.
They allocate nothing and do no borrow check or mutex lock. `Handler` does not
implement the fragment callback traits, and no poll takes one. Replace 0.2's
`poll(Some(&handler), limit)` by owning the callback and calling
`poll(&mut callback, limit)`. If the state must stay in a `Handler`,
`poll(&mut *handler.lock(), limit)` works but adds that `RefCell` check (or
mutex for a `SharedHandler`) once per poll call.

### Reusable fragment callbacks

In 0.2, a reusable callback uses `Handler`, and the callback receives its header
by value:

```rust,ignore
use rusteron_archive::*;

#[derive(Default)]
struct Receiver(usize);

impl AeronFragmentHandlerCallback for Receiver {
    fn handle_aeron_fragment_handler(&mut self, bytes: &[u8], _header: AeronHeader) {
        self.0 += bytes.len();
    }
}

# fn receive(subscription: &AeronSubscription) -> Result<(), AeronCError> {
let callback = Handler::new(Receiver::default());
subscription.poll(Some(&callback), 32)?;
# Ok(()) }
```

In 0.3, keep the callback on the stack and pass a mutable borrow. The header is
borrowed too:

```rust,no_run
use rusteron_archive::*;

#[derive(Default)]
struct Receiver(usize);

impl AeronFragmentHandlerCallback for Receiver {
    fn handle_aeron_fragment_handler(&mut self, bytes: &[u8], _header: &AeronHeader) {
        self.0 += bytes.len();
    }
}

# fn receive(subscription: &AeronSubscription) -> Result<(), AeronCError> {
let mut callback = Receiver::default();
subscription.poll(&mut callback, 32)?;
# Ok(()) }
```

The callback can be reused between polls. It needs no reference count, `RefCell`
or mutex. `poll_fn` keeps the same call syntax.

### Fragment assembly

In 0.2, `Handler::with_fragment_assembler` returns two reference-counted handlers,
the assembler to poll with and the inner callback to read state from.

```rust,ignore
use rusteron_archive::*;

#[derive(Default)]
struct Messages(usize);

impl AeronFragmentHandlerCallback for Messages {
    fn handle_aeron_fragment_handler(&mut self, _message: &[u8], _header: AeronHeader) {
        self.0 += 1;
    }
}

# fn receive(subscription: &AeronSubscription) -> Result<(), AeronCError> {
let (assembler, messages) = Handler::with_fragment_assembler(Messages::default())?;
subscription.poll(Some(&assembler), 64)?;
println!("{} complete messages", messages.0);
# Ok(()) }
```

In 0.3, `FragmentAssembler` owns the callback. Poll through the assembler, or pass
`&mut assembler` to any poll method, and read state with `handler()`:

```rust,no_run
use rusteron_archive::*;

#[derive(Default)]
struct Messages(usize);

impl AeronFragmentHandlerCallback for Messages {
    fn handle_aeron_fragment_handler(&mut self, _message: &[u8], _header: &AeronHeader) {
        self.0 += 1;
    }
}

# fn receive(subscription: &AeronSubscription, image: &AeronImage) -> Result<(), AeronCError> {
let mut assembler = FragmentAssembler::new(Messages::default())?;
assembler.poll(subscription, 64)?;
image.poll(&mut assembler, 64)?;
println!("{} complete messages", assembler.handler().0);
# Ok(()) }
```

The controlled form changes the same way. `Handler::with_controlled_fragment_assembler`
becomes `ControlledFragmentAssembler::new`, and the callback returns an action:

```rust,no_run
use rusteron_archive::*;

struct UntilFull(Vec<u8>);

impl AeronControlledFragmentHandlerCallback for UntilFull {
    fn handle_aeron_controlled_fragment_handler(
        &mut self,
        message: &[u8],
        _header: &AeronHeader,
    ) -> bindings::aeron_controlled_fragment_handler_action_t {
        if self.0.len() + message.len() > self.0.capacity() {
            // Leave the message in the log and stop this poll
            return bindings::aeron_controlled_fragment_handler_action_t::AERON_ACTION_ABORT;
        }
        self.0.extend_from_slice(message);
        bindings::aeron_controlled_fragment_handler_action_t::AERON_ACTION_CONTINUE
    }
}

# fn receive(subscription: &AeronSubscription) -> Result<(), AeronCError> {
let mut assembler = ControlledFragmentAssembler::new(UntilFull(Vec::with_capacity(1 << 20)))?;
assembler.poll(subscription, 64)?;
assembler.handler_mut().0.clear();
# Ok(()) }
```

An assembler is not `Clone`, so one owner polls it. Dropping it frees the native
assembler and any partial messages.

### Accessing callback state

Handlers no longer dereference to callback state.

- Local `Handler::lock()` returns a mutable `RefCell` guard and `borrow()` a read
  guard. Both check borrowing on the current thread, with no mutex or atomic
  operation. `poll(&mut *handler.lock(), limit)` holds one guard across the whole poll.
- `SharedHandler::lock()` takes a mutex. A retained shared callback takes the same
  mutex when invoked, so contention can delay either side. Keep guards short.
- `poll(&mut callback, limit)`, `poll_fn` and the owned assemblers use plain Rust
  borrowing with no guards. Use them when one polling thread owns the callback.

Release a state guard before invoking the same handler through C. A local handler
rejects the second borrow and a shared handler deadlocks on its own mutex. See
[synchronization costs](thread-safety.md#hot-path-behaviour).

For a simple polling loop, a closure assembler avoids shared callback storage.
The closure borrows local variables for the poll call only:

```rust,no_run
use rusteron_archive::*;

# fn receive(subscription: &AeronSubscription) -> Result<(), AeronCError> {
let mut bytes = 0;
let mut assembler = AeronFragmentClosureAssembler::new()?;
assembler.poll(subscription, |message, _header| bytes += message.len(), 64)?;
println!("received {bytes} bytes");
# Ok(()) }
```

With a fixed-callback `FragmentAssembler`, `handler()` and `handler_mut()` expose
the callback between polls.

Callback wrapper types now reflect borrowing. Image callbacks and `for_each_image`
use `&AeronImageRef`, and headers use `&AeronHeader`. Update explicit callback
signatures and copy any fields you need after the callback returns.

Registered handlers stay alive until native cleanup. Archive connections keep the
callback registrations copied when the connection starts. Use
`AeronErrorHandlerSlot` when a replacement should affect existing connections.
See [callback ownership](archive-guide.md#why-replaced-handlers-stay-alive) and
[panic behaviour](callback-panics.md).

## Buffer claims and query results

Prefer `try_claim_owned()` for publishing:

```rust,no_run
use rusteron_archive::*;

# fn send(publication: &AeronPublication) -> Result<(), Box<dyn std::error::Error>> {
let mut claim = publication.try_claim_owned(5)?;
claim.data().copy_from_slice(b"hello");
claim.commit()?;
# Ok(()) }
```

The claim, copy and commit sequence is unchanged. If a helper returns a claim, add
the publication's lifetime to its return type.

0.2:

```rust,ignore
use rusteron_archive::*;

fn claim(publication: &AeronExclusivePublication) -> Result<AeronClaim, AeronOfferError> {
    publication.try_claim_owned(32)
}
```

0.3:

```rust
use rusteron_archive::*;

fn claim(publication: &AeronExclusivePublication) -> Result<AeronClaim<'_>, AeronOfferError> {
    publication.try_claim_owned(32)
}
```

`AeronClaim<'a>` borrows the publication, is not cloneable, and aborts if dropped
unfinished. `data()` needs mutable access, so mutable slices cannot overlap. The
lifetime adds no runtime cost.

The low-level `AeronBufferClaim` API needs `unsafe` to construct claims from
external buffers or access their mutable buffers. The same applies to
pointer-bearing wrappers such as `AeronMappedBuffer`. An unsafe constructor does
not extend the backing buffer's lifetime.

Use `get_constants()` and `get_buffers()` to get initialized query results.
Pointer-bearing results carry the queried handle's lifetime, so keep that handle
alive. Raw output-parameter variants such as `get_buffers_into()` need `unsafe`
when their output borrows native memory. Pointer-free header snapshots are
independent values.

`addr()` on a counter returns a raw pointer. Use `addr_atomic()` with an explicit
atomic ordering for safe concurrent access.

## Archive ownership and strings

Params built with `AeronArchiveReplicationParams::builder()` retain their strings
and credentials, including after you clone the params and drop the builder's
owner. `build()` returns an error for embedded NULs in string fields.

- `set_aeron(&client)` retains the Rust-owned client. Safe code cannot transfer
  that client's ownership to native context cleanup. `set_owns_aeron_client(true)`
  returns an error.
- Borrowed Archive contexts cannot be reconfigured or start a connection.
  Configure an owning context first.
- Set a Rust-owned client with `context.set_aeron(&client)` before starting an
  archive connection. `AeronArchiveAsyncConnect::new()` rejects a context without
  one. `new_with_aeron()` also checks that the context uses the supplied client.
- Complete registrations through their async handle's `poll()` or
  `poll_blocking()`. Constructors such as `AeronPublication::new(&pending)` are
  internal. Calling the native poll alone skips dependency transfer. Completed or
  cancelled async handles cannot expose a cached borrowed view.
- Deleting a separately created `AeronArchiveProxy` leaves its Rust-owned
  publication open. It closes when its last owner is released.
- String getters return borrowed `&str` without allocating. Getters for immutable
  text, such as version strings, publication channels and recording descriptor
  fields, are safe. Getters for replaceable text, such as
  `get_control_request_channel()`, need `unsafe`. Keep the owner alive and do not
  let any setter, native call or alias change the text while it is borrowed. Use
  `get_control_request_channel_owned()` for a copy.
- Optional persistent-subscription counter getters return
  `Option<Ref<'_, AeronCounter>>`. Handle `None` for an unset counter. Raw setters
  and builder counter methods that transfer native ownership need `unsafe` and
  exclusive ownership.

Persistent subscriptions retain their context, client, archive context and
listener. Set a Rust-owned client on the persistent-subscription context or its
archive context before `create()`. Both contexts must use the same client. Context
aliases stay valid after the subscription closes, but a used subscription context
cannot be reconfigured or used to create another subscription.

The archive context cannot be reconfigured while a persistent subscription uses
it, because native reconnects read it and copy its callbacks. It can be
reconfigured after the subscription is cleaned up. Use separate archive contexts
for independently configured subscriptions.

Every assembler has one mutable owner, frees its native assembler on drop, cannot
be cloned and does not expose the native pointer.

## System counter IDs

`AeronSystemCounterType` uses the constants in the native bindings. The `DummyLast`
variant is removed. Aeron defines counters at IDs 40 through 45, including
`PublicationsRevoked` and `NativeResourceAgentProxyFails`. Use
`AeronSystemCounterType::try_from(id)` to check an ID instead of a sentinel value.

`AeronCounterTypeId` represents built-in counter types, such as `PublisherLimit`
and `ArchiveRecordingPosition`. It is not a counter's allocated ID.
`AeronCounterState` represents `Unused`, `Allocated` and `Reclaimed`.
`reader.counter_type(id)` and `reader.counter_record_state(id)` return these enums.
`foreach_typed_counter_fn`, `find_by_counter_type` and
`find_by_counter_type_and_registration_id` take or pass `AeronCounterTypeId`.
`reader.system_counter_value(AeronSystemCounterType::BytesSent)` reads a driver
system counter. Both enums keep unknown integers and return the raw value through
`code()`. The integer methods still work.

## Native enum conversions

The generator adds Rust names for native enums and their values. The C names still
work. For example:

```rust
use rusteron_archive::AeronControlledFragmentHandlerAction as Action;

let action = Action::Continue;
assert_eq!(u32::from(action), 4);
assert_eq!(Action::try_from(99_u32), Err(99));
```

Controlled-poll callbacks can return `Action::Continue` or `Action::Abort`
directly. The Media Driver accepts `AeronThreadingMode::Shared`, and Archive
methods accept `AeronArchiveSourceLocation::Local`. These use the native enum
representation, so passing them needs no conversion or allocation.

Native enums have generated `TryFrom` and `From` implementations. Use them instead
of copying numeric values into a match or using `transmute`. Unknown integers
return `Err(value)`.

```rust
use rusteron_archive::bindings::aeron_controlled_fragment_handler_action_t;

let action = aeron_controlled_fragment_handler_action_t::try_from(4_u32).unwrap();
assert_eq!(u32::from(action), 4);
assert_eq!(aeron_controlled_fragment_handler_action_t::try_from(99_u32), Err(99));
```

The implementation uses the enum's native integer type. Controlled-poll actions
use `u32`, and Archive recording signals use `i32`. The recording-signal helper
still returns `None` for null and unknown codes. Direct enum conversion accepts
the native null variant.

## Replay parameters and Archive protocol versions

The Archive bindings include `aeron_archive_replay_params.h` and
`aeron_archive_configuration.h`.

- `params.is_bounded()` checks whether a replay has a bounding counter.
- `destination.copy_from(&source)` copies every replay parameter into existing
  storage. The source can use owning or borrowed storage and is left unchanged.
- `AeronArchive::semantic_version()` returns the Archive protocol version.
- `AeronArchive::protocol_version_with_archive_id()` returns the first protocol
  version that supports Archive IDs.

Use `Aeron::semantic_version_major`, `semantic_version_minor` and
`semantic_version_patch` to read the version components.

## Native comparisons with two handles

When a native function took two pointers of the receiver's type, the generator
used `self` for both. The second pointer is now a separate argument. This fixes
Media Driver channel-tag matching, IPv4 and IPv6 prefix comparisons, and
socket-address comparisons. Pass the second handle at each affected call site. For
example, `first.aeron_ipv4_does_prefix_match(&second, 24)` compares two IPv4
addresses. IPv4 prefix utilities panic on lengths above 32, and IPv6 comparisons
panic on lengths above 128, before calling C.

## Archive error codes

Archive control requests read the native error code instead of parsing the
message. `poll_for_error()` also returns a typed code from the response poller,
with the full message. An error with an empty message still returns `Some(error)`.

Use `AeronArchiveErrorCode::from_native_code(code)` for error-handler codes or
`aeron_errcode()`. Known Archive codes accept either sign. Other native codes keep
their value and sign in `Unknown(code)`. `AeronArchiveError::parse` is removed.

Converting `AeronArchiveError` to `AeronCError` keeps the native code instead of
using `-1`. Match on the typed Archive code rather than assuming every failed
control request has the same numeric code.

`AeronDriverErrorCode` covers the separate driver protocol codes. Its `from_code()`
and `code()` methods keep the wire value and its sign. Do not pass publication
offer results or system errors to this enum, because the same number means
something different in those APIs.

## Raw pointers and native structs

Safe `From<*mut T>`, `From<*const T>` and native-struct conversions are removed.
`unsafe from_ptr()` produces a borrowed handle and needs a valid, non-null pointer
for every use. `unsafe from_native()` stores a native value inline without
validating the memory its pointers reach.

Low-level native initializers that accept external storage need `unsafe`. Their
retained pointers must stay valid until cleanup, and initialization must not
overwrite live native state. Use the owning constructors where available.

Native structs no longer appear through an implicit `Deref`. Use the wrapper's
field methods. `get_inner_ref()` and `get_inner_mut()` need `unsafe` and
coordination with every Rust and native alias. `get_inner()` returns a raw pointer.
It does not transfer ownership or make dereferencing safe.

Zeroed native-handle constructors need `unsafe`, because zeroed memory is not an
initialized connection. `Aeron`, `AeronPublication`, `AeronExclusivePublication`,
`AeronSubscription`, `AeronCounter` and `AeronCountersReader` have none. `Default`
exists only where zeroed memory is a valid value, such as `AeronBufferClaim` and
`AeronUriParam`. Prefer typed constructors and safe query methods.

`unsafe clone_reference()` on `AeronHeader` and `AeronPublicationErrorValues` is
removed. Owned values implement `Clone`. Callback arguments are borrowed, so they
cannot be cloned or outlive the callback. Copy out the fields you need.

The archive pollers no longer expose `fragment_assembler()`. Their `poll()` still
reassembles responses, so nothing replaces it.

`clone_struct()` makes a shallow copy and needs `unsafe` when pointer fields could
keep borrowing the original memory, including pointers inside unions, arrays and
nested structs. It does not create an independent native resource or duplicate
cleanup rights. Prefer copying individual fields into Rust values. Copies of
structs that contain only plain values stay safe.

`Debug` shows safe fields and immutable queries, including a runner's `role_name`
and `running` flag and publication constants. Replaceable text and opaque callback
headers need explicit queries. `Debug` does not call unsafe getters or `_owned`
getters.

Generated methods and constructors that accept raw pointers need `unsafe`,
including their closure variants. Internal resource constructors are private.
Native readers that cannot accept a null callback reject `None` before entering C.
Publication reserved-value suppliers stay optional. Invalid UTF-8 in
length-delimited text callbacks produces an empty string, as with C-string
callbacks. Message buffers stay byte slices.

Archive's generated `get_and_own_archive_context()` and
`get_and_own_control_response_subscription()` are removed. C transfers cleanup
responsibility, which a borrowed return value cannot represent. Use the borrowed
getters or `retain_archive_context()`. Raw ownership transfers remain available
through the unsafe C bindings.

`AeronArchivePersistentSubscription::get_and_own_persistent_subscription_context()`
is removed. `build()` already moves the context into Rust ownership and closes it
when the last handle drops, so closing the pointer yourself would free it twice.
Use `get_persistent_subscription_context()` to borrow it.

Recording descriptor pollers and recording subscription descriptor pollers no
longer implement `Clone`. Their `poll()` methods take `&mut self`, and borrowed
Archive poller views cannot be polled. For asynchronous listing, create an owned
poller and call `reset()` with a non-null consumer before polling. Archive's
blocking listing methods still accept borrowed closures.

`Inline<T>` stores an initialized native value and does not implement `Clone`.
`PinnedInline<T>` needs `Pin<&Self>` to get its raw pointer and does not implement
`ResourceStorage`. Pinning does not manage the native lifetime or cleanup. Callers
using raw FFI are responsible for both.

## Enums and durations

Public Rust status, error and configuration enums are `#[non_exhaustive]` where
variants may be added. Include a fallback arm when matching outside the crate. Raw
C bindings keep their native representation.

Use the checked `Duration` helpers where available, including
`AeronContext::set_driver_timeout`, the resource-linger and idle-sleep helpers,
`AeronArchiveContext::set_message_timeout`, and the URI `*_duration` setters. Raw
methods keep unit suffixes such as `_ns` or `_ms`.
