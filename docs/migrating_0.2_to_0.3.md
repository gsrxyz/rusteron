# Migrating from 0.2 to 0.3

0.3 changes resource storage, ownership and thread safety. Common send and poll
methods keep their direct native calls. Most changes affect setup, callbacks,
explicit type annotations and low-level FFI access.

[Checklist](#upgrade-checklist) · [Handle types](#owning-and-borrowed-handles) ·
[Threading](#thread-safety) · [Callbacks](#callbacks-and-fragment-assembly) ·
[Raw APIs](#raw-pointers-and-native-structs) · [Benchmarks](#performance-compared-with-0210)

## Upgrade checklist

| Check existing code for | Change for 0.3 |
| --- | --- |
| `multi-threaded` dependency feature | Remove it; thread traits are selected per native type |
| `Handler::new(...)` passed to a retained conductor callback | Pass an owned callback, or use `SharedHandler::new_shared(...)` |
| Direct access through a handler's `Deref` | Use checked state access: local `Handler` uses borrow checks; `SharedHandler` uses a mutex. [Costs and usage](#accessing-callback-state) |
| Cloning a getter's `Ref` | Borrow again with `as_ref()`, or use an explicit retained-child getter |
| Function arguments annotated as the default owning wrapper | Add a storage parameter if the function should also accept borrowed views |
| Cloning an Archive context or native assembler | Keep one owner; borrow it, or share a newly created context through `Rc` during setup |
| Cloning a recording descriptor poller | Keep one mutable owner; `poll()` requires `&mut self` and a consumer installed by `reset()` |
| `close()` or `delete()` followed by reuse | These operations consume the owning handle; use another owning clone if supported |
| `From<*mut T>`, `From<native_struct>` or implicit native struct `Deref` | Use the documented unsafe constructors or explicit field getters |
| Mutable configuration strings borrowed as `&str` | Borrowing requires `unsafe`; use `_owned()` for an explicit copy |
| Raw counter addresses used as references | Use `addr_atomic()` for safe atomic access |
| `timestamp_offset_reserved("...")` | Use `media_rcv_timestamp_offset("reserved")`, `channel_rcv_timestamp_offset("reserved")` or `channel_snd_timestamp_offset("reserved")`. Aeron defines `reserved` as a value, not a key. |
| Exhaustive matches on public Rust enums | Add a fallback arm for `#[non_exhaustive]` enums |
| Raw manual conductor calls | Coordinate all native callers and use the documented unsafe entry point |

Update the crate versions together:

```toml
[dependencies]
rusteron-client = { version = "0.3", features = ["static"] }
```

Use `rusteron-archive` instead if you need both transport and Archive APIs.

## Borrowed strings and allocations

```rust,no_run
# use rusteron_archive::*;
# fn example() -> Result<(), AeronCError> {
let context = AeronArchiveContext::new()?;
context.set_control_response_channel(c"aeron:ipc")?;

// No allocation. This local context stays unchanged while channel is borrowed.
let channel = unsafe { context.get_control_response_channel() };
assert_eq!(channel, "aeron:ipc");

// One explicit copy. The snapshot stays valid after the context changes.
let saved = context.get_control_response_channel_owned();
context.set_control_response_channel(c"aeron:udp?endpoint=localhost:8010")?;
assert_eq!(saved, "aeron:ipc");
# Ok(()) }
```

The safe `&str` getter in 0.2.10 allowed a setter to free or overwrite text that
was still borrowed. Version 0.3 keeps the borrowed return without restoring that
use-after-free. A mutable borrow alone would not fix setters called through other
owning handles or native operations that also replace the text.

Compared with 0.2.10, publishing, claiming, committing, direct polling and reading
recording descriptor fields add no Rust heap allocation. Owned fragment
assemblers allocate their callback once when created, then reuse it while
polling. Aeron can allocate native reassembly buffers for fragmented messages.
Retained callbacks and parent dependencies allocate during registration so their
storage survives until native cleanup. Replication builders retain their channel
strings for the same reason. These allocations happen during setup.

Error construction and formatting do not allocate Rust heap memory by default.
String literals passed to `AeronCError::with_message` are borrowed. An owned
`String` moves into the error without another copy. Copy a non-static borrowed
message explicitly with `.to_owned()`. `capture_errmsg()` retains a native message
with one copy. Formatting an uncaptured native message uses a stack snapshot so
formatting code can call Aeron without invalidating the text. Optional logging,
backtraces and the caller's output destination can allocate.

## Owning and borrowed handles

Generated handles now have a sealed storage parameter. `Aeron` and
`AeronPublication` still mean their normal owning forms, so most applications do
not need to name it.

| Form | Purpose | Clone |
| --- | --- | --- |
| `Aeron`, `AeronPublication`, other supported owning handles | Own native cleanup, retaining their dependencies | Where supported |
| `Ref<'a, T>` | Borrow a child or an owning handle | No |
| `AeronRef`, `AeronPublicationRef`, other `*Ref` aliases | The wrapper specialization inside a borrowed view | No |
| `OwnedWithReference<Owner, T>` | Retain a parent while accessing a stable child | When `Owner: Clone` |
| `Handle<Inline<Native>>` | Store an initialized native value directly | No |
| `Handle<ScopedInline<'a, Native>>` | Hold query results whose pointers borrow a parent | No |

The old `CResource` enum and its variants are gone. Storage is selected at compile
time. Generic access introduces no runtime tag check, trait-object call or owner
clone. Shared heap owners use `Arc`; cloning or dropping an owner still updates
its reference count.

### Accept any storage type

Use a generic parameter when a function needs common methods on both owning and
borrowed handles. `_` is not allowed in a named function's parameter type.

```rust
use rusteron_client::{Aeron, ResourceStorage, bindings::aeron_t};

fn client_id<S: ResourceStorage<aeron_t>>(client: &Aeron<S>) -> i64 {
    client.client_id()
}

# fn check(client: &Aeron) {
let owning_id = client_id(client);
let borrowed = client.as_ref();
assert_eq!(owning_id, client_id(&borrowed));
# }
```

Require `S: Clone` as well if a generic function needs to clone its handle.
Borrowed and inline storage cannot meet that bound. Constructors and cleanup
methods that require ownership remain on the owning specialization.

### Borrow a child or retain its parent

Getters such as `client.context()`, `archive.aeron()` and
`archive.get_archive_context()` return views tied to their parent. Do not try to
turn them into owning handles by cloning or copying native structs.

When a child must survive beyond a short borrow, use an audited retained getter:

```rust,no_run
use rusteron_archive::*;

# fn retain(archive: AeronArchive) -> Result<(), AeronCError> {
let client = archive.aeron_owned();
let context = archive.retain_archive_context()?;
archive.close()?;

// These retained handles keep the archive and its children alive.
let id = client.as_ref().client_id();
let view = context.as_ref();
// SAFETY: context stays alive and unchanged while request_channel is borrowed.
let request_channel = unsafe { view.get_control_request_channel() };
# let _ = (id, request_channel);
# Ok(()) }
```

Use the original owning transport client to create publications or subscriptions.
A borrowed client view cannot create owning children.

`AeronArchiveContext` and native fragment assemblers are not cloneable. If setup
code needs shared ownership of a newly created context, use `Rc<AeronArchiveContext>`.
This does not make its configuration methods valid after initialization.

## Close and delete

`close(self)` and native-wrapper `delete(self)` release that owning handle.
Other supported owning clones keep the allocation alive. Children retain their
required parents; native cleanup runs after the final owner releases it.

Code that previously expected closing one leaf clone to invalidate every other
clone must stop using and release those other owners.

Borrowed views have no owning cleanup method. Unsafe forced cleanup and native
removal calls bypass the safe ownership rules; follow their safety contracts.

## Thread safety

Remove `multi-threaded` from Cargo features. It no longer exists.

This table describes Rusteron's default owning handle types. These traits depend
on the Rust wrapper's ownership, callbacks and exposed methods as well as the
native object's threading contract.

| Handle | Send | Sync |
| --- | --- | --- |
| `Aeron`, `AeronPublication`, `AeronCounter`, `AeronCountersReader` | Yes | Yes |
| `AeronSubscription`, `AeronExclusivePublication`, `AeronImage` | No | No |
| Contexts, async registrations, native assemblers, Archive and driver handles | No | No |
| Local `Handler<T>` | No | No |
| `SharedHandler<T>` with `T: Send` | Yes | Yes |

Create local resources on the thread that uses them. A regular publication can
be shared among producer threads. Borrowed and retained views also depend on the
thread traits of their storage and parent.

`Send` allows moving ownership to another thread. `Sync` allows sharing references
between threads. Aeron's single-consumer rule does not by itself forbid moving a
unique owner. Rusteron's subscriptions and exclusive publications are cloneable,
and their poll/send methods take `&self`. They remain local so separate clones
cannot use the same native object concurrently. Removing only `Sync` would not
prevent that. A transferable single-consumer API would also need to enforce
exclusive native access across every handle and retained child.

The grouped `No` entries are conservative wrapper restrictions. For example,
Archive requests already use a native mutex, but that does not protect direct
poller access, retained local callback state or every conductor invocation exposed
by this API. See [thread safety](thread-safety.md#native-contracts-and-wrapper-traits)
for the distinction.

Configure contexts before native initialization. Setters reject later mutation
with an error, or panic if their signature has no error result. Manual conductor
methods such as `main_do_work()` and `main_idle_strategy()` require `unsafe`.
The caller must prevent concurrent and recursive conductor calls, including
calls from native Archive operations. A background conductor avoids this manual
coordination for shared clients.

See [thread safety](thread-safety.md) for the audited types and native contracts.

## Callbacks and fragment assembly

`FnMutMessageHandler::set` and `FnMutControlledMessageHandler::set` now require
`unsafe`. They store a context pointer without a Rust lifetime. Prefer
`FragmentAssembler` or `ControlledFragmentAssembler`, which keep the callback's
borrow in its type. The older closure assemblers' `poll` methods remain safe and
clear the context pointer on return and on panic.

Choose callback storage by how C invokes it:

| Callback | API |
| --- | --- |
| Reusable fragment callback | `subscription.poll(&mut callback, limit)` |
| Synchronous closure | `subscription.poll_fn(closure, limit)` |
| Shared local callback | `Handler::new(callback)` with `poll_with_handler` |
| Retained callback that may run on a conductor | Owned setter, or `SharedHandler::new_shared(callback)` |
| Local complete-message callback | `FragmentAssembler::new(callback)` |
| Controlled complete-message callback | `ControlledFragmentAssembler::new(callback)` |

An owned setter selects the shared callback storage for you:

```rust,no_run
use rusteron_client::*;

# fn configure(context: &AeronContext) -> Result<(), AeronCError> {
context.set_error_handler(Some(|code: i32, message: &str| {
    eprintln!("Aeron {code}: {message}");
}))?;
# Ok(()) }
```

Normal `poll`, `controlled_poll`, bounded polls and block polls now take
`&mut callback`. The callback stays exclusively borrowed until the call returns.
These methods allocate nothing and perform no callback borrow check or mutex lock.
`Handler` does not implement the fragment callback traits, so it cannot add checks
to an ordinary poll. Code that needs shared callback ownership must call the
explicit `poll_with_handler` or `controlled_poll_with_handler` method.

### Reusable fragment callbacks

In 0.2.10, a reusable callback uses `Handler`, and the callback receives its
header by value:

```rust,ignore
use rusteron_client::*;

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
also borrowed:

```rust,no_run
use rusteron_client::*;

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

Both forms allow the callback to be reused between polls. The 0.3 form needs no
callback reference count, `RefCell` or mutex. The
[reusable callback benchmark](#performance-compared-with-0210) compares these
two APIs with the same callback work. `poll_fn` keeps the same call syntax.

### Accessing callback state

Handlers no longer dereference to callback state. The cost depends on
the handler type:

- Local `Handler::lock()` returns a mutable `RefCell` guard; `borrow()` returns a
  read guard. Both check borrowing on the current thread, without a mutex or
  atomic operation. The explicit `poll_with_handler` form holds one guard across
  the whole poll.
- `SharedHandler::lock()` acquires a mutex to coordinate state access with
  callbacks on other threads. A retained shared callback also acquires this
  mutex when invoked. Contention can delay either caller, so keep guards short.
- `poll(&mut callback, limit)`, `poll_fn` and an owned `FragmentAssembler` use
  direct Rust borrowing without these guards. Prefer them when one polling
  thread owns the receive callback.

Release a state guard before invoking the same handler through C. This prevents
overlapping access. A local handler would reject the borrow, while a shared
handler could deadlock trying to acquire its own mutex. See
[synchronization costs](thread-safety.md#hot-path-behaviour) for each callback path.

For a simple polling loop, an owned assembler avoids shared callback storage and
accepts a closure that borrows local variables:

```rust,no_run
use rusteron_client::*;

# fn receive(subscription: &AeronSubscription) -> Result<(), AeronCError> {
let mut bytes = 0;
let mut assembler = FragmentAssembler::new(|message: &[u8], _header: &AeronHeader| {
    bytes += message.len();
})?;
assembler.poll(subscription, 64)?;
drop(assembler);
println!("received {bytes} bytes");
# Ok(()) }
```

Native assembler wrappers require mutable access for state-changing methods;
`delete(self)` consumes the wrapper. The shared `Handler::with_*_fragment_assembler`
forms remain available. `handler()` and `handler_mut()` expose an owned assembler's
callback between polls.

Callback wrapper types now reflect borrowing. For example, image callbacks and
`for_each_image` use `&AeronImageRef`; headers use `&AeronHeader`. Update explicit
callback signatures, and copy any fields needed after the callback returns.

Registered handlers stay alive through native cleanup. Archive connections keep
the callback registrations copied when connection starts. Use
`AeronErrorHandlerSlot` when a replacement should affect existing connections.
See [callback ownership](archive-guide.md#why-replaced-handlers-stay-alive) and
[panic behaviour](callback-panics.md).

## Buffer claims and query results

Prefer `try_claim_owned()` for publishing:

```rust,no_run
use rusteron_client::*;

# fn send(publication: &AeronPublication) -> Result<(), Box<dyn std::error::Error>> {
let mut claim = publication.try_claim_owned(5)?;
claim.data().copy_from_slice(b"hello");
claim.commit()?;
# Ok(()) }
```

The claim, copy and commit sequence is unchanged. If a helper returns a claim,
add the publication's lifetime to its return type.

0.2.10:

```rust,ignore
use rusteron_client::*;

fn claim(publication: &AeronExclusivePublication) -> Result<AeronClaim, AeronOfferError> {
    publication.try_claim_owned(32)
}
```

0.3:

```rust
use rusteron_client::*;

fn claim(publication: &AeronExclusivePublication) -> Result<AeronClaim<'_>, AeronOfferError> {
    publication.try_claim_owned(32)
}
```

`AeronClaim<'a>` borrows the publication, is not cloneable, and aborts if dropped
unfinished. `data()` requires mutable access, so mutable slices cannot overlap.
Safe close of another owning clone leaves the claim's owner alive. The lifetime
adds no runtime work. See the [claim benchmark](#performance-compared-with-0210)
for the measured cost of claiming, copying 32 bytes and committing.

The low-level `AeronBufferClaim` API requires `unsafe` to construct claims from
external buffers or access their mutable buffers. The same
applies to pointer-bearing wrappers such as `AeronMappedBuffer`. An unsafe
constructor does not extend the backing buffer's lifetime.

Use `get_constants()` and `get_buffers()` to obtain initialized query results.
Pointer-bearing results carry the queried handle's lifetime. Keep that handle
alive while using them. Raw output-parameter variants such as `get_buffers_into()`
require `unsafe` when their output borrows native memory. Pointer-free header
snapshots are independent values.

For counters, `addr()` returns a raw pointer. Prefer `addr_atomic()` and explicit
atomic ordering for safe concurrent access.

## Archive ownership and strings

Params built with `AeronArchiveReplicationParams::builder()` retain their strings
and credentials, including when you clone the params and drop the builder's
returned owner. `build()` returns an error for embedded NULs in string fields.

- `set_aeron(&client)` retains the Rust-owned client. Safe code cannot transfer
  that client's ownership to native context cleanup. `set_owns_aeron_client(true)`
  returns an error.
- Borrowed Archive contexts cannot be reconfigured or start a new connection.
  Configure an owning context first.
- Set a Rust-owned client with `context.set_aeron(&client)` before starting an
  archive connection. `AeronArchiveAsyncConnect::new()` rejects a context without
  one. `new_with_aeron()` also checks that the context uses the supplied client.
- Complete registrations through their async handle's `poll()` or
  `poll_blocking()`. Constructors such as `AeronPublication::new(&pending)` are
  internal. Calling the native poll alone bypassed dependency transfer and left
  the registration eligible for a second cleanup. Completed or cancelled async
  handles cannot expose a cached borrowed view; native getters reject reuse.
- Deleting a separately created `AeronArchiveProxy` leaves its Rust-owned
  publication open. The publication closes when its last owner is released.
- String getters return borrowed `&str` without allocating. Getters for immutable
  text, such as version strings, publication channels and recording descriptor
  fields, remain safe. Getters for replaceable text, such as
  `get_control_request_channel()`, require `unsafe`. Keep the owner alive and do
  not let any setter, native call or alias change the text while borrowed.
  Use `get_control_request_channel_owned()` when the text must survive those
  changes. The `_owned` suffix makes that allocation explicit.
- Optional persistent-subscription counter getters return `Option<Ref<'_, AeronCounter>>`.
  Handle `None` for an unset counter. Raw setters and builder counter methods that
  transfer native ownership require `unsafe` and exclusive ownership transfer.

Persistent subscriptions retain their context, client, archive context and listener.
Set a Rust-owned client on the persistent-subscription context or its archive
context before `create()`. The two contexts must use the same client. Context
aliases remain valid after the subscription closes, but a used subscription
context cannot be reconfigured or used to create another subscription.

The archive context cannot be reconfigured while a persistent subscription uses
it. Native reconnects read this context and copy its callbacks. Reconfiguration
is allowed again after subscription cleanup finishes. Use separate archive
contexts for independently configured subscriptions.

All four native fragment assembler wrappers have one mutable owner. Their
`delete(self)` methods consume it; they cannot be cloned or deleted through a
borrowed view. Constructors reject a missing delegate. Prefer the owned closure
assembler APIs for ordinary polling.

## System counter IDs

`AeronSystemCounterType` now uses the constants in the native bindings. The stale
`DummyLast` variant is removed. Aeron defines real counters at IDs 40 through 45,
including `PublicationsRevoked` and `NativeResourceAgentProxyFails`. Use
`AeronSystemCounterType::try_from(id)` to check an ID instead of a sentinel value.

`AeronCounterTypeId` represents built-in counter types, such as
`PublisherLimit` and `ArchiveRecordingPosition`. This differs from a counter's
allocated ID. `AeronCounterState` represents `Unused`, `Allocated` and `Reclaimed`.
Use `AeronCounterTypeId::from(reader.counter_type_id(id)?)` or
`AeronCounterState::from(reader.counter_state(id)?)` when a typed result helps.
Both enums preserve unknown integers and return the raw value through `code()`.
The existing integer methods still work.

## Native enum conversions

The generator now adds Rust names for native enums and their values. Existing
C names still work. For example:

```rust
use rusteron_client::AeronControlledFragmentHandlerAction as Action;

let action = Action::Continue;
assert_eq!(u32::from(action), 4);
assert_eq!(Action::try_from(99_u32), Err(99));
```

Controlled-poll callbacks can return `Action::Continue` or `Action::Abort`
directly. The Media Driver accepts `AeronThreadingMode::Shared`, and Archive
methods accept `AeronArchiveSourceLocation::Local`. These names use the native
enum type and representation. Passing them requires no conversion or allocation.

Native enums now have generated `TryFrom` and `From` implementations. Use these
to convert raw integers instead of copying numeric values into a match or using
`transmute`. Unknown integers return `Err(value)` without an allocation.

```rust
use rusteron_client::bindings::aeron_controlled_fragment_handler_action_t;

let action = aeron_controlled_fragment_handler_action_t::try_from(4_u32).unwrap();
assert_eq!(u32::from(action), 4);
assert_eq!(aeron_controlled_fragment_handler_action_t::try_from(99_u32), Err(99));
```

The generated implementation uses the enum's native integer type. For example,
controlled-poll actions use `u32`, while Archive recording signals use `i32`.
The recording-signal helper still returns `None` for null and unknown codes.
Direct enum conversion accepts the native null variant.

## Replay parameters and Archive protocol versions

The Archive bindings now include `aeron_archive_replay_params.h` and
`aeron_archive_configuration.h`.

- `params.is_bounded()` checks whether a replay has a bounding counter.
- `destination.copy_from(&source)` copies every replay parameter into existing
  storage. The source can use owning or borrowed storage.
- `AeronArchive::semantic_version()` returns the Archive protocol version.
- `AeronArchive::protocol_version_with_archive_id()` returns the first protocol
  version that supports Archive IDs.

These methods call the native functions directly. With binding logging disabled,
they do not allocate Rust heap storage. Copying parameters leaves the source
unchanged. Use
`Aeron::semantic_version_major`, `semantic_version_minor` and
`semantic_version_patch` to read the version components.

## Native comparisons with two handles

The generator previously replaced both pointers with `self` when a native
function took two pointers of the receiver's type. The second pointer is now
a separate argument. This fixes Media Driver channel-tag matching, IPv4 and IPv6
prefix comparisons, and socket-address comparisons. Pass the second handle at
each affected call site. For example,
`first.aeron_ipv4_does_prefix_match(&second, 24)` compares the two IPv4 addresses.
IPv4 prefix utilities reject lengths above 32, and IPv6 comparisons reject
lengths above 128. They panic before calling C with an invalid shift width.

## Archive error codes

Archive control requests read the native error code instead of parsing the
message. `poll_for_error()` also returns a typed code from the response poller,
with the full message. An error with an empty message still returns `Some(error)`.

Use `AeronArchiveErrorCode::from_native_code(code)` for error-handler codes or
`aeron_errcode()`. Known Archive codes accept either sign. Other native codes
retain their value and sign in `Unknown(code)`. `AeronArchiveError::parse` has
been removed.

Converting `AeronArchiveError` to `AeronCError` now preserves the native code
instead of replacing it with `-1`. Match on the typed Archive code rather than
assuming every failed control request has the same numeric code.

Archive error variants and native-code conversions now come from Aeron's
constants. `AeronDriverErrorCode` covers the separate driver protocol codes.
Its `from_code()` and `code()` methods preserve the wire value and its sign.
Do not pass publication offer results or system errors to this enum. The same
number has different meanings in those APIs.

## Raw pointers and native structs

Safe `From<*mut T>`, `From<*const T>` and native-struct conversions have been
removed. `unsafe from_ptr()` produces a borrowed specialization and requires a
valid, non-null pointer for every use. `unsafe from_native()` stores a native
value inline without validating the memory reached by its pointers.

Low-level native initializers that accept external storage require `unsafe`.
Their retained pointers must remain valid until cleanup, and initialization must
not overwrite live native state. Use the owning constructors where available.

Native structs no longer appear through an implicit `Deref`. Use the wrapper's
field methods. `get_inner_ref()` and `get_inner_mut()` require `unsafe` and
coordination with every Rust and native alias. `get_inner()` returns a raw pointer;
it does not transfer ownership or make dereferencing safe.

Zeroed native-handle constructors require `unsafe`; zeroed memory is not an
initialized connection. Prefer typed constructors and safe query methods.
`clone_struct()` makes a shallow copy and requires `unsafe` when pointer fields
could continue borrowing the original memory. This includes pointers inside
unions, arrays and nested structs. It does not create an independent native
resource or duplicate its cleanup rights. Prefer copying individual fields into
Rust values. Copies of structs proven to contain only plain values remain safe.

`Debug` keeps safe fields and immutable queries, including a runner's `role_name`
and `running` flag and publication constants. Formatting adds no heap allocation;
an output buffer can allocate. These reads only run when you format the wrapper.
Completed async handles report storage without reading freed native memory.
Replaceable text and opaque callback headers require explicit queries because
their backing storage can change or require initialization. `Debug` does not
call unsafe getters or allocating `_owned` getters.

Generated methods and constructors that accept raw pointers require `unsafe`,
including their closure variants. Internal resource constructors are private.
Native readers that cannot accept a null callback reject `None` before entering
C. Publication reserved-value suppliers remain optional. Invalid UTF-8 in
length-delimited text callbacks produces an empty string, matching C-string
callback conversion. Message buffers remain byte slices.

Archive's generated `get_and_own_archive_context()` and
`get_and_own_control_response_subscription()` are removed. C transfers cleanup
responsibility, which a borrowed return value cannot represent. Use the normal
borrowed getters or `retain_archive_context()`. Raw ownership transfers remain
available through the unsafe C bindings.

Recording descriptor pollers and recording subscription descriptor pollers no
longer implement `Clone`. Their `poll()` methods need `&mut self`; borrowed
Archive poller views cannot be polled. Create an owned poller for asynchronous
listing, then call `reset()` with a non-null consumer before polling. Archive's
blocking listing methods continue to accept borrowed closures.

`Inline<T>` stores an initialized native value and does not implement `Clone`.
`PinnedInline<T>` requires `Pin<&Self>` to obtain its raw pointer and does not
implement `ResourceStorage`. Pinning does not manage the native lifetime or
cleanup. Callers using raw FFI remain responsible for both.

## Enums and durations

Public Rust status, error and configuration enums are `#[non_exhaustive]` where
new variants may be added. Include a fallback arm when matching outside the
crate. Raw C bindings retain their native representation.

Use checked `Duration` helpers where available, including
`AeronContext::set_driver_timeout`, resource-linger and idle-sleep helpers,
`AeronArchiveContext::set_message_timeout`, and the URI `*_duration` setters.
Raw methods keep unit suffixes such as `_ns` or `_ms`; no implicit unit change
is intended.

## Performance compared with 0.2.10

Measured on 2026-11-17 against the `rusteron-client-v0.2.10` tag. "Current" means
the working tree with the 0.3 API changes, whose package manifests still say
0.2.21. These results do not describe the published 0.2.21 crate.

Both wrappers link the same Aeron 1.54.2 C client archive and use the same native
media driver. The original 0.2.10 release bundled Aeron 1.52.2. This comparison
holds Aeron constant to measure the Rust wrapper changes. Building the older
wrapper required renaming two unused CnC helper calls for compatibility with
1.54.2. The measured methods were unchanged.

### Per-operation time and IPC throughput

Each cell shows the median of five runs, followed by the smallest and largest
run in brackets. Operation times are elapsed batch time divided by messages,
not individual message latency percentiles. Lower ns/message is better. Higher
IPC throughput is better.

| Workload | 0.2.10 | Current |
| --- | ---: | ---: |
| Offer, ns/message | 23.19 [15.57 to 30.62] | 18.19 [14.53 to 28.76] |
| Claim/copy/commit, ns/message | 31.64 [28.02 to 52.65] | 19.56 [15.19 to 30.91] |
| `poll_fn`, ns/message | 6.91 [4.85 to 10.82] | 8.40 [5.10 to 9.45] |
| Reusable callback poll, ns/message | 9.04 [7.70 to 9.24] | 9.10 [5.35 to 9.89] |
| IPC throughput, million messages/second | 20.19 [11.93 to 29.35] | 23.54 [21.37 to 28.71] |

Claim/copy/commit had a 38% lower median time in the current code and was lower
in each of the five paired runs. The current guard stores the native frame
pointers directly and inlines the release store that commits the frame. It
avoids the old claim storage dispatch, finalisation flag and C commit call.
The public publishing sequence stays the same.

### Checking the apparent polling regression

The five-run series above put current `poll_fn` behind 0.2.10. A separate series
ran only the two polling workloads, with 20 repetitions per version and the
same warmup, message counts, fragment limit and CPU placement:

| Workload, ns/message | 0.2.10 | Current |
| --- | ---: | ---: |
| `poll_fn` | 7.98 [4.46 to 12.55] | 6.80 [4.72 to 10.02] |
| Reusable callback poll | 7.73 [5.02 to 10.55] | 6.71 [4.91 to 9.92] |

The rerun also saved batch durations after stopping each timer, in both
versions. Sorting and reporting those samples happened after measurement.

The ranking reversed, and the ranges overlap. Current `poll_fn` was lower in
10 of the 20 paired runs; the reusable callback was lower in 14. These results
do not establish a consistent polling regression or improvement.

An assembly check with a byte-counting callback found the same five x86-64
instructions in both `poll_fn` callbacks. The current wrapper also removes the
old runtime storage-enum checks. Neither ordinary polling path uses `RefCell`
or a mutex. Shared local handlers use the separate `poll_with_handler` API.

### UDP round-trip latency

These values are microseconds. Each cell is the median of that statistic across
the five runs. Samples from separate runs are not pooled.

| Version | Min | p50 | p99 | p99.9 | p99.99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.2.10 | 9.870 | 10.739 | 27.273 | 1,081.818 | 5,049.746 | 332,863.604 |
| Current | 9.850 | 10.664 | 28.106 | 1,571.132 | 6,649.708 | 54,787.518 |

The current code had higher p99, p99.9 and p99.99 values in this series. Run
maxima ranged from 18.85 to 359.71 ms for 0.2.10 and 23.02 to 396.04 ms for
current. Those pauses are too large to use these results to rank wrapper
overhead or predict production tail latency.

A separate instrumented diagnostic, excluded from the tables, reproduced a
423 ms round trip whose offer took 632 ns with no retry. The native driver
recorded a 398 ms maximum work cycle in that run. An IDE used more than one CPU
during the trial. Native error counters and kernel UDP send/receive buffer error
counters stayed at zero. Driver or scheduling pauses explain at least part of the tails;
this does not identify every pause.

### Workloads and build settings

The machine was an Intel Xeon Platinum 8375C VM with two physical cores and
four logical CPUs. No CPUs were isolated, and the VM also ran an IDE. Trials
ran one at a time, with version order reversed on alternate repetitions.
The runner checked for active Cargo and compiler processes before each trial.

Both versions used Rust 1.98.1, optimisation level 3, fat LTO, one codegen unit
and `target-cpu=native`. The shared C archive used a release build with `-O3`,
`-DNDEBUG`, loop unrolling and `-march=native`. Logging was disabled.

| Workload | Warmup messages per run | Measured messages per run | Timed work |
| --- | ---: | ---: | --- |
| Offer, claim/copy/commit, each poll form | 1,000,000 | 10,000,000 | One operation at a time, in batches of up to 512 messages |
| IPC throughput | 5,000,000 | 100,000,000 | Publish and receive on separate threads, ending after receipt is acknowledged |
| UDP round trip | 100,000 | 1,000,000 | One request in flight, echoed by another thread |

All messages were 32 bytes, with exclusive publications, a fragment limit of
32, 1 MiB term buffers, RAM-backed Aeron files and a shared native driver.
Application retry and poll loops and the shared driver used a no-op idle
strategy. Client conductors used the native default, `sleep-ns`.
Single-operation tests used one client. IPC throughput and UDP round trips
used two clients.

In single-operation tests, the polling or publishing thread used CPU 0, the
driver used CPU 1 and client conductors used CPU 3. In the threaded tests, the
sender used CPU 0, receiver CPU 1, driver CPU 2 and conductors CPU 3. CPUs 0 and
2 share a physical core, as do CPUs 1 and 3. Pinning here does not isolate the
tests from other work or remove SMT interference.

Offer and claim tests drain received messages outside the timer. Poll tests
publish their batches outside the timer. Before publishing, every batch waits
outside the timer for enough publication window space. Timed publication
batches include term-rotation retries, with 610 retries in every initial run.

Both `poll_fn` callbacks check message length. Both reusable callbacks check
message length and count bytes. The 0.2.10 callback's `Handler` is created once
outside measurement. The current callback is stored on the stack. Neither
version uses fragment assembly, reads Aeron header fields or allocates in the
receive callback. Claim tests copy the same payload into the claimed buffer.

Both round-trip tests use `std::time::Instant`, validate the full reply and
sequence number, and store samples in a preallocated vector. Sorting and
percentile calculation happen after measurement, using nearest-rank
percentiles. Timing starts before the initial offer, includes retries and ends
at the start of the receive callback.

For benchmarks in a normal checkout, see the maintained
[Criterion benchmarks](../rusteron-client/benches) and
[benchmarking guide](../BENCHMARKS.md#how-to-benchmark). For example:

```sh
cargo bench -p rusteron-client --features static --bench offer_claim_poll
cargo bench -p rusteron-client --features static --bench hot_path
```

Criterion uses a different sampling method from the fixed-count comparison
above. Its output should not be compared directly with these tables.

## Release version

These changes require a minor release from 0.2 to 0.3. The repository's release
workflow updates the crate versions and their internal dependencies together;
select `minor` when releasing this branch. The working manifests keep their
existing version until that release step.
