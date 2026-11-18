# Thread safety

Rusteron selects threading rules per native type. There is no `multi-threaded`
feature. `Send` and `Sync` are compile-time traits; they do not introduce dynamic
dispatch.

`Aeron`, `AeronPublication`, `AeronCounter` and `AeronCountersReader` are `Send + Sync`.
Other generated handles are local by default. The audited list is in
[`thread_safety.rs`](../rusteron-code-gen/src/thread_safety.rs). New native types
remain local until reviewed.

The native contracts come from the bundled Aeron C headers, including
[`aeronc.h`](https://github.com/aeron-io/aeron/blob/1.54.2/aeron-client/src/main/c/aeronc.h).
A shared owner and a shared API are separate properties. A frozen context can
stay alive behind a shared client without making context setters thread-safe.
The generator checks the cleanup closure and every retained dependency of a
shared owner for the required traits.

## Native contracts and wrapper traits

`Send` permits moving an owner to another thread. `Sync` permits sharing `&T`
between threads. These traits apply to the complete Rust API, including clones,
callbacks, borrowed children and cleanup. See the
[Rust definitions](https://doc.rust-lang.org/nomicon/send-and-sync.html).

Aeron documents regular publications as concurrent, while subscriptions, images
and exclusive publications require single-consumer or single-writer access. See
the [subscription contract](https://github.com/aeron-io/aeron/blob/1.54.2/aeron-client/src/main/c/aeronc.h),
[publication contract](https://github.com/aeron-io/aeron/blob/1.54.2/aeron-client/src/main/cpp_wrapper/Publication.h)
and [exclusive publication contract](https://github.com/aeron-io/aeron/blob/1.54.2/aeron-client/src/main/cpp_wrapper/ExclusivePublication.h).
Single-consumer access can allow a unique owner to move between threads, provided
its callbacks and dependencies are transferable too. It does not allow separate
owners to poll or publish through the same native object concurrently.

Rusteron's owning subscriptions and exclusive publications implement `Clone`,
and their poll/send methods take `&self`. They are therefore neither `Send` nor
`Sync`. Making them `Send` would let separate clones reach different threads and
mutate the same native object. Requiring `&mut self` alone would not fix this
while independently usable owning clones remain available. A unique transferable
wrapper could enforce exclusive access through Rust's type system without
adding a mutex to each poll or offer.

Other local handles are also conservative API choices. The
[Archive C client](https://github.com/aeron-io/aeron/blob/1.54.2/aeron-archive/src/main/c/client/aeron_archive_client.c)
already locks its blocking request operations. That lock does not cover every
child poller or local callback exposed by Rusteron, nor does it serialize the
same transport client's conductor across different Archive connections. A
shareable Archive wrapper must account for those paths too. Context mutation,
async completion and driver duty cycles each need their own ownership and
access rules. A local wrapper does not establish that the native object has
permanent thread affinity.

Thread traits add no runtime instructions. The current local restrictions do
not add locks to `offer` or `poll_fn`. A broader set of thread traits would need
an API that enforces the native access rules. Adding `unsafe impl` declarations
alone would not enforce them.

## Handles and lifetime

Subscriptions, exclusive publications and images are local. Making a cloneable
single-consumer handle `Send` would let a program move separate clones to different
threads and call the same native object concurrently. Create these handles on the
thread that uses them. Regular publications support concurrent publishers.

Native cleanup waits until the final owning handle drops. Calling `close()` on a
shared handle releases that reference, leaving other clones and active claims
valid. Explicit native removal APIs bypass this ownership and are unsafe.

Configure a client or driver context before handing it to native code. Retained
context clones cannot change it afterwards. Client initialization failure releases
this reservation, allowing configuration changes and another attempt.

For shared clients, the normal background conductor needs no manual calls.
`main_do_work` and `main_idle_strategy` are unsafe native entry points. Their caller
must prevent concurrent or recursive conductor calls, including calls made inside
native archive operations. Adding a Rust mutex around those two methods would not
protect calls made directly by C. An agent-invoker client is bound to its start
thread for safe archive setup and blocking helpers. Other threads can still use
its thread-safe publication and registration APIs.

Getters returning `Ref` borrow the parent. Neither the view nor its borrowed
wrapper implements `Clone`. Call `as_ref()` for another short-lived view. An
archive's `aeron_owned()` or `retain_archive_context()` retains the archive when
the child must outlive a borrow. Wrap a newly created owning context in `Rc` if
configuration code needs shared ownership. Wrapping a raw pointer requires
`unsafe from_ptr` and an external lifetime guarantee. Raw pointers returned by
`get_inner()` still require unsafe native calls. See the
[owning and borrowed handles](migrating_0.2_to_0.3.md#owning-and-borrowed-handles).

Subscriptions, exclusive publications and native assemblers cache their native
pointer. Safe constructors keep that pointer valid until cleanup; these types
therefore have no safe zeroed or raw-pointer constructors. Native assembler
wrappers are not cloneable. Their state-changing methods require mutable access,
and `delete(self)` consumes the wrapper. Clone a `Handler` to share checked access
to one assembler.

## Callbacks

- `poll(&mut callback, limit)` borrows a callback exclusively. `poll_fn` and
  other synchronous `_fn` methods borrow a stack closure. These calls have
  no handler allocation, reference-count change, callback mutex or trait-object call.
- `FragmentAssembler::new(callback)` owns a local callback and polls through
  `&mut self`. The callback can borrow local variables. `handler()` and
  `handler_mut()` expose its state between polls. Exclusive access removes the
  runtime callback borrow checks needed by shared callback ownership.
- `Handler::new` creates local callback state. It uses `Rc` for shared ownership
  and `RefCell` to reject overlapping mutable borrows. It works with local
  fragment assemblers and is neither `Send` nor `Sync`. Acquiring a state guard
  checks local borrows, without a mutex or atomic operation.
- `SharedHandler::new_shared` serializes callback state with a mutex. It is
  `Send + Sync` when its state is `Send`. Use it for callbacks retained by a
  conductor. Owned callback setters create this storage automatically.

All these APIs use statically selected callback functions. `handler.lock()` gives
mutable state access. A local `Handler` returns a `RefCell` guard, while a
`SharedHandler` acquires a mutex. Local `handler.borrow()` gives read access.
Acquiring these guards does not clone an owner or allocate memory. Explicit
`poll_with_handler` calls acquire their handler guard once for the whole call,
outside the per-fragment callback. A retained shared callback acquires its mutex
on each call and may wait if another thread holds a guard.

Release the guard before calling an API that invokes the same handler. A local
handler rejects overlapping borrows; a shared handler can deadlock if it tries
to acquire a mutex already held by its calling thread. `poll_fn` and an owned
`FragmentAssembler` use direct Rust borrowing without callback guards. Ordinary
`poll(&mut callback, limit)` has the same property. Prefer these receive APIs
when one polling thread owns the callback.
The old `Deref` to callback state allowed a shared reference to overlap a mutable
native callback and has been removed. `get_mut()` remains unsafe.

For a polling loop that uses one local callback, borrow it once before the loop
and pass it to `poll`:

```rust,ignore
let mut callback = handler.lock();
loop {
    subscription.poll(&mut *callback, 64)?;
}
```

The `RefCell` check runs when acquiring `callback`, outside the loop. Each poll
borrows that callback directly. Other handler clones cannot access its state
until the guard is dropped. A callback already owned by the loop can be borrowed
directly, without a `Handler`. For reassembly, use the owned `FragmentAssembler`.

Replacing the local handler's `RefCell` with unchecked `UnsafeCell` access would
make its existing safe API unsound. An alias can hold `borrow()` or `lock()` while
another alias calls `poll_with_handler`, or a callback can invoke an alias recursively.
Both cases need to be rejected before creating another mutable reference.
`UnsafeCell` permits interior mutation; it does not relax exclusive-reference
rules. See the [Rust aliasing rules for UnsafeCell](https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html#aliasing-rules).

For example:

```rust,ignore
let callback = SharedHandler::new_shared(|code: i32, message: &str| {
    eprintln!("Aeron {code}: {message}");
});
context.set_error_handler(Some(callback.clone()))?;
// Register callbacks before Aeron::new(&context).
```

## Migration

Remove `multi-threaded` from dependency features. Replace retained
`Handler::new(...)` callbacks with `SharedHandler::new_shared(...)`; synchronous
polling and assembler callbacks keep `Handler::new(...)`. Replace direct callback
state access with `handler.lock()`; local handlers use a borrow check and shared
handlers acquire a mutex.

Manual conductor calls now require an `unsafe` block and coordination with every
other native user of that client. Prefer a background conductor when sharing a
client with several threads.

Counter `addr()` methods return a raw pointer. Use `addr_atomic()` for safe atomic
loads and updates. `AeronCountersReader::addr_atomic` checks the counter id before
accessing the mapped values buffer.

## Hot-path behaviour

Publication handles cache their stable native pointer. Ownership checks and
cleanup synchronization are outside the send path. Accessing the native pointer
does not clone an owner or select storage at runtime.

| Operation | Rust synchronization on the call path |
| --- | --- |
| `offer`, `offer_raw`, claim/copy/commit | No ownership mutex or reference-count update |
| `poll_fn` | No callback mutex or reference-count update |
| Owned `FragmentAssembler::poll` | No callback mutex, reference-count update or runtime callback borrow check |
| `poll(&mut callback, limit)` | Exclusive Rust borrow; no callback guard |
| `poll_with_handler(Some(&local_handler), limit)` | One checked borrow for the whole poll; no mutex |
| `poll_with_handler(Some(&shared_handler), limit)` | One mutex acquisition for the whole poll |
| Retained `SharedHandler` callback | One mutex acquisition per callback |
| Retained shared handler containing a replacement slot | Three mutex acquisitions and an `Arc` clone per non-empty callback |

Resource dependencies are changed during setup, registration completion and
callback replacement. Their mutex is not used by the send and synchronous poll
paths above. Explicit shared cleanup uses a separate mutex to prevent duplicate
native cleanup. Final destruction has exclusive access and acquires neither mutex.
These are Rust wrapper costs; native Aeron synchronization still applies.

For a latency-sensitive receive loop, keep state on its polling thread and use
`poll_fn` or an owned assembler. A shared callback can wait if another thread
holds its state guard. Keep those guards short, and do not use a replacement slot
unless callback replacement during an active call is required. An uncontended
mutex benchmark cannot establish a latency bound under contention. Replacing it
with a spin lock would still wait for the other thread, and can increase delays
when that thread is descheduled.

The isolated [`callback_overhead` benchmark](../rusteron-client/benches/callback_overhead.rs)
compares identical callback work using owned, local, shared and replaceable
storage. Run it with
`cargo bench -p rusteron-client --features static --bench callback_overhead`.
It measures callback dispatch costs without a driver; it does not measure network
latency or per-message percentiles.

Synchronous assembler polls pass the native assembler directly to Aeron while
holding the outer handler borrow. The delegate still checks its own callback borrow.
See [how to benchmark](../BENCHMARKS.md#how-to-benchmark) when measuring these paths.
