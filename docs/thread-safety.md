# Thread safety

Rusteron sets threading rules per native type. There is no `multi-threaded` feature.
`Send` and `Sync` are compile-time traits and add no runtime cost.

`Aeron`, `AeronPublication`, `AeronCounter` and `AeronCountersReader` are `Send + Sync`.
Other generated handles are local. The list is in
[`thread_safety.rs`](../rusteron-code-gen/src/thread_safety.rs). New native types stay
local until reviewed.

<a id="native-contracts-and-wrapper-traits"></a>

## Handles and lifetime

- Regular publications accept concurrent publishers from several threads.
- Aeron allows one consumer per subscription or image and one writer per exclusive
  publication. Subscriptions have no `Clone`. Images, exclusive publications, replay
  merges and persistent subscriptions are cloneable and local, so their clones stay
  on one thread.
- `AeronSubscription` is `Send` but not `Sync`. Create it on one thread and move it to
  the thread that polls it. Exclusive publications and images stay local.
- A retained image from `image_at_index` keeps the subscription's native object
  alive. Do not poll it inside a poll of its own subscription, and do not keep
  polling it on this thread after the subscription moves to another one. Both race
  on the image position and can deliver fragments twice.
- Native cleanup runs when the last owning handle drops. `close()` on a shared handle
  releases that handle's reference. Other clones and active claims stay valid.
  Explicit native removal APIs bypass this and are unsafe.
- Configure a client or driver context before passing it to native code. Retained
  clones of a context cannot change it afterwards. If client initialisation fails,
  the context can be changed and used again.
- `AeronDriver` has one owner. Starting it consumes the handle. A `ManualDriver`
  stays on its starting thread and requires `&mut self` for work and idle calls.
  A `ThreadedDriver` has no manual work entry point. Strategy callbacks split
  mutable agent state from concurrent `Shared` reads. See [driver extensions](driver-extensions.md).
- The client background conductor needs no manual calls. With a client agent invoker, call
  `main_do_work` and `main_idle_strategy` from one thread at a time, as in Aeron Java
  and C++. Other threads can still use the client's publication and registration APIs.
- Getters that return `Ref` borrow their parent. A `Ref` has no `Clone`. Call
  `as_ref()` for another short-lived view. An archive's `aeron_owned()` or
  `retain_archive_context()` keeps the archive alive when a child must outlive the
  borrow. Wrapping a raw pointer needs `unsafe from_ptr` and a lifetime guarantee from
  you. See [owning and borrowed handles](migrating_0.2_to_0.3.md#owning-and-borrowed-handles).
- Subscriptions, exclusive publications and fragment assemblers have no safe zeroed or
  raw-pointer constructors. `FragmentAssembler` and `ControlledFragmentAssembler` are
  not cloneable and poll through `&mut self`, so one owner uses each assembler.

<a id="nested-polls"></a>

## Nested polls

Rusteron does not check for nested polls, as Aeron Java and C++ do not. Polls and
supplier offers take `&self`.

Do not poll a subscription, image, replay merge or persistent subscription from its own
fragment callback, and do not offer on an exclusive publication from its own reserved
value supplier. Aeron C writes the new subscriber position back only after a poll
finishes, and an exclusive publication moves its term offset only after the supplier
returns. A nested call starts from the old position, so the same fragments are
delivered twice or the same bytes are written twice. Archive client calls are safe to
nest, because Aeron C returns an error for a nested Archive call.

<a id="hot-path-behaviour"></a>

## Callbacks

**The poll path uses no `Rc`, `Arc`, `RefCell` or `Mutex`.** `poll(&mut callback, limit)`,
`poll_fn` and the owned assemblers pass Aeron C a plain `&mut` to your callback as
`clientd`. For each fragment, the trampoline casts `clientd` back to `&mut F` and calls
it. Rusteron adds no lock, borrow check, reference count or allocation per fragment or
per poll. `Handler` (`Rc<RefCell<T>>`) and `SharedHandler` (`Arc<Mutex<T>>`) exist only for
callbacks that C keeps after the call returns, such as error, available image,
unavailable image and close handlers. The conductor calls those on events, not per
message.

| API | State storage | Synchronisation per call |
| --- | --- | --- |
| `poll(&mut callback, limit)` | Your local callback | None. Exclusive Rust borrow |
| `poll_fn` and other synchronous `_fn` methods | A stack closure | None |
| `FragmentAssembler::new(callback)` | The assembler owns the callback. Read it between polls with `handler()` and `handler_mut()` | None |
| `AeronFragmentClosureAssembler::new()` | Holds only the partial-message buffers. Each `poll` takes a new closure | None |
| Local `Handler::new` | `Rc<RefCell>`. Not `Send` or `Sync` | One borrow check per `lock()`. Not accepted by any poll method |
| `SharedHandler::new_shared` | Mutex. `Send + Sync` when its state is `Send` | One mutex lock per callback when retained, and one per `lock()`. Used for close, error and image callbacks, not the poll path. To check callback state on every poll, share an atomic with the callback instead of calling `lock()` ([example](client-guide.md#retained-callbacks)) |
| `SharedHandler` holding a replacement slot | Mutex plus the slot's own locks | Three mutex locks and an `Arc` clone per non-empty callback |

None of these allocate per call. `offer`, `offer_raw` and claim, copy and commit take no
ownership mutex and change no reference count.

Notes:

- `SharedHandler` is the storage for callbacks retained by the conductor. Owned
  callback setters create it for you.
- `handler.lock()` gives mutable access to callback state. A local `Handler` returns
  a `RefCell` guard and a `SharedHandler` locks its mutex. `handler.borrow()` gives
  read access on a local handler. Neither allocates or clones an owner.
- Drop the guard before calling an API that invokes the same handler. A local handler
  rejects the overlapping borrow. A shared handler deadlocks.
- A retained shared callback can wait if another thread holds its guard. Keep guards
  short. Do not use a replacement slot unless you must replace a callback during an
  active call.
- For a latency-sensitive receive loop, keep state on the polling thread and use
  `poll_fn` or an owned assembler.
- `Handler` has no `Deref` to callback state. `get_mut()` is unsafe.
- The owned assemblers contain `PhantomData<Rc<()>>`. This is a zero-sized marker
  that makes them `!Send`. No `Rc` exists at run time. The callback is boxed once in
  `new()`, and C keeps that stable pointer for every poll.

To poll with a reusable callback, own it on the polling thread and read its fields
between polls:

```rust,ignore
let mut callback = MyCallback::default();
loop {
    subscription.poll(&mut callback, 64)?;
}
```

For reassembly, use the owned `FragmentAssembler`.

<a id="where-handler-lock-runs"></a>

### Where `Handler::lock` runs

`Handler::lock()` is `RefCell::borrow_mut` for a local `Handler` and `Mutex::lock`
for a `SharedHandler`. Rusteron calls it only from the trampolines of methods that
take a `&Handler`, and no poll method takes one. `Handler` does not implement
`AeronFragmentHandlerCallback`, `AeronControlledFragmentHandlerCallback` or
`AeronBlockHandlerCallback`, so passing one to a poll does not compile. The
trampolines that would lock a `Handler` for those callbacks exist in the generated
code but have no call sites.

| Methods that take a `&Handler` | When the lock runs | On the hot path |
| --- | --- | --- |
| Image handlers of `add_subscription` and `async_add_subscription`, `AeronAvailableCounterPair::new`, `AeronUnavailableCounterPair::new`, `AeronCloseClientPair::new`, context callback setters | Once per conductor event, such as an image becoming available, a counter being added or the client closing | No. Once per event, not per message |
| `foreach_counter`, `error_log_read`, `loss_reporter_read`, `AeronLossReporter::read`, `parse_params` | Once per item during that call | No. Diagnostic and setup calls |
| Archive `list_recording`, `list_recordings`, `list_recordings_for_uri`, `list_recording_subscriptions`, `AeronArchiveRecordingDescriptorPoller::reset` | Once per descriptor | No. Control requests |
| `offer_raw`, `offer_with_reserved_value` and `offerv` with `Some(&handler)` | Once per fragment written, when Aeron C asks for the reserved value | Only if you pass `Some`. `offer(buf)` and `try_claim` pass none. `offerv_fn` takes a plain `&mut` closure |
| `unsafe AeronAgentRunner::aeron_agent_init` | `on_start` once, `idle_strategy_func` once per duty cycle | The idle strategy runs every duty cycle. Rusteron itself never calls `aeron_agent_init` |

Your own code can also call `lock()` to read or replace retained callback state.
Do that outside the poll loop. To check that state on every poll, share an atomic
with the callback instead.

To register a retained callback:

```rust,ignore
let callback = SharedHandler::new_shared(|code: i32, message: &str| {
    eprintln!("Aeron {code}: {message}");
});
context.set_error_handler(Some(callback.clone()))?;
// Register callbacks before Aeron::new(&context)
```

## Migration from 0.2

- Remove `multi-threaded` from dependency features.
- Replace retained `Handler::new(...)` callbacks with `SharedHandler::new_shared(...)`.
- Replace `Handler::with_fragment_assembler` with `FragmentAssembler::new`. Pass other
  synchronous callbacks as `&mut callback`.
- Read retained callback state with `handler.lock()`, outside the poll loop.
- Call manual conductor methods from one thread at a time. Prefer the background
  conductor when several threads share a client.
- Counter `addr()` returns a raw pointer. Use `addr_atomic()` for safe atomic loads and
  updates. `AeronCountersReader::addr_atomic` checks the counter id first.

## Measuring callback cost

The [`callback_overhead` benchmark](../rusteron-client/benches/callback_overhead.rs)
compares owned, local, shared and replaceable callback storage without a driver:

```sh
cargo bench -p rusteron-client --features static --bench callback_overhead
```

See [how to benchmark](../BENCHMARKS.md#how-to-benchmark) for the other benchmarks.
