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
- Subscriptions, exclusive publications and images are neither `Send` nor `Sync`.
  Aeron allows one consumer or one writer per native object, and these types are
  `Clone`, so making them `Send` would let two clones use the same object from
  different threads. Create them on the thread that uses them.
- Native cleanup runs when the last owning handle drops. `close()` on a shared handle
  releases that handle's reference. Other clones and active claims stay valid.
  Explicit native removal APIs bypass this and are unsafe.
- Configure a client or driver context before passing it to native code. Retained
  clones of a context cannot change it afterwards. If client initialisation fails,
  the context can be changed and used again.
- The background conductor needs no manual calls. `main_do_work` and
  `main_idle_strategy` are unsafe. Prevent concurrent or recursive conductor calls,
  including calls made inside native archive operations. An agent-invoker client is
  bound to its start thread for archive setup and blocking helpers. Other threads can
  still use its publication and registration APIs.
- Getters that return `Ref` borrow their parent. A `Ref` has no `Clone`. Call
  `as_ref()` for another short-lived view. An archive's `aeron_owned()` or
  `retain_archive_context()` keeps the archive alive when a child must outlive the
  borrow. Wrapping a raw pointer needs `unsafe from_ptr` and a lifetime guarantee from
  you. See [owning and borrowed handles](migrating_0.2_to_0.3.md#owning-and-borrowed-handles).
- Subscriptions, exclusive publications and fragment assemblers have no safe zeroed or
  raw-pointer constructors. `FragmentAssembler` and `ControlledFragmentAssembler` are
  not cloneable and poll through `&mut self`, so one owner uses each assembler.

<a id="hot-path-behaviour"></a>

## Callbacks

| API | State storage | Synchronisation per call |
| --- | --- | --- |
| `poll(&mut callback, limit)` | Your local callback | None. Exclusive Rust borrow |
| `poll_fn` and other synchronous `_fn` methods | A stack closure | None |
| `FragmentAssembler::new(callback)` | The assembler owns the callback. Read it between polls with `handler()` and `handler_mut()` | None |
| `AeronFragmentClosureAssembler::new()` | Holds only the partial-message buffers. Each `poll` takes a new closure | None |
| `poll(&mut *handler.lock(), limit)` on a `Handler` | Local `Rc<RefCell>`. Not `Send` or `Sync` | One borrow check per poll call, no mutex |
| `SharedHandler::new_shared` | Mutex. `Send + Sync` when its state is `Send` | One mutex lock per callback when retained. Used for close, error and image callbacks, not the poll path |
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

To poll with one local callback, borrow it once before the loop:

```rust,ignore
let mut callback = handler.lock();
loop {
    subscription.poll(&mut *callback, 64)?;
}
```

Other clones of the handler cannot reach its state until the guard drops. For
reassembly, use the owned `FragmentAssembler`.

To register a retained callback:

```rust,ignore
let callback = SharedHandler::new_shared(|code: i32, message: &str| {
    eprintln!("Aeron {code}: {message}");
});
context.set_error_handler(Some(callback.clone()))?;
// Register callbacks before Aeron::new(&context).
```

## Migration from 0.2

- Remove `multi-threaded` from dependency features.
- Replace retained `Handler::new(...)` callbacks with `SharedHandler::new_shared(...)`.
- Replace `Handler::with_fragment_assembler` with `FragmentAssembler::new`. Pass other
  synchronous callbacks as `&mut callback`.
- Replace direct callback state access with `handler.lock()`.
- Manual conductor calls need an `unsafe` block and coordination with every other
  native user of that client. Prefer the background conductor when several threads
  share a client.
- Counter `addr()` returns a raw pointer. Use `addr_atomic()` for safe atomic loads and
  updates. `AeronCountersReader::addr_atomic` checks the counter id first.

## Measuring callback cost

The [`callback_overhead` benchmark](../rusteron-client/benches/callback_overhead.rs)
compares owned, local, shared and replaceable callback storage without a driver:

```sh
cargo bench -p rusteron-client --features static --bench callback_overhead
```

See [how to benchmark](../BENCHMARKS.md#how-to-benchmark) for the other benchmarks.
