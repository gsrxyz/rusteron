# PRD: `multi-threaded` feature (Rc → Arc, sound `Send + Sync` resources)

**Status:** parked (design only, not implemented). Last updated 2026-07-03.
**Branch target:** `v2-redesign` follow-up.

## Problem

Rusteron's resource handles (`Aeron`, `AeronPublication`, `AeronSubscription`, …) are
backed by `Rc<ManagedCResource>` and `Handler<T>` is `Arc<UnsafeCell<T>>` deliberately
`!Sync`. This is correct for the single-threaded hot-path the library is tuned for, but it
makes the legitimate multi-threaded use cases **unsound**:

- Aeron C documents `aeron_publication_offer` / `aeron_try_claim` / `aeron_publication_position`
  / `aeron_publication_is_connected` as **thread-safe** (`aeron-client/.../Publication.h`).
  Java/.NET users routinely share one publication across threads.
- Rusteron forces those callers into `unsafe impl Send` / raw-pointer borrow handles, which
  re-introduces the use-after-free footguns the Rc redesign removed.

A narrow `into_shared()` escape hatch **cannot be made sound on the current Rc foundation**
(you cannot share a `!Send` interior behind `Arc`). The correct fix is an opt-in `Arc` mode.

## Goal

A `multi-threaded` cargo feature that, when enabled, makes the publication / subscription /
context layer `Send + Sync` for the operations Aeron C guarantees thread-safe, without
regressing the default single-threaded hot path.

## Non-goals

- Making **poll** thread-safe per subscription/image (Aeron C does not guarantee it; one
  subscription polls on one thread).
- Removing the `Rc` default. The fast path stays `Rc` when the feature is off.
- Binary compatibility between crates built with the feature on vs off (the `Send`/`Sync`
  bound changes — documented as a whole-crate feature).

## Design

### 1. `ManagedCResource`: `Rc` ↔ `Arc` behind a type alias

```rust
// common.rs
#[cfg(not(feature = "multi-threaded"))]
pub type RcOrArc<T> = std::rc::Rc<T>;
#[cfg(feature = "multi-threaded")]
pub type RcOrArc<T> = std::sync::Arc<T>;
```

Every `OwnedOnHeap(Rc<ManagedCResource>)` becomes `OwnedOnHeap(RcOrArc<ManagedCResource>)`,
and `dependencies: Vec<Rc<dyn Any>>` becomes `Vec<RcOrArc<dyn Any + Send + Sync>>`.

### 2. Cleanup closure tightens to `Send + Sync`

Today: `UnsafeCell<Option<Box<dyn FnMut(&mut T) -> i32>>>`.

Under the feature the bound becomes `FnMut(&mut T) -> i32 + Send + Sync`, still behind
`UnsafeCell` (the conductor is single-threaded *inside* the client; `Sync` is required only
because the `Arc` makes the *handle* shareable, not the callback re-entrant). Document that
the cleanup closure runs once from a single thread.

### 3. `Handler<T>` becomes `Sync` under the feature

`Arc<UnsafeCell<T>>` with `unsafe impl Sync for Handler<T> where T: Sync` (feature-gated).
Callbacks still fire from the conductor thread — `Sync` only permits *sharing* the handle,
not concurrent invocation.

### 4. Scope: which types go `Arc`

In scope (Aeron C thread-safe): `Aeron`, `AeronContext`, `AeronPublication`,
`AeronExclusivePublication`, `AeronSubscription` (constants/position only — **not** poll),
`AeronCountersReader`, the async-add pollers.

Out of scope (per-resource single-threaded): `AeronImage::poll`, `AeronSubscription::poll`
(on a given subscription), `AeronFragmentAssembler`, the archive control session
(`AeronArchive`'s control channel is request/reply on one thread).

### 5. Generator changes

`rusteron-code-gen` emits `Rc` literally today. Two options:

- **(a) Token-stream alias** — emit `RcOrArc<…>` everywhere and define the alias in
  `common.rs`. Lowest churn, one cfg switch.
- **(b) Generator cfg** — pass the feature through `build.rs` and emit `Arc`/`Rc` directly.

Recommend **(a)**; the alias keeps the generated source feature-agnostic.

## Validation plan

1. **Compile matrix.** `cargo check --all-targets` with and without the feature, on
   client + archive + media-driver.
2. **Send/Sync assertions** (static):
   ```rust
   #[cfg(feature = "multi-threaded")]
   fn _assert_send_sync<T: Send + Sync>() {}
   #[cfg(feature = "multi-threaded")]
   fn _static_checks() {
       _assert_send_sync::<Aeron>();
       _assert_send_sync::<AeronPublication>();
       _assert_send_sync::<AeronSubscription>(); // constants only
   }
   #[cfg(not(feature = "multi-threaded"))]
   fn _assert_not_sync() { /* compile-fail negative */ }
   ```
3. **Threaded offer test.** Spawn N threads each calling `publication.offer(...)` on a
   shared `Arc<AeronPublication>`; assert all messages received in order per stream.
   Validates against the documented thread-safety of `aeron_publication_offer`.
4. **Miri / loom** for the cleanup-once + dependency-drop ordering under `Arc`.
5. **No-regression for the Rc path.** `just bench` (`offer_claim_poll`) must show no
   latency change with the feature off (guarded by a benchmark comparison threshold).
6. **ASan** (`just test-asan`): a deliberate cross-thread offer after close must still
   surface as a clean error, not a UAF.

## Risks

- **Cleanup closure `Send + Sync` bound** ripples to every `ManagedCResource` construction
  site; closures capturing `!Send` state (e.g. the `AgentStartHandler` raw pointer in the
  media-driver test) must be rewritten or excluded.
- **Dependency graph** `Arc<dyn Any + Send + Sync>` requires every dependency type to be
  `Send + Sync` — `Aeron`, `Handler<T>`, etc. This is the main blast radius.
- **Binary incompat** (on vs off) must be a documented release note; downstream crates must
  match the feature across the dependency tree.
- **Handler `Sync`** is sound only if callbacks are never invoked concurrently. The
  conductor is single-threaded, but a misbehaving embedder could poll two subscriptions
  sharing a handler from two threads — document and, where possible, runtime-debug-assert.

## Open questions

1. Should `AeronArchive`'s control session be `Arc`-able (it has its own single-threaded
   control poller), or stay `Rc` even under the feature?
2. Do we ship `into_shared()` as a typed escape hatch (`pub fn into_shared(self) ->
   Arc<SharedPublication>`) as syntactic sugar, or just let users wrap the now-`Sync`
   handle in their own `Arc`?
3. Feature name: `multi-threaded` vs `thread-safe` vs `sync` — `multi-threaded` reads
   clearest at the dependency line.

## Decision

Park until the v2 ergonomics work settles. Implement as a focused branch with the
compile-matrix + threaded-offer test as the acceptance gate. Do **not** ship a raw-pointer
`into_shared()` in the meantime.
