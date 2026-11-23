# Driver extensions

Configure typed extensions before creating the driver. Context clones reject
changes once driver initialization succeeds. Suppliers run at initialization or
resource creation. Each supplier owns the state it returns. A failed supplier
publishes no pointer and drops its partial Rust state.

## Closures and constructors

Idle strategies accept `FnMut(i32)`. A closure can keep its backoff count in a
captured variable. Positive work should reset that count. The closure moves into
one agent's state and does not need `Sync`.

`DutyCycleTimer::new(record)` accepts `FnMut(i64)`. The timer calls `record` with
elapsed nanoseconds. Its first timestamp sets a baseline. `update` resets the
baseline without reporting a duration. Measurements update the baseline after
each call. Backward timestamps report zero. Differences saturate at `i64::MAX`.
Use the full `DutyCycleTracker` trait when a policy needs both timestamp events.

`set_name_resolver_fn` accepts a closure with the name, URI parameter, and
re-resolution flag. It returns an `IpAddr` or an `AeronCError`. The closure can
own a routing table or other runtime configuration. `set_name_resolver_instance`
accepts an existing `NameResolver` for policies with lookup or lifecycle methods.
The native resolver takes ownership during initialization and drops the value
when it closes. Initialization consumes an instance once. If initialization
fails after that transfer, install a new instance before retrying.

Port managers accept a closure with `ChannelRef` and `SocketAddr` arguments.
Annotate these two argument types when passing a closure to `set_port_manager`.
The closure returns a port or an error. Its release callback does nothing.
Implement `PortManager` on a named type when the policy reserves ports and must
release them after socket close.

An interceptor can derive `Default` and implement only `inspect`. The default
packet callback runs `inspect`, then forwards once with the original arguments.
Outgoing callbacks return the next interceptor's result, including errors.
Override `send` or `on_datagram` for deliberate drops or other forwarding rules.
An override controls whether `inspect` runs.
Implement `InterceptorFactory` instead of `Default` for fallible construction
that needs `StrategyContext` or `TransportAffinity`. Aeron constructs a separate
instance for each agent that uses the interceptor. Its native constructor has
no user-data argument, so interceptors use a type-selected factory.

Implement `InterceptorStateFactory` when instances must report to application
state, such as shared counters or a queue read by another thread. Pass that
state to `add_outgoing_interceptor_with_state` or
`add_incoming_interceptor_with_state`. The context keeps it in Aeron's named
per-interceptor slot until native close, and each agent's constructor borrows
it. Separate drivers in one process keep separate state. Each interceptor type
and direction has one state per context. Installing the type again replaces it.

Resolvers can derive `Default` and use `set_name_resolver::<T>()` without a
constructor method. Implement `NameResolverFactory` instead of `Default` when
construction needs `set_name_resolver_init_args` or can fail. The packet and
resolver traits contain callback behavior. The factory traits contain setup.

These adapters keep concrete Rust types in their state. Closure calls add no
trait-object dispatch, allocation, or lock. An owned resolver uses the native
context's named state entries during initialization. Resolution callbacks use
the resolver's state pointer directly. Flow control and congestion control keep
their separate agent state because Aeron calls their methods concurrently.

## Latency choices

Use `inspect` for observation. Combine related measurements in one interceptor
when they have the same lifetime and order. Each separate interceptor adds a
native indirect call. Helper functions inside one implementation can inline.

Keep counters and backoff state in the instance when only its agent needs them.
`Send` allows that state to move to the agent. It does not require a mutex or
atomic access on each callback. The observation examples use shared atomics to
expose their counts. Those operations have a cost. Publish local measurements
at a chosen interval, or during shutdown when live measurements are unnecessary.

Keep flow-control and congestion-control state separate for each agent. Only
measurements read by another agent belong in `Shared`. If profiling finds cache
line contention between mutable receiver and conductor state, give those state
types the alignment required by the target CPU. Padding increases storage per
image, so it is not the default for policies with no mutable receiver state.

Initialize tables and reserve policy storage before packet processing. The
bounded flow-control policy reserves its receiver slots during setup. Resolver
lookup copies retain their capacity. Reserve `LOOKUP_CAPACITY` for known maximum
replacement lengths. A larger replacement still allocates.
Idle callbacks, tracker callbacks, packet views, and packet delegation have no
adapter allocation. Custom callbacks control their own allocation and blocking.
Port managers run during socket setup and close. Their address conversion does
not run for each packet. Unchanged native transport callbacks stay native.

## Driver ownership

`AeronDriver` has one owner. `start(self)` returns `ThreadedDriver` and runs all
agents on native threads. `start_manual(self)` returns `ManualDriver` and runs
the conductor on the caller. `ManualDriver` has no `Clone`, `Send`, or `Sync`.
Its `do_work` and `idle` methods require `&mut self`. A started handle has no
second start method. Dropping either started handle stops and joins the agents.

A context selects its threading mode at runtime. `start` rejects INVOKER mode
before calling C. `start_manual` accepts INVOKER mode and drives all agents in
one duty cycle. Dedicated and shared-network modes still start the other agents
on native threads when the conductor is manual.

## Error messages

`ManualDriver::do_work`, driver startup, and extension configuration return native
errors without capturing message text. `AeronCError::from_code` does not allocate
unless the diagnostic `backtrace` feature is enabled. Call
`error.capture_errmsg()` to allocate an owned message when needed. Capture before
another native call can overwrite this thread's error buffer. Failed startup
consumes and closes the driver, so cleanup can change that buffer before return.
Only the returned numeric code is retained by default.

Callback errors pass their message to Aeron's native error buffer through a
bounded stack buffer. The adapter escapes interior NUL bytes and treats percent
signs as text. It adds no Rust heap allocation. Aeron allocates its native error
state on the first error on each thread. Initialize and warm up the driver before
measuring steady-state latency. Applications control allocation inside their
own callbacks.

## Callback ownership

| Callback state | Mutable caller | Concurrent reads |
|---|---|---|
| Flow-control strategy | Sender | `Shared` on conductor and sender |
| Congestion receiver | Receiver | `Shared` on conductor and receiver |
| Congestion conductor | Conductor | `Shared` on conductor and receiver |
| Interceptor instance | Its data-path agent | None |
| Resolver | Native resource agent | None |
| Idle strategy | Selected agent runner | None |
| Duty-cycle tracker | Selected measured agent | None |
| Port manager | Conductor, then closing thread during shutdown | None |
| Native transport bindings | Sender, receiver, or resolver | Custom callbacks define their state contract |

The adapters project raw pointers to each field before creating references.
They never borrow the whole strategy allocation while another agent owns a
mutable field. Mutable state requires `Send`. Shared state requires `Send + Sync`.
Native shutdown joins the agents before Rust drops their state on the closing
thread. Factory borrows expire at the end of `new`. Copy anything needed later.

All extension callbacks use `extern "C"`. A panic that escapes a callback aborts.
This includes supplier setup and finalizers called by C. Context-retained values
drop in Rust after native context close. The adapters have no `catch_unwind` or
shared-handler lock. The existing retained termination, start, and channel-loss
hooks keep their existing panic policy.

## Numeric and packet contracts

`Position::new` saturates negative limits to zero. Aeron subtracts a nonnegative
sender position from the returned limit with signed arithmetic. `ReceiverWindow`
is positive. The congestion adapter caps it at half the term length. The flow
adapter passes the context's retransmission window multiple to the default helper
and caps retransmission length at the requested length and term remainder.
A direct unsafe implementation needs the same bounds to satisfy C's contract.

Status, setup, and error views read packed integers by value with unaligned
loads. Their memory length comes from the callback, never the wire frame length.
A group tag requires both a declared 44-byte status frame and 44 available bytes.
Socket address conversion is optional. IPv4 and IPv6 fields are copied only when
`to_socket_addr` is called.

Interceptor packets expose read-only payload borrows. C can provide aliased
outgoing buffers, so the API does not manufacture mutable byte slices or mutable
`IoSlice` values. The next callback is one-use and cannot outlive its call.
`OutgoingPacket::datagrams` borrows each datagram in send order, including empty
datagrams. The iterator has no allocation or payload copy. It supports reverse
iteration and reports the remaining count. Interceptors can inspect, forward,
or drop a packet. This API does not retain native packets for delayed delivery.

Resolver lookup results are copied into adapter-owned storage. A later mutable
resolver callback cannot free the text C still uses. The adapter reuses that
storage across lookups and literal fallbacks. Set `NameResolver::LOOKUP_CAPACITY`
to reserve bytes during initialization, including the trailing NUL. Replacements
within that capacity allocate nothing, including the first lookup. Larger
replacements grow the buffer without truncation. The default capacity is zero.
Resolution returns an `IpAddr`. Aeron retains control of the port.

## Bounded archive waits

`BoundedTaggedFlowControl<WAIT_NS, CAPACITY>` uses the context's tag, group minimum,
receiver timeout, and multicast retransmission multiple. A full group clears the
wait deadline. If too few receivers remain through the deadline, the policy sets
the minimum to zero. With no tagged receivers, status messages from healthy
subscribers can advance the sender limit. Partial groups retain their minimum
receiver limit. A full group restores the configured minimum.

The policy allocates `CAPACITY` receiver slots at setup. It ignores an unknown
receiver when all slots are occupied. Select capacity for the maximum group size.
A group minimum larger than capacity fails setup. Expired or EOS receivers free
slots on idle. A zero minimum keeps zero-minimum tagged behavior without a
degradation transition.

Each receiver occupies 32 bytes. Both idle and status callbacks calculate the
minimum limit in the receiver scan. The idle callback removes expired receivers
in reverse order. The conductor's flag has 64-byte alignment and storage. It does
not share a cache line with sender state on CPUs with 64-byte cache lines. The
sender writes the flag only when its value changes. An empty degraded group
returns its limit before receiver maintenance.

Each degradation increments driver counter type 20001. Its label includes
"bounded tagged flow control degradations" and the publication registration ID.
Native counter observers can detect recording gaps. There is no counter work in
strategies whose `COUNTER_LABEL` is `None`.
Only the sender writes the counter. Increment uses an atomic load and release
store, as Aeron's single-writer counters do. It needs no atomic read-modify-write.

This policy does not parse `fc=`. Omit that option and use context settings.
`fc=` overrides the custom multicast supplier in the vendored Aeron loader.

## Tests

```sh
cargo test -p rusteron-media-driver --features static --lib extensions
cargo test -p rusteron-media-driver --features static --examples
cargo test -p rusteron-media-driver --features static --test driver_extensions
cargo test -p rusteron-media-driver-links-test --features rusteron-media-driver/static --test transport_runtime
```

The last command compiles a C transport fixture against the exported Aeron
headers. It checks runtime ownership through agent join and native context close,
and combines the custom C table with incoming and outgoing interceptors and
custom flow control in both installation orders. The fixture checks Rust's
busy-spin marker from C. The main integration target also checks reliable
recovery under destination-specific burst loss and native duty-cycle counters.

The extension ThreadSanitizer CI job rebuilds Rust's standard library and the
vendored C with instrumentation. The unit tests also exercise the receiver and
conductor halves concurrently. A child-process test checks SIGABRT for a panic.
Context setup runs serially in this job because the native transport loader writes a process-global
`source_symbol` on each context initialization. Parallel context initialization
produces native TSan reports at
`aeron_udp_channel_transport_bindings.c:107`. The explicit split-state test
still runs its receiver and conductor callbacks concurrently.

The TSan job runs on ARM because Aeron's ARM implementation uses compiler
atomics. The x86 implementation uses volatile accesses and inline assembly.
TSan reports native ring-buffer races on x86 because it cannot observe that
synchronization.

## Idle strategies

Implement `IdleStrategy::idle` or pass a closure to `set_idle_strategy(role, value)`.
Positive work should reset backoff. Each installation owns a separate instance. Inactive
roles remain allocated but receive no callbacks.

| Threading mode | Active main runner roles |
|---|---|
| Dedicated | `Conductor`, `Sender`, `Receiver` |
| Shared network | `Conductor`, `SharedNetwork` |
| Shared | `Shared` |
| Invoker | `Shared`, called by `ManualDriver::idle` |

Dedicated and shared-network modes also use the `NativeResourceAgent` role.
Shared and invoker modes run that agent in the shared duty cycle. Native
idle-strategy name setters still replace the callback for their role.

Set `IdleStrategy::BUSY_SPIN` to `true` when the strategy never sleeps, yields,
or parks. The default is `false`, including closure implementations. Native name
getters return `"rusteron-spin"` for a declared busy-spin strategy and `"rusteron"`
otherwise. C transports can inspect the selected runner's context name during
initialization. Shared threading modes need the shared runner's marker, rather
than the inactive sender and receiver names. This is an implementation promise,
not a check of the callback's behavior.

```rust
use rusteron_media_driver::extensions::IdleStrategy;
struct Spin;
impl IdleStrategy for Spin {
    const BUSY_SPIN: bool = true;
    fn idle(&mut self, work: i32) {
        if work <= 0 {
            std::hint::spin_loop();
        }
    }
}
```

The marker changes initialization metadata only. The callback has no marker
read or branch, and the native-state storage optimization still applies.

Aeron frees each idle state's allocation with its allocator. Values without
destructors live directly in that allocation when their alignment is no greater
than pointer alignment. Their callbacks need no pointer indirection. Other
values stay in Rust-owned storage retained by the context. A native pointer
slot refers to that storage. The storage choice is constant for each type and
adds no runtime branch. This preserves destructor calls and stronger alignment.
There is no symbol lookup or callback lock. Native name setters can replace
either form before the driver starts.

## Duty-cycle trackers

Pass a `DutyCycleTimer` or implement `DutyCycleTracker` and call
`add_duty_cycle_tracker(role, value)` to preserve the existing tracker.
`update` records a baseline timestamp. `measure_and_update` measures elapsed
nanoseconds and records the next baseline. The timestamps use Aeron's monotonic
clock. A tracker needs one baseline per installation.

`Conductor`, `Sender`, and `Receiver` measure their duty cycles. `NameResolver`
measures lookup and resolve calls on the native resource agent. Shared threading
modes still invoke the three main trackers. `add_duty_cycle_tracker` calls the
existing tracker before the new observer. If the existing tracker is Aeron's
default, it continues updating the native maximum and stall counters. Multiple
additions run in installation order. `set_duty_cycle_tracker` replaces the
entire chain and stops the default counter updates for that role.

```rust,no_run
use rusteron_media_driver::{AeronDriverContext, extensions::*};
let context = AeronDriverContext::new()?;
context.add_duty_cycle_tracker(
    DutyCycleRole::Sender,
    DutyCycleTimer::new(|elapsed_ns| {
        // Record elapsed_ns in application-owned storage here.
        std::hint::black_box(elapsed_ns);
    }),
)?;
# Ok::<(), rusteron_media_driver::AeronCError>(())
```

The native tracker receives every update and measurement before the observer.
It updates its maximum duration and threshold-exceeded counter first. Each
addition allocates during setup and adds one indirect call per event to the
previous tracker. The adapter adds no allocation, lock, atomic read-modify-write,
or trait-object dispatch per event. Keeping the default counter writes is an
explicit cost of `add_duty_cycle_tracker`. Use `set_duty_cycle_tracker` to replace
those writes with your own measurements.

## Port managers

Implement `PortManager` and install separate `Sender` and `Receiver` instances
with `set_port_manager`. `get_managed_port` receives a borrowed channel and a
copied `SocketAddr`. It returns a port or an error. The adapter preserves the
address family, IP address, IPv6 scope, and flow information. Return the requested
port to preserve an explicit channel port. Return zero for kernel allocation.

`free_managed_port` releases a successful reservation after the transport closes.
It also runs after a failed socket setup. Aeron passes the managed bind address,
so a kernel-selected port can still appear as zero in this callback. A custom
manager replaces the native wildcard range policy. A range allocator must track
its own reservations and handle explicit ports used by several endpoints.

The context retains Rust-owned idle state, trackers, port managers, and transport
tables until native context close, including registrations replaced during
configuration. Agent shutdown finishes before these Rust values drop. Idle
values stored directly in native state have no destructor. Native replacement
can free that state during configuration.

## Native transport bindings

`NativeTransportBindings::udp()` creates an owned table for Aeron's native UDP
implementation. `callbacks()` copies that table. Replace selected function
pointers and pass the result to `unsafe NativeTransportBindings::from_callbacks`.
Its second argument retains callback resources until context close. Custom
callbacks must satisfy the native socket, poller, buffer, and concurrency
contracts. The constructor checks that every required function pointer is present.

A C library can return a complete static table and a separate runtime handle.
Copy the table into `from_callbacks(table, keepalive)`. Make `keepalive` a
non-Clone Rust guard whose `Drop` calls the library's runtime-close function.
Retain a dynamic library handle in that guard too if it owns the callback code.
The [owned transport example](../rusteron-media-driver/examples/extensions/native_transport_owned.rs)
shows the guard and installation. Its executable uses Aeron's C allocator as
the demonstration open/close pair. The `links-test` integration test uses a real
C runtime and a complete static C table.

The context closes its native resources before dropping the guard. Driver close
stops and joins the agents first. A retained context clone extends the guard's
lifetime after driver shutdown. Replaced transport tables also stay retained
until context close. A guard passed to an invalid table or a rejected setter
drops during that failed call because the context never retained it.

Install the result with `set_native_transport_bindings`. `TransportRole::Data`
selects the sender and receiver table. `TransportRole::Conductor` selects the
native driver name resolver's table. That role runs only with the driver name
resolver. It is separate from a `NameResolver` implementation that returns IP
addresses without UDP discovery.

Unchanged table entries remain direct native function pointers. They incur no
extra Rust dispatch. Use interceptors when the extension only needs to inspect
or drop packets through the safe packet API.

Installing a custom data table preserves the interceptor chains regardless of
setter order. Outgoing calls pass through the most recently added interceptor
first, then the remaining chain, then the custom table's `send_func`. Incoming
polling uses the custom table's receive functions. Those functions must call the
supplied receive callback, which passes through the incoming chain before the
endpoint processes the datagram. A custom receive function that bypasses this
callback also bypasses incoming interception. Flow control remains in Aeron's
publication logic and still receives status messages through this path.

When delegating poller operations to native UDP, preserve its
`poller.bindings_clientd` storage. Native UDP uses that pointer for epoll or poll
events and frees it during close. A custom poller cannot overwrite it with a
runtime handle and then delegate unchanged UDP operations.

## Test loss policies

These outgoing interceptors deliberately report dropped batches as sent.
Reliable subscriptions request retransmission through Aeron's normal loss
recovery. One decision applies to the entire outgoing batch, which can contain
several datagrams. The policies also affect control traffic on matching paths.

`SeededLoss<N, SEED>` drops every Nth batch after a seeded offset. `N=0` disables
loss. `GilbertElliottLoss` uses a seeded two-state model for bursts. Pass a
`GilbertElliottConfig` to `add_outgoing_interceptor_with_state`. Every probability
is an integer number of parts per million, from zero to 1,000,000. Larger values
fail during construction. The model starts in the good state, samples that
state's loss probability, then samples a transition for the next batch.

`DestinationLoss<L>` applies an outgoing loss policy only to an exact destination
IP address and port. Other destinations pass unchanged without advancing `L`.
Install it with state `(destination, inner_state)`. For periodic loss, use
`DestinationLoss<SeededLoss<7, 3>>` with `(destination, ())`. For burst loss, use
`DestinationLoss<GilbertElliottLoss>` with `(destination, config)`.
The [burst-loss example](../rusteron-media-driver/examples/extensions/burst_loss.rs)
combines both features.

Each agent owns its model state. Given the same seed and sequence of batches,
that instance makes the same decisions. Thread scheduling, packet batching, and
retransmissions can change a real driver's batch sequence. The seed does not
make an entire concurrent network run identical. The model uses SplitMix64,
with two draws per batch and no heap allocation, lock, atomic operation, or
trait-object dispatch. None of these test policies runs unless installed.

### Delay and reorder investigation

Aeron C's interceptor table has initialization, synchronous packet callbacks,
notifications, and close callbacks. It has no timer or periodic work callback.
The packet callback borrows payloads, vector arrays, addresses, timestamps, and
native transport or endpoint pointers. Those borrows end when it returns.
The Rust next-callback value is also limited to that call.

A preallocated ring could own payload copies and their addresses. It would
still need an agent-owned drain callback, a clock, an overflow policy, and a
way to keep or invalidate the transport, endpoint, destination, and interceptor
chain when native resources close. A duty-cycle observer supplies timestamps,
but no retained send or receive capability with those lifetimes. Extending it
to invoke old next-callback pointers would require unsafe lifetime assumptions.

A complete custom transport can implement its own queue and release packets
from its poller. That is a transport implementation, with its own ownership and
shutdown rules. The current packet interceptor API has no clean delayed-send
or reorder operation, so this change adds neither.

## Examples for every extension

Each file in the [extension examples](../rusteron-media-driver/examples/extensions/README.md)
installs one feature. Run an example with:

```sh
cargo run -p rusteron-media-driver --features static --example idle_strategy
```

| Feature | Example name | Behavior |
|---|---|---|
| [Flow control](../rusteron-media-driver/examples/extensions/flow_control.rs) | `flow_control` | Count status messages and delegate sender limits to MaxFlowControl. |
| [Congestion control](../rusteron-media-driver/examples/extensions/congestion_control.rs) | `congestion_control` | Set a fixed receiver window capped at half the term. |
| [Outgoing interceptor](../rusteron-media-driver/examples/extensions/outgoing_interceptor.rs) | `outgoing_interceptor` | Count outgoing batches and forward them unchanged. |
| [Incoming interceptor](../rusteron-media-driver/examples/extensions/incoming_interceptor.rs) | `incoming_interceptor` | Count incoming packets and forward them unchanged. |
| [Name resolver](../rusteron-media-driver/examples/extensions/name_resolver.rs) | `name_resolver` | Resolve example.local and IP literals without DNS. |
| [Idle strategy](../rusteron-media-driver/examples/extensions/idle_strategy.rs) | `idle_strategy` | Spin for 100 empty cycles, then yield. Reset after work. |
| [Duty-cycle tracker](../rusteron-media-driver/examples/extensions/duty_cycle_tracker.rs) | `duty_cycle_tracker` | Record the longest measured cycle in nanoseconds. |
| [Port manager](../rusteron-media-driver/examples/extensions/port_manager.rs) | `port_manager` | Preserve explicit ports and use kernel allocation for port zero. |
| [Native transport](../rusteron-media-driver/examples/extensions/native_transport.rs) | `native_transport` | Count sends and delegate the native UDP callbacks. |
| [Bounded flow control](../rusteron-media-driver/examples/extensions/bounded_flow_control.rs) | `bounded_flow_control` | Wait 50 milliseconds for a missing tagged receiver before degrading. |
| [Packet loss](../rusteron-media-driver/examples/extensions/packet_loss.rs) | `packet_loss` | Drop every seventh outgoing batch after a fixed seeded offset. |

Copy the implementation and its `configure` function. Each file starts and stops
its own embedded driver. Configuration stays in each file.
Atomic counters in observation examples show where application measurements belong.
Those counters add atomic operations to the examples, not to the adapters.

Unit tests check callback arguments and results, exact packet bytes and byte totals,
seeded loss patterns, sender-limit arithmetic, timer durations, address conversion,
replacement lifetimes, and failure behavior. Native integration tests run each
example independently and combine the nine extension points in all four threading
modes. They verify ordered UDP delivery, packet-loss recovery and retransmissions,
bounded-flow degradation, native transport cleanup, and port release after failed
socket setup. The tests use client bindings included in the media driver package.

Examples and tests stay in the media driver package. Client and Archive applications
need no matching extension configuration.
