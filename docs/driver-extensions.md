# Driver extensions

Typed Rust extension points for the embedded media driver. Runnable examples are in
[`examples/extensions`](../rusteron-media-driver/examples/extensions/README.md).

## Rules for every extension

- Configure extensions before `AeronDriver::new`. Setters fail once the driver has initialised.
- The context owns each installed value until native context close, after all agents have joined.
- Callbacks are `extern "C"`. A panic aborts the process.
- The adapters add no allocation, lock, atomic read-modify-write or trait-object dispatch per callback.
- Mutable state must be `Send`. State read by more than one agent (`Shared`) must be `Send + Sync`.
- Callback errors are written to Aeron's error buffer through a stack buffer. Startup and
  `do_work` errors carry only the code. Call `capture_errmsg()` straight away if you need the text.

## Driver ownership

- `start(self)` returns `ThreadedDriver`. All agents run on native threads.
- `start_manual(self)` returns `ManualDriver`. Call `do_work(&mut self)` and `idle(&mut self, work)` on the owning thread.
- `ManualDriver` is not `Clone`, `Send` or `Sync`. Dropping either handle stops and joins the agents.
- INVOKER mode needs `start_manual`. `start` returns an error before native startup.

## Callback threads

| Extension | Mutable state on | Shared reads |
|---|---|---|
| Flow control | Sender | Conductor and sender |
| Congestion control | Receiver and conductor, separate state | Conductor and receiver |
| Interceptor | Its data-path agent | None |
| Name resolver | Native resource agent | None |
| Idle strategy | Its agent runner | None |
| Duty-cycle tracker | Its measured agent | None |
| Port manager | Conductor, then the closing thread | None |
| Native transport | Sender, receiver or resolver | Defined by the custom callbacks |

## Flow and congestion control

- Implement `FlowControl` or `CongestionControl` and install with `set_unicast_flow_control::<T>`,
  `set_multicast_flow_control::<T>` or `set_congestion_control::<T>`.
- Return `Position::new(..)`. It saturates negative limits to zero to keep C's signed arithmetic in range.
- `ReceiverWindow` is capped at half the term length.
- Omit `fc=` on channels that use a custom multicast supplier. Aeron gives `fc=` priority.

### Bounded tagged flow control

- `BoundedTaggedFlowControl<WAIT_NS, CAPACITY>` waits up to `WAIT_NS` for the tagged group minimum,
  then drops the minimum to zero until the full group returns. The archive may then miss data.
- Each degradation increments driver counter type 20001.
- `CAPACITY` (default 64) receiver slots are allocated at setup. Unknown receivers are ignored when full.
  A group minimum larger than `CAPACITY` fails setup.

## Interceptors

- Implement `OutgoingInterceptor` or `IncomingInterceptor`. Overriding only `inspect` forwards each packet once.
- Override `send` or `on_datagram` to drop or control forwarding.
- `add_*_interceptor` prepends, so the latest addition runs first.
- Construction uses `Default`, `InterceptorFactory` (fallible) or `InterceptorStateFactory` with
  `add_*_interceptor_with_state` to share application state. Aeron builds one instance per agent.
- Packet borrows and the next callback end when the callback returns. Delayed or reordered delivery is not supported.
- `OutgoingPacket::datagrams()` iterates the batch without allocation.

### Test loss policies

- `SeededLoss<N, SEED>` drops every Nth batch. `GilbertElliottLoss` models bursts, with probabilities in parts per million.
- `DestinationLoss<L>` applies `L` to one destination IP address and port.
- Dropped batches are reported as sent so Aeron's normal NAK recovery retransmits them. For tests only.
- Aeron 1.54.2 passes an uninitialised byte counter on the RTT reply path
  (`aeron_network_publication.c:897`). This affects any interceptor on that path.

## Name resolvers

- `set_name_resolver_fn(|name, uri_param, re_resolve| ..)` for a closure that returns an `IpAddr`.
- `set_name_resolver::<T>()` with `Default` or `NameResolverFactory`, or `set_name_resolver_instance(value)`.
- Initialisation consumes an instance once. Install a new one before retrying after a failed start.
- Set `LOOKUP_CAPACITY` to reserve lookup storage so replacements do not allocate.

## Idle strategies

- `set_idle_strategy(role, value)` accepts an `IdleStrategy` or `FnMut(i32)`. Reset backoff when work is positive.
- Set `BUSY_SPIN = true` if the strategy never sleeps, yields or parks. The native name becomes `rusteron-spin`.

| Threading mode | Active roles |
|---|---|
| Dedicated | `Conductor`, `Sender`, `Receiver`, `NativeResourceAgent` |
| Shared network | `Conductor`, `SharedNetwork`, `NativeResourceAgent` |
| Shared | `Shared` |
| Invoker | `Shared`, called by `ManualDriver::idle` |

## Duty-cycle trackers

- `add_duty_cycle_tracker(role, value)` runs after the existing tracker, so Aeron's stall counters keep working.
- `set_duty_cycle_tracker` replaces the chain and stops those counters.
- `DutyCycleTimer::new(|elapsed_ns| ..)` handles the baseline timestamp.

## Port managers

- `set_port_manager(role, value)` accepts a `PortManager` or a closure `|_: ChannelRef<'_>, addr: SocketAddr| Ok(port)`.
- Return the requested port to keep an explicit port, or zero for a kernel-assigned port.
- Closures have no release callback. Implement `PortManager::free_managed_port` if the policy reserves ports.

## Native transport bindings

- `NativeTransportBindings::udp()` copies Aeron's UDP table. Replace function pointers and pass the
  table to `unsafe from_callbacks(table, keepalive)`.
- `keepalive` drops after native context close. Use it to close a C runtime.
- Install with `set_native_transport_bindings(TransportRole::Data | Conductor, ..)`.
- Interceptor chains still apply. A custom receive function must call the supplied callback.
- When delegating to native UDP pollers, keep `poller.bindings_clientd` unchanged.

## Tests

```sh
cargo test -p rusteron-media-driver --features static --lib extensions
cargo test -p rusteron-media-driver --features static --examples
cargo test -p rusteron-media-driver --features static --test driver_extensions
cargo test -p rusteron-media-driver-links-test --features rusteron-media-driver/static --test transport_runtime
```
