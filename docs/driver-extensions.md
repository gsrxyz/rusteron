# Driver extensions

Typed Rust hooks for the embedded media driver. One runnable file per extension is in
[`examples/extensions`](../rusteron-media-driver/examples/extensions/README.md).

## Rules

- Install extensions before `AeronDriver::new`. Setters fail after that.
- A panic in a callback aborts the process.
- The adapters add no allocation, lock or dynamic dispatch. Your callback's own cost
  still applies.
- Mutable state must be `Send`. `Shared` state must be `Send + Sync`.
- State drops when Aeron closes the instance, which can be during driver shutdown.
- Errors carry only the code. Call `capture_errmsg()` straight away for the text.
- `start(false)` runs every agent on native threads. `start(true)` leaves the main agent
  on the calling thread through `main_do_work` and `main_idle_strategy`. Other agents still
  use native threads in dedicated and shared-network modes. INVOKER mode requires `start(true)`.
- Call `start` once. After `start(true)`, call `main_do_work` and `main_idle_strategy`
  only from the thread that runs the main loop. After `start(false)`, do not call them.
  Nothing enforces this.

## Extensions

| Extension | Install with | Runs on |
|---|---|---|
| Flow control | `set_unicast_flow_control::<T>`, `set_multicast_flow_control::<T>` | Sender |
| Congestion control | `set_congestion_control::<T>` | Receiver and conductor |
| Interceptor | `add_outgoing_interceptor::<T>`, `add_incoming_interceptor::<T>` | Its data-path agent |
| Name resolver | `set_name_resolver_fn`, `set_name_resolver::<T>` | Native resource agent |
| Idle strategy | `set_idle_strategy(role, ..)` | Its agent |
| Duty-cycle tracker | `add_duty_cycle_tracker(role, ..)` | Its agent |
| Port manager | `set_port_manager(role, ..)` | Conductor |
| Native transport | `unsafe from_callbacks`, then `set_native_transport_bindings` | Sender, receiver or resolver |

## Notes

- Custom multicast flow control needs channels without `fc=`, which takes priority.
- Interceptors see plaintext and run inside environment-loaded ones such as ATS.
- A native transport still runs interceptors. A custom receive function must call the supplied callback.

## Idle strategies

Set `const BUSY_SPIN: bool = true` on an `IdleStrategy` that never sleeps, yields or
parks. The driver then reports the strategy as `rusteron-spin` instead of `rusteron`.
Closures always report `rusteron`. See the
[idle strategy example](../rusteron-media-driver/examples/extensions/idle_strategy.rs).

## Duty-cycle trackers

`add_duty_cycle_tracker` runs the existing tracker first, so Aeron's max-cycle-time
and stall counters keep updating. Added trackers run in the order you add them.
`set_duty_cycle_tracker` replaces the chain, and the built-in counters stop unless
your tracker updates them. The
[duty-cycle example](../rusteron-media-driver/examples/extensions/duty_cycle_tracker.rs)
uses `set_duty_cycle_tracker`.

## Loss testing

Copy the example interceptors into your own tests:

- `SeededLoss` drops every Nth outgoing batch after a seeded offset.
- `GilbertElliottLoss` applies seeded two-state burst loss.
- `DestinationLoss` limits another loss model to one destination address and port.

They do not allocate per packet. They drop batches, and one batch can hold several
datagrams. See [packet loss](../rusteron-media-driver/examples/extensions/packet_loss.rs)
and [burst loss](../rusteron-media-driver/examples/extensions/burst_loss.rs).

Interceptors borrow the packet for the call only, so they cannot delay or reorder
packets. To do that, write a native transport with its own queue.

## Native transports

`NativeTransportBindings::from_callbacks(table, keepalive)` keeps `keepalive` until
the driver context closes. Make it a guard whose `Drop` shuts down your C runtime.
Stop and join all agents before the context closes. See the
[owned runtime example](../rusteron-media-driver/examples/extensions/native_transport_owned.rs).

Outgoing interceptors run before your send callback. Incoming interceptors run when
your transport calls the supplied receive callback. Flow control still runs in the
driver.

## Tests

```sh
cargo test -p rusteron-media-driver --features static --lib extensions
cargo test -p rusteron-media-driver --features static --examples
cargo test -p rusteron-media-driver --features static --test driver_extensions
cargo test -p rusteron-media-driver-links-test --features rusteron-media-driver/static
```

`just test-tsan` runs the ThreadSanitizer checks on Linux. See [thread safety](thread-safety.md#threadsanitizer).
