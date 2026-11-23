# Driver extensions

Typed Rust hooks for the embedded media driver. One runnable file per feature is in
[`examples/extensions`](../rusteron-media-driver/examples/extensions/README.md).

## Rules

- Install extensions before `AeronDriver::new`. Setters fail after that.
- Callbacks are `extern "C"`. A panic aborts the process.
- Callbacks add no allocation, lock or dynamic dispatch.
- Mutable state must be `Send`. `Shared` state read by several agents must be `Send + Sync`.
- Context configuration lives until native context close. Instances Aeron builds from it drop
  on their native close callback, which can be during driver shutdown.
- Errors carry only the code. Call `capture_errmsg()` straight away if you need the text.

## Starting the driver

- `start(self)` returns `ThreadedDriver`, with every agent on native threads.
- `start_manual(self)` returns `ManualDriver`. Call `do_work` and `idle` on the starting thread.
- INVOKER mode needs `start_manual`.

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

- Flow control: omit `fc=` on the channel when using a custom multicast strategy, because `fc=` takes priority.
- Interceptors run on the driver side of environment-loaded ones such as ATS, so they see plaintext.
  Override `inspect` to observe, or `send` / `on_datagram` to drop.
- Interceptors cannot delay or reorder packets. The bindings have no timer, and the packet is borrowed.
- Interceptors that drop packets, such as the loss examples, should not run with RTT measurement on
  Aeron 1.54.2. It has an uninitialised byte counter on the RTT reply path, fixed in
  aeron-io/aeron@c0457af122.
- Idle strategies: set `BUSY_SPIN = true` if the strategy never sleeps or yields.
- `add_duty_cycle_tracker` keeps Aeron's stall counters. `set_duty_cycle_tracker` replaces them.
- Native transport: interceptors still apply, and a custom receive function must call the supplied callback.

## Tests

```sh
cargo test -p rusteron-media-driver --features static
cargo test -p rusteron-media-driver-links-test --features rusteron-media-driver/static
cargo bench -p rusteron-media-driver --features static --bench extension_trampolines
```
