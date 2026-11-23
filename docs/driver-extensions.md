# Driver extensions

Typed Rust hooks for the embedded media driver. One runnable file per extension is in
[`examples/extensions`](../rusteron-media-driver/examples/extensions/README.md).

## Rules

- Install extensions before `AeronDriver::new`. Setters fail after that.
- A panic in a callback aborts the process.
- Callbacks add no allocation, lock or dynamic dispatch.
- Mutable state must be `Send`. `Shared` state must be `Send + Sync`.
- State drops when Aeron closes the instance, which can be during driver shutdown.
- Errors carry only the code. Call `capture_errmsg()` straight away for the text.
- `start(false)` runs every agent on native threads. `start(true)` leaves the main agent
  on the calling thread through `main_do_work` and `main_idle_strategy`. Other agents still
  use native threads in dedicated and shared-network modes. INVOKER mode requires `start(true)`.

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
- Interceptors see plaintext, run inside environment-loaded ones such as ATS, and cannot delay or reorder packets.
- On Aeron 1.54.2, do not drop packets with RTT measurement enabled (fixed in aeron-io/aeron@c0457af122).
- Set `BUSY_SPIN = true` on an idle strategy that never sleeps or yields.
- `add_duty_cycle_tracker` keeps Aeron's stall counters. `set_duty_cycle_tracker` replaces them.
- A native transport still runs interceptors. A custom receive function must call the supplied callback.

## Tests

```sh
cargo test -p rusteron-media-driver --features static
cargo test -p rusteron-media-driver-links-test --features rusteron-media-driver/static
```

`just test-tsan` runs the ThreadSanitizer checks on Linux. See [thread safety](thread-safety.md#threadsanitizer).
