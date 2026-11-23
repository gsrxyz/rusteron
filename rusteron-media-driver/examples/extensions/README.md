# Media driver extension examples

Each file installs one extension in its `configure` function and starts its own driver.

```sh
cargo run -p rusteron-media-driver --features static --example idle_strategy
```

Connect clients with `aeron:udp?endpoint=127.0.0.1:40123` (`example.local:40123` for
`name_resolver`). Press Enter to stop.

| Feature | Example name | Behaviour |
|---|---|---|
| [Flow control](flow_control.rs) | `flow_control` | Count status messages and advance to the furthest receiver. |
| [Congestion control](congestion_control.rs) | `congestion_control` | Set a fixed receiver window capped at half the term. |
| [Outgoing interceptor](outgoing_interceptor.rs) | `outgoing_interceptor` | Count outgoing batches and forward them unchanged. |
| [Incoming interceptor](incoming_interceptor.rs) | `incoming_interceptor` | Count incoming packets and forward them unchanged. |
| [Name resolver](name_resolver.rs) | `name_resolver` | Resolve example.local and IP literals without DNS. |
| [Idle strategy](idle_strategy.rs) | `idle_strategy` | Spin for 100 empty cycles, then yield. Reset after work. |
| [Duty-cycle tracker](duty_cycle_tracker.rs) | `duty_cycle_tracker` | Record the longest measured cycle in nanoseconds. |
| [Port manager](port_manager.rs) | `port_manager` | Preserve explicit ports and use kernel allocation for port zero. |
| [Native transport](native_transport.rs) | `native_transport` | Count sends and delegate the native UDP callbacks. |
| [Owned native runtime](native_transport_owned.rs) | `native_transport_owned` | Close a native runtime after driver join and context close. |
| [Bounded flow control](bounded_flow_control.rs) | `bounded_flow_control` | Wait 50 milliseconds for a missing tagged receiver before degrading. |
| [Packet loss](packet_loss.rs) | `packet_loss` | Drop every seventh outgoing batch after a fixed seeded offset. |
| [Burst loss on one path](burst_loss.rs) | `burst_loss` | Apply seeded Gilbert-Elliott loss to one destination IP address and port. |

