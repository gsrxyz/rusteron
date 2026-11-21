# Media driver extension examples

Each executable installs one feature. Its `configure` function contains the registration.
Copy that function and the implementation into your driver setup.

```sh
cargo run -p rusteron-media-driver --features static --example idle_strategy
```

Replace `idle_strategy` with any example name below. Connect clients to the directory
printed by the executable. Use `aeron:udp?endpoint=127.0.0.1:40123`, or
`aeron:udp?endpoint=example.local:40123` for the resolver example. Press Enter to stop.
The driver stops and joins its agents before the process exits.

| Feature | Example name | Behavior |
|---|---|---|
| [Flow control](flow_control.rs) | `flow_control` | Count status messages and delegate sender limits to MaxFlowControl. |
| [Congestion control](congestion_control.rs) | `congestion_control` | Set a fixed receiver window capped at half the term. |
| [Outgoing interceptor](outgoing_interceptor.rs) | `outgoing_interceptor` | Count outgoing batches and forward them unchanged. |
| [Incoming interceptor](incoming_interceptor.rs) | `incoming_interceptor` | Count incoming packets and forward them unchanged. |
| [Name resolver](name_resolver.rs) | `name_resolver` | Resolve example.local and IP literals without DNS. |
| [Idle strategy](idle_strategy.rs) | `idle_strategy` | Spin for 100 empty cycles, then yield. Reset after work. |
| [Duty-cycle tracker](duty_cycle_tracker.rs) | `duty_cycle_tracker` | Record the longest measured cycle in nanoseconds. |
| [Port manager](port_manager.rs) | `port_manager` | Preserve explicit ports and use kernel allocation for port zero. |
| [Native transport](native_transport.rs) | `native_transport` | Count sends and delegate the native UDP callbacks. |
| [Bounded flow control](bounded_flow_control.rs) | `bounded_flow_control` | Wait 50 milliseconds for a missing tagged receiver before degrading. |
| [Packet loss](packet_loss.rs) | `packet_loss` | Drop every seventh outgoing batch after a fixed seeded offset. |

Each file starts its own embedded driver and closes it after Enter.

The observation examples use atomic counters instead of logging on driver threads.
Their atomic operations belong to the examples. The adapters call concrete Rust types.
`packet_loss` is for tests. Omit `fc=` when using a custom flow-control supplier.
The bounded example uses group tag 555 and a group minimum of one.

Run the example unit tests and native integration tests with:

```sh
cargo test -p rusteron-media-driver --features static --examples
cargo test -p rusteron-media-driver --features static --test driver_extensions
```

The integration tests run each example configuration independently. They also combine
all nine extension points in every threading mode. The packet-loss test checks native
NAK and retransmission counters. The bounded-flow test checks the degradation counter.
