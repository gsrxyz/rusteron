# rusteron-media-driver

`rusteron-media-driver` provides a fast, idiomatic Rust API over the Aeron C Media
Driver, which manages low-latency messaging between producers and consumers. Run
it as a standalone process or embed it in a Rust application.

> For production deployments, we recommend the Aeron Java or C media driver.
> The embedded version here is best suited to integration tests and lightweight environments.

## Sponsored by GSR

Rusteron is sponsored and maintained by [GSR](https://www.gsr.io), a global leader in algorithmic trading and market making in digital assets.
Rusteron has been running in production in GSR's low-latency trading system since the beginning of 2025. It is mature and battle tested.
It is developed under the GSR GitHub organisation. We welcome contributions, feedback and discussion. To integrate or contribute, open an issue or reach out directly.

[Installation](#installation) · [Examples](#usage-examples) ·
[UDP tuning](#udp-polling-and-send-batches) ·
[Migration to 0.3](https://github.com/gsrxyz/rusteron/blob/main/docs/migrating_0.2_to_0.3.md)

## Installation

For a static build:

```toml
[dependencies]
rusteron-media-driver = { version = "0.3", features = ["static"] }
```

Use default features for dynamic linking. Add `precompile` or `precompile-rustls`
alongside `static` to use prebuilt C libraries on macOS and Linux.

The default source build compiles the Aeron C libraries. Dynamic builds also need
those shared libraries available at runtime; `static` embeds them in the binary.
See [BUILD.md](https://github.com/gsrxyz/rusteron/blob/main/BUILD.md) for prerequisites.

---

## Usage examples

<details>
<summary>Standalone media driver</summary>

```rust,no_run
// A standalone media driver; `cargo run -p rusteron-media-driver --bin media_driver` ships the same.
use rusteron_media_driver::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = AeronDriverContext::new()?;
    context.set_dir(c"target/aeron")?;
    // Native enum setters take the Rust names directly
    context.set_threading_mode(AeronThreadingMode::Shared)?;
    let driver = AeronDriver::new(&context)?;
    let mut driver = driver.start_manual()?;
    // SAFETY: printing copies the text before any setter can change it
    println!("media driver running in {}", unsafe { context.get_dir() });
    loop {
        let work = driver.do_work()?;
        driver.idle(work);
    }
}
```

</details>

<details>
<summary>Embedded media driver</summary>

```rust
// Embeds the media driver in this process: unique directory, stopped and joined on drop.
use rusteron_media_driver::testing::EmbeddedDriver;
use rusteron_media_driver::Aeron;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = EmbeddedDriver::launch()?; // or EmbeddedDriver::launch_with(|ctx| { /* tune ctx */ Ok(()) })
    // Applications usually connect with rusteron_client::Aeron::connect_dir(driver.dir()).
    let aeron = Aeron::connect_dir(driver.dir())?;
    // add publications / subscriptions on `aeron` ...
    drop(aeron); // close the client before the driver stops
    Ok(())
}
```

For a caller-built `AeronDriverContext`, use
`AeronDriver::launch_embedded_guard(ctx, false)` to stop and join the driver on drop.

</details>

---

## UDP polling and send batches

Aeron 1.54 lets the C driver configure the threshold for iterating over sockets directly
instead of using the platform poller. It also limits how many times each ready socket is
polled in one duty cycle. Configure these before creating the driver.

```rust
use rusteron_media_driver::{AeronCError, AeronDriverContext};

let context = AeronDriverContext::new()?;
context.set_udp_channel_transport_poller_iteration_threshold(5)?;
context.set_udp_channel_transport_poller_socket_poll_limit(4)?;
context.set_sender_io_vector_capacity(64)?;
context.set_network_publication_max_messages_per_send(4)?;
# Ok::<(), AeronCError>(())
```

The socket poll limit defaults to 4. A send batch must fit within
`sender_io_vector_capacity`. Driver initialisation rejects a larger batch. The
maximum send batch is 64 messages.

---

## Building C against Aeron's headers

Some driver extensions are C compiled against Aeron's own headers. The main one is a custom UDP
transport, such as kernel bypass with DPDK or ef_vi. It fills in Aeron's
`aeron_udp_channel_transport_bindings_t` and is installed with
`AeronDriverContext::set_udp_channel_transport_bindings`. That C must see the headers of the
Aeron this crate builds, and they exist only in this crate's vendored Aeron sources. The
crate declares `links = "aeron_driver"`, so Cargo passes their paths to the build script of
every crate that depends on it directly:

| Variable | Directory |
|---|---|
| `DEP_AERON_DRIVER_INCLUDE` | the media driver's headers |
| `DEP_AERON_DRIVER_CLIENT_INCLUDE` | the client headers they include |
| `DEP_AERON_DRIVER_AERON_ROOT` | the Aeron source tree |

```rust,no_run
// build.rs of a crate that depends on rusteron-media-driver
fn main() {
    let dep = |key: &str| std::env::var(format!("DEP_AERON_DRIVER_{key}")).unwrap();
    cc::Build::new()
        .file("transport.c")
        .include(dep("INCLUDE"))
        .include(dep("CLIENT_INCLUDE"))
        .compile("my_transport");
}
```

Cargo allows one package per `links` name in a build, so two semver-incompatible versions
of `rusteron-media-driver` cannot share a dependency graph. The workspace crate
[`links-test`](https://github.com/gsrxyz/rusteron/blob/main/rusteron-media-driver/links-test)
compiles a C file against these paths.

<a id="contributing--license"></a>

## Contributing and license

See the root [README](https://github.com/gsrxyz/rusteron#readme) and [CONTRIBUTING.md](https://github.com/gsrxyz/rusteron/blob/main/CONTRIBUTING.md). Build requirements are in [BUILD.md](https://github.com/gsrxyz/rusteron/blob/main/BUILD.md).
Dual-licensed under MIT or Apache-2.0.

---

## Acknowledgements

Special thanks to:

* [@mimran1980](https://github.com/mimran1980), a core low-latency developer at GSR and the original creator of Rusteron. Your work made this possible.
* [@bspeice](https://github.com/bspeice) for the original [`libaeron-sys`](https://github.com/bspeice/libaeron-sys)
* The [Aeron](https://github.com/real-logic/aeron) community for the open protocol

## Custom extensions

`extensions` has safe traits for flow control, congestion control, UDP
interceptors, name resolution, idle strategies, duty-cycle trackers, and port
managers. Configure each extension before `AeronDriver::new`. Native transport
bindings have an owned table with an unsafe constructor for custom callbacks.
The raw supplier setters require `unsafe`.

The [extension examples](examples/extensions/README.md) each install one feature.
They cover every extension point, bounded flow control, and packet-loss injection.
Each file contains its implementation and `configure` function. Run one with:

```sh
cargo run -p rusteron-media-driver --features static --example idle_strategy
```

Packet observers implement `inspect` on `OutgoingInterceptor` or
`IncomingInterceptor`. The default callback forwards the packet once. Outgoing
packets provide `datagrams()` for borrowed iteration without allocation.
Override `send` or `on_datagram` when the policy must control forwarding.

```rust,no_run
use rusteron_media_driver::{AeronDriverContext, extensions::MaxFlowControl};
let context = AeronDriverContext::new()?;
context.set_unicast_flow_control::<MaxFlowControl>()?;
# Ok::<(), rusteron_media_driver::AeronCError>(())
```

Closures can own callback state for idle strategies, name resolution, and port
selection. `DutyCycleTimer` keeps the timestamp baseline and passes elapsed
nanoseconds to a closure. For example:

```rust,no_run
use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};
use std::{net::{Ipv4Addr, SocketAddr}, sync::{Arc, atomic::{AtomicU64, Ordering}}};

let context = AeronDriverContext::new()?;
let mut empty_cycles = 0u32;
context.set_idle_strategy(IdleStrategyRole::Shared, move |work| {
    if work > 0 {
        empty_cycles = 0;
    } else if empty_cycles < 100 {
        empty_cycles += 1;
        std::hint::spin_loop();
    } else {
        std::thread::yield_now();
    }
})?;

let maximum_ns = Arc::new(AtomicU64::new(0));
let recorded_ns = maximum_ns.clone();
context.add_duty_cycle_tracker(DutyCycleRole::Conductor, DutyCycleTimer::new(move |elapsed_ns| {
    recorded_ns.fetch_max(elapsed_ns as u64, Ordering::Relaxed);
}))?;

let peer_name = c"peer.local".to_owned();
context.set_name_resolver_fn(move |name, _, _| {
    if name == peer_name.as_c_str() {
        Ok(Ipv4Addr::LOCALHOST.into())
    } else {
        Err(AeronCError::with_message(-1, "unknown peer"))
    }
})?;
context.set_port_manager(PortManagerRole::Sender,
    |_: ChannelRef<'_>, address: SocketAddr| Ok(address.port()))?;
# Ok::<(), AeronCError>(())
```

Each installation owns its closure. Install separate values for the active idle
roles in your threading mode. Port-selection closures have no release callback.
Implement `PortManager` on a named type if the policy reserves ports.
`add_duty_cycle_tracker` also runs the current tracker, preserving Aeron's
stall counters when the current tracker is the native default.
`set_duty_cycle_tracker` replaces the current tracker instead.

Incoming and outgoing interceptors can derive `Default` and implement only their
packet method. Implement `InterceptorFactory` instead of `Default` when setup
needs the frozen context or agent affinity. Implement `InterceptorStateFactory`
and pass state to `add_incoming_interceptor_with_state` when instances report to
the application:

```rust,no_run
use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

struct CountIncoming(Arc<AtomicU64>);
impl InterceptorStateFactory for CountIncoming {
    type State = Arc<AtomicU64>;
    fn new(count: &Self::State, _: StrategyContext, _: TransportAffinity) -> Result<Self, AeronCError> {
        Ok(Self(count.clone()))
    }
}
impl IncomingInterceptor for CountIncoming {
    fn inspect(&mut self, _: &IncomingPacket<'_>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
let count = Arc::new(AtomicU64::new(0));
let context = AeronDriverContext::new()?;
context.add_incoming_interceptor_with_state::<CountIncoming>(count.clone())?;
# Ok::<(), AeronCError>(())
```

Each context retains its own state. A resolver can derive `Default` and use
`set_name_resolver::<T>()`. Implement `NameResolverFactory` for fallible
construction from native initialization arguments, or pass a configured value
to `set_name_resolver_instance`.

`FlowControl` keeps sender state separate from `Shared`, which both the conductor
and sender read. `CongestionControl` has separate receiver and conductor state.
Callbacks use monomorphized C function pointers. The adapters add no locks,
reference-count operations, or panic catches. A callback panic aborts the process.
Closure adapters use the same concrete callback dispatch. `DutyCycleTimer` stores
one baseline and the closure. Resolver instance lookup runs only at initialization.

`BoundedTaggedFlowControl<WAIT_NS, CAPACITY>` waits for the context's receiver
group minimum. If the group stays below that minimum for `WAIT_NS`, the policy
uses a zero minimum until the full group returns. Counter type 20001 records each
degradation. This policy is opt-in. It can leave a recording gap when the archive
is absent. The default driver policy still waits as configured.

```rust,no_run
use rusteron_media_driver::{AeronDriverContext, extensions::BoundedTaggedFlowControl};
let context = AeronDriverContext::new()?;
context.set_flow_control_group_tag(555)?;
context.set_flow_control_group_min_size(1)?;
context.set_multicast_flow_control::<BoundedTaggedFlowControl<100_000_000>>()?;
# Ok::<(), rusteron_media_driver::AeronCError>(())
```

Omit `fc=` on channels that use a custom multicast supplier. Aeron gives `fc=`
precedence over that supplier. Runtime settings can come from the context fields
in `StrategyContext`, a copied channel URI, or constants on the strategy type.
There is no process-global configuration registry.

`add_outgoing_interceptor` and `add_incoming_interceptor` prepend to the existing
chain. The latest addition runs first. Outgoing plaintext test interceptors belong
before encryption. Incoming placement must match the existing chain's decoding
order. Packet borrows and delegation expire at the callback return. Forwarding
consumes both the packet and the next callback. `SeededLoss<N, SEED>` drops every
Nth outgoing batch after a fixed offset and reports a successful send so Aeron
can retransmit it. Use it in tests.

The context allocates interceptor bindings with Aeron's allocator. Rust owns the
per-agent boxes. Aeron closes them after the agents join. If an interceptor fails
to initialize, Aeron can leak earlier instances in the chain. If native driver
close fails, the driver and its dependencies stay allocated. These are native
failure-path limits. A successful strategy supplier followed by a publication
setup error still receives its native finalizer.

See [driver extensions](../docs/driver-extensions.md) for contracts and validation.
`environment::inspect_environment` copies startup configuration and Linux CPU,
mount, and hugepage information. It does not change the host or run in the driver
loop.
