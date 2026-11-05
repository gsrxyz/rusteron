# Multi-destination subscription (MDC / MDS) guide

Multi-destination connections let one publication send to several endpoints.
Multi-destination subscriptions merge messages from several endpoints into one
subscription. Use them to:
- Consume redundant feeds, such as A/B feeds.
- Poll sharded publishers through one subscription.

---

## Official Aeron documentation
For the protocol and API details, see:
- [Aeron Wiki: Multi-Destination Connections](https://github.com/aeron-io/aeron/wiki/Multi-Destination-Connections)
- [Aeron Wiki: C++ Programming Guide (MDC section)](https://github.com/aeron-io/aeron/wiki/Cpp-Programming-Guide#multi-destination-connections)
- [Aeron Cookbook (aeron.io)](https://aeron.io/docs/)

---

## Rust `rusteron-client` snippets

### 1. Manual MDS subscriber (adding destinations programmatically)

Set `control-mode=manual` on the channel URI to add or remove subscription destinations in code.

```rust,no_run
use rusteron_client::*;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = AeronContext::new()?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    // Create a subscription in manual control mode.
    let subscription = aeron
        .async_add_subscription(c"aeron:udp?control-mode=manual", 1003, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;

    // Add the endpoints to the subscription.
    let destination_a = AeronUriStringBuilder::udp("127.0.0.1:20201")?.build(256)?;
    let destination_b = AeronUriStringBuilder::udp("127.0.0.1:20202")?.build(256)?;
    subscription.add_destination(&cformat!("{destination_a}"), Duration::from_secs(5))?;
    subscription.add_destination(&cformat!("{destination_b}"), Duration::from_secs(5))?;

    // Poll messages from both ports through the same subscription.
    loop {
        subscription.poll_fn(
            |buf, header| println!("Received {} bytes on stream {:?}", buf.len(), header.stream_id()),
            16,
        )?;
    }
}
```

### 2. Removing destinations programmatically

Remove an endpoint when a network path changes or a server fails:

```rust,no_run
# use rusteron_client::*;
# use std::time::Duration;
# fn snippet(subscription: &AeronSubscription, destination_a: &str) -> Result<(), AeronCError> {
// Remove a destination from the subscription
subscription.remove_destination(&cformat!("{destination_a}"), Duration::from_secs(5))?;
# Ok(()) }
```

### 3. MDC publication (dynamic control mode)

A publication with a control endpoint and `control-mode=dynamic` sends to every subscriber that registers with it. Each subscriber names its own endpoint and the publisher's control address:

```rust
use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = EmbeddedDriver::launch()?; // or Aeron::connect(None) for a driver already running
    let aeron = Aeron::connect_dir(driver.dir())?;

    let mdc = AeronUriStringBuilder::udp_control("127.0.0.1:20200", ControlMode::Dynamic)?.build(256)?;
    let publication = aeron.add_publication(&cformat!("{mdc}"), 1004, Duration::from_secs(5))?;

    let subscription = aeron.add_subscription(
        c"aeron:udp?endpoint=127.0.0.1:20201|control=127.0.0.1:20200",
        1004,
        Handlers::NONE,
        Handlers::NONE,
        Duration::from_secs(5),
    )?;
    println!("connected: {} / {}", publication.is_connected(), subscription.is_connected());
    Ok(())
}
```

Run these examples: [`multi_destination_subscription.rs`](../rusteron-client/examples/multi_destination_subscription.rs) (manual MDS) and [`replay_merge.rs`](../rusteron-archive/examples/replay_merge.rs) (a replay merged onto a live MDC stream).
