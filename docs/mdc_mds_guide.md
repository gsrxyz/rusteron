# Multi-Destination Subscription (MDC / MDS) Guide

Multi-Destination Connections (MDC) allow an Aeron publisher or subscriber to dynamically bind to multiple destination endpoints under a single publication or subscription handle. This is useful for:
- Consuming redundant feeds (A/B feed arbitration).
- Aggregating sharded publisher data into a single subscriber polling loop.

---

## Official Aeron Documentation
For in-depth concepts, architectural details, and protocol mechanics:
- [Aeron Wiki: Multi-Destination Connections](https://github.com/aeron-io/aeron/wiki/Multi-Destination-Connections)
- [Aeron Wiki: C++ Programming Guide (MDC section)](https://github.com/aeron-io/aeron/wiki/Cpp-Programming-Guide#multi-destination-connections)
- [Aeron Cookbook (aeron.io)](https://aeron.io/docs/)

---

## Rust `rusteron-client` Snippets

### 1. Manual MDS Subscriber (Programmatically Adding Destinations)

To programmatically control the destinations bound to a subscription, specify `control-mode=manual` on the channel URI.

```rust
use rusteron_client::*;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = AeronContext::new()?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    // 1. Configure the subscription in manual control mode
    let mds_channel = AeronUriStringBuilder::new_zeroed_on_heap();
    mds_channel.init_new()?;
    mds_channel.media(Media::Udp)?.control_mode(ControlMode::Manual)?;

    let subscription = aeron
        .async_add_subscription(
            &cformat!("{mds_channel}"), 
            1003, 
            Handlers::NONE, 
            Handlers::NONE
        )?
        .poll_blocking(Duration::from_secs(5))?;

    // 2. Add destination endpoints dynamically
    let destination_a = AeronUriStringBuilder::udp("127.0.0.1:20201")?.build(256)?;
    let destination_b = AeronUriStringBuilder::udp("127.0.0.1:20202")?.build(256)?;
    
    subscription.add_destination(&cformat!("{destination_a}"), Duration::from_secs(5))?;
    subscription.add_destination(&cformat!("{destination_b}"), Duration::from_secs(5))?;

    // 3. Poll normally — messages from both ports will merge
    loop {
        subscription.poll_fn(
            |buf, header| {
                println!("Received {} bytes on stream {}", buf.len(), header.stream_id());
            },
            16,
        )?;
    }
}
```

### 2. Removing Destinations Programmatically

You can dynamically detach endpoints as network paths change or servers fail:

```rust
// Remove a destination from the subscription
subscription.remove_destination(&cformat!("{destination_a}"), Duration::from_secs(5))?;
```
