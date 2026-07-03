//! # Multi-destination subscription (MDS)
//!
//! Port of Aeron's `basic_mds_subscriber.c`: one subscription in **manual control mode**
//! aggregates several transport destinations — here two UDP endpoints fed by two
//! independent publications. Typical uses: consuming redundant feeds (A/B arbitration)
//! or merging shards onto one polling loop.
//!
//! ```bash
//! cargo run --release --features "static precompile" --example multi_destination_subscription
//! ```

use rusteron_client::*;
use rusteron_media_driver::testing::{find_unused_udp_port, EmbeddedDriver};
use std::collections::HashSet;
use std::thread::sleep;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 1003;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // embedded media driver with RAII teardown (stops + joins on drop)
    let driver = EmbeddedDriver::launch()?;

    let ctx = AeronContext::new()?;
    ctx.set_dir(&driver.dir().into_c_string())?;
    ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    // The MDS subscription: control-mode=manual means destinations are added explicitly.
    // Channels are built with the typed URI builder instead of format!-strings.
    let mds_channel = {
        let builder = AeronUriStringBuilder::new_zeroed_on_heap();
        builder.init_new()?;
        builder.media(Media::Udp)?.control_mode(ControlMode::Manual)?;
        builder.build(256)?
    };
    let subscription = aeron
        .async_add_subscription(
            &mds_channel.into_c_string(),
            STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?
        .poll_blocking(Duration::from_secs(5))?;

    // Two independent feeds on their own endpoints.
    let port_a = find_unused_udp_port(20200).expect("no free port");
    let port_b = find_unused_udp_port(port_a + 1).expect("no free port");
    let destination_a = AeronUriStringBuilder::udp(&format!("127.0.0.1:{port_a}"))?.build(256)?;
    let destination_b = AeronUriStringBuilder::udp(&format!("127.0.0.1:{port_b}"))?.build(256)?;
    subscription.add_destination(&destination_a.clone().into_c_string(), Duration::from_secs(5))?;
    subscription.add_destination(&destination_b.clone().into_c_string(), Duration::from_secs(5))?;
    println!("subscription aggregating {destination_a} + {destination_b}");

    let publication_a = aeron
        .async_add_publication(&destination_a.into_c_string(), STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let publication_b = aeron
        .async_add_publication(&destination_b.into_c_string(), STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;

    let start = Instant::now();
    while (!publication_a.is_connected() || !publication_b.is_connected()) && start.elapsed() < Duration::from_secs(10)
    {
        sleep(Duration::from_millis(10));
    }
    assert!(
        publication_a.is_connected() && publication_b.is_connected(),
        "publications did not connect to the MDS destinations (a={}, b={})",
        publication_a.is_connected(),
        publication_b.is_connected()
    );

    // Publish distinct messages on each feed; both must arrive through the one subscription.
    for (publication, tag) in [(&publication_a, "feed-A"), (&publication_b, "feed-B")] {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match publication.offer(tag.as_bytes()) {
                Ok(_) => break,
                Err(e) if e.is_retryable() && Instant::now() < deadline => sleep(Duration::from_millis(1)),
                Err(e) => return Err(format!("offer on {tag} failed: {e}").into()),
            }
        }
    }

    let mut seen: HashSet<String> = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen.len() < 2 && Instant::now() < deadline {
        subscription.poll_fn(
            |buf, _hdr| {
                let msg = String::from_utf8_lossy(buf).to_string();
                println!("received {msg}");
                seen.insert(msg);
            },
            16,
        )?;
        sleep(Duration::from_millis(1));
    }
    assert!(
        seen.contains("feed-A") && seen.contains("feed-B"),
        "expected both feeds through the MDS subscription, got {seen:?}"
    );
    println!("both destinations delivered through one subscription");

    drop(publication_a);
    drop(publication_b);
    drop(subscription);
    drop(aeron);
    Ok(())
}
