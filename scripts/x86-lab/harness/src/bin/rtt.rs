//! cping/cpong RTT through an external driver (`AERON_DIR`): ping on the main thread and
//! pong on a second, each with its own client and pinned to its CPU once connected, so the
//! clients' conductor threads stay on the CPUs the process was started on.
//!
//! rtt <ipc|udp> <messages> <warmup> <ping cpu|-> <pong cpu|->
//! Across two hosts, pong runs on the peer and ping here (endpoints are host:port, ping's
//! on the pong host and pong's on the ping host):
//! rtt xpong <ping endpoint> <pong endpoint> <pong cpu|->
//! rtt xping <ping endpoint> <pong endpoint> <messages> <warmup> <ping cpu|->
//! prints: rtt,<label>,<mode>,p50,p90,p99,p99.9,p99.99,max,mean (ns),samples (xping as udp)
//!
//! A slow configuration stops early: warmup after a quarter of `RTT_SECONDS` (default 20)
//! and the measured run after all of it.

use hdrhistogram::Histogram;
use rusteron_client::*;
use std::ffi::{CStr, CString};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use x86lab::{aeron_dir, cpu_arg, label, pin, wait_until};

const PING_STREAM_ID: i32 = 1002;
const PONG_STREAM_ID: i32 = 1003;
const MESSAGE_LENGTH: usize = 32;
const FRAGMENT_COUNT_LIMIT: usize = 10;

fn client() -> Aeron {
    let context = AeronContext::new().unwrap();
    context.set_dir(&aeron_dir()).unwrap();
    let aeron = Aeron::new(&context).unwrap();
    aeron.start().unwrap();
    aeron
}

fn udp(endpoint: Option<String>) -> CString {
    CString::new(format!("aeron:udp?endpoint={}", endpoint.expect("host:port"))).unwrap()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().expect("ipc|udp|xping|xpong");
    let number = |a: Option<String>| -> usize { a.expect("messages and warmup").parse().unwrap() };
    if mode == "xpong" {
        let (ping_channel, pong_channel) = (udp(args.next()), udp(args.next()));
        run_pong(
            &AtomicBool::new(true),
            None,
            &ping_channel,
            &pong_channel,
            cpu_arg(args.next()),
        );
        return;
    }
    let (ping_channel, pong_channel) = match mode.as_str() {
        "ipc" => (c"aeron:ipc".to_owned(), c"aeron:ipc".to_owned()),
        "udp" => (
            c"aeron:udp?endpoint=localhost:20123".to_owned(),
            c"aeron:udp?endpoint=localhost:20124".to_owned(),
        ),
        "xping" => (udp(args.next()), udp(args.next())),
        other => panic!("unknown mode {other}"),
    };
    let (messages, warmup) = (number(args.next()), number(args.next()));
    let ping_cpu = cpu_arg(args.next());

    let running = Arc::new(AtomicBool::new(true));
    // set once pong's subscription has the ping image: ping's publication alone is no proof, as
    // another subscriber (an archive recording it) also connects it, and a ping sent before pong
    // joins is never echoed
    let pong_ready = Arc::new(AtomicBool::new(mode == "xping"));
    let pong = (mode != "xping").then(|| {
        let pong_cpu = cpu_arg(args.next());
        let (running, ready) = (Arc::clone(&running), Arc::clone(&pong_ready));
        let (ping_channel, pong_channel) = (ping_channel.clone(), pong_channel.clone());
        std::thread::spawn(move || run_pong(&running, Some(&ready), &ping_channel, &pong_channel, pong_cpu))
    });

    let aeron = client();
    let publication = aeron
        .async_add_exclusive_publication(&ping_channel, PING_STREAM_ID)
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();
    let subscription = aeron
        .async_add_subscription(&pong_channel, PONG_STREAM_ID, Handlers::NONE, Handlers::NONE)
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();
    wait_until("pong", 30, || {
        publication.is_connected() && subscription.image_at_index(0).is_some() && pong_ready.load(Ordering::Acquire)
    });
    let image = subscription.image_at_index(0).unwrap();
    pin(ping_cpu);

    let budget = Duration::from_secs(std::env::var("RTT_SECONDS").map_or(20, |s| s.parse().unwrap()));
    let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
    let mut buffer = [0u8; MESSAGE_LENGTH];
    let start = Instant::now();
    for i in 0..warmup {
        round_trip(&publication, &image, &mut buffer, |_| {});
        if i % 64 == 0 && start.elapsed() > budget / 4 {
            break;
        }
    }
    let start = Instant::now();
    for i in 0..messages {
        round_trip(&publication, &image, &mut buffer, |rtt| {
            histogram.saturating_record(rtt)
        });
        if i % 64 == 0 && start.elapsed() > budget {
            break;
        }
    }
    running.store(false, Ordering::Release);
    if let Some(pong) = pong {
        pong.join().unwrap();
    }
    let mode = if mode == "xping" { "udp" } else { mode.as_str() };

    let q = |quantile| histogram.value_at_quantile(quantile);
    println!(
        "rtt,{},{mode},{},{},{},{},{},{},{:.0},{}",
        label(),
        q(0.5),
        q(0.9),
        q(0.99),
        q(0.999),
        q(0.9999),
        histogram.max(),
        histogram.mean(),
        histogram.len()
    );
}

/// Sends one stamped ping and polls until pong's echo of it is back.
#[inline]
fn round_trip(
    publication: &AeronExclusivePublication,
    image: &AeronImage,
    buffer: &mut [u8; MESSAGE_LENGTH],
    mut record: impl FnMut(u64),
) {
    let mut idle = BusySpinIdleStrategy::new();
    let position = loop {
        // stamped on each attempt, so time back-pressured is not counted
        buffer[..8].copy_from_slice(&Aeron::nano_clock().to_le_bytes());
        match publication.offer(buffer) {
            Ok(position) => break position,
            Err(e) if e.is_retryable() => idle.idle(0),
            Err(e) => panic!("offer: {e:?}"),
        }
    };
    // pong echoes at the same length, so the echo is read once the image reaches our position
    while image.position() < position {
        let fragments = image
            .poll_fn(
                |reply, _header| {
                    let sent = i64::from_le_bytes(reply[..8].try_into().unwrap());
                    record((Aeron::nano_clock() - sent) as u64);
                },
                FRAGMENT_COUNT_LIMIT,
            )
            .unwrap();
        if fragments == 0 {
            assert!(!image.is_closed(), "pong went away");
        }
        idle.idle(fragments);
    }
}

fn run_pong(
    running: &AtomicBool,
    ready: Option<&AtomicBool>,
    ping_channel: &CStr,
    pong_channel: &CStr,
    cpu: Option<usize>,
) {
    let aeron = client();
    let publication = aeron
        .async_add_exclusive_publication(pong_channel, PONG_STREAM_ID)
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();
    let subscription = aeron
        .async_add_subscription(ping_channel, PING_STREAM_ID, Handlers::NONE, Handlers::NONE)
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();
    if let Some(ready) = ready {
        wait_until("ping", 30, || subscription.image_at_index(0).is_some());
        ready.store(true, Ordering::Release);
    }
    pin(cpu);
    let mut idle = BusySpinIdleStrategy::new();
    while running.load(Ordering::Acquire) {
        let fragments = subscription
            .poll_fn(|ping, _header| echo(&publication, ping), FRAGMENT_COUNT_LIMIT)
            .unwrap();
        idle.idle(fragments);
    }
}

#[inline]
fn echo(publication: &AeronExclusivePublication, ping: &[u8]) {
    let mut idle = BusySpinIdleStrategy::new();
    loop {
        match publication.try_claim_owned(ping.len()) {
            Ok(mut claim) => {
                claim.data().copy_from_slice(ping);
                claim.commit().unwrap();
                return;
            }
            Err(e) if e.is_retryable() => idle.idle(0),
            Err(e) => panic!("claim: {e:?}"),
        }
    }
}
