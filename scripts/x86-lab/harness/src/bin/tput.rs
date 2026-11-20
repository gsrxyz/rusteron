//! Exclusive IPC throughput through an external driver (`AERON_DIR`), as Aeron's
//! EmbeddedExclusiveIpcThroughput: a publisher thread offers 32-byte messages flat out and the
//! main thread polls them, each with its own client and pinned to its CPU once connected.
//!
//! tput <seconds> <publisher cpu|-> <subscriber cpu|->
//! prints: tput,<label>,ipc,<median msgs/s>,<min>,<max> over one-second samples after a
//! one-second warmup
//!
//! Across two hosts, the subscriber binds <endpoint> (host:port) on its host and prints the
//! same line with mode udp; the publisher sends to it for <seconds>:
//! tput xsub <endpoint> <seconds> <cpu|->
//! tput xpub <endpoint> <seconds> <cpu|->

use rusteron_client::*;
use std::ffi::{CStr, CString};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use x86lab::{aeron_dir, cpu_arg, label, pin, wait_until};

const STREAM_ID: i32 = 1001;
const MESSAGE_LENGTH: usize = 32;
const FRAGMENT_COUNT_LIMIT: usize = 256;

fn client() -> Aeron {
    let context = AeronContext::new().unwrap();
    context.set_dir(&aeron_dir()).unwrap();
    let aeron = Aeron::new(&context).unwrap();
    aeron.start().unwrap();
    aeron
}

fn main() {
    let mut args = std::env::args().skip(1);
    let first = args.next().expect("seconds, xsub or xpub");
    if first == "xsub" || first == "xpub" {
        let channel = CString::new(format!("aeron:udp?endpoint={}", args.next().expect("host:port"))).unwrap();
        let seconds: usize = args.next().expect("seconds").parse().unwrap();
        let cpu = cpu_arg(args.next());
        if first == "xpub" {
            let running = AtomicBool::new(true);
            std::thread::scope(|scope| {
                scope.spawn(|| publish(&running, &channel, cpu));
                std::thread::sleep(Duration::from_secs(seconds as u64));
                running.store(false, Ordering::Release);
            });
        } else {
            let aeron = client();
            let subscription = aeron
                .async_add_subscription(&channel, STREAM_ID, Handlers::NONE, Handlers::NONE)
                .unwrap()
                .poll_blocking(Duration::from_secs(5))
                .unwrap();
            wait_until("publisher", 30, || subscription.image_at_index(0).is_some());
            pin(cpu);
            report("udp", &measure(&subscription, seconds));
        }
        return;
    }
    let seconds: usize = first.parse().unwrap();
    let (publisher_cpu, subscriber_cpu) = (cpu_arg(args.next()), cpu_arg(args.next()));

    let aeron = client();
    let subscription = aeron
        .async_add_subscription(c"aeron:ipc", STREAM_ID, Handlers::NONE, Handlers::NONE)
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let publisher = {
        let running = Arc::clone(&running);
        std::thread::spawn(move || publish(&running, c"aeron:ipc", publisher_cpu))
    };
    wait_until("publisher", 10, || subscription.image_at_index(0).is_some());
    pin(subscriber_cpu);
    let samples = measure(&subscription, seconds);
    running.store(false, Ordering::Release);
    publisher.join().unwrap();
    report("ipc", &samples);
}

/// Messages per second over `seconds` one-second samples after a one-second warmup, sorted.
fn measure(subscription: &AeronSubscription, seconds: usize) -> Vec<f64> {
    let mut count = 0u64;
    let mut samples = Vec::with_capacity(seconds);
    let mut window_start = Instant::now();
    let warmup_end = window_start + Duration::from_secs(1);
    let mut warming = true;
    let mut idle = BusySpinIdleStrategy::new();
    while samples.len() < seconds {
        let fragments = subscription
            .poll_fn(|_message, _header| count += 1, FRAGMENT_COUNT_LIMIT)
            .unwrap();
        idle.idle(fragments);
        let now = Instant::now();
        if warming {
            if now >= warmup_end {
                warming = false;
                count = 0;
                window_start = now;
            }
        } else if now.duration_since(window_start) >= Duration::from_secs(1) {
            samples.push(count as f64 / now.duration_since(window_start).as_secs_f64());
            count = 0;
            window_start = now;
        }
    }
    samples.sort_by(f64::total_cmp);
    samples
}

fn report(mode: &str, samples: &[f64]) {
    println!(
        "tput,{},{mode},{:.0},{:.0},{:.0}",
        label(),
        samples[samples.len() / 2],
        samples[0],
        samples[samples.len() - 1]
    );
}

fn publish(running: &AtomicBool, channel: &CStr, cpu: Option<usize>) {
    let aeron = client();
    let publication = aeron
        .async_add_exclusive_publication(channel, STREAM_ID)
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();
    wait_until("subscriber", 30, || publication.is_connected());
    pin(cpu);
    let message = [0u8; MESSAGE_LENGTH];
    let mut idle = BusySpinIdleStrategy::new();
    while running.load(Ordering::Relaxed) {
        match publication.offer(&message) {
            Ok(_) => {}
            Err(e) if e.is_retryable() => idle.idle(0),
            Err(e) => panic!("offer: {e:?}"),
        }
    }
}
