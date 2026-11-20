use hdrhistogram::Histogram;
use std::error::Error;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Runs `peer` on a named thread. However it ends, by returning or by panicking, `running` is
/// cleared so the thread on the other side stops waiting for it. The peer's error comes back
/// through the join handle as text, because `Box<dyn Error>` cannot cross threads
pub fn spawn_peer(
    name: &str,
    running: Arc<AtomicBool>,
    peer: impl FnOnce(&Arc<AtomicBool>) -> Result<(), Box<dyn Error>> + Send + 'static,
) -> std::io::Result<JoinHandle<Result<(), String>>> {
    thread::Builder::new().name(name.into()).spawn(move || {
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| peer(&running)));
        running.store(false, Ordering::Release);
        match outcome {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(_) => Err("panicked".into()),
        }
    })
}

pub struct MessageCounts {
    pub warmup: usize,
    pub measured: usize,
}

impl MessageCounts {
    pub fn parse(default_measured: usize) -> Result<Self, Box<dyn Error>> {
        let mut counts = Self {
            warmup: 100_000,
            measured: default_measured,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let target = match arg.as_str() {
                "--warmup" => &mut counts.warmup,
                "--messages" => &mut counts.measured,
                _ => return Err(format!("unknown argument {arg}; use --warmup N --messages N").into()),
            };
            *target = args.next().ok_or("missing message count")?.parse()?;
        }
        if counts.measured == 0 {
            return Err("--messages must be greater than zero".into());
        }
        Ok(counts)
    }
}

pub fn print_histogram(hist: &Histogram<u64>) {
    println!("message length 32 bytes\n");
    println!("Histogram of RTT latencies:");
    println!("# of samples: {}", hist.len());
    println!("min: {:?}", Duration::from_nanos(hist.min()));
    for (label, quantile) in [("50", 0.5), ("99", 0.99), ("99.9", 0.999), ("99.99", 0.9999)] {
        println!(
            "{label}th percentile: {:?}",
            Duration::from_nanos(hist.value_at_quantile(quantile))
        );
    }
    println!("max: {:?}", Duration::from_nanos(hist.max()));
    println!("avg: {:?}", Duration::from_nanos(hist.mean() as u64));
}
