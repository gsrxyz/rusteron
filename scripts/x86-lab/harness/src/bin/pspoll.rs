//! Idle poll cost of N live persistent subscriptions against N plain subscriptions on the
//! same stream, with a Java Archive started for the run.
//!
//! pspoll <thread|invoker> <fixed|ephemeral> <n>...
//! - thread: the client has its conductor thread; invoker: the caller runs the conductor,
//!   which each persistent subscription poll does itself, and the plain-subscription cycle
//!   does once per cycle.
//! - fixed: every archive client shares one control-response port; ephemeral: each gets
//!   its own (`localhost:0`).
//!
//! prints: pspoll,<label>,<mode>,<ports>,<n>,<ns per persistent subscription poll>,
//! <ns per subscription poll>,live_after_ms=<time for all n to go live>

use rusteron_archive::testing::{EmbeddedArchiveMediaDriverProcess, find_unused_udp_port};
use rusteron_archive::*;
use std::error::Error;
use std::hint::black_box;
use std::time::{Duration, Instant};
use x86lab::label;

const STREAM_ID: i32 = 1401;
const REPLAY_STREAM_ID: i32 = 2401;
const POLLS: usize = 2_000_000;

/// The client, and whether this program must run its conductor.
struct Client {
    aeron: Aeron,
    invoker: bool,
}

impl Client {
    fn work(&self) -> Result<(), AeronCError> {
        if self.invoker {
            self.aeron.main_do_work()?;
        }
        Ok(())
    }

    /// Runs the conductor (when ours to run) and `poll` until it yields a value.
    fn until<T>(
        &self,
        what: &str,
        secs: u64,
        mut poll: impl FnMut() -> Result<Option<T>, AeronCError>,
    ) -> Result<T, Box<dyn Error>> {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            self.work()?;
            if let Some(value) = poll()? {
                return Ok(value);
            }
            if Instant::now() > deadline {
                return Err(format!("timed out waiting for {what}").into());
            }
            std::thread::yield_now();
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().expect("thread|invoker");
    let ports = args.next().expect("fixed|ephemeral");
    let counts: Vec<usize> = args.map(|a| a.parse().unwrap()).collect();

    EmbeddedArchiveMediaDriverProcess::kill_all_java_processes().ok();
    let id = Aeron::nano_clock();
    let base = std::env::var("PSPOLL_DIR").unwrap_or_else(|_| "target/aeron".into());
    let aeron_dir = format!("{base}/{id}_pspoll/shm");
    let archive_dir = format!("{base}/{id}_pspoll/archive");
    let req_port = find_unused_udp_port(9600).ok_or("no free port")?;
    let resp_port = find_unused_udp_port(req_port + 1).ok_or("no free port")?;
    let events_port = find_unused_udp_port(resp_port + 1).ok_or("no free port")?;
    let request_channel = format!("aeron:udp?endpoint=localhost:{req_port}");
    let response_channel = match ports.as_str() {
        "fixed" => format!("aeron:udp?endpoint=localhost:{resp_port}"),
        "ephemeral" => "aeron:udp?endpoint=localhost:0".to_string(),
        other => panic!("unknown ports {other}"),
    };
    let _process = EmbeddedArchiveMediaDriverProcess::build_and_start(
        &aeron_dir,
        &archive_dir,
        &request_channel,
        &format!("aeron:udp?endpoint=localhost:{resp_port}"),
        &format!("aeron:udp?endpoint=localhost:{events_port}"),
    )?;

    let ctx = AeronContext::new()?;
    ctx.set_dir(&cformat!("{aeron_dir}"))?;
    ctx.set_use_conductor_agent_invoker(mode == "invoker")?;
    let client = Client {
        aeron: Aeron::new(&ctx)?,
        invoker: mode == "invoker",
    };
    client.aeron.start()?;
    let aeron = &client.aeron;

    let archive_ctx = AeronArchiveContext::new()?;
    archive_ctx.set_aeron(aeron)?;
    archive_ctx.set_control_request_channel(&cformat!("{request_channel}"))?;
    archive_ctx.set_control_response_channel(&cformat!("{response_channel}"))?;
    let connect = AeronArchiveAsyncConnect::new_with_aeron(&archive_ctx, aeron)?;
    let archive = client.until("archive", 30, || connect.poll())?;

    archive.start_recording(AERON_IPC_STREAM, STREAM_ID, SOURCE_LOCATION_LOCAL, true)?;
    let adding = aeron.async_add_exclusive_publication(AERON_IPC_STREAM, STREAM_ID)?;
    let publication = client.until("publication", 10, || adding.poll())?;
    let session_id = publication.get_constants()?.session_id();
    let counters = aeron.counters_reader();
    let recording_id = client.until("recording", 10, || {
        let counter_id = RecordingPos::find_counter_id_by_session(&counters, session_id);
        Ok(if counter_id >= 0 {
            Some(RecordingPos::get_recording_id(&counters, counter_id)?)
        } else {
            None
        })
    })?;
    let mut sent = 0;
    client.until("history", 10, || {
        sent += publication.offer(b"history").is_ok() as usize;
        Ok((sent >= 10).then_some(()))
    })?;

    for n in counts {
        let mut persistent = Vec::with_capacity(n);
        for i in 0..n {
            persistent.push(
                PersistentSubscriptionBuilder::new_with_aeron(&archive_ctx, aeron)?
                    .live_channel("aeron:ipc")?
                    .live_stream_id(STREAM_ID)?
                    .replay_channel("aeron:ipc")?
                    .replay_stream_id(REPLAY_STREAM_ID + i as i32)?
                    .start_from_live()?
                    .recording_id(recording_id)?
                    .build()?,
            );
        }
        let started = Instant::now();
        client.until("persistent subscriptions live", 300, || {
            for ps in &persistent {
                ps.poll_fn(|_, _| {}, 10)?;
                if ps.has_failed() {
                    let (code, message) = ps.get_failure_reason().unwrap_or((-1, "unknown".into()));
                    panic!("persistent subscription failed ({code}): {message}");
                }
            }
            Ok(persistent.iter().all(|ps| ps.is_live()).then_some(()))
        })?;
        let live_after = started.elapsed();

        let mut plain = Vec::with_capacity(n);
        for _ in 0..n {
            let adding = aeron.async_add_subscription(AERON_IPC_STREAM, STREAM_ID, Handlers::NONE, Handlers::NONE)?;
            plain.push(client.until("subscription", 10, || adding.poll())?);
        }
        client.until("images", 10, || {
            Ok(plain.iter().all(|s| s.image_at_index(0).is_some()).then_some(()))
        })?;

        let rounds = (POLLS / n).max(20_000);
        let time_per_poll = |poll_all: &mut dyn FnMut() -> Result<(), AeronCError>| {
            for _ in 0..rounds / 10 {
                poll_all()?;
            }
            let start = Instant::now();
            for _ in 0..rounds {
                poll_all()?;
            }
            Ok::<f64, AeronCError>(start.elapsed().as_nanos() as f64 / (rounds * n) as f64)
        };
        let ps_ns = time_per_poll(&mut || {
            for ps in &persistent {
                black_box(ps.poll_fn(|_, _| {}, 10)?);
            }
            Ok(())
        })?;
        let plain_ns = time_per_poll(&mut || {
            for subscription in &plain {
                black_box(subscription.poll_fn(|_, _| {}, 10)?);
            }
            client.work()
        })?;
        assert!(
            persistent.iter().all(|ps| ps.is_live()),
            "a persistent subscription left live"
        );
        // subscriber positions on the control-response channel: one per image per subscription
        let response_port = format!("localhost:{resp_port}");
        let mut response_images = 0;
        counters.foreach_counter_fn(|_value, _id, type_id, _key, label| {
            let response = label.contains(&response_port) || label.contains("localhost:0");
            // 4: AERON_COUNTER_SUBSCRIPTION_POSITION_TYPE_ID
            response_images += (type_id == 4 && response) as usize;
        });
        println!(
            "pspoll,{},{mode},{ports},{n},{ps_ns:.1},{plain_ns:.1},live_after_ms={},response_images={response_images}",
            label(),
            live_after.as_millis()
        );

        for ps in persistent {
            ps.close()?;
        }
        drop(plain);
        let settle = Instant::now() + Duration::from_millis(500);
        while Instant::now() < settle {
            client.work()?;
            std::thread::yield_now();
        }
    }
    Ok(())
}
