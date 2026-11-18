//! The ping and pong halves the RTT benches share, as Aeron's `cping`/`cpong`: ping stamps a
//! time into each message, pong echoes it, and ping polls its image until the echo is back.

use criterion::Criterion;
use rusteron_client::*;
use std::error::Error;
use std::ffi::CStr;
use std::hint::black_box;
use std::thread::sleep;
use std::time::{Duration, Instant};

const PING_STREAM_ID: i32 = 1002;
const PONG_STREAM_ID: i32 = 1003;
const MESSAGE_LENGTH: usize = 32;
const FRAGMENT_COUNT_LIMIT: usize = 10;

/// The channel each direction travels on: pings on `ping`, echoes on `pong`.
pub struct Channels {
    pub ping: &'static CStr,
    pub pong: &'static CStr,
}

/// Benchmarks round trips as `name` against a pong on `dir`'s driver, once both directions
/// connect within `connect_timeout`.
pub fn run_ping(
    c: &mut Criterion,
    dir: &str,
    channels: &Channels,
    name: &str,
    connect_timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let (publication, subscription) = connect(dir, (channels.pong, PONG_STREAM_ID), (channels.ping, PING_STREAM_ID))?;
    println!("PING: pong publisher {:?} {PONG_STREAM_ID}", channels.pong);
    println!("PING: ping subscriber {:?} {PING_STREAM_ID}", channels.ping);

    let image = wait_for_pong(&publication, &subscription, connect_timeout)?;
    let mut buffer = [0u8; MESSAGE_LENGTH];
    c.bench_function(name, |b| {
        b.iter(|| record_rtt(&publication, &image, &mut buffer).expect("round trip"));
    });
    Ok(())
}

/// Echoes pings on `dir`'s driver while `running` returns true.
pub fn run_pong(
    dir: &str,
    channels: &Channels,
    running: impl Fn() -> bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (publication, subscription) = connect(dir, (channels.ping, PING_STREAM_ID), (channels.pong, PONG_STREAM_ID))?;
    println!("PONG: ping publisher {:?} {PING_STREAM_ID}", channels.ping);
    println!("PONG: pong subscriber {:?} {PONG_STREAM_ID}", channels.pong);

    // the poll callback cannot return an error, so it parks the first one here
    let mut failure = None;
    let mut idle = BusySpinIdleStrategy::new();
    while running() {
        let fragments = subscription.poll_fn(
            |buffer, _header| {
                if failure.is_none() {
                    failure = echo(&publication, buffer).err();
                }
            },
            FRAGMENT_COUNT_LIMIT,
        )?;
        if let Some(e) = failure.take() {
            return Err(e);
        }
        idle.idle(fragments);
    }
    Ok(())
}

/// A client on `dir` with an exclusive publication on `publish` and a subscription on
/// `subscribe`, each a channel and stream id. The handles keep the client open.
fn connect(
    dir: &str,
    publish: (&CStr, i32),
    subscribe: (&CStr, i32),
) -> Result<(AeronExclusivePublication, AeronSubscription), AeronCError> {
    let context = AeronContext::new()?;
    context.set_dir(&cformat!("{dir}"))?;
    let aeron = Aeron::new(&context)?;
    aeron.start()?;
    let publication = aeron
        .async_add_exclusive_publication(publish.0, publish.1)?
        .poll_blocking(Duration::from_secs(5))?;
    let subscription = aeron
        .async_add_subscription(subscribe.0, subscribe.1, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;
    Ok((publication, subscription))
}

/// Both directions must be up: our pings reach pong, and pong's echoes reach our image.
fn wait_for_pong(
    publication: &AeronExclusivePublication,
    subscription: &AeronSubscription,
    timeout: Duration,
) -> Result<AeronImage, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        if publication.is_connected()
            && let Some(image) = subscription.image_at_index(0)
        {
            return Ok(image);
        }
        if Instant::now() > deadline {
            return Err("pong never connected".into());
        }
        sleep(Duration::from_millis(10));
    }
}

#[inline]
fn echo(publication: &AeronExclusivePublication, buffer: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut idle = BusySpinIdleStrategy::new();
    loop {
        match publication.try_claim_owned(buffer.len()) {
            // pings never fragment, so the claim's own BEGIN|END flags are the right ones
            Ok(mut claim) => {
                claim.data().copy_from_slice(buffer);
                claim.commit()?;
                return Ok(());
            }
            Err(e) if e.is_retryable() => idle.idle(0),
            Err(e) => return Err(e.into()),
        }
    }
}

#[inline]
fn record_rtt(
    publication: &AeronExclusivePublication,
    image: &AeronImage,
    buffer: &mut [u8],
) -> Result<(), Box<dyn Error>> {
    let mut idle = BusySpinIdleStrategy::new();
    let position = loop {
        // stamp each attempt, so time spent back-pressured is not counted as RTT
        buffer[0..8].copy_from_slice(&Aeron::nano_clock().to_le_bytes());
        match publication.offer(buffer) {
            Ok(position) => break position,
            Err(e) if e.is_retryable() => idle.idle(0),
            Err(e) => return Err(e.into()),
        }
    };

    // pong echoes at the same length, so the echo has been read once the image reaches our position
    while image.position() < position {
        let fragments = image.poll_fn(
            |reply, _header| {
                black_box(Aeron::nano_clock() - read_i64(reply));
            },
            FRAGMENT_COUNT_LIMIT,
        )?;
        if fragments == 0 {
            if image.is_closed() {
                return Err("pong went away".into());
            }
        }
        idle.idle(fragments);
    }
    Ok(())
}

fn read_i64(buffer: &[u8]) -> i64 {
    i64::from_le_bytes(buffer[0..8].try_into().expect("ping shorter than 8 bytes"))
}
