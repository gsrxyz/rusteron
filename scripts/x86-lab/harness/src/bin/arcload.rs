//! Archive load through an archive that may be local or remote (control requests on
//! `ARCHIVE_CONTROL`, responses on `ARCHIVE_RESPONSE`, default `aeron:udp?endpoint=localhost:0`).
//! Publishers and subscribers run here, each thread with its own client on the driver in
//! `AERON_DIR`, pinned round-robin to <cpu list> (`2,3,4`, or `-` for unpinned).
//!
//! <channel> is `ipc`, or `udp:<host>:<base port>` for `aeron:udp?endpoint=<host>:<base port + i>`
//! on stream i.
//!
//! arcload record <streams> <message bytes> <seconds> <rate msgs/s per stream, 0 = flat out> <channel> <cpus>
//!   records streams 3000 + i (udp: from the archive's host, so <host> is the archive's) while
//!   one exclusive publication per stream offers for <seconds>, then waits until every
//!   recording reaches its publication's final position. A rate-limited offer that completes
//!   after its slot ended counts as missed (stderr); the schedule never re-anchors.
//!   prints each second: arcload,sample,<label>,<t>,<msgs/s>,<MB/s>,<backpressure retries/s>
//!   then: arcload,record,<label>,<streams>,<bytes>,<rate>,<median msgs/s>,<median MB/s>,
//!         <backpressure % of offers>,<catch-up s>,<MB/s from first offer to last byte recorded>
//! arcload replay <concurrency> <seconds> <channel> <cpus>
//!   replays the largest <concurrency> complete recordings, whole, to streams 4000 + i
//!   (udp: <host> is this host), until all end or <seconds> pass.
//!   prints each second: arcload,rsample,<label>,<t>,<MB/s>
//!   then: arcload,replay,<label>,<replays>,<median first-fragment ms>,<aggregate MB/s>,
//!         <total MB>,<replays completed>

use rusteron_archive::*;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::*};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use x86lab::{aeron_dir, label, pin, wait_until};

type Res<T> = Result<T, Box<dyn Error>>;

const RECORD_STREAM: i32 = 3000;
const REPLAY_STREAM: i32 = 4000;
const FRAGMENT_LIMIT: usize = 4096;
const NULL_POSITION: i64 = -1;
const USAGE: &str = "usage: arcload record <streams> <bytes> <seconds> <rate> <channel> <cpus> | \
                     arcload replay <concurrency> <seconds> <channel> <cpus>";

/// One thread's counters, written only by that thread, on its own cache lines.
#[derive(Default)]
#[repr(align(128))]
struct Stats {
    count: AtomicU64,
    retries: AtomicU64,
    missed: AtomicU64,
    session: AtomicI64,
    start_ns: AtomicU64,
    first_ns: AtomicU64,
    done: AtomicBool,
}

struct Shared {
    go: AtomicBool,
    running: AtomicBool,
    release: AtomicBool,
    stats: Vec<Stats>,
}

impl Shared {
    fn new(threads: usize) -> Self {
        Self {
            go: AtomicBool::new(false),
            running: AtomicBool::new(true),
            release: AtomicBool::new(false),
            stats: (0..threads).map(|_| Stats::default()).collect(),
        }
    }

    fn total(&self, field: fn(&Stats) -> &AtomicU64) -> u64 {
        self.stats.iter().map(|s| field(s).load(Relaxed)).sum()
    }

    fn finish(&self) {
        self.running.store(false, Release);
        self.release.store(true, Release);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("record") => record(&args[1..]),
        Some("replay") => replay(&args[1..]),
        _ => Err(USAGE.into()),
    };
    if let Err(e) = result {
        eprintln!("arcload: {e}");
        std::process::exit(1);
    }
}

fn arg<T: FromStr>(args: &[String], i: usize, what: &str) -> Res<T> {
    let value = args.get(i).ok_or_else(|| format!("missing <{what}>; {USAGE}"))?;
    Ok(value.parse().map_err(|_| format!("bad <{what}>: {value}"))?)
}

fn cpus(arg: &str) -> Res<Vec<Option<usize>>> {
    if arg == "-" {
        return Ok(vec![None]);
    }
    arg.split(',').map(|c| Ok(Some(c.parse()?))).collect()
}

fn channel(spec: &str, i: usize) -> Res<CString> {
    if spec == "ipc" {
        return Ok(AERON_IPC_STREAM.to_owned());
    }
    let (host, port) = spec
        .strip_prefix("udp:")
        .and_then(|s| s.rsplit_once(':'))
        .ok_or("<channel>: ipc or udp:<host>:<base port>")?;
    let port: usize = port.parse()?;
    Ok(CString::new(format!("aeron:udp?endpoint={host}:{}", port + i))?)
}

fn client() -> Res<Aeron> {
    let context = AeronContext::new()?;
    context.set_dir(&aeron_dir())?;
    context.set_error_handler(Some(|code: i32, msg: &str| {
        eprintln!("arcload: aeron error {code}: {msg}")
    }))?;
    let aeron = Aeron::new(&context)?;
    aeron.start()?;
    Ok(aeron)
}

fn connect() -> Res<(Aeron, AeronArchive)> {
    let aeron = client()?;
    let control = std::env::var("ARCHIVE_CONTROL").map_err(|_| "ARCHIVE_CONTROL: the archive's control channel")?;
    let response = std::env::var("ARCHIVE_RESPONSE").unwrap_or_else(|_| "aeron:udp?endpoint=localhost:0".into());
    let context = AeronArchiveContext::new()?;
    context.set_aeron(&aeron)?;
    context.set_control_request_channel(&CString::new(control)?)?;
    context.set_control_response_channel(&CString::new(response)?)?;
    context.set_error_handler(Some(|code: i32, msg: &str| {
        eprintln!("arcload: archive error {code}: {msg}")
    }))?;
    let archive = AeronArchiveAsyncConnect::new_with_aeron(&context, &aeron)?
        .poll_blocking(Duration::from_secs(20))
        .map_err(|e| format!("connecting to the archive: {e}"))?;
    Ok((aeron, archive))
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied().unwrap_or(f64::NAN)
}

fn record(args: &[String]) -> Res<()> {
    let streams: usize = arg(args, 0, "streams")?;
    let length: usize = arg(args, 1, "message bytes")?;
    let seconds: u64 = arg(args, 2, "seconds")?;
    let rate: u64 = arg(args, 3, "rate")?;
    let spec: String = arg(args, 4, "channel")?;
    let cpus = cpus(&arg::<String>(args, 5, "cpus")?)?;
    if streams == 0 || seconds == 0 {
        return Err("<streams> and <seconds> must be at least 1".into());
    }
    let channels = (0..streams).map(|i| channel(&spec, i)).collect::<Res<Vec<_>>>()?;
    let source = if spec == "ipc" {
        SOURCE_LOCATION_LOCAL
    } else {
        SOURCE_LOCATION_REMOTE
    };

    let (_aeron, archive) = connect()?;
    let shared = Shared::new(streams);
    std::thread::scope(|scope| {
        let (session_tx, sessions) = mpsc::channel();
        let (final_tx, finals) = mpsc::channel();
        for (i, channel) in channels.iter().enumerate() {
            let (session_tx, final_tx, shared) = (session_tx.clone(), final_tx.clone(), &shared);
            let cpu = cpus[i % cpus.len()];
            scope.spawn(move || {
                publish(shared, i, channel, (length, rate), cpu, (session_tx, final_tx));
            });
        }
        drop((session_tx, final_tx));
        let mut subscriptions = Vec::with_capacity(streams);
        let result = start_recordings(&archive, &channels, source, &sessions, &mut subscriptions)
            .and_then(|sessions| measure_record(&archive, &shared, &sessions, &finals, (length, rate, seconds)));
        stop_recordings(&archive, &subscriptions);
        shared.finish();
        result
    })
}

/// Records each stream from its own publication's session only: an earlier run's publication
/// can still linger on the same channel, and its recording's autoStop would close the shared
/// recording subscription.
fn start_recordings(
    archive: &AeronArchive,
    channels: &[CString],
    source: SourceLocation,
    sessions: &mpsc::Receiver<(usize, i32)>,
    subscriptions: &mut Vec<i64>,
) -> Res<Vec<i32>> {
    let mut ids = vec![0; channels.len()];
    for _ in channels {
        let (i, session) = sessions
            .recv_timeout(Duration::from_secs(60))
            .map_err(|_| "timed out waiting for publications")?;
        ids[i] = session;
    }
    for (i, (channel, &session)) in channels.iter().zip(&ids).enumerate() {
        let stream = RECORD_STREAM + i as i32;
        let channel = CString::new(ChannelUri::add_session_id(channel.to_str()?, session))?;
        let id = archive
            .start_recording(&channel, stream, source, true)
            .map_err(|e| format!("start recording stream {stream}: {e}"))?;
        subscriptions.push(id);
    }
    Ok(ids)
}

fn stop_recordings(archive: &AeronArchive, subscriptions: &[i64]) {
    for &id in subscriptions {
        if let Err(e) = archive.stop_recording_subscription(id) {
            eprintln!("arcload: stop recording subscription {id}: {e}");
        }
    }
}

/// (stream index, session id) once added, and (stream index, final position) once stopped.
type Signals = (mpsc::Sender<(usize, i32)>, mpsc::Sender<(usize, i64)>);

/// One stream's publisher: reports its session, waits for the recording to connect, offers from `go` until
/// `running` clears, reports its final position, then holds the publication open until
/// `release` so that autoStop does not end the recording before it catches up.
fn publish(
    shared: &Shared,
    i: usize,
    channel: &CStr,
    (length, rate): (usize, u64),
    cpu: Option<usize>,
    (sessions, finals): Signals,
) {
    let stream = RECORD_STREAM + i as i32;
    let aeron = client().expect("aeron client");
    // pinned once the client's conductor has started on the process's CPUs, but before the
    // spinning blocking poll: many threads spinning on those CPUs starve the driver's conductor
    pin(cpu);
    let publication = aeron
        .async_add_exclusive_publication(channel, stream)
        .and_then(|p| p.poll_blocking(Duration::from_secs(30)))
        .unwrap_or_else(|e| panic!("stream {stream}: add publication: {e}"));
    let _ = sessions.send((i, publication.get_constants().expect("constants").session_id()));
    wait_until(&format!("the archive to connect to stream {stream}"), 30, || {
        publication.is_connected() || !shared.running.load(Acquire)
    });
    wait_until("the start signal", 60, || {
        shared.go.load(Acquire) || !shared.running.load(Acquire)
    });

    let stats = &shared.stats[i];
    let message = vec![7u8; length];
    let interval = Duration::from_nanos(1_000_000_000 / rate.max(1));
    let (mut sent, mut retries, mut missed) = (0u64, 0u64, 0u64);
    let mut next = Instant::now();
    let mut idle = BusySpinIdleStrategy::new();
    'run: while shared.running.load(Relaxed) {
        if rate > 0 {
            while Instant::now() < next {
                if !shared.running.load(Relaxed) {
                    break 'run;
                }
                idle.idle(0);
            }
        }
        loop {
            match publication.offer(&message) {
                Ok(_) => break,
                Err(e) if e.is_retryable() => {
                    retries += 1;
                    stats.retries.store(retries, Relaxed);
                    if !shared.running.load(Relaxed) {
                        break 'run;
                    }
                    idle.idle(0);
                }
                Err(e) => panic!("stream {stream}: offer failed after {sent} messages: {e}"),
            }
        }
        sent += 1;
        stats.count.store(sent, Relaxed);
        if rate > 0 {
            next += interval;
            if Instant::now() > next {
                missed += 1;
                stats.missed.store(missed, Relaxed);
            }
        }
    }
    let _ = finals.send((i, publication.position()));
    wait_until("the recordings to catch up", 180, || shared.release.load(Acquire));
}

fn measure_record(
    archive: &AeronArchive,
    shared: &Shared,
    sessions: &[i32],
    finals: &mpsc::Receiver<(usize, i64)>,
    (length, rate, seconds): (usize, u64, u64),
) -> Res<()> {
    let streams = sessions.len();
    let mut recordings = Vec::with_capacity(streams);
    for (i, &session) in sessions.iter().enumerate() {
        let stream = RECORD_STREAM + i as i32;
        let deadline = Instant::now() + Duration::from_secs(10);
        let id = loop {
            if let Some(d) = archive.find_recording(|d| d.session_id() == session && d.stream_id() == stream)? {
                break d.recording_id;
            }
            if Instant::now() > deadline {
                return Err(format!("no recording of stream {stream} session {session} after 10 s").into());
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        eprintln!("arcload: stream {stream} session {session} recording {id}");
        recordings.push(id);
    }

    let start = Instant::now();
    shared.go.store(true, Release);
    let mb = |msgs: f64| msgs * length as f64 / 1e6;
    let (mut rates, mut mbs) = (Vec::new(), Vec::new());
    let (mut last_sent, mut last_retries, mut last_at) = (0, 0, start);
    for t in 1..=seconds {
        std::thread::sleep((start + Duration::from_secs(t)).saturating_duration_since(Instant::now()));
        let (sent, retries, now) = (shared.total(|s| &s.count), shared.total(|s| &s.retries), Instant::now());
        let dt = now.duration_since(last_at).as_secs_f64();
        let msgs = (sent - last_sent) as f64 / dt;
        println!(
            "arcload,sample,{},{t},{msgs:.0},{:.1},{:.0}",
            label(),
            mb(msgs),
            (retries - last_retries) as f64 / dt
        );
        rates.push(msgs);
        mbs.push(mb(msgs));
        (last_sent, last_retries, last_at) = (sent, retries, now);
    }

    shared.running.store(false, Release);
    let stopped = Instant::now();
    let mut targets = vec![0; streams];
    for _ in 0..streams {
        let (i, position) = finals
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| "timed out waiting for publishers to stop")?;
        targets[i] = position;
    }
    let mut pending: Vec<usize> = (0..streams).collect();
    while !pending.is_empty() {
        if stopped.elapsed() > Duration::from_secs(120) {
            return Err(format!("recordings {pending:?} did not catch up within 120 s").into());
        }
        let mut still = Vec::with_capacity(pending.len());
        for i in pending {
            let (id, target) = (recordings[i], targets[i]);
            let mut position = archive.get_recording_position(id)?;
            if position == NULL_POSITION {
                position = archive.get_stop_position(id)?;
                if position < target {
                    return Err(format!("recording {id} stopped at {position}, short of {target}").into());
                }
            }
            if position < target {
                still.push(i);
            }
        }
        pending = still;
        if !pending.is_empty() {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    let caught_up = Instant::now();

    let (sent, retries) = (shared.total(|s| &s.count), shared.total(|s| &s.retries));
    if rate > 0 {
        eprintln!(
            "arcload: {} of {sent} offers missed their slot",
            shared.total(|s| &s.missed)
        );
    }
    println!(
        "arcload,record,{},{streams},{length},{rate},{:.0},{:.1},{:.2},{:.3},{:.1}",
        label(),
        median(rates),
        median(mbs),
        100.0 * retries as f64 / (retries + sent).max(1) as f64,
        caught_up.duration_since(stopped).as_secs_f64(),
        mb(sent as f64) / caught_up.duration_since(start).as_secs_f64()
    );
    Ok(())
}

fn replay(args: &[String]) -> Res<()> {
    let concurrency: usize = arg(args, 0, "concurrency")?;
    let seconds: u64 = arg(args, 1, "seconds")?;
    let spec: String = arg(args, 2, "channel")?;
    let cpus = cpus(&arg::<String>(args, 3, "cpus")?)?;
    if concurrency == 0 {
        return Err("<concurrency> must be at least 1".into());
    }

    let (_aeron, archive) = connect()?;
    let mut recordings =
        archive.collect_recordings(|d| d.stop_position() != NULL_POSITION && d.stop_position() > d.start_position())?;
    recordings.sort_by_key(|d| std::cmp::Reverse(d.stop_position - d.start_position));
    recordings.truncate(concurrency);
    if recordings.is_empty() {
        return Err("the archive has no complete recordings to replay".into());
    }
    if recordings.len() < concurrency {
        eprintln!("arcload: only {} complete recordings to replay", recordings.len());
    }
    let channels = (0..recordings.len())
        .map(|i| channel(&spec, i))
        .collect::<Res<Vec<_>>>()?;

    let shared = Shared::new(recordings.len());
    let base = Instant::now();
    std::thread::scope(|scope| {
        let (ready_tx, ready) = mpsc::channel();
        for (i, channel) in channels.iter().enumerate() {
            let (ready_tx, shared) = (ready_tx.clone(), &shared);
            let (cpu, stop) = (cpus[i % cpus.len()], recordings[i].stop_position);
            scope.spawn(move || consume(shared, i, channel, stop, (cpu, base, ready_tx)));
        }
        drop(ready_tx);
        let result = measure_replay(&archive, &shared, &recordings, &channels, (&ready, base, seconds));
        shared.finish();
        result
    })
}

/// One replay's subscriber: reports once subscribed, waits for its replay's session (an
/// earlier run's lingering replay can still have an image on the same stream), then polls
/// that image until it reaches the recording's stop position (or ends) or `running` clears.
fn consume(
    shared: &Shared,
    i: usize,
    channel: &CStr,
    stop: i64,
    (cpu, base, ready): (Option<usize>, Instant, mpsc::Sender<usize>),
) {
    let stream = REPLAY_STREAM + i as i32;
    let aeron = client().expect("aeron client");
    // as in publish: pinned before the spinning blocking poll
    pin(cpu);
    let subscription = aeron
        .async_add_subscription(channel, stream, Handlers::NONE, Handlers::NONE)
        .and_then(|s| s.poll_blocking(Duration::from_secs(30)))
        .unwrap_or_else(|e| panic!("stream {stream}: add subscription: {e}"));
    let _ = ready.send(i);
    let stats = &shared.stats[i];
    wait_until("the replay to start", 30, || {
        stats.start_ns.load(Acquire) != 0 || !shared.running.load(Acquire)
    });
    let session = stats.session.load(Relaxed) as i32;
    let (mut bytes, mut image) = (0u64, None);
    let mut idle = BusySpinIdleStrategy::new();
    while shared.running.load(Relaxed) {
        let Some(image) = image.as_ref() else {
            image = subscription.image_by_session_id(session);
            idle.idle(0);
            continue;
        };
        let fragments = image
            .poll_fn(|message, _header| bytes += message.len() as u64, FRAGMENT_LIMIT)
            .unwrap_or_else(|e| panic!("stream {stream}: poll: {e}"));
        if fragments > 0 {
            if stats.first_ns.load(Relaxed) == 0 {
                stats.first_ns.store(base.elapsed().as_nanos() as u64, Release);
            }
            stats.count.store(bytes, Relaxed);
        } else if image.position() >= stop || image.is_end_of_stream() {
            stats.done.store(true, Release);
            return;
        } else {
            idle.idle(0);
        }
    }
}

fn measure_replay(
    archive: &AeronArchive,
    shared: &Shared,
    recordings: &[RecordingDescriptor],
    channels: &[CString],
    (ready, base, seconds): (&mpsc::Receiver<usize>, Instant, u64),
) -> Res<()> {
    for _ in recordings {
        ready
            .recv_timeout(Duration::from_secs(60))
            .map_err(|_| "timed out waiting for subscribers")?;
    }
    let start = Instant::now();
    let mut sessions = Vec::with_capacity(recordings.len());
    for (i, recording) in recordings.iter().enumerate() {
        let params = AeronArchiveReplayParams::builder()
            .position(recording.start_position)
            .build()?;
        let start_ns = base.elapsed().as_nanos() as u64;
        let session = archive
            .start_replay(recording.recording_id, &channels[i], REPLAY_STREAM + i as i32, &params)
            .map_err(|e| format!("start replay of recording {}: {e}", recording.recording_id))?;
        eprintln!(
            "arcload: replaying recording {} ({} bytes) to stream {}",
            recording.recording_id,
            recording.stop_position - recording.start_position,
            REPLAY_STREAM + i as i32
        );
        shared.stats[i].session.store(session, Relaxed);
        shared.stats[i].start_ns.store(start_ns, Release);
        sessions.push(session);
    }

    let deadline = start + Duration::from_secs(seconds);
    let (mut next_sample, mut t, mut last_bytes, mut last_at) = (start + Duration::from_secs(1), 1, 0, start);
    let result = loop {
        let now = Instant::now();
        if shared.stats.iter().all(|s| s.done.load(Acquire)) || now >= deadline {
            break Ok(now);
        }
        if now.duration_since(start) > Duration::from_secs(10)
            && let Some(i) = shared.stats.iter().position(|s| s.first_ns.load(Acquire) == 0)
        {
            break Err(format!(
                "the replay of recording {} sent no data within 10 s",
                recordings[i].recording_id
            ));
        }
        if now >= next_sample {
            let bytes = shared.total(|s| &s.count);
            let mbs = (bytes - last_bytes) as f64 / now.duration_since(last_at).as_secs_f64() / 1e6;
            println!("arcload,rsample,{},{t},{mbs:.1}", label());
            (next_sample, t, last_bytes, last_at) = (next_sample + Duration::from_secs(1), t + 1, bytes, now);
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    for (stats, &session) in shared.stats.iter().zip(&sessions) {
        if !stats.done.load(Acquire)
            && let Err(e) = archive.stop_replay(session)
        {
            eprintln!("arcload: stop replay {session}: {e}");
        }
    }
    let end = result?;

    let first_ms = shared
        .stats
        .iter()
        .filter(|s| s.first_ns.load(Acquire) > 0)
        .map(|s| (s.first_ns.load(Acquire) - s.start_ns.load(Relaxed)) as f64 / 1e6)
        .collect();
    let mb = shared.total(|s| &s.count) as f64 / 1e6;
    println!(
        "arcload,replay,{},{},{:.2},{:.1},{mb:.1},{}",
        label(),
        recordings.len(),
        median(first_ms),
        mb / end.duration_since(start).as_secs_f64(),
        shared.stats.iter().filter(|s| s.done.load(Acquire)).count()
    );
    Ok(())
}
