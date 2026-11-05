use log::{error, info};
use rusteron_archive::*;
use rusteron_dummy_example::{archive_connect, init_logger, start_media_driver, TICKER_CHANNEL, TICKER_STREAM_ID};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use websocket_lite::Result;

fn main() -> Result<()> {
    init_logger();

    // Include the Media Driver in the binary.
    if 0 == Aeron::epoch_clock() {
        start_media_driver().unwrap();
    }

    let (archive, aeron) = archive_connect()?;

    let shutdown = rusteron_dummy_example::register_exit_signals()?;

    let mut archive_log_time = Instant::now().checked_sub(Duration::from_secs(300)).unwrap();
    let archive_log = Duration::from_secs(120);

    let mut live_log_time = Instant::now().checked_sub(Duration::from_secs(300)).unwrap();
    let live_log = Duration::from_secs(30);

    let stream_id = TICKER_STREAM_ID;
    let (mut live_count, mut live_bytes) = (0usize, 0usize);
    let mut live_subscription: Option<AeronSubscription> = None;

    while !shutdown.load(Ordering::Acquire) {
        if archive_log_time.elapsed() > archive_log {
            archive_log_time = Instant::now();
            match archive.collect_recordings(|recording| recording.stream_id == stream_id) {
                Ok(recordings) => {
                    let last_stopped = recordings.iter().rev().find(|recording| recording.stop_position > 0);
                    info!("found {} recordings [lastStopped={last_stopped:?}]", recordings.len());

                    // merge only when there is no live stream: a writer restart starts a new session and recording
                    let live_connected = live_subscription.as_ref().is_some_and(|s| s.is_connected());
                    if let Some(record) = recordings.last().filter(|_| !live_connected) {
                        // free the live port before the merge adds it as a destination
                        live_subscription = None;
                        info!("trying replay merge {record:?}");
                        let replay_channel = format!("aeron:udp?control-mode=manual|session-id={}", record.session_id);
                        let subscription = aeron.add_subscription(
                            &replay_channel.as_str().into_c_string(),
                            stream_id,
                            Handlers::NONE,
                            Handlers::NONE,
                            Duration::from_secs(5),
                        )?;
                        let merge = AeronArchiveReplayMerge::new(
                            &subscription,
                            &archive,
                            &replay_channel.as_str().into_c_string(),
                            c"aeron:udp?endpoint=localhost:0",
                            // the writer's channel is already a full URI
                            &TICKER_CHANNEL.into_c_string(),
                            record.recording_id,
                            record.start_position,
                            Aeron::epoch_clock(),
                            10_000,
                        )?;

                        // poll only the merge until it finishes: the archive client must not be used meanwhile
                        let (mut count, mut bytes) = (0usize, 0usize);
                        while !merge.is_merged() && !shutdown.load(Ordering::Acquire) {
                            let polled = merge.poll_fn(
                                |buffer, _header| {
                                    count += 1;
                                    bytes += buffer.len();
                                },
                                1024,
                            );
                            // a failed merge (including its progress timeout) returns Err
                            if let Err(e) = polled {
                                error!("replay merge failed: {e}");
                                break;
                            }
                        }
                        info!(
                            "replay merge finished [merged={}, count={count}, bytes={bytes}]",
                            merge.is_merged()
                        );
                        // once merged the subscription is on the live stream, so keep reading it as the live one
                        if merge.is_merged() {
                            live_subscription = Some(subscription);
                        }
                    }

                    if let Some(record) = last_stopped {
                        let params = AeronArchiveReplayParams::builder()
                            .position(record.start_position)
                            .length(record.stop_position - record.start_position)
                            .build()?;
                        // keep the subscription bound for the whole replay: the archive replays to its port
                        let replay_subscription = aeron.add_subscription(
                            c"aeron:udp?endpoint=localhost:0",
                            stream_id,
                            Handlers::NONE,
                            Handlers::NONE,
                            Duration::from_secs(5),
                        )?;
                        let replay_channel = replay_subscription.try_resolve_channel_endpoint_port_as_string(4096)?;
                        info!("replaying {record:?} to {replay_channel}");
                        archive.start_replay(
                            record.recording_id,
                            &replay_channel.into_c_string(),
                            stream_id,
                            &params,
                        )?;

                        let (mut count, mut bytes) = (0usize, 0usize);
                        let started = Instant::now();
                        let mut idle_since = started;
                        while idle_since.elapsed() < Duration::from_secs(5)
                            && started.elapsed() < Duration::from_secs(60)
                            && !shutdown.load(Ordering::Acquire)
                        {
                            let read = replay_subscription.poll_fn(
                                |buffer, _header| {
                                    count += 1;
                                    bytes += buffer.len();
                                },
                                1000,
                            )?;
                            if read > 0 {
                                idle_since = Instant::now();
                            }
                            // prevent live sub from building up
                            if let Some(live_subscription) = &live_subscription {
                                live_subscription.poll_fn(
                                    |buffer, _header| {
                                        live_count += 1;
                                        live_bytes += buffer.len();
                                    },
                                    1000,
                                )?;
                            }
                        }
                        info!(
                            "replay finished of last inactive recording [took={:?}, count={count}, bytes={bytes}]",
                            started.elapsed()
                        );
                    }
                }
                Err(e) => {
                    // ideally should retry
                    error!("failed to read from aeron archiver {e}");
                }
            }
        }

        if live_subscription.is_none() {
            live_subscription = aeron
                .add_subscription(
                    &TICKER_CHANNEL.into_c_string(),
                    stream_id,
                    Handlers::NONE,
                    Handlers::NONE,
                    Duration::from_millis(100),
                )
                .ok();
        }

        if let Some(live_subscription) = &live_subscription {
            live_subscription.poll_fn(
                |buffer, _header| {
                    live_count += 1;
                    live_bytes += buffer.len();
                },
                1000,
            )?;
        }

        if live_log_time.elapsed() > live_log {
            live_log_time = Instant::now();
            info!("live channel sent {live_count} messages, {live_bytes} bytes since previous log");
            (live_count, live_bytes) = (0, 0);
        }
    }

    info!("shutting down");

    Ok(())
}
