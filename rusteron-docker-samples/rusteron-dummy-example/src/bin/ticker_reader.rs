use log::{error, info};
use rusteron_archive::*;
use rusteron_dummy_example::{
    archive_connect, init_logger, start_media_driver, TICKER_CHANNEL, TICKER_STREAM_ID,
};
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;
use websocket_lite::Result;

fn main() -> Result<()> {
    init_logger();

    // just make sure it includes media driver in binary
    if 0 == Aeron::epoch_clock() {
        start_media_driver().unwrap();
    }

    let (archive, aeron) = archive_connect()?;

    let shutdown = rusteron_dummy_example::register_exit_signals()?;

    let mut archive_log_time = Instant::now()
        .checked_sub(Duration::from_secs(300))
        .unwrap();
    let archive_log = Duration::from_secs(120);

    let mut live_log_time = Instant::now()
        .checked_sub(Duration::from_secs(300))
        .unwrap();
    let live_log = Duration::from_secs(30);

    let mut record_reader = Handler::leak(RecorderDescriptorReader::default());
    let mut replay_msg_count_handler = Handler::leak(MessageCountHandler::default());
    let mut live_msg_count_handler = Handler::leak(MessageCountHandler::default());

    let channel = TICKER_CHANNEL;
    let stream_id = TICKER_STREAM_ID;

    let mut live_subscription: Option<AeronSubscription> = None;

    while !shutdown.load(Ordering::Acquire) {
        if archive_log_time.elapsed() > archive_log {
            archive_log_time = Instant::now();
            record_reader.reset();
            match archive.list_recordings_for_uri(
                0,
                i32::MAX,
                &channel.into_c_string(),
                stream_id,
                Some(&record_reader),
            ) {
                Ok(recordings) => {
                    info!(
                        "found {recordings} recordings [stopPos={:?}]",
                        record_reader.last_recording_with_stop_position
                    );

                    if let Some(record) = &record_reader.last_recording {
                        info!("trying replay merge {record:?}");
                        let session_id = record.session_id;

                        let replay_channel =
                            format!("aeron:udp?control-mode=manual|session-id={session_id}");
                        info!("replay channel {replay_channel}");
                        let subscription = aeron.add_subscription(
                            &replay_channel.clone().into_c_string(),
                            TICKER_STREAM_ID,
                            Handlers::no_available_image_handler(),
                            Handlers::no_unavailable_image_handler(),
                            Duration::from_secs(5),
                        )?;

                        let replay_destination = "aeron:udp?endpoint=localhost:0".to_string();
                        info!("replay destination {replay_destination}");

                        let live_destination = format!("aeron:udp?endpoint={TICKER_CHANNEL}");
                        info!("live destination {live_destination}");

                        let merge = AeronArchiveReplayMerge::new(
                            &subscription,
                            &archive,
                            &replay_channel.clone().into_c_string(),
                            &replay_destination.clone().into_c_string(),
                            &live_destination.clone().into_c_string(),
                            record.recording_id,
                            record.start_position,
                            Aeron::epoch_clock(),
                            10_000,
                        )?;

                        while !merge.is_merged() {
                            merge.poll_once(
                                |buff, _header| {
                                    println!("buffer {buff:?}");
                                },
                                1024,
                            )?;
                        }
                    }

                    if let Some(record) = &record_reader.last_recording_with_stop_position {
                        let params = AeronArchiveReplayParams::new(
                            0,
                            i32::MAX,
                            record.start_position,
                            record.stop_position - record.start_position,
                            0,
                            0,
                        )?;

                        // change from ephemeral port to real port
                        let replay_channel = aeron
                            .add_subscription(
                                &"aeron:udp?endpoint=localhost:0".into_c_string(),
                                stream_id,
                                Handlers::no_available_image_handler(),
                                Handlers::no_unavailable_image_handler(),
                                Duration::from_secs(5),
                            )?
                            .try_resolve_channel_endpoint_port_as_string(4096)?;
                        let recording_id = record.recording_id();
                        assert_eq!(record.recording_id(), recording_id);
                        info!(
                            "resolved replay channel: {replay_channel} [recording_id={recording_id}, replayingRecord={record:?}"
                        );
                        assert_eq!(record.recording_id(), recording_id);

                        let replay_session_id = archive.start_replay(
                            recording_id,
                            &replay_channel.clone().into_c_string(),
                            stream_id,
                            &params,
                        )?;
                        let session_id = replay_session_id as i32;

                        let channel_replay = format!("{channel}|session-id={session_id}");
                        info!("replay subscription {channel_replay}");
                        match aeron.add_subscription(
                            &channel_replay.to_string().into_c_string(),
                            stream_id,
                            Some(&Handler::leak(AeronAvailableImageLogger)),
                            Some(&Handler::leak(AeronUnavailableImageLogger)),
                            Duration::from_secs(5),
                        ) {
                            Ok(subscription) => {
                                replay_msg_count_handler.reset();
                                let time = Instant::now();

                                let mut count = 0;
                                while subscription
                                    .poll(Some(&replay_msg_count_handler), 1000)
                                    .is_ok()
                                    && !subscription.is_closed()
                                    && time.elapsed() < Duration::from_secs(60)
                                    && (count > 0 || time.elapsed() < Duration::from_secs(5))
                                {
                                    count += 1;
                                    // prevent live sub from building up
                                    if let Some(live_subscription) = &live_subscription {
                                        let _ = live_subscription
                                            .poll(Some(&live_msg_count_handler), 1000);
                                    }
                                }

                                info!(
                                    "replay finished of last inactive recording [took={:?} {:?}]",
                                    time.elapsed(),
                                    *replay_msg_count_handler
                                );
                            }
                            Err(err) => {
                                error!("failed to subscribe to replay channel in time {err:?}");
                            }
                        }
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
                    &channel.to_string().into_c_string(),
                    stream_id,
                    Handlers::no_available_image_handler(),
                    Handlers::no_unavailable_image_handler(),
                    Duration::from_millis(100),
                )
                .ok();
        }

        if let Some(live_subscription) = &live_subscription {
            let _ = live_subscription.poll(Some(&live_msg_count_handler), 1000);
        }

        if live_log_time.elapsed() > live_log {
            live_log_time = Instant::now();
            info!(
                "live channel sent {:?} since previous log",
                *live_msg_count_handler
            );
            live_msg_count_handler.reset();
        }
    }

    info!("shutting down");

    Ok(())
}

#[derive(Debug, Default)]
struct RecorderDescriptorReader {
    last_recording_with_stop_position: Option<AeronArchiveRecordingDescriptor>,
    last_recording: Option<AeronArchiveRecordingDescriptor>,
}

impl RecorderDescriptorReader {
    fn reset(&mut self) {
        self.last_recording_with_stop_position = None;
        self.last_recording = None;
    }
}

impl AeronArchiveRecordingDescriptorConsumerFuncCallback for RecorderDescriptorReader {
    fn handle_aeron_archive_recording_descriptor_consumer_func(
        &mut self,
        recording_descriptor: AeronArchiveRecordingDescriptor,
    ) {
        info!("found recording {recording_descriptor:?}");
        let copy = recording_descriptor.clone_struct();
        if recording_descriptor.stop_position > 0 {
            self.last_recording_with_stop_position = Some(copy.clone_struct());
        }
        assert_eq!(copy.recording_id, recording_descriptor.recording_id);
        assert_eq!(
            copy.control_session_id,
            recording_descriptor.control_session_id
        );
        assert_eq!(copy.mtu_length, recording_descriptor.mtu_length);
        assert_eq!(
            copy.source_identity_length,
            recording_descriptor.source_identity_length
        );
        assert_eq!(copy.deref(), recording_descriptor.deref());
        self.last_recording = Some(copy);
    }
}

#[derive(Debug, Default)]
struct MessageCountHandler {
    count: usize,
    bytes: usize,
}

impl MessageCountHandler {
    fn reset(&mut self) {
        self.count = 0;
        self.bytes = 0;
    }
}

impl AeronFragmentHandlerCallback for MessageCountHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        self.count += 1;
        self.bytes += buffer.len();
    }
}
