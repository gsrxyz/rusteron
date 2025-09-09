/**/
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::all)]
#![allow(unused_unsafe)]
#![allow(unused_variables)]
#![doc = include_str!("../README.md")]
//! # Features
//!
//! - **`static`**: When enabled, this feature statically links the Aeron C code.
//!   By default, the library uses dynamic linking to the Aeron C libraries.
//! - **`backtrace`** - When enabled will log a backtrace for each AeronCError
//! - **`extra-logging`** - When enabled will log when resource is created and destroyed. useful if your seeing a segfault due to a resource being closed
//! - **`precompile`** - When enabled will use precompiled c code instead of requiring cmake and java to me installed

pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use bindings::*;
use std::cell::Cell;
use std::os::raw::c_int;
use std::time::{Duration, Instant};

pub mod testing;

include!(concat!(env!("OUT_DIR"), "/aeron.rs"));
include!(concat!(env!("OUT_DIR"), "/aeron_custom.rs"));

pub type SourceLocation = bindings::aeron_archive_source_location_t;
pub const SOURCE_LOCATION_LOCAL: aeron_archive_source_location_en =
    SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL;
pub const SOURCE_LOCATION_REMOTE: aeron_archive_source_location_en =
    SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_REMOTE;

pub struct NoOpAeronIdleStrategyFunc;

impl AeronIdleStrategyFuncCallback for NoOpAeronIdleStrategyFunc {
    fn handle_aeron_idle_strategy_func(&mut self, _work_count: c_int) -> () {}
}

pub struct RecordingPos;
impl RecordingPos {
    pub fn find_counter_id_by_session(
        counter_reader: &AeronCountersReader,
        session_id: i32,
    ) -> i32 {
        unsafe {
            aeron_archive_recording_pos_find_counter_id_by_session_id(
                counter_reader.get_inner(),
                session_id,
            )
        }
    }
    pub fn find_counter_id_by_recording_id(
        counter_reader: &AeronCountersReader,
        recording_id: i64,
    ) -> i32 {
        unsafe {
            aeron_archive_recording_pos_find_counter_id_by_recording_id(
                counter_reader.get_inner(),
                recording_id,
            )
        }
    }

    /// Return the recordingId embedded in the key of the given counter
    /// if it is indeed a "recording position" counter. Otherwise return -1.
    pub fn get_recording_id_block(
        counters_reader: &AeronCountersReader,
        counter_id: i32,
        wait: Duration,
    ) -> Result<i64, AeronCError> {
        let mut result = Self::get_recording_id(counters_reader, counter_id);
        let instant = Instant::now();

        while result.is_err() && instant.elapsed() < wait {
            result = Self::get_recording_id(counters_reader, counter_id);
            #[cfg(debug_assertions)]
            std::thread::sleep(Duration::from_millis(10));
        }

        return result;
    }

    /// Return the recordingId embedded in the key of the given counter
    /// if it is indeed a "recording position" counter. Otherwise return -1.
    pub fn get_recording_id(
        counters_reader: &AeronCountersReader,
        counter_id: i32,
    ) -> Result<i64, AeronCError> {
        /// The type id for an Aeron Archive recording position counter.
        /// In Aeron Java, this is AeronCounters.ARCHIVE_RECORDING_POSITION_TYPE_ID (which is typically 100).
        pub const RECORDING_POSITION_TYPE_ID: i32 = 100;

        /// from Aeron Java code
        pub const RECORD_ALLOCATED: i32 = 1;

        /// A constant to mean "no valid recording ID".
        pub const NULL_RECORDING_ID: i64 = -1;

        if counter_id < 0 {
            return Err(AeronCError::from_code(NULL_RECORDING_ID as i32));
        }

        let state = counters_reader.counter_state(counter_id)?;
        if state != RECORD_ALLOCATED {
            return Err(AeronCError::from_code(NULL_RECORDING_ID as i32));
        }

        let type_id = counters_reader.counter_type_id(counter_id)?;
        if type_id != RECORDING_POSITION_TYPE_ID {
            return Err(AeronCError::from_code(NULL_RECORDING_ID as i32));
        }

        // Read the key area. For a RECORDING_POSITION_TYPE_ID counter:
        //    - offset 0..8 => the i64 recording_id
        //    - offset 8..12 => the session_id (int)
        //    etc...
        // only need the first 8 bytes to get the recordingId.
        let recording_id = Cell::new(-1);
        counters_reader.foreach_counter_once(|value, id, type_id, key, label| {
            if id == counter_id && type_id == RECORDING_POSITION_TYPE_ID {
                let mut val = [0u8; 8];
                val.copy_from_slice(&key[0..8]);
                let Ok(value) = i64::from_le_bytes(val).try_into();
                recording_id.set(value);
            }
        });
        let recording_id = recording_id.get();
        if recording_id < 0 {
            return Err(AeronCError::from_code(NULL_RECORDING_ID as i32));
        }

        Ok(recording_id)
    }
}

unsafe extern "C" fn default_encoded_credentials(
    _clientd: *mut std::os::raw::c_void,
) -> *mut aeron_archive_encoded_credentials_t {
    // Allocate a zeroed instance of `aeron_archive_encoded_credentials_t`
    let empty_credentials = Box::new(aeron_archive_encoded_credentials_t {
        data: std::ptr::null(),
        length: 0,
    });
    Box::into_raw(empty_credentials)
}

impl AeronArchive {
    pub fn aeron(&self) -> Aeron {
        self.get_archive_context().get_aeron()
    }
}

impl AeronArchiveAsyncConnect {
    #[inline]
    /// recommend using this method instead of standard `new` as it will link the archive to aeron so if a drop occurs archive is dropped before aeron
    pub fn new_with_aeron(ctx: &AeronArchiveContext, aeron: &Aeron) -> Result<Self, AeronCError> {
        let resource_async = Self::new(ctx)?;
        resource_async.inner.add_dependency(aeron.clone());
        Ok(resource_async)
    }
}

macro_rules! impl_archive_position_methods {
    ($pub_type:ty) => {
        impl $pub_type {
            /// Retrieves the current active live archive position using the Aeron counters.
            /// Returns an error if not found.
            pub fn get_archive_position(&self) -> Result<i64, AeronCError> {
                if let Some(aeron) = self.inner.get_dependency::<Aeron>() {
                    let counter_reader = &aeron.counters_reader();
                    self.get_archive_position_with(counter_reader)
                } else {
                    Err(AeronCError::from_code(-1))
                }
            }

            /// Retrieves the current active live archive position using the provided counter reader.
            /// Returns an error if not found.
            pub fn get_archive_position_with(
                &self,
                counters: &AeronCountersReader,
            ) -> Result<i64, AeronCError> {
                let session_id = self.get_constants()?.session_id();
                let counter_id = RecordingPos::find_counter_id_by_session(counters, session_id);
                if counter_id < 0 {
                    return Err(AeronCError::from_code(counter_id));
                }
                let position = counters.get_counter_value(counter_id);
                if position < 0 {
                    return Err(AeronCError::from_code(position as i32));
                }
                Ok(position)
            }

            /// Checks if the publication's current position is within a specified inclusive length
            /// of the archive position.
            pub fn is_archive_position_with(&self, length_inclusive: usize) -> bool {
                let archive_position = self.get_archive_position().unwrap_or(-1);
                if archive_position < 0 {
                    return false;
                }
                self.position() - archive_position <= length_inclusive as i64
            }
        }
    };
}

impl_archive_position_methods!(AeronPublication);
impl_archive_position_methods!(AeronExclusivePublication);

impl AeronArchiveContext {
    // The method below sets no credentials supplier, which is essential for the operation
    // of the Aeron Archive Context. The `set_credentials_supplier` must be set to prevent
    // segmentation faults in the C bindings.
    pub fn set_no_credentials_supplier(&self) -> Result<i32, AeronCError> {
        self.set_credentials_supplier(
            Some(default_encoded_credentials),
            None,
            None::<&Handler<AeronArchiveCredentialsFreeFuncLogger>>,
        )
    }

    /// This method creates a new `AeronArchiveContext` with a no-op credentials supplier.
    /// If you do not set a credentials supplier, it will segfault.
    /// This method ensures that a non-functional credentials supplier is set to avoid the segfault.
    pub fn new_with_no_credentials_supplier(
        aeron: &Aeron,
        request_control_channel: &str,
        response_control_channel: &str,
        recording_events_channel: &str,
    ) -> Result<AeronArchiveContext, AeronCError> {
        let context = Self::new()?;
        context.set_no_credentials_supplier()?;
        context.set_aeron(aeron)?;
        context.set_control_request_channel(&request_control_channel.into_c_string())?;
        context.set_control_response_channel(&response_control_channel.into_c_string())?;
        context.set_recording_events_channel(&recording_events_channel.into_c_string())?;
        // see https://github.com/gsrxyz/rusteron/issues/18
        context.set_idle_strategy(Some(&Handler::leak(NoOpAeronIdleStrategyFunc)))?;
        Ok(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::{error, info};

    use crate::testing::EmbeddedArchiveMediaDriverProcess;
    use serial_test::serial;
    use std::cell::Cell;
    use std::error;
    use std::error::Error;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread::{sleep, JoinHandle};
    use std::time::{Duration, Instant};

    #[derive(Default, Debug)]
    struct ErrorCount {
        error_count: usize,
    }

    impl AeronErrorHandlerCallback for ErrorCount {
        fn handle_aeron_error_handler(&mut self, error_code: c_int, msg: &str) {
            error!("Aeron error {}: {}", error_code, msg);
            self.error_count += 1;
        }
    }

    pub const ARCHIVE_CONTROL_REQUEST: &str = "aeron:udp?endpoint=localhost:8010";
    pub const ARCHIVE_CONTROL_RESPONSE: &str = "aeron:udp?endpoint=localhost:8011";
    pub const ARCHIVE_RECORDING_EVENTS: &str =
        "aeron:udp?control-mode=dynamic|control=localhost:8012";

    #[test]
    fn test_uri_string_builder() -> Result<(), AeronCError> {
        let builder = AeronUriStringBuilder::default();

        builder.init_new()?;
        builder
            .media(Media::Udp)? // very important to set media else set_initial_position will give an error of -1
            .mtu_length(1024 * 64)?
            .set_initial_position(127424949617280, 1182294755, 65536)?;
        let uri = builder.build(1024)?;
        assert_eq!("aeron:udp?term-id=-1168322114|term-length=65536|mtu=65536|init-term-id=1182294755|term-offset=33408", uri);

        builder.init_new()?;
        let uri = builder
            .media(Media::Udp)?
            .control_mode(ControlMode::Dynamic)?
            .reliable(false)?
            .ttl(2)?
            .endpoint("localhost:1235")?
            .control("localhost:1234")?
            .build(1024)?;
        assert_eq!("aeron:udp?ttl=2|control-mode=dynamic|endpoint=localhost:1235|control=localhost:1234|reliable=false", uri);

        let uri = AeronUriStringBuilder::from_str("aeron:udp?endpoint=localhost:8010")?
            .ttl(5)?
            .build(1024)?;

        assert_eq!("aeron:udp?ttl=5|endpoint=localhost:8010", uri);

        let uri = uri.parse::<AeronUriStringBuilder>()?.ttl(6)?.build(1024)?;

        assert_eq!("aeron:udp?ttl=6|endpoint=localhost:8010", uri);

        Ok(())
    }

    pub const STREAM_ID: i32 = 1033;
    pub const MESSAGE_PREFIX: &str = "Message-Prefix-";
    pub const CONTROL_ENDPOINT: &str = "localhost:23265";
    pub const RECORDING_ENDPOINT: &str = "localhost:23266";
    pub const LIVE_ENDPOINT: &str = "localhost:23267";
    pub const REPLAY_ENDPOINT: &str = "localhost:0";
    // pub const REPLAY_ENDPOINT: &str = "localhost:23268";

    #[test]
    #[serial]
    fn test_simple_replay_merge() -> Result<(), AeronCError> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();

        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes()
            .expect("failed to kill all java processes");

        assert!(is_udp_port_available(23265));
        assert!(is_udp_port_available(23266));
        assert!(is_udp_port_available(23267));
        assert!(is_udp_port_available(23268));
        let id = Aeron::nano_clock();
        let aeron_dir = format!("target/aeron/{}/shm", id);
        let archive_dir = format!("target/aeron/{}/archive", id);

        info!("starting archive media driver");
        let media_driver = EmbeddedArchiveMediaDriverProcess::build_and_start(
            &aeron_dir,
            &format!("{}/archive", aeron_dir),
            ARCHIVE_CONTROL_REQUEST,
            ARCHIVE_CONTROL_RESPONSE,
            ARCHIVE_RECORDING_EVENTS,
        )
        .expect("Failed to start embedded media driver");

        info!("connecting to archive");
        let (archive, aeron) = media_driver
            .archive_connect()
            .expect("Could not connect to archive client");

        let running = Arc::new(AtomicBool::new(true));

        info!("connected to archive, adding publication");
        assert!(!aeron.is_closed());

        let (session_id, publisher_thread) =
            reply_merge_publisher(&archive, aeron.clone(), running.clone())?;

        {
            let context = AeronContext::new()?;
            context.set_dir(&media_driver.aeron_dir)?;
            let error_handler = Handler::leak(ErrorCount::default());
            context.set_error_handler(Some(&error_handler))?;
            let aeron = Aeron::new(&context)?;
            aeron.start()?;
            let aeron_archive_context = archive.get_archive_context();
            let aeron_archive_context = AeronArchiveContext::new_with_no_credentials_supplier(
                &aeron,
                aeron_archive_context.get_control_request_channel(),
                aeron_archive_context.get_control_response_channel(),
                aeron_archive_context.get_recording_events_channel(),
            )?;
            aeron_archive_context.set_error_handler(Some(&error_handler))?;
            let archive = AeronArchiveAsyncConnect::new_with_aeron(&aeron_archive_context, &aeron)?
                .poll_blocking(Duration::from_secs(30))
                .expect("failed to connect to archive");
            replay_merge_subscription(&archive, aeron.clone(), session_id)?;
        }

        running.store(false, Ordering::Release);
        publisher_thread.join().unwrap();

        Ok(())
    }

    fn reply_merge_publisher(
        archive: &AeronArchive,
        aeron: Aeron,
        running: Arc<AtomicBool>,
    ) -> Result<(i32, JoinHandle<()>), AeronCError> {
        let publication = aeron.add_publication(
            // &format!("aeron:udp?control={CONTROL_ENDPOINT}|control-mode=dynamic|term-length=65536|fc=tagged,g:99901/1,t:5s"),
            &format!("aeron:udp?control={CONTROL_ENDPOINT}|control-mode=dynamic|term-length=65536")
                .into_c_string(),
            STREAM_ID,
            Duration::from_secs(5),
        )?;

        info!(
            "publication {} [status={:?}]",
            publication.channel(),
            publication.channel_status()
        );
        assert_eq!(1, publication.channel_status());

        let session_id = publication.session_id();
        let recording_channel = format!(
            // "aeron:udp?endpoint={RECORDING_ENDPOINT}|control={CONTROL_ENDPOINT}|session-id={session_id}|gtag=99901"
            "aeron:udp?endpoint={RECORDING_ENDPOINT}|control={CONTROL_ENDPOINT}|session-id={session_id}"
        );
        info!("recording channel {}", recording_channel);
        archive.start_recording(
            &recording_channel.into_c_string(),
            STREAM_ID,
            SOURCE_LOCATION_REMOTE,
            true,
        )?;

        info!("waiting for publisher to be connected");
        while !publication.is_connected() {
            thread::sleep(Duration::from_millis(100));
        }
        info!("publisher to be connected");
        let counters_reader = aeron.counters_reader();
        let mut caught_up_count = 0;
        let publisher_thread = thread::spawn(move || {
            let mut message_count = 0;

            while running.load(Ordering::Acquire) {
                let message = format!("{}{}", MESSAGE_PREFIX, message_count);
                while publication.offer(
                    message.as_bytes(),
                    Handlers::no_reserved_value_supplier_handler(),
                ) <= 0
                {
                    thread::sleep(Duration::from_millis(10));
                }
                message_count += 1;
                if message_count % 10_000 == 0 {
                    info!(
                        "Published {} messages [position={}]",
                        message_count,
                        publication.position()
                    );
                }
                // slow down publishing so can catch up
                if message_count > 10_000 {
                    // ensure archiver is caught up
                    while !publication.is_archive_position_with(0) {
                        thread::sleep(Duration::from_micros(300));
                    }
                    caught_up_count += 1;
                }
            }
            assert!(caught_up_count > 0);
            info!("Publisher thread terminated");
        });
        Ok((session_id, publisher_thread))
    }

    fn replay_merge_subscription(
        archive: &AeronArchive,
        aeron: Aeron,
        session_id: i32,
    ) -> Result<(), AeronCError> {
        // let replay_channel = format!("aeron:udp?control-mode=manual|session-id={session_id}");
        let replay_channel = format!("aeron:udp?session-id={session_id}").into_c_string();
        info!("replay channel {:?}", replay_channel);

        let replay_destination = format!("aeron:udp?endpoint={REPLAY_ENDPOINT}").into_c_string();
        info!("replay destination {:?}", replay_destination);

        let live_destination =
            format!("aeron:udp?endpoint={LIVE_ENDPOINT}|control={CONTROL_ENDPOINT}")
                .into_c_string();
        info!("live destination {:?}", live_destination);

        let counters_reader = aeron.counters_reader();
        let mut counter_id = -1;

        while counter_id < 0 {
            counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
        }
        info!(
            "counter id {} {:?}",
            counter_id,
            counters_reader.get_counter_label(counter_id, 1024)
        );
        info!(
            "counter id {} position={:?}",
            counter_id,
            counters_reader.get_counter_value(counter_id)
        );

        // let recording_id = Cell::new(-1);
        // let start_position = Cell::new(-1);

        // let mut count = 0;
        // assert!(
        //     archive.list_recordings_once(&mut count, 0, 1000, |descriptor| {
        //         info!("Recording descriptor: {:?}", descriptor);
        //         recording_id.set(descriptor.recording_id);
        //         start_position.set(descriptor.start_position);
        //         assert_eq!(descriptor.session_id, session_id);
        //         assert_eq!(descriptor.stream_id, STREAM_ID);
        //     })? >= 0
        // );
        // assert!(count > 0);
        // assert!(recording_id.get() >= 0);

        // let record_id = RecordingPos::get_recording_id(&aeron.counters_reader(), counter_id)?;
        // assert_eq!(recording_id.get(), record_id);
        //
        // let recording_id = recording_id.get();
        // let start_position = start_position.get();
        let start_position = 0;
        let recording_id = RecordingPos::get_recording_id_block(
            &aeron.counters_reader(),
            counter_id,
            Duration::from_secs(5),
        )?;

        let subscribe_channel =
            format!("aeron:udp?control-mode=manual|session-id={session_id}").into_c_string();
        info!("subscribe channel {:?}", subscribe_channel);
        let subscription = aeron.add_subscription(
            &subscribe_channel,
            STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
            Duration::from_secs(5),
        )?;

        let replay_merge = AeronArchiveReplayMerge::new(
            &subscription,
            &archive,
            &replay_channel,
            &replay_destination,
            &live_destination,
            recording_id,
            start_position,
            Aeron::epoch_clock(),
            10_000,
        )?;

        info!(
            "ReplayMerge initialization: recordingId={}, startPosition={}, subscriptionChannel={:?}, replayChannel={:?}, replayDestination={:?}, liveDestination={:?}",
            recording_id,
            start_position,
            subscribe_channel,
            &replay_channel,
            &replay_destination,
            &live_destination
        );

        // media_driver
        //     .run_aeron_stats()
        //     .expect("Failed to run aeron stats");

        // info!("Waiting for subscription to connect...");
        // while !subscription.is_connected() {
        //     thread::sleep(Duration::from_millis(100));
        // }
        // info!("Subscription connected");

        info!(
            "about to start_replay [maxRecordPosition={:?}]",
            archive.get_max_recorded_position(recording_id)
        );

        let mut reply_count = 0;
        while !replay_merge.is_merged() {
            assert!(!replay_merge.has_failed());
            if replay_merge.poll_once(
                |buffer, _header| {
                    reply_count += 1;
                    if reply_count % 10_000 == 0 {
                        info!(
                            "replay-merge [count={}, isMerged={}, isLive={}]",
                            reply_count,
                            replay_merge.is_merged(),
                            replay_merge.is_live_added()
                        );
                    }
                },
                100,
            )? == 0
            {
                let err = archive.poll_for_error_response_as_string(4096)?;
                if !err.is_empty() {
                    panic!("{}", err);
                }
                if Aeron::errmsg().len() > 0 && "no error" != Aeron::errmsg() {
                    panic!("{}", Aeron::errmsg());
                }
                archive.poll_for_recording_signals()?;
                thread::sleep(Duration::from_millis(100));
            }
        }
        assert!(!replay_merge.has_failed());
        assert!(replay_merge.is_live_added());
        assert!(reply_count > 10_000);
        Ok(())
    }

    #[test]
    fn version_check() {
        let major = unsafe { crate::aeron_version_major() };
        let minor = unsafe { crate::aeron_version_minor() };
        let patch = unsafe { crate::aeron_version_patch() };

        let aeron_version = format!("{}.{}.{}", major, minor, patch);

        let cargo_version = "1.48.6";
        assert_eq!(aeron_version, cargo_version);
    }

    use std::thread;

    pub fn start_aeron_archive() -> Result<
        (
            Aeron,
            AeronArchiveContext,
            EmbeddedArchiveMediaDriverProcess,
        ),
        Box<dyn Error>,
    > {
        let id = Aeron::nano_clock();
        let aeron_dir = format!("target/aeron/{}/shm", id);
        let archive_dir = format!("target/aeron/{}/archive", id);

        let request_port = find_unused_udp_port(8000).expect("Could not find port");
        let response_port = find_unused_udp_port(request_port + 1).expect("Could not find port");
        let recording_event_port =
            find_unused_udp_port(response_port + 1).expect("Could not find port");
        let request_control_channel = &format!("aeron:udp?endpoint=localhost:{}", request_port);
        let response_control_channel = &format!("aeron:udp?endpoint=localhost:{}", response_port);
        let recording_events_channel =
            &format!("aeron:udp?endpoint=localhost:{}", recording_event_port);
        assert_ne!(request_control_channel, response_control_channel);

        let archive_media_driver = EmbeddedArchiveMediaDriverProcess::build_and_start(
            &aeron_dir,
            &archive_dir,
            request_control_channel,
            response_control_channel,
            recording_events_channel,
        )
        .expect("Failed to start Java process");

        let aeron_context = AeronContext::new()?;
        aeron_context.set_dir(&aeron_dir.into_c_string())?;
        aeron_context.set_client_name(&"test".into_c_string())?;
        aeron_context.set_publication_error_frame_handler(Some(&Handler::leak(
            AeronPublicationErrorFrameHandlerLogger,
        )))?;
        let error_handler = Handler::leak(ErrorCount::default());
        aeron_context.set_error_handler(Some(&error_handler))?;
        let aeron = Aeron::new(&aeron_context)?;
        aeron.start()?;

        let archive_context = AeronArchiveContext::new_with_no_credentials_supplier(
            &aeron,
            request_control_channel,
            response_control_channel,
            recording_events_channel,
        )?;
        archive_context.set_error_handler(Some(&error_handler))?;
        Ok((aeron, archive_context, archive_media_driver))
    }

    #[test]
    #[serial]
    pub fn test_aeron_archive() -> Result<(), Box<dyn error::Error>> {
        let _ = env_logger::Builder::new()
            .is_test(true)
            .filter_level(log::LevelFilter::Info)
            .try_init();
        EmbeddedArchiveMediaDriverProcess::kill_all_java_processes()
            .expect("failed to kill all java processes");

        let (aeron, archive_context, media_driver) = start_aeron_archive()?;

        assert!(!aeron.is_closed());

        info!("connected to aeron");

        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to aeron archive media driver");

        assert!(archive.get_archive_id() > 0);

        let channel = AERON_IPC_STREAM;
        let stream_id = 10;

        let subscription_id =
            archive.start_recording(channel, stream_id, SOURCE_LOCATION_LOCAL, true)?;

        assert!(subscription_id >= 0);
        info!("subscription id {}", subscription_id);

        let publication = aeron
            .async_add_exclusive_publication(channel, stream_id)?
            .poll_blocking(Duration::from_secs(5))?;

        for i in 0..11 {
            while publication.offer(
                "123456".as_bytes(),
                Handlers::no_reserved_value_supplier_handler(),
            ) <= 0
            {
                sleep(Duration::from_millis(50));
                archive.poll_for_recording_signals()?;
                let err = archive.poll_for_error_response_as_string(4096)?;
                if !err.is_empty() {
                    panic!("{}", err);
                }
                archive.idle();
            }
            info!("sent message {i} [test_aeron_archive]");
        }

        archive.idle();
        let session_id = publication.get_constants()?.session_id;
        info!("publication session id {}", session_id);
        // since this is single threaded need to make sure it did write to archiver, usually not required in multi-proccess app
        let stop_position = publication.position();
        info!(
            "publication stop position {} [publication={:?}]",
            stop_position,
            publication.get_constants()
        );
        let counters_reader = aeron.counters_reader();
        info!("counters reader ready {:?}", counters_reader);

        let mut counter_id = -1;

        let start = Instant::now();
        while counter_id <= 0 && start.elapsed() < Duration::from_secs(5) {
            counter_id = RecordingPos::find_counter_id_by_session(&counters_reader, session_id);
            info!("counter id {}", counter_id);
        }

        assert!(counter_id >= 0);

        info!("counter id {counter_id}, session id {session_id}");
        while counters_reader.get_counter_value(counter_id) < stop_position {
            info!(
                "current archive publication stop position {}",
                counters_reader.get_counter_value(counter_id)
            );
            sleep(Duration::from_millis(50));
        }
        info!(
            "found archive publication stop position {}",
            counters_reader.get_counter_value(counter_id)
        );

        archive.stop_recording_channel_and_stream(channel, stream_id)?;
        drop(publication);

        info!("list recordings");
        let found_recording_id = Cell::new(-1);
        let start_pos = Cell::new(-1);
        let end_pos = Cell::new(-1);
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(5) && found_recording_id.get() == -1 {
            let mut count = 0;
            archive.list_recordings_for_uri_once(
                &mut count,
                0,
                i32::MAX,
                channel,
                stream_id,
                |d: AeronArchiveRecordingDescriptor| {
                    assert_eq!(d.stream_id, stream_id);
                    info!("found recording {:#?}", d);
                    info!(
                        "strippedChannel={}, originalChannel={}",
                        d.stripped_channel(),
                        d.original_channel()
                    );
                    if d.stop_position > d.start_position && d.stop_position > 0 {
                        found_recording_id.set(d.recording_id);
                        start_pos.set(d.start_position);
                        end_pos.set(d.stop_position);
                    }

                    // verify clone_struct works
                    let copy = d.clone_struct();
                    assert_eq!(copy.deref(), d.deref());
                    assert_eq!(copy.recording_id, d.recording_id);
                    assert_eq!(copy.control_session_id, d.control_session_id);
                    assert_eq!(copy.mtu_length, d.mtu_length);
                    assert_eq!(copy.source_identity_length, d.source_identity_length);
                },
            )?;
            archive.poll_for_recording_signals()?;
            let err = archive.poll_for_error_response_as_string(4096)?;
            if !err.is_empty() {
                panic!("{}", err);
            }
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        info!("start replay");
        let params = AeronArchiveReplayParams::new(
            0,
            i32::MAX,
            start_pos.get(),
            end_pos.get() - start_pos.get(),
            0,
            0,
        )?;
        info!("replay params {:#?}", params);
        let replay_stream_id = 45;
        let replay_session_id =
            archive.start_replay(found_recording_id.get(), channel, replay_stream_id, &params)?;
        let session_id = replay_session_id as i32;

        info!("replay session id {}", replay_session_id);
        info!("session id {}", session_id);
        let channel_replay =
            format!("{}?session-id={}", channel.to_str().unwrap(), session_id).into_c_string();
        info!("archive id: {}", archive.get_archive_id());

        info!("add subscription {:?}", channel_replay);
        let subscription = aeron
            .async_add_subscription(
                &channel_replay,
                replay_stream_id,
                Some(&Handler::leak(AeronAvailableImageLogger)),
                Some(&Handler::leak(AeronUnavailableImageLogger)),
            )?
            .poll_blocking(Duration::from_secs(10))?;

        #[derive(Default)]
        struct FragmentHandler {
            count: Cell<usize>,
        }

        impl AeronFragmentHandlerCallback for FragmentHandler {
            fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
                assert_eq!(buffer, "123456".as_bytes());

                // Update count (using Cell for interior mutability)
                self.count.set(self.count.get() + 1);
            }
        }

        let poll = Handler::leak(FragmentHandler::default());

        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(10) && subscription.poll(Some(&poll), 100)? <= 0
        {
            let err = archive.poll_for_error_response_as_string(4096)?;
            if !err.is_empty() {
                panic!("{}", err);
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "messages not received {:?}",
            poll.count
        );
        info!("aeron {:?}", aeron);
        info!("ctx {:?}", archive_context);
        assert_eq!(11, poll.count.get());
        Ok(())
    }

    #[test]
    #[serial]
    fn test_invalid_recording_channel() -> Result<(), Box<dyn Error>> {
        let (aeron, archive_context, _media_driver) = start_aeron_archive()?;
        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        let invalid_channel = "invalid:channel".into_c_string();
        let result =
            archive.start_recording(&invalid_channel, STREAM_ID, SOURCE_LOCATION_LOCAL, true);
        assert!(
            result.is_err(),
            "Expected error when starting recording with an invalid channel"
        );
        Ok(())
    }

    #[test]
    #[serial]
    fn test_stop_recording_on_nonexistent_channel() -> Result<(), Box<dyn Error>> {
        let (aeron, archive_context, _media_driver) = start_aeron_archive()?;
        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        let nonexistent_channel = &"aeron:udp?endpoint=localhost:9999".into_c_string();
        let result = archive.stop_recording_channel_and_stream(nonexistent_channel, STREAM_ID);
        assert!(
            result.is_err(),
            "Expected error when stopping recording on a non-existent channel"
        );
        Ok(())
    }

    #[test]
    #[serial]
    fn test_replay_with_invalid_recording_id() -> Result<(), Box<dyn Error>> {
        let (aeron, archive_context, _media_driver) = start_aeron_archive()?;
        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        let invalid_recording_id = -999;
        let params = AeronArchiveReplayParams::new(0, i32::MAX, 0, 100, 0, 0)?;
        let result = archive.start_replay(
            invalid_recording_id,
            &"aeron:udp?endpoint=localhost:8888".into_c_string(),
            STREAM_ID,
            &params,
        );
        assert!(
            result.is_err(),
            "Expected error when starting replay with an invalid recording id"
        );
        Ok(())
    }

    #[test]
    #[serial]
    fn test_archive_reconnect_after_close() -> Result<(), Box<dyn std::error::Error>> {
        let (aeron, archive_context, media_driver) = start_aeron_archive()?;
        let archive_connector =
            AeronArchiveAsyncConnect::new_with_aeron(&archive_context.clone(), &aeron)?;
        let archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to connect to archive");

        drop(archive);

        let archive_connector = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron)?;
        let new_archive = archive_connector
            .poll_blocking(Duration::from_secs(30))
            .expect("failed to reconnect to archive");
        assert!(
            new_archive.get_archive_id() > 0,
            "Reconnected archive should have a valid archive id"
        );

        drop(media_driver);
        Ok(())
    }
}
