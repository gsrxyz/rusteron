use crate::IntoCString;
use crate::{Aeron, AeronArchive, AeronArchiveAsyncConnect, AeronArchiveContext, AeronCError, AeronContext};
use log::info;
use log::{error, warn};
use regex::Regex;
use std::backtrace::Backtrace;
use std::ffi::CString;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, Weak};
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::{fs, io, panic, process};

/// Check whether the test is running under Valgrind, used to scale Rust-side
/// timeouts that would otherwise fire before their C counterpart finishes.
pub(crate) fn running_under_valgrind() -> bool {
    std::env::var_os("RUSTERON_VALGRIND").is_some()
}

/// Return a `Duration` scaled by 3× when running under Valgrind: the same
/// multiplier the client crate uses for driver / liveness timeouts.
pub(crate) fn valgrind_timeout(base_secs: u64) -> Duration {
    if running_under_valgrind() {
        Duration::from_secs(base_secs.saturating_mul(3))
    } else {
        Duration::from_secs(base_secs)
    }
}

static ARCHIVE_PROCESSES: Mutex<Vec<Weak<Mutex<Child>>>> = Mutex::new(Vec::new());

pub struct EmbeddedArchiveMediaDriverProcess {
    child: Arc<Mutex<Child>>,
    owns_driver: bool,
    pub aeron_dir: CString,
    pub archive_dir: CString,
    pub control_request_channel: String,
    pub control_response_channel: String,
    pub recording_events_channel: String,
}

impl EmbeddedArchiveMediaDriverProcess {
    /// Builds missing Aeron jars with Gradle and starts an Archive Media Driver.
    ///
    /// Uses the Gradle wrapper in `aeron` under `CARGO_MANIFEST_DIR`. The build
    /// writes jars to `aeron/aeron-all/build/libs`. Java must be on `PATH`.
    /// The returned guard stops the process and removes its directories on drop.
    ///
    /// # Parameters
    /// - `aeron_dir`: Media Driver directory for IPC and client communication.
    /// - `archive_dir`: Directory for recordings and archive metadata.
    /// - `control_request_channel`: URI for archive control requests.
    /// - `control_response_channel`: URI for archive control responses.
    /// - `recording_events_channel`: URI for recording event notifications.
    ///
    /// # Errors
    /// Returns an error if the Gradle build or process startup fails.
    ///
    /// # Example
    /// ```
    /// use rusteron_archive::testing::EmbeddedArchiveMediaDriverProcess;
    /// let driver = EmbeddedArchiveMediaDriverProcess::build_and_start(
    ///     "/tmp/aeron-dir",
    ///     "/tmp/archive-dir",
    ///     "aeron:udp?endpoint=localhost:8010",
    ///     "aeron:udp?endpoint=localhost:8011",
    ///     "aeron:udp?endpoint=localhost:8012",
    /// ).expect("Failed to build and start Aeron Archive Media Driver");
    /// ```
    ///
    pub fn build_and_start(
        aeron_dir: &str,
        archive_dir: &str,
        control_request_channel: &str,
        control_response_channel: &str,
        recording_events_channel: &str,
    ) -> io::Result<Self> {
        let path = std::path::MAIN_SEPARATOR;
        let gradle = if cfg!(target_os = "windows") {
            &format!("{}{path}aeron{path}gradlew.bat", env!("CARGO_MANIFEST_DIR"),)
        } else {
            "./gradlew"
        };
        let dir = format!("{}{path}aeron", env!("CARGO_MANIFEST_DIR"),);
        info!("running {} in {}", gradle, dir);

        if !Path::new(&format!(
            "{}{path}aeron{path}aeron-all{path}build{path}libs",
            env!("CARGO_MANIFEST_DIR")
        ))
        .exists()
        {
            Command::new(&gradle)
                .current_dir(dir)
                .args([":aeron-samples:jar", ":aeron-archive:jar", ":aeron-all:build"])
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()?
                .wait()?;
        }

        return Self::start(
            &aeron_dir,
            archive_dir,
            control_request_channel,
            control_response_channel,
            recording_events_channel,
        );
    }

    pub fn run_aeron_stats(&self) -> std::io::Result<Child> {
        let main_dir = env!("CARGO_MANIFEST_DIR");
        let dir = format!("{}/{}", main_dir, &self.aeron_dir.to_str().unwrap());
        info!("running 'just aeron-stat {}'", dir);
        Command::new("just")
            .args(["aeron-stat", dir.as_str()])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
    }

    /// An archive context on `aeron` with this archive's control request, control response
    /// and recording events channels.
    ///
    /// # Errors
    ///
    /// Returns the Aeron error if the context cannot be created or rejects a setting.
    pub fn archive_context(&self, aeron: &Aeron) -> Result<AeronArchiveContext, AeronCError> {
        let context = AeronArchiveContext::new()?;
        context.set_aeron(aeron)?;
        context.set_control_request_channel(&self.control_request_channel.as_str().into_c_string())?;
        context.set_control_response_channel(&self.control_response_channel.as_str().into_c_string())?;
        context.set_recording_events_channel(&self.recording_events_channel.as_str().into_c_string())?;
        Ok(context)
    }

    pub fn archive_connect(&self) -> Result<(AeronArchive, Aeron), io::Error> {
        let start = Instant::now();
        let deadline = valgrind_timeout(30);
        while start.elapsed() < deadline {
            if let Ok(aeron_context) = AeronContext::new() {
                aeron_context.set_dir(&self.aeron_dir).expect("invalid dir");
                aeron_context
                    .set_client_name(&CString::new("unit_test_client")?)
                    .expect("invalid client name");
                if let Ok(aeron) = Aeron::new(&aeron_context) {
                    if aeron.start().is_ok() {
                        if let Ok(archive_context) = self.archive_context(&aeron) {
                            if let Ok(connect) = AeronArchiveAsyncConnect::new_with_aeron(&archive_context, &aeron) {
                                if let Ok(archive) = connect.poll_blocking(valgrind_timeout(10)) {
                                    let i = archive.get_archive_id();
                                    assert!(i > 0);
                                    info!("aeron archive media driver is up [connected with archive id {i}]");
                                    sleep(Duration::from_millis(100));
                                    return Ok((archive, aeron));
                                };
                            }
                        }
                        error!("aeron error: {}", Aeron::errmsg());
                    }
                }
            }
            info!("waiting for aeron to start up, retrying...");
        }

        assert!(start.elapsed() < deadline, "failed to start up aeron media driver");

        return Err(std::io::Error::other("unable to start up aeron media driver client"));
    }

    /// Starts an Archive Media Driver using the existing Aeron jars.
    ///
    /// Removes and recreates `aeron_dir` and `archive_dir` before launching Java.
    /// Requires jars under `aeron/aeron-all/build/libs` in `CARGO_MANIFEST_DIR`.
    /// The returned guard stops the process and removes its directories on drop.
    /// See [`Self::build_and_start`] for the channel parameters.
    ///
    /// # Errors
    /// Returns an error if directory setup, jar discovery or Java startup fails.
    ///
    /// # Example
    /// ```
    /// use rusteron_archive::testing::EmbeddedArchiveMediaDriverProcess;
    /// let driver = EmbeddedArchiveMediaDriverProcess::start(
    ///     "/tmp/aeron-dir",
    ///     "/tmp/archive-dir",
    ///     "aeron:udp?endpoint=localhost:8010",
    ///     "aeron:udp?endpoint=localhost:8011",
    ///     "aeron:udp?endpoint=localhost:8012",
    /// ).expect("Failed to start Aeron Archive Media Driver");
    /// ```
    ///
    pub fn start(
        aeron_dir: &str,
        archive_dir: &str,
        control_request_channel: &str,
        control_response_channel: &str,
        recording_events_channel: &str,
    ) -> io::Result<Self> {
        Self::start_process(
            aeron_dir,
            archive_dir,
            control_request_channel,
            control_response_channel,
            recording_events_channel,
            None,
        )
    }

    /// Start only an Archive, preserving the existing shared Media Driver directory.
    #[cfg(test)]
    pub(crate) fn start_on_existing_driver(
        &self,
        archive_dir: &str,
        control_request_channel: &str,
        control_response_channel: &str,
        recording_events_channel: &str,
        archive_id: i64,
    ) -> io::Result<Self> {
        Self::start_process(
            self.aeron_dir.to_str().unwrap(),
            archive_dir,
            control_request_channel,
            control_response_channel,
            recording_events_channel,
            Some(archive_id),
        )
    }

    fn start_process(
        aeron_dir: &str,
        archive_dir: &str,
        control_request_channel: &str,
        control_response_channel: &str,
        recording_events_channel: &str,
        existing_driver_archive_id: Option<i64>,
    ) -> io::Result<Self> {
        if existing_driver_archive_id.is_none() {
            Self::clean_directory(aeron_dir)?;
        }
        Self::clean_directory(archive_dir)?;

        // Create the directories before launching Java.
        fs::create_dir_all(aeron_dir)?;
        fs::create_dir_all(archive_dir)?;

        let binding = fs::read_dir(format!("{}/aeron/aeron-all/build/libs", env!("CARGO_MANIFEST_DIR")))?
            .filter(|f| f.is_ok())
            .map(|f| f.unwrap())
            .filter(|f| f.file_name().to_string_lossy().to_string().ends_with(".jar"))
            .next()
            .unwrap()
            .path();
        let mut jar_path = binding.to_str().unwrap();
        assert!(fs::exists(jar_path).unwrap_or_default());
        let mut args = vec![];
        let separator = if cfg!(target_os = "windows") { ";" } else { ":" };

        let combined_jars = format!(
            "{}{separator}{}",
            jar_path,
            jar_path.replace("aeron-all", "aeron-archive")
        );
        jar_path = &combined_jars;

        args.push("--add-opens".to_string());
        args.push("java.base/jdk.internal.misc=ALL-UNNAMED".to_string());
        args.push("-cp".to_string());
        args.push(jar_path.to_string());
        args.push(format!("-Daeron.dir={}", aeron_dir));
        args.push(format!("-Daeron.archive.dir={}", archive_dir));
        args.push("-Daeron.spies.simulate.connection=true".to_string());
        args.push("-Daeron.event.log=all".to_string());
        args.push("-Daeron.event.log.disable=FRAME_IN,FRAME_OUT".to_string());
        args.push("-Daeron.event.archive.log=all".to_string());
        args.push("-Daeron.event.cluster.log=all".to_string());
        args.push("-Dagrona.disable.bounds.checks=true".to_string());
        args.push(format!("-Daeron.archive.control.channel={}", control_request_channel));
        args.push(format!(
            "-Daeron.archive.control.response.channel={}",
            control_response_channel
        ));
        args.push(format!(
            "-Daeron.archive.recording.events.channel={}",
            recording_events_channel
        ));
        args.push("-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0".to_string());
        args.push("-Daeron.client.liveness.timeout=60000000000".to_string());
        args.push("-Daeron.image.liveness.timeout=60000000000".to_string());
        args.push("-Daeron.publication.unblock.timeout=65000000000".to_string());
        if let Some(archive_id) = existing_driver_archive_id {
            args.push(format!("-Daeron.archive.id={archive_id}"));
            // The first Archive also listens on the default local control stream.
            args.push("-Daeron.archive.local.control.stream.id=15402".to_string());
            args.push("io.aeron.archive.Archive".to_string());
        } else {
            args.push("io.aeron.archive.ArchivingMediaDriver".to_string());
        }

        info!("starting archive media driver [\n\tjava {}\n]", args.join(" "));

        let child = Command::new("java")
            .args(args)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;
        let child = Arc::new(Mutex::new(child));
        let mut processes = ARCHIVE_PROCESSES.lock().expect("archive process registry poisoned");
        processes.retain(|process| process.strong_count() > 0);
        processes.push(Arc::downgrade(&child));
        drop(processes);

        info!(
            "started archive media driver [{:?}",
            fs::read_dir(aeron_dir)?.collect::<Vec<_>>()
        );

        Ok(EmbeddedArchiveMediaDriverProcess {
            child,
            owns_driver: existing_driver_archive_id.is_none(),
            aeron_dir: aeron_dir.into_c_string(),
            archive_dir: archive_dir.into_c_string(),
            control_request_channel: control_request_channel.to_string(),
            control_response_channel: control_response_channel.to_string(),
            recording_events_channel: recording_events_channel.to_string(),
        })
    }

    fn clean_directory(dir: &str) -> io::Result<()> {
        info!("cleaning directory {}", dir);
        let path = Path::new(dir);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    /// Stops only archive Java children launched by this process, never unrelated JVMs.
    pub fn kill_all_java_processes() -> io::Result<ExitStatus> {
        let mut processes = ARCHIVE_PROCESSES.lock().expect("archive process registry poisoned");
        let mut status = ExitStatus::default();
        for process in processes.iter().filter_map(Weak::upgrade) {
            let mut child = process.lock().expect("archive child lock poisoned");
            if let Some(exited) = child.try_wait()? {
                status = exited;
            } else {
                child.kill()?;
                status = child.wait()?;
            }
        }
        processes.retain(|process| process.strong_count() > 0);
        Ok(status)
    }
}

// Stop the child before removing its directories.
impl Drop for EmbeddedArchiveMediaDriverProcess {
    fn drop(&mut self) {
        warn!("WARN: stopping aeron archive media driver!!!!");
        let mut child = self.child.lock().expect("archive child lock poisoned");
        match child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Err(e) = child.kill() {
                    error!("Failed to kill Java process: {}", e);
                }
                if let Err(e) = child.wait() {
                    error!("Failed to reap Java process: {}", e);
                }
            }
            Err(e) => error!("Failed to check Java process: {}", e),
        }

        // Clean up directories after the process has terminated
        if self.owns_driver {
            if let Err(e) = Self::clean_directory(&self.aeron_dir.to_str().unwrap()) {
                error!("Failed to clean up Aeron directory: {}", e);
            }
        }
        if let Err(e) = Self::clean_directory(&self.archive_dir.to_str().unwrap()) {
            error!("Failed to clean up Archive directory: {}", e);
        }
    }
}

/// True if a `java` executable is resolvable on `PATH`.
///
/// The embedded archive media driver is a Java process, so the
/// persistent-subscription tests use this to **skip themselves** when Java
/// isn't installed, instead of being blanket `#[ignore]`d. With Java present
/// they run normally under `cargo test`; without it they no-op and pass.
pub fn java_available() -> bool {
    Command::new("java")
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

/// Place at the top of any test that needs the Java archive. Skips the test
/// (early-returns `Ok(())`) when `java` is not on `PATH`. Requires the test to
/// return a `Result<(), _>`.
#[macro_export]
macro_rules! skip_unless_java {
    () => {
        if !$crate::testing::java_available() {
            eprintln!("skipping: java not available on PATH");
            return Ok(());
        }
    };
}

pub fn set_panic_hook() {
    panic::set_hook(Box::new(|info| {
        // Get the backtrace
        let backtrace = Backtrace::force_capture();
        error!("Stack trace: {backtrace:#?}");

        let backtrace = format!("{:?}", backtrace);
        // Regular expression to match the function, file, and line
        let re = Regex::new(r#"fn: "([^"]+)", file: "([^"]+)", line: (\d+)"#).unwrap();

        // Extract and print in IntelliJ format with function
        for cap in re.captures_iter(&backtrace) {
            let function = &cap[1];
            let file = &cap[2];
            let line = &cap[3];
            info!("{file}:{line} in {function}");
        }

        error!("Panic occurred: {:#?}", info);

        if let Some(payload) = info.payload().downcast_ref::<&str>() {
            error!("Panic message: {}", payload);
        } else if let Some(payload) = info.payload().downcast_ref::<String>() {
            error!("Panic message: {}", payload);
        } else {
            // If it's not a &str or String, try to print it as Debug
            error!("Panic with non-standard payload: {:?}", info.payload().type_id());
        }

        warn!("shutdown");

        process::abort();
    }))
}

/// First free UDP port at or above `start` (binds a probe socket to check).
pub fn find_unused_udp_port(start: u16) -> Option<u16> {
    (start..65535).find(|p| std::net::UdpSocket::bind(("127.0.0.1", *p)).is_ok())
}

/// Waits for a recording counter in any archive. Prefer
/// [`find_counter_id_by_session_in_archive_blocking`] when an Archive client is available.
pub fn find_counter_id_by_session_blocking(
    counters_reader: &crate::AeronCountersReader,
    session_id: i32,
    wait: Duration,
) -> Result<i32, crate::AeronCError> {
    find_counter_id_blocking(wait, || {
        crate::RecordingPos::find_counter_id_by_session(counters_reader, session_id)
    })
}

/// Waits for the recording counter belonging to `archive_id` (Aeron 1.54+).
/// A session may be recorded by multiple Archives sharing the same Media Driver.
pub fn find_counter_id_by_session_in_archive_blocking(
    counters_reader: &crate::AeronCountersReader,
    session_id: i32,
    archive_id: i64,
    wait: Duration,
) -> Result<i32, crate::AeronCError> {
    find_counter_id_blocking(wait, || {
        crate::RecordingPos::find_counter_id_by_session_in_archive(counters_reader, session_id, archive_id)
    })
}

fn find_counter_id_blocking(wait: Duration, mut find: impl FnMut() -> i32) -> Result<i32, crate::AeronCError> {
    let start = Instant::now();
    loop {
        let counter_id = find();
        if counter_id >= 0 {
            return Ok(counter_id);
        }
        if start.elapsed() >= wait {
            return Err(crate::AeronCError::from_code(-1));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
