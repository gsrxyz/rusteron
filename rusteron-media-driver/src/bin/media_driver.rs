use log::info;
use rusteron_media_driver::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Flag to indicate when the application should stop (set on Ctrl+C)
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = Arc::clone(&running);

    // Register signal handler for SIGINT (Ctrl+C)
    ctrlc::set_handler(move || {
        running_clone.store(false, Ordering::SeqCst);
    })?;

    // Create Aeron context
    let aeron_context = AeronDriverContext::new()?;
    // as aeronmd does: without these, AERON_THREAD_AFFINITY_USE_CPUSET_OFFSETS and the
    // AERON_*_CPU_AFFINITY settings are read but never applied
    aeron_context.apply_cgroup_cpuset_affinity()?;
    aeron_context.set_thread_affinity_on_start();
    info!("aeron dir: {:?}", aeron_context.get_dir());
    aeron_context.print_configuration();

    // Create Aeron driver; each error captures Aeron's message where it happens, as dropping the
    // context on the way out of main overwrites it
    let aeron_driver = AeronDriver::new(&aeron_context).map_err(AeronCError::capture_errmsg)?;
    aeron_driver.start(true).map_err(AeronCError::capture_errmsg)?;
    // Start the Aeron driver
    info!("Aeron media driver started successfully. Press Ctrl+C to stop.");

    // Poll for work until Ctrl+C is pressed
    while running.load(Ordering::Acquire) {
        aeron_driver.main_idle_strategy(aeron_driver.main_do_work().map_err(AeronCError::capture_errmsg)?);
    }
    info!("Received signal to stop the media driver.");
    info!("Aeron media driver stopped successfully.");
    Ok(())
}
