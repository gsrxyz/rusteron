use log::info;
use rusteron_media_driver::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = Arc::clone(&running);

    ctrlc::set_handler(move || {
        running_clone.store(false, Ordering::SeqCst);
    })?;

    let aeron_context = AeronDriverContext::new()?;
    // Apply affinity as aeronmd does: without these calls, AERON_THREAD_AFFINITY_USE_CPUSET_OFFSETS
    // and the AERON_*_CPU_AFFINITY settings are read but never applied
    // Retain diagnostics before main drops the context and the runtime prints the error.
    aeron_context
        .apply_cgroup_cpuset_affinity()
        .map_err(AeronCError::capture_errmsg)?;
    aeron_context.set_thread_affinity_on_start();
    // SAFETY: logging copies the text before any setter can change it
    info!("aeron dir: {:?}", unsafe { aeron_context.get_dir() });
    aeron_context.print_configuration();
    info!(
        "driver environment before start: {:#?}",
        environment::inspect_environment(&aeron_context)
    );

    // Retain diagnostics before main drops the driver and context.
    let aeron_driver = AeronDriver::new(&aeron_context).map_err(AeronCError::capture_errmsg)?;
    aeron_driver.start(true).map_err(AeronCError::capture_errmsg)?;
    info!(
        "driver environment after start: {:#?}",
        environment::inspect_environment(&aeron_context)
    );
    info!("Aeron media driver started successfully. Press Ctrl+C to stop.");

    // Poll for work until Ctrl+C is pressed
    while running.load(Ordering::Acquire) {
        let work = aeron_driver.main_do_work().map_err(AeronCError::capture_errmsg)?;
        aeron_driver.main_idle_strategy(work);
    }
    info!("Received signal to stop the media driver.");
    info!("Aeron media driver stopped successfully.");
    Ok(())
}
