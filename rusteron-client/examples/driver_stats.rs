//! # Driver stats (aeron_stat + error_stat + loss_stat)
//!
//! Port of Aeron's `aeron_stat.c`, `error_stat.c` and `loss_stat.c` tools in one example,
//! reading the driver's CnC (command-and-control) file via [`AeronCnc`]:
//!
//! - all driver/client counters (`counters_reader`),
//! - the distinct error log with occurrence counts (`error_log_read_once`),
//! - the loss report (`loss_reporter_read_once`).
//!
//! Run it against a live driver by setting `AERON_DIR`; without it, an embedded driver plus
//! a little traffic (including a deliberate client error) is spun up so there is something
//! to show.
//!
//! ```bash
//! cargo run --release --features "static precompile" --example driver_stats
//! ```

use rusteron_client::*;
use rusteron_media_driver::{AeronDriver, AeronDriverContext};
use std::sync::atomic::Ordering;
use std::thread::sleep;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Attach to an external driver via AERON_DIR, or start an embedded one with traffic.
    let (dir, _embedded) = match std::env::var("AERON_DIR") {
        Ok(dir) => (dir, None),
        Err(_) => {
            let driver_ctx = AeronDriverContext::new()?;
            driver_ctx.set_dir_delete_on_shutdown(true)?;
            driver_ctx.set_dir_delete_on_start(true)?;
            driver_ctx.set_dir(&format!("{}{}", driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
            let (stop, driver_handle) = AeronDriver::launch_embedded(driver_ctx.clone(), false);

            let ctx = AeronContext::new()?;
            ctx.set_dir(&driver_ctx.get_dir().into_c_string())?;
            let aeron = Aeron::new(&ctx)?;
            aeron.start()?;
            let publication = aeron
                .async_add_publication(AERON_IPC_STREAM, 77)?
                .poll_blocking(Duration::from_secs(5))?;
            let subscription = aeron
                .async_add_subscription(
                    AERON_IPC_STREAM,
                    77,
                    Handlers::no_available_image_handler(),
                    Handlers::no_unavailable_image_handler(),
                )?
                .poll_blocking(Duration::from_secs(5))?;
            for _ in 0..100 {
                let _ = publication.offer_simple(b"stats-traffic");
                subscription.poll_once(|_, _| {}, 16)?;
            }
            // seed the error log with a deliberately invalid channel
            let _ = aeron
                .async_add_publication(&"aeron:udp?endpoint=bad-host:0|interface=1.2.3.4.5".into_c_string(), 78)
                .and_then(|p| p.poll_blocking(Duration::from_secs(1)));
            sleep(Duration::from_millis(200));

            let dir = driver_ctx.get_dir().to_string();
            (
                dir,
                Some((aeron, publication, subscription, stop, driver_handle, driver_ctx)),
            )
        }
    };

    let cnc = AeronCnc::new_on_heap(&dir)?;
    println!(
        "CnC version {}; driver heartbeat age {} ms",
        cnc.get_constants()?.cnc_version,
        {
            let now = Aeron::epoch_clock();
            now - cnc.get_to_driver_heartbeat_ms()?
        }
    );

    // ── aeron_stat: every counter with id, value and label ───────────────
    println!("\n===== counters =====");
    cnc.counters_reader()
        .foreach_counter_once(|value, id, type_id, _key, label| {
            println!("{id:>4} (type {type_id:>3}): {value:>16} — {label}");
        });

    // ── error_stat: the distinct error log ───────────────────────────────
    println!("\n===== distinct errors =====");
    let count = cnc.error_log_read_once(
        |observation_count, first_ts, last_ts, error| {
            println!("{observation_count:>4}x [{first_ts} .. {last_ts}] {error}");
        },
        0, // since the beginning
    );
    println!("({count} distinct error(s))");

    // ── loss_stat: the loss report (empty unless gaps went unrecovered) ──
    println!("\n===== loss report =====");
    let entries = cnc.loss_reporter_read_once(
        |observations, total_bytes, first_ts, last_ts, session_id, stream_id, channel, source| {
            println!(
                "{observations:>4}x {total_bytes:>10}B [{first_ts} .. {last_ts}] session {session_id} stream {stream_id} {channel} <- {source}"
            );
        },
    )?;
    println!("({entries} loss entrie(s))");

    if let Some((aeron, publication, subscription, stop, driver_handle, _driver_ctx)) = _embedded {
        drop(publication);
        drop(subscription);
        drop(aeron);
        stop.store(true, Ordering::SeqCst);
        driver_handle.join().ok();
    }
    Ok(())
}
