use log::error;
use rusteron_client::*;
use std::cell::Cell;
use std::error;
use std::time::Duration;

pub fn main() -> Result<(), Box<dyn error::Error>> {
    let ctx = AeronContext::new()?;

    // set the directory
    // ctx.set_dir(&media_driver_ctx.get_dir().into_c_string())?;

    // Install the built-in error logger so async client errors are surfaced (Aeron samples
    // always set an error handler on the context). Keep it alive for the run, then release.
    let error_handler = Handler::new(AeronErrorHandlerLogger);
    ctx.set_error_handler(Some(error_handler.clone()))?;

    println!("creating client");
    let aeron = Aeron::new(&ctx)?;
    println!("starting client");

    aeron.start()?;
    println!("client started");
    let publisher = aeron
        .async_add_publication(AERON_IPC_STREAM, 123)?
        .poll_blocking(Duration::from_secs(5))?;
    // Wait for a subscriber before offering (Aeron's BasicPublisher checks is_connected()).
    let conn_start = std::time::Instant::now();
    while !publisher.is_connected() && conn_start.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(10));
    }
    println!("created publisher");

    let subscription = aeron
        .async_add_subscription(
            AERON_IPC_STREAM,
            123,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?
        .poll_blocking(Duration::from_secs(5))?;
    println!("created subscription");

    // pick a large enough size to confirm fragment assembler is working
    let large_string_len = 1024 * 1024;
    println!("string length: {large_string_len}");

    let _publisher_handler = {
        std::thread::spawn(move || loop {
            // Typed offer: retry the retryable errors, stop on fatal ones.
            match publisher.offer_simple("1".repeat(large_string_len).as_bytes()) {
                Ok(_) => {}
                Err(e) if e.is_retryable() => error!("failed to send message ({e}); retrying"),
                Err(e) => {
                    error!("publication gone ({e}); stopping publisher thread");
                    break;
                }
            }
        })
    };

    struct FragmentHandler {
        count: Cell<usize>,
        large_string_len: usize,
    }

    impl AeronFragmentHandlerCallback for FragmentHandler {
        fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
            println!(
                "received a message from aeron [position: {:?}, msg length:{}]",
                header.position(),
                buffer.len()
            );

            // Increment count
            self.count.set(self.count.get() + 1);

            // Check if the message matches the expected pattern
            assert_eq!(buffer, "1".repeat(self.large_string_len).as_bytes());
        }
    }
    // if you don't need fragmentation support use Handler::new instead
    let (closure, _inner) = Handler::with_fragment_assembler(FragmentHandler {
        count: Cell::new(0),
        large_string_len,
    })?;

    // Back off the poll loop when there's no work — Aeron's samples drive their loops with an
    // `IdleStrategy` (idle(fragments)) instead of a bare tight loop.
    let mut idle = BackoffIdleStrategy::new();
    loop {
        if _inner.count.get() > 100 {
            break;
        }
        let fragments = subscription.poll(Some(&closure), 1024)?;
        idle.idle(fragments);
    }

    println!("stopping client");

    Ok(())
}
