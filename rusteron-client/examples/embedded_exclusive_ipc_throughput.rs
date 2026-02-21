use rusteron_client::*;
use std::ffi::CStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

const BURST_LENGTH: usize = 1_000_000;
const MESSAGE_LENGTH: usize = 32;
static CHANNEL: &CStr = AERON_IPC_STREAM;
const STREAM_ID: i32 = 1001;

/// this code is based on Aeron samples EmbeddedExclusiveIpcThroughput
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running = Arc::new(AtomicBool::new(true));

    println!("message length {MESSAGE_LENGTH}, channel {CHANNEL:?}");

    let running_ctrl_c = Arc::clone(&running);
    ctrlc::set_handler(move || {
        running_ctrl_c.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let running_publisher = Arc::clone(&running);
    let running_subscriber = Arc::clone(&running);

    let ctx = AeronContext::new()?;
    let dir = std::env::var("AERON_DIR").expect("AERON_DIR must be set");
    ctx.set_dir(&dir.into_c_string())?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    let publication = aeron
        .async_add_exclusive_publication(CHANNEL, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;

    let subscription = aeron
        .async_add_subscription(
            CHANNEL,
            STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?
        .poll_blocking(Duration::from_secs(5))?;

    let subscriber_thread = thread::spawn(move || {
        let mut image_rate_subscriber =
            ImageRateSubscriber::new(running_subscriber, subscription, MESSAGE_LENGTH);
        image_rate_subscriber.run();
        Ok::<_, AeronCError>(())
    });

    Publisher::new(running_publisher, publication).run();
    subscriber_thread
        .join()
        .expect("Subscriber thread failed")
        .unwrap();

    Ok(())
}

struct Publisher {
    running: Arc<AtomicBool>,
    publication: AeronExclusivePublication,
}

impl Publisher {
    fn new(running: Arc<AtomicBool>, publication: AeronExclusivePublication) -> Self {
        Publisher {
            running,
            publication,
        }
    }

    fn run(&self) {
        let mut back_pressure_count = 0;
        let mut total_message_count = 0;
        let buffer = vec![1u8; MESSAGE_LENGTH];

        while self.running.load(Ordering::Acquire) {
            while self
                .publication
                .offer(&buffer, Handlers::no_reserved_value_supplier_handler())
                < 0
            {
                back_pressure_count += 1;
                if !self.running.load(Ordering::Acquire) {
                    let back_pressure_ratio =
                        back_pressure_count as f64 / total_message_count as f64;
                    println!("Publisher back pressure ratio: {back_pressure_ratio:.6}");
                    return;
                }
            }
            total_message_count += 1;
        }

        let back_pressure_ratio = back_pressure_count as f64 / total_message_count as f64;
        println!("Publisher back pressure ratio: {back_pressure_ratio:.6}");
    }
}

struct ImageRateSubscriber {
    running: Arc<AtomicBool>,
    subscription: AeronSubscription,
    poll_handler: Handler<MsgCount>,
    message_length: usize,
    start_time: Instant,
}

struct MsgCount {
    message_count: usize,
}

impl AeronFragmentHandlerCallback for MsgCount {
    fn handle_aeron_fragment_handler(&mut self, _buffer: &[u8], _header: AeronHeader) {
        self.message_count += 1;
    }
}

impl ImageRateSubscriber {
    fn new(
        running: Arc<AtomicBool>,
        subscription: AeronSubscription,
        message_length: usize,
    ) -> Self {
        let poll_handler = Handler::leak(MsgCount { message_count: 0 });
        ImageRateSubscriber {
            running,
            subscription,
            poll_handler,
            message_length,
            start_time: Instant::now(),
        }
    }

    fn run(&mut self) {
        let mut next_check = BURST_LENGTH;
        while self.running.load(Ordering::Acquire) {
            self.subscription
                .poll(Some(&self.poll_handler), MESSAGE_LENGTH)
                .unwrap();

            if self.poll_handler.message_count >= next_check
                && self.start_time.elapsed() >= Duration::from_secs(1)
            {
                next_check += BURST_LENGTH;
                let elapsed = self.start_time.elapsed().as_secs_f64();
                let rate = self.poll_handler.message_count as f64 / elapsed;
                let throughput = rate * self.message_length as f64;

                use num_format::{Locale, ToFormattedString};
                println!(
                    "Throughput: {} msgs/sec, {} bytes/sec",
                    (rate.round() as u64).to_formatted_string(&Locale::en),
                    (throughput.round() as u64).to_formatted_string(&Locale::en)
                );

                self.start_time = Instant::now();
                self.poll_handler.message_count = 0;
            }
        }
    }
}
