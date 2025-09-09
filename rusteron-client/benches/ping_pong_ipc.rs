use criterion::{black_box, Criterion};
use rusteron_client::*;
use rusteron_media_driver::{AeronDriver, AeronDriverContext};
use std::clone::Clone;
use std::ffi::CStr;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PING_STREAM_ID: i32 = 1002;
const PONG_STREAM_ID: i32 = 1003;
static PING_CHANNEL: &CStr = AERON_IPC_STREAM;
static PONG_CHANNEL: &CStr = AERON_IPC_STREAM;
const MESSAGE_LENGTH: usize = 32;
const FRAGMENT_COUNT_LIMIT: usize = 10;

fn criterion_benchmark(c: &mut Criterion) {
    // Launch embedded media driver (acts as the shared media driver for both processes)
    let ctx = AeronDriverContext::new().unwrap();
    ctx.set_dir(&format!("{}{}", ctx.get_dir(), Aeron::nano_clock()).into_c_string())
        .unwrap();
    ctx.set_dir_delete_on_start(true).unwrap();
    ctx.set_dir_delete_on_shutdown(true).unwrap();
    ctx.set_print_configuration(true).unwrap();
    let dir = ctx.get_dir().to_string().leak();
    let dir_for_client = ctx.get_dir().to_string().leak();
    let (driver_stop, _handle) = AeronDriver::launch_embedded(ctx.clone(), false);

    // Spawn pong as a separate process running this same binary in pong mode.
    let mut pong_child = spawn_pong_process(dir).expect("Failed to spawn pong process");

    let context = AeronContext::new().unwrap();
    context.set_idle_sleep_duration_ns(0).unwrap();
    context.set_dir(&dir_for_client.into_c_string()).unwrap();
    let aeron = Aeron::new(&context).unwrap();
    aeron.start().unwrap();

    let pong_publication = aeron
        .async_add_publication(PONG_CHANNEL, PONG_STREAM_ID)
        .unwrap()
        .poll_blocking(Duration::from_secs(5))
        .unwrap();
    let ping_subscription = aeron
        .async_add_subscription(
            PING_CHANNEL,
            PING_STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )
        .unwrap()
        .poll_blocking(Duration::from_secs(4))
        .unwrap();

    println!("PING: pong publisher {PONG_CHANNEL:?} {PONG_STREAM_ID}");
    println!("PING: ping subscriber {PING_CHANNEL:?} {PING_STREAM_ID}");

    let mut buffer = vec![0u8; MESSAGE_LENGTH];
    let mut handler = Handler::leak(PingRoundTripHandler {});

    c.bench_function("ping_pong_ipc_process_benchmark", |b| {
        b.iter(|| {
            record_rtt(
                &pong_publication,
                &ping_subscription,
                &mut buffer,
                &mut handler,
            );
        });
    });

    driver_stop.store(false, Ordering::SeqCst);
    if let Err(e) = pong_child.kill() {
        eprintln!("Failed to kill pong child: {e}");
    }
    let _ = pong_child.wait();
}

fn spawn_pong_process(dir: &str) -> std::io::Result<Child> {
    let exe = std::env::current_exe()?;
    Command::new(exe)
        .arg("--pong")
        .arg(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
}

fn run_pong_process(dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = AeronContext::new()?;
    context.set_dir(&dir.into_c_string())?;
    context.set_idle_sleep_duration_ns(0)?;
    let aeron = Aeron::new(&context)?;
    aeron.start()?;
    let ping_publication = aeron
        .async_add_publication(PING_CHANNEL, PING_STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let pong_subscription = aeron
        .async_add_subscription(
            PONG_CHANNEL,
            PONG_STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?
        .poll_blocking(Duration::from_secs(4))?;

    println!("PONG (process): ping publisher {PING_CHANNEL:?} {PING_STREAM_ID}");
    println!("PONG (process): pong subscriber {PONG_CHANNEL:?} {PONG_STREAM_ID}");

    pub struct PongRoundTripHandler {
        publisher: AeronPublication,
        buffer_claim: AeronBufferClaim,
    }

    impl AeronFragmentHandlerCallback for PongRoundTripHandler {
        #[inline]
        fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: AeronHeader) {
            let header_values = header.get_values().unwrap();
            let flags = header_values.frame.flags;

            while self.publisher.try_claim(buffer.len(), &self.buffer_claim) < 0 {}
            self.buffer_claim.frame_header_mut().flags = flags;
            self.buffer_claim.data_mut().copy_from_slice(buffer);
            self.buffer_claim.commit().unwrap();
        }
    }

    let handler = Handler::leak(PongRoundTripHandler {
        publisher: ping_publication.clone(),
        buffer_claim: Default::default(),
    });

    // Loop forever until killed by parent process.
    loop {
        let _ = pong_subscription.poll(Some(&handler), FRAGMENT_COUNT_LIMIT);
    }
}

pub struct PingRoundTripHandler {}

impl AeronFragmentHandlerCallback for PingRoundTripHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        let time = read_i64(buffer);
        let rtt = Aeron::nano_clock() - time;
        black_box(rtt);
        debug_assert!(rtt >= 0);
    }
}

fn read_i64(buffer: &[u8]) -> i64 {
    i64::from_le_bytes(
        buffer[0..8]
            .try_into()
            .expect("Slice with incorrect length"),
    )
}

#[inline]
fn record_rtt(
    pong_publication: &AeronPublication,
    ping_subscription: &AeronSubscription,
    buffer: &mut [u8],
    handler: &mut Handler<PingRoundTripHandler>,
) {
    let now = Aeron::nano_clock();
    write_i64(buffer, &now);
    while pong_publication.offer(buffer, Handlers::no_reserved_value_supplier_handler()) < 0 {}

    while ping_subscription
        .poll(Some(handler), FRAGMENT_COUNT_LIMIT)
        .unwrap_or_default()
        == 0
    {}
}

fn write_i64(buffer: &mut [u8], now: &i64) {
    buffer[0..8].copy_from_slice(&now.to_le_bytes());
}

fn main() {
    let mut args = std::env::args();
    let _prog = args.next();
    let mut raw: Vec<String> = Vec::new();
    let mut pong_mode = false;
    let mut pong_dir: Option<String> = None;

    while let Some(arg) = args.next() {
        if arg == "--pong" {
            pong_mode = true;
            pong_dir = args.next();
            break; // remaining args irrelevant in pong mode
        } else {
            raw.push(arg);
        }
    }

    if pong_mode {
        let dir = pong_dir.expect("--pong requires <dir>");
        if let Err(e) = run_pong_process(&dir) {
            eprintln!("Pong process error: {e}");
        }
        return;
    }

    let mut c = Criterion::default().configure_from_args();
    criterion_benchmark(&mut c);
    c.final_summary();
}
