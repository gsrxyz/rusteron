//! Basic Aeron subscriber (Rust port of Aeron C sample).
//!
//! Ported from:
//! https://github.com/aeron-io/aeron/blob/8cd9efa5eaba00c874e0828920050798284d4161/aeron-samples/src/main/c/basic_subscriber.c
//! using rusteron-client.

use std::ffi::CStr;
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use rusteron_client as rusteron;
use rusteron_client::{Aeron, AeronContext, AeronFragmentClosureAssembler, Handler, IntoCString};

const DEFAULT_CHANNEL: &str = "aeron:udp?endpoint=localhost:20121";
const DEFAULT_FRAGMENT_COUNT_LIMIT: i32 = 10;

// FFI for Aeron version, to match exact `-v` output of the C sample
#[allow(non_camel_case_types)]
type c_int = i32;
#[allow(non_camel_case_types)]
type c_char = i8;

extern "C" {
    fn aeron_version_full() -> *const c_char;
    fn aeron_version_gitsha() -> *const c_char;
    fn aeron_version_major() -> c_int;
    fn aeron_version_minor() -> c_int;
    fn aeron_version_patch() -> c_int;
}

/// Print Aeron library version in the same format as the C sample `-v`.
fn print_version_and_exit(bin_name: &str) -> ! {
    unsafe {
        let full = CStr::from_ptr(aeron_version_full() as *const std::os::raw::c_char)
            .to_string_lossy()
            .into_owned();
        let git = CStr::from_ptr(aeron_version_gitsha() as *const std::os::raw::c_char)
            .to_string_lossy()
            .into_owned();
        let maj = aeron_version_major();
        let min = aeron_version_minor();
        let pat = aeron_version_patch();
        println!(
            "{} <{}> major {} minor {} patch {} git {}",
            bin_name, full, maj, min, pat, git
        );
    }
    std::process::exit(0);
}

#[derive(Parser, Debug)]
#[command(disable_help_flag = true)]
struct Args {
    /// help
    #[arg(short = 'h', long = "help", default_value_t = false)]
    help: bool,

    /// show version and exit
    #[arg(short = 'v', default_value_t = false)]
    show_version: bool,

    /// use channel specified in uri
    #[arg(short = 'c', value_name = "uri", default_value = DEFAULT_CHANNEL)]
    channel: String,

    /// aeron.dir location specified as prefix
    #[arg(short = 'p', value_name = "prefix")]
    aeron_dir: Option<String>,

    /// stream-id to use
    #[arg(short = 's', value_name = "stream-id", default_value_t = 1001)]
    stream_id: i32,
}

fn print_usage_and_exit(bin: &str) -> ! {
    // Match C sample usage lines
    const USAGE: &str = concat!(
        "[-h][-v][-c uri][-p prefix][-s stream-id]\n",
        "    -h               help\n",
        "    -v               show version and exit\n",
        "    -c uri           use channel specified in uri\n",
        "    -p prefix        aeron.dir location specified as prefix\n",
        "    -s stream-id     stream-id to use\n",
    );
    eprint!("Usage: {} {}", bin, USAGE);
    std::process::exit(1);
}

fn install_sigint_flag(flag: Arc<AtomicBool>) -> Result<()> {
    ctrlc::set_handler(move || {
        flag.store(false, Ordering::Release);
    })
    .context("install ctrlc handler")
}

fn main() -> Result<()> {
    let bin_name = std::env::args()
        .next()
        .unwrap_or_else(|| "basic-subscriber".to_string());
    let args = Args::parse();

    if args.help {
        print_usage_and_exit(&bin_name);
    }
    if args.show_version {
        print_version_and_exit(&bin_name);
    }

    let channel = args.channel.clone();
    let stream_id = args.stream_id;
    println!(
        "Subscribing to channel {} on Stream ID {}",
        channel, stream_id
    );

    // Aeron context and client
    let ctx = AeronContext::new().context("aeron context init")?;
    if let Some(dir) = args.aeron_dir.as_ref() {
        ctx.set_dir(&dir.clone().into_c_string())
            .context("aeron_context_set_dir")?;
    }

    let aeron = Aeron::new(&ctx).context("aeron init")?;
    aeron.start().context("aeron start")?;

    // Available/unavailable image handlers to match C sample output
    struct AvailPrinter;
    impl rusteron::AeronAvailableImageCallback for AvailPrinter {
        fn handle_aeron_on_available_image(
            &mut self,
            subscription: rusteron::AeronSubscription,
            image: rusteron::AeronImage,
        ) {
            let subc = subscription.get_constants().ok();
            let imgc = image.get_constants().ok();
            let channel = subc.as_ref().map(|c| c.channel()).unwrap_or("");
            let stream_id = subc.as_ref().map(|c| c.stream_id()).unwrap_or_default();
            if let Some(img) = imgc {
                println!(
                    "Available image on {} streamId={} sessionId={} mtu={} term-length={} from {}",
                    channel,
                    stream_id,
                    img.session_id(),
                    img.mtu_length(),
                    img.term_buffer_length(),
                    img.source_identity()
                );
            }
        }
    }
    struct UnavailPrinter;
    impl rusteron::AeronUnavailableImageCallback for UnavailPrinter {
        fn handle_aeron_on_unavailable_image(
            &mut self,
            subscription: rusteron::AeronSubscription,
            image: rusteron::AeronImage,
        ) {
            let subc = subscription.get_constants().ok();
            let imgc = image.get_constants().ok();
            let channel = subc.as_ref().map(|c| c.channel()).unwrap_or("");
            let stream_id = subc.as_ref().map(|c| c.stream_id()).unwrap_or_default();
            let session_id = imgc.as_ref().map(|i| i.session_id()).unwrap_or_default();
            println!(
                "Unavailable image on {} streamId={} sessionId={}",
                channel, stream_id, session_id
            );
        }
    }

    // Wrap handlers for rusteron-client
    let chan_c = CString::new(channel.clone()).unwrap();
    let available_handler: Handler<AvailPrinter> = Handler::leak(AvailPrinter);
    let unavailable_handler: Handler<UnavailPrinter> = Handler::leak(UnavailPrinter);

    // Create subscription (blocking add to mirror the C sample poll loop semantics)
    let sub = aeron
        .add_subscription(
            &chan_c,
            stream_id,
            Some(&available_handler),
            Some(&unavailable_handler),
            Duration::from_secs(5),
        )
        .context("add subscription")?;

    // Print channel status like the C sample
    let status = sub.channel_status();
    println!("Subscription channel status {}", status);

    // Assemble fragments into whole messages, print each
    let mut assembler = AeronFragmentClosureAssembler::new().context("fragment assembler")?;
    struct PrintCtx;
    fn on_message(_ctx: &mut PrintCtx, msg: &[u8], hdr: rusteron::AeronHeader) {
        let values = hdr.get_values().ok();
        let (stream_id, session_id) = if let Some(vals) = values {
            let frame = vals.frame();
            (frame.stream_id(), frame.session_id())
        } else {
            (0, 0)
        };
        println!(
            "Message to stream {} from session {} ({} bytes) <<{}>>",
            stream_id,
            session_id,
            msg.len(),
            String::from_utf8_lossy(msg)
        );
    }
    let mut ctx = PrintCtx;
    let print_handler = assembler
        .process(&mut ctx, on_message)
        .expect("assembler handler");

    // SIGINT to stop
    let running = Arc::new(AtomicBool::new(true));
    install_sigint_flag(running.clone())?;

    let idle_ns = Duration::from_millis(1);
    while running.load(Ordering::Acquire) {
        let _ = sub.poll(Some(print_handler), DEFAULT_FRAGMENT_COUNT_LIMIT as usize)?;
        std::thread::sleep(idle_ns);
    }

    println!("Shutting down...");
    Ok(())
}
