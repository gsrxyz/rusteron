pub mod model;

use crate::model::Subscribe;
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use rusteron_archive::*;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};
use websocket_lite::{ClientBuilder, Message, Opcode};

pub const TICKER_CHANNEL: &str = "aeron:udp?endpoint=localhost:9123";
pub const TICKER_STREAM_ID: i32 = 10;

pub trait JsonMesssageHandler {
    fn on_msg(&mut self, msg: &str);
}

pub fn start_media_driver() -> Result<(), Box<dyn std::error::Error>> {
    let aeron_context = rusteron_media_driver::AeronDriverContext::new()?;
    let aeron_driver = rusteron_media_driver::AeronDriver::new(&aeron_context)?;
    let mut aeron_driver = aeron_driver.start_manual()?;
    info!("Aeron media driver started successfully. Press Ctrl+C to stop.");

    aeron_driver.conductor().context().print_configuration();
    aeron_driver.do_work()?;
    info!("aeron dir: {:?}", aeron_context.get_dir());

    loop {
        let work = aeron_driver.do_work()?;
        aeron_driver.idle(work);
    }
}

/// Streams `subscription` from `url` into `handler`, reconnecting on errors, until `stop` is set.
pub async fn download_ws(
    url: &str,
    subscription: Subscribe,
    mut handler: impl JsonMesssageHandler,
    stop: &AtomicBool,
) -> websocket_lite::Result<()> {
    while !stop.load(Ordering::Acquire) {
        let mut client = match ClientBuilder::new(url)?.async_connect().await {
            Ok(client) => client,
            Err(e) => {
                error!("connect {url}: {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        let request = Message::text(serde_json::to_string(&subscription)?);
        info!("{url} sending request: {subscription:#?}");
        if let Err(e) = client.send(request).await {
            info!("error sending websocket msg: {e}");
            continue;
        }
        while let Some(msg) = client.next().await {
            if stop.load(Ordering::Acquire) {
                return Ok(());
            }
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => {
                    info!("Error while receiving message: {e:?}");
                    break;
                }
            };
            match msg.opcode() {
                Opcode::Text => match msg.as_text() {
                    Some(text) => handler.on_msg(text),
                    None => error!("text frame is not valid UTF-8"),
                },
                Opcode::Binary => {
                    error!("unsupported binary format");
                }
                Opcode::Close => {
                    info!("closed");
                    break;
                }
                Opcode::Ping => {}
                Opcode::Pong => {}
            }
        }
    }
    Ok(())
}

pub fn init_logger() {
    env_logger::Builder::new().filter_level(log::LevelFilter::Info).init()
}

/// Connects a client (with an error handler) and an archive, retrying for up to 30 s while
/// the media driver and archive start.
pub fn archive_connect() -> websocket_lite::Result<(AeronArchive, Aeron)> {
    let request = std::env::var("AERON_ARCHIVE_CONTROL_CHANNEL")?;
    let response = std::env::var("AERON_ARCHIVE_CONTROL_RESPONSE_CHANNEL")?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let attempt = AeronContext::new().and_then(|context| {
            context.set_error_handler(Some(AeronErrorHandlerLogger))?;
            let aeron = Aeron::new(&context)?;
            aeron.start()?;
            let archive = AeronArchive::connect(&aeron, &request, &response, None, Duration::from_secs(10))?;
            Ok((archive, aeron))
        });
        match attempt {
            Ok((archive, aeron)) => {
                info!("connected to the archive [archiveId={}]", archive.get_archive_id());
                return Ok((archive, aeron));
            }
            Err(e) if Instant::now() < deadline => {
                error!("connecting to the archive: {e}; retrying");
                sleep(Duration::from_secs(5));
            }
            Err(e) => return Err(e.into()),
        }
    }
}

pub fn register_exit_signals() -> websocket_lite::Result<Arc<AtomicBool>> {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let signals = &[SIGINT, SIGTERM, SIGQUIT];
    for &signal in signals {
        let flag_clone = Arc::clone(&shutdown_flag);
        signal_hook::flag::register(signal, flag_clone.clone())?;
    }

    Ok(shutdown_flag)
}
