use log::{error, info, warn};
use rusteron_archive::*;
use rusteron_dummy_example::model::Subscribe;
use rusteron_dummy_example::{
    archive_connect, download_ws, init_logger, register_exit_signals, JsonMesssageHandler, TICKER_CHANNEL,
    TICKER_STREAM_ID,
};
use std::sync::atomic::Ordering;
use std::thread::sleep;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::main]
async fn main() -> websocket_lite::Result<()> {
    init_logger();

    let stop = register_exit_signals()?;

    let pairs = vec![
        "btcusdt",
        "ethusdt",
        "bnbusdt",
        "ltcusdt",
        "solusdt",
        "dotusdt",
        "maticusdt",
        "avaxusdt",
        "nearusdt",
        "adausdt",
        "xrpusdt",
    ];

    let id = 0;
    let url = "wss://stream.binance.com/ws";

    let mut params = vec![];
    for pair in &pairs {
        params.push(format!("{pair}@ticker"));
    }

    let subscription = Subscribe {
        method: "SUBSCRIBE".to_string(),
        params,
        id,
    };

    let (archive, aeron) = archive_connect()?;

    let archive_copy = archive.clone();
    let aeron_copy = aeron.clone();

    let mut recorder = AeronRecorder::new(archive.clone(), aeron.clone());
    while !stop.load(Ordering::Acquire) {
        match &recorder {
            Ok(recorder) => {
                let handle = tokio::spawn(download_ws(url, subscription.clone(), recorder.clone()));

                handle
                    .await
                    .expect("Error occurred during download")
                    .expect("Error occurred during retrieval");
            }
            Err(err) => {
                error!("Error: {err:?}");
                sleep(Duration::from_secs(5));
                recorder = AeronRecorder::new(archive.clone(), aeron.clone());
            }
        }
    }

    drop(archive_copy);
    drop(aeron_copy);

    Ok(())
}

unsafe impl Send for AeronRecorder {}
unsafe impl Sync for AeronRecorder {}

#[derive(Debug, Clone)]
struct AeronRecorder {
    publication: AeronPublication,
    published_count: usize,
    aeron: Aeron,
}

impl AeronRecorder {
    pub fn new(archive: AeronArchive, aeron: Aeron) -> websocket_lite::Result<Self> {
        let channel = TICKER_CHANNEL;
        let stream_id = TICKER_STREAM_ID;

        info!(
            "attempting to starting recording {} streamId={} [archive={archive:?}, aeronError={}, aeronClosed={}]",
            channel,
            stream_id,
            Aeron::errmsg(),
            archive.aeron().is_closed(),
        );
        let subscription_id =
            archive.start_recording(&channel.into_c_string(), stream_id, SOURCE_LOCATION_REMOTE, true)?;
        info!("started recording ticker stream [subscriptionId={subscription_id}]");

        let publication = aeron.add_publication(&channel.into_c_string(), stream_id, Duration::from_secs(60))?;

        info!(
            "created ticker publication [sessionId={}]",
            publication.get_constants()?.session_id
        );

        Ok(Self {
            publication,
            published_count: 0,
            aeron: aeron.clone(),
        })
    }
}

impl JsonMesssageHandler for AeronRecorder {
    fn on_msg(&mut self, msg: &str) {
        let mut result = self.publication.offer(msg.as_bytes());

        // Handle retryable errors (back pressure)
        let needs_retry = result.is_err() || result.as_ref().unwrap_or(&0) <= &0;
        if needs_retry {
            let duration = Duration::from_millis(100);
            let start = Instant::now();

            while start.elapsed() < duration {
                result = self.publication.offer(msg.as_bytes());
                match result {
                    Ok(n) if n > 0 => break,
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }

            match result {
                Ok(0) | Err(_) => {
                    warn!("failed to publish [error={:?}, payload={}]", result, msg);

                    if let Err(e) = &result {
                        if matches!(e, AeronOfferError::Closed) {
                            let channel = TICKER_CHANNEL;
                            let stream_id = TICKER_STREAM_ID;
                            self.publication = self
                                .aeron
                                .add_publication(&channel.into_c_string(), stream_id, Duration::from_secs(60))
                                .expect("failed to add exclusive publication");

                            info!(
                                "created ticker publication [sessionId={}]",
                                self.publication.get_constants().unwrap().session_id()
                            );
                        }
                    }
                }
                _ => {}
            }
        }

        if result.as_ref().unwrap_or(&0) > &0 {
            self.published_count += 1;

            if self.published_count.is_multiple_of(1000) {
                info!("published {} ticker messages so far", self.published_count);
            }
        }
    }
}
