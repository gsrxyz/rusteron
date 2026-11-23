use log::{error, info, warn};
use rusteron_archive::*;
use rusteron_dummy_example::model::Subscribe;
use rusteron_dummy_example::{
    archive_connect, download_ws, init_logger, register_exit_signals, JsonMesssageHandler, TICKER_CHANNEL,
    TICKER_STREAM_ID,
};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

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

    // The archive stays on this task because AeronArchive is not Send.
    let mut recorder = AeronRecorder::new(archive.clone(), aeron.clone());
    while !stop.load(Ordering::Acquire) {
        match &recorder {
            Ok(recorder) => download_ws(url, subscription.clone(), recorder.clone(), &stop).await?,
            Err(err) => {
                error!("Error: {err:?}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                recorder = AeronRecorder::new(archive.clone(), aeron.clone());
            }
        }
    }

    Ok(())
}

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
            "starting recording {} streamId={} [archive={archive:?}, aeronClosed={}]",
            channel,
            stream_id,
            archive.aeron().is_closed(),
        );
        let subscription_id =
            archive.start_recording(&channel.into_c_string(), stream_id, SOURCE_LOCATION_REMOTE, true)?;
        info!("started recording ticker stream [subscriptionId={subscription_id}]");

        let publication = aeron.add_publication(&channel.into_c_string(), stream_id, Duration::from_secs(60))?;

        info!(
            "created ticker publication [sessionId={}]",
            publication.get_constants()?.session_id()
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
        let deadline = Instant::now() + Duration::from_millis(100);
        loop {
            match self.publication.offer(msg.as_bytes()) {
                Ok(_) => {
                    self.published_count += 1;
                    if self.published_count.is_multiple_of(1000) {
                        info!("published {} ticker messages so far", self.published_count);
                    }
                    return;
                }
                Err(e) if e.is_retryable() && Instant::now() < deadline => std::thread::yield_now(),
                Err(AeronOfferError::Closed) => {
                    match self.aeron.add_publication(
                        &TICKER_CHANNEL.into_c_string(),
                        TICKER_STREAM_ID,
                        Duration::from_secs(60),
                    ) {
                        Ok(publication) => self.publication = publication,
                        Err(e) => error!("failed to recreate the ticker publication: {e}"),
                    }
                    return;
                }
                // a fatal error, or still not accepted at the deadline
                Err(e) => {
                    warn!("dropped a ticker message [error={e}, payload={msg}]");
                    return;
                }
            }
        }
    }
}
