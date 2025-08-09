use log::{error, info, warn};
use rusteron_archive::*;
use rusteron_dummy_example::model::Subscribe;
use rusteron_dummy_example::{
    archive_connect, download_ws, init_logger, register_exit_signals, JsonMesssageHandler,
    TICKER_CHANNEL, TICKER_STREAM_ID,
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
            channel, stream_id,
            Aeron::errmsg(),
            archive.aeron().is_closed(),
        );
        let subscription_id = archive.start_recording(
            &channel.into_c_string(),
            stream_id,
            SOURCE_LOCATION_REMOTE,
            true,
        )?;
        info!("started recording ticker stream [subscriptionId={subscription_id}]");

        let publication =
            aeron.add_publication(&channel.into_c_string(), stream_id, Duration::from_secs(60))?;

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
        let mut result = self.publication.offer(
            msg.as_bytes(),
            Handlers::no_reserved_value_supplier_handler(),
        );
        if result <= 0 {
            // this is poor way to handle back pressure, just for simple example
            let duration = Duration::from_millis(100);
            let start = Instant::now();

            while start.elapsed() < duration && result <= 0 {
                result = self.publication.offer(
                    msg.as_bytes(),
                    Handlers::no_reserved_value_supplier_handler(),
                );
            }

            if result <= 0 {
                warn!(
                    "failed to publish [error={:?}, payload={}]",
                    AeronCError::from_code(result as i32),
                    msg
                );
                if result as i32 == AeronErrorType::PublicationClosed.code() {
                    let channel = TICKER_CHANNEL;
                    let stream_id = TICKER_STREAM_ID;
                    self.publication = self
                        .aeron
                        .add_publication(
                            &channel.into_c_string(),
                            stream_id,
                            Duration::from_secs(60),
                        )
                        .expect("failed to add exclusive publication");

                    info!(
                        "created ticker publication [sessionId={}]",
                        self.publication.get_constants().unwrap().session_id()
                    );
                }
            }
        }

        if result > 0 {
            self.published_count += 1;

            if self.published_count % 1000 == 0 {
                info!("published {} ticker messages so far", self.published_count);
            }
        }
    }
}
