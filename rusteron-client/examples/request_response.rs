//! # Request/response channels
//!
//! Port of Aeron's `response_server.c` + `response_client.c` samples (response channels,
//! aeron 1.44+): a client subscribes on a `control-mode=response` channel and stamps its
//! subscription registration id onto the request publication (`response-correlation-id=`);
//! the server answers each connected client on a per-image response publication keyed by
//! the request image's correlation id. No addressing logic in user code — the driver wires
//! responses back to the right requester.
//!
//! ```bash
//! cargo run --release --features "static precompile" --example request_response
//! ```

use rusteron_client::*;
use rusteron_media_driver::{AeronDriver, AeronDriverContext};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

const REQUEST_STREAM_ID: i32 = 10001;
const RESPONSE_STREAM_ID: i32 = 10002;

fn find_unused_udp_port(start: u16) -> Option<u16> {
    (start..65535).find(|p| std::net::UdpSocket::bind(("127.0.0.1", *p)).is_ok())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver_ctx = AeronDriverContext::new()?;
    driver_ctx.set_dir_delete_on_shutdown(true)?;
    driver_ctx.set_dir_delete_on_start(true)?;
    driver_ctx.set_dir(&format!("{}{}", driver_ctx.get_dir(), Aeron::epoch_clock()).into_c_string())?;
    let (stop, driver_handle) = AeronDriver::launch_embedded(driver_ctx.clone(), false);

    let ctx = AeronContext::new()?;
    ctx.set_dir(&driver_ctx.get_dir().into_c_string())?;
    ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    let request_port = find_unused_udp_port(21000).expect("no free port");
    let response_port = find_unused_udp_port(request_port + 1).expect("no free port");
    let request_endpoint = format!("localhost:{request_port}");
    let response_control_endpoint = format!("localhost:{response_port}");
    let request_channel = AeronUriStringBuilder::udp(&request_endpoint)?.build(256)?;

    // ── server (its own Aeron client, as it would be in a separate process) ──
    // Requests arrive on the request channel; each connecting client's image carries a
    // correlation id, which keys the response publication back to that client.
    let running = Arc::new(AtomicBool::new(true));
    let server = {
        let running = running.clone();
        let response_control = response_control_endpoint.clone();
        let request_channel = request_channel.clone();
        let aeron_dir = driver_ctx.get_dir().to_string();
        std::thread::spawn(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let ctx = AeronContext::new()?;
            ctx.set_dir(&aeron_dir.into_c_string())?;
            let aeron = Aeron::new(&ctx)?;
            aeron.start()?;

            let pending_clients: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
            let on_image_clients = pending_clients.clone();
            let image_handler = Handler::new(move |_subscription: AeronSubscription, image: AeronImage| {
                if let Ok(constants) = image.get_constants() {
                    on_image_clients.lock().unwrap().push(constants.correlation_id());
                }
            });
            let server_subscription = aeron
                .async_add_subscription(
                    &request_channel.into_c_string(),
                    REQUEST_STREAM_ID,
                    Some(&image_handler),
                    Handlers::no_unavailable_image_handler(),
                )?
                .poll_blocking(Duration::from_secs(5))?;

            let mut responders: Vec<AeronPublication> = Vec::new();
            let mut inbox: Vec<Vec<u8>> = Vec::new();
            while running.load(Ordering::Acquire) {
                // connect a response publication for each newly-arrived client image
                for correlation_id in pending_clients.lock().unwrap().drain(..) {
                    let channel = AeronUriStringBuilder::udp_control(&response_control, ControlMode::Response)?
                        .response_correlation_id(correlation_id)?
                        .build(256)?;
                    let publication = aeron
                        .async_add_publication(&channel.into_c_string(), RESPONSE_STREAM_ID)?
                        .poll_blocking(Duration::from_secs(5))?;
                    println!("[server] response publication ready for client image {correlation_id}");
                    responders.push(publication);
                }
                server_subscription.poll_once(|buf, _hdr| inbox.push(buf.to_vec()), 16)?;
                for request in inbox.drain(..) {
                    let reply = String::from_utf8_lossy(&request).to_uppercase();
                    println!(
                        "[server] request {:?} -> reply {:?}",
                        String::from_utf8_lossy(&request),
                        reply
                    );
                    for publication in &responders {
                        let deadline = Instant::now() + Duration::from_secs(5);
                        loop {
                            match publication.offer_simple(reply.as_bytes()) {
                                Ok(_) => break,
                                Err(e) if e.is_retryable() && Instant::now() < deadline => {
                                    sleep(Duration::from_millis(1))
                                }
                                Err(e) => return Err(format!("server reply failed: {e}").into()),
                            }
                        }
                    }
                }
                sleep(Duration::from_millis(1));
            }
            Ok(())
        })
    };

    // ── client ────────────────────────────────────────────────────────────
    // 1. subscribe for responses on a control-mode=response channel
    let response_subscription = aeron
        .async_add_subscription(
            &AeronUriStringBuilder::udp_control(&response_control_endpoint, ControlMode::Response)?
                .build(256)?
                .into_c_string(),
            RESPONSE_STREAM_ID,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        )?
        .poll_blocking(Duration::from_secs(5))?;
    // 2. stamp the subscription's registration id onto the request publication
    let registration_id = response_subscription.get_constants()?.registration_id();
    let request_publication = aeron
        .async_add_publication(
            &AeronUriStringBuilder::udp(&request_endpoint)?
                .response_correlation_id(registration_id)?
                .build(256)?
                .into_c_string(),
            REQUEST_STREAM_ID,
        )?
        .poll_blocking(Duration::from_secs(5))?;
    println!("[client] requesting with response-correlation-id={registration_id}");

    // 3. send a request (retry until the server's subscription connects)
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match request_publication.offer_simple(b"hello response channels") {
            Ok(_) => break,
            Err(e) if e.is_retryable() && Instant::now() < deadline => sleep(Duration::from_millis(10)),
            Err(e) => return Err(format!("request failed: {e}").into()),
        }
    }

    // 4. await the response
    let reply: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let reply_sink = reply.clone();
    let deadline = Instant::now() + Duration::from_secs(10);
    while reply.lock().unwrap().is_none() && Instant::now() < deadline {
        response_subscription.poll_once(
            |buf, _hdr| {
                *reply_sink.lock().unwrap() = Some(String::from_utf8_lossy(buf).to_string());
            },
            16,
        )?;
        sleep(Duration::from_millis(1));
    }
    let reply = reply.lock().unwrap().clone().ok_or("no response received")?;
    println!("[client] got response {reply:?}");
    assert_eq!(reply, "HELLO RESPONSE CHANNELS");

    running.store(false, Ordering::Release);
    server
        .join()
        .expect("server thread panicked")
        .map_err(|e| e.to_string())?;
    drop(request_publication);
    drop(response_subscription);
    drop(aeron);
    stop.store(true, Ordering::SeqCst);
    driver_handle.join().ok();
    println!("request/response roundtrip complete");
    Ok(())
}
