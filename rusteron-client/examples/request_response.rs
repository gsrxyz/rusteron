//! # Request/response channels
//!
//! Port of Aeron's `response_server.c` and `response_client.c` samples.
//! This example also sets an explicit response session ID, covered by Aeron 1.53.2 system tests.
//! A client subscribes on a `control-mode=response` channel and stamps its
//! subscription registration id onto the request publication (`response-correlation-id=`);
//! the server reads each client's requests from that client's own image and answers on a
//! response publication keyed by the image's correlation id. No addressing logic in user
//! code. The driver wires responses back to the requester.
//!
//! ```bash
//! cargo run --release --features "static precompile" --example request_response
//! ```

use rusteron_client::*;
use rusteron_media_driver::testing::{EmbeddedDriver, find_unused_udp_port};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

const REQUEST_STREAM_ID: i32 = 10001;
const RESPONSE_STREAM_ID: i32 = 10002;
const RESPONSE_SESSION_ID: i32 = 71;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // embedded media driver with RAII teardown (stops + joins on drop)
    let driver = EmbeddedDriver::launch()?;

    let ctx = AeronContext::new()?;
    ctx.set_dir(&cformat!("{}", driver.dir()))?;
    ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    let request_port = find_unused_udp_port(21000).expect("no free port");
    let response_port = find_unused_udp_port(request_port + 1).expect("no free port");
    let request_endpoint = format!("localhost:{request_port}");
    let response_control_endpoint = format!("localhost:{response_port}");
    let request_channel = AeronUriStringBuilder::udp(&request_endpoint)?.build(256)?;

    // server (its own Aeron client, as it would be in a separate process)
    // Requests arrive on the request channel; each connecting client's image carries a
    // correlation id, which keys the response publication back to that client.
    let running = Arc::new(AtomicBool::new(true));
    let server = {
        let running = running.clone();
        let response_control = response_control_endpoint.clone();
        let request_channel = request_channel.clone();
        let aeron_dir = driver.dir().to_string();
        std::thread::spawn(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let ctx = AeronContext::new()?;
            ctx.set_dir(&cformat!("{aeron_dir}"))?;
            let aeron = Aeron::new(&ctx)?;
            aeron.start()?;

            // the image callback runs on the client conductor thread, so it only queues the new client
            let new_clients: Arc<Mutex<Vec<(i32, i64)>>> = Arc::default();
            let on_image_clients = new_clients.clone();
            let image_handler = Handler::new(move |_subscription: &AeronSubscription, image: &AeronImage| {
                if let Ok(constants) = image.get_constants() {
                    on_image_clients
                        .lock()
                        .unwrap()
                        .push((constants.session_id(), constants.correlation_id()));
                }
            });
            let server_subscription = aeron
                .async_add_subscription(
                    &cformat!("{request_channel}"),
                    REQUEST_STREAM_ID,
                    Some(&image_handler),
                    Handlers::NONE,
                )?
                .poll_blocking(Duration::from_secs(5))?;

            // declared after the subscription, so the retained images drop before it closes
            let mut clients: Vec<Client> = Vec::new();
            let mut reply = Vec::new();
            while running.load(Ordering::Acquire) {
                let arrived = std::mem::take(&mut *new_clients.lock().unwrap());
                for (session_id, correlation_id) in arrived {
                    // `None` when the client has already gone away
                    let Some(image) = server_subscription.image_by_session_id(session_id) else {
                        continue;
                    };
                    let channel = AeronUriStringBuilder::udp_control(&response_control, ControlMode::Response)?
                        .session_id(&RESPONSE_SESSION_ID.to_string())?
                        .response_correlation_id(correlation_id)?
                        .build(256)?;
                    let pending = aeron.async_add_publication(&cformat!("{channel}"), RESPONSE_STREAM_ID)?;
                    clients.push(Client {
                        image,
                        pending: Some(pending),
                        publication: None,
                    });
                }

                for client in &mut clients {
                    if let Some(pending) = &client.pending {
                        client.publication = pending.poll()?;
                        if client.publication.is_none() {
                            continue;
                        }
                        client.pending = None;
                        println!(
                            "[server] response publication ready for session {session_id}",
                            session_id = client.image.get_constants()?.session_id()
                        );
                    }
                    let Some(publication) = &client.publication else {
                        continue;
                    };
                    // requests wait in the client's image until its reply can be delivered
                    if !publication.is_connected() {
                        continue;
                    }
                    // the poll callback cannot return an error, so it parks the first one here
                    let mut failure = None;
                    client.image.poll_fn(
                        |request, _header| {
                            if failure.is_some() {
                                return;
                            }
                            reply.clear();
                            reply.extend(request.iter().map(u8::to_ascii_uppercase));
                            println!(
                                "[server] request {:?} -> reply {:?}",
                                String::from_utf8_lossy(request),
                                String::from_utf8_lossy(&reply)
                            );
                            failure = offer_reply(publication, &reply).err();
                        },
                        16,
                    )?;
                    if let Some(e) = failure {
                        return Err(format!("server reply failed: {e}").into());
                    }
                }
                clients.retain(|client| !client.image.is_closed());
                sleep(Duration::from_millis(1));
            }
            Ok(())
        })
    };

    // client
    // 1. subscribe for responses on a control-mode=response channel
    let response_subscription = aeron
        .async_add_subscription(
            &AeronUriStringBuilder::udp_control(&response_control_endpoint, ControlMode::Response)?
                .session_id(&RESPONSE_SESSION_ID.to_string())?
                .build(256)?
                .into_c_string(),
            RESPONSE_STREAM_ID,
            Handlers::NONE,
            Handlers::NONE,
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
        match request_publication.offer(b"hello response channels") {
            Ok(_) => break,
            Err(e) if e.is_retryable() && Instant::now() < deadline => sleep(Duration::from_millis(10)),
            Err(e) => return Err(format!("request failed: {e}").into()),
        }
    }

    // 4. await the response
    let mut reply = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while reply.is_none() && Instant::now() < deadline {
        response_subscription.poll_fn(
            |buf, header| {
                assert_eq!(header.session_id(), Some(RESPONSE_SESSION_ID));
                reply = Some(String::from_utf8_lossy(buf).into_owned());
            },
            16,
        )?;
        sleep(Duration::from_millis(1));
    }
    let reply = reply.ok_or("no response received")?;
    println!("[client] got response {reply:?}");
    assert_eq!(reply, "HELLO RESPONSE CHANNELS");

    running.store(false, Ordering::Release);
    server
        .join()
        .expect("server thread panicked")
        .map_err(|e| e.to_string())?;
    println!("request/response roundtrip complete");
    Ok(())
}

/// A connected requester: its request image, and the response publication keyed by the
/// image's correlation id, added without blocking the server loop.
struct Client {
    image: AeronImage,
    pending: Option<AeronAsyncAddPublication>,
    publication: Option<AeronPublication>,
}

fn offer_reply(publication: &AeronPublication, reply: &[u8]) -> Result<i64, AeronOfferError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match publication.offer(reply) {
            Err(e) if e.is_retryable() && Instant::now() < deadline => sleep(Duration::from_millis(1)),
            result => return result,
        }
    }
}
