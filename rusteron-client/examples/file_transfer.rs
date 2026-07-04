//! # File transfer
//!
//! Port of Aeron's `FileSender` / `FileReceiver` samples: stream a file as a header message
//! (correlation id, length, name) followed by offset-stamped chunks. Chunks are larger than
//! the MTU, so the receiver polls through a fragment assembler; delivery is complete when
//! every byte up to the announced length has landed. The received copy is verified against
//! the source.
//!
//! ```bash
//! cargo run --release --features "static precompile" --example file_transfer
//! ```

use rusteron_client::*;
use rusteron_media_driver::testing::EmbeddedDriver;
use std::thread::sleep;
use std::time::{Duration, Instant};

const STREAM_ID: i32 = 1005;
const FILE_SIZE: usize = 8 * 1024 * 1024;
const CHUNK_SIZE: usize = 64 * 1024;

// message tags, mirroring FileSender's message types
const TAG_HEADER: u8 = 1;
const TAG_CHUNK: u8 = 2;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // embedded media driver with RAII teardown (stops + joins on drop)
    let driver = EmbeddedDriver::launch()?;

    let ctx = AeronContext::new()?;
    ctx.set_dir(&cformat!("{}", driver.dir()))?;
    ctx.set_error_handler(Some(|code: i32, msg: &str| eprintln!("aeron error {code}: {msg}")))?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;

    let publication = aeron
        .async_add_publication(AERON_IPC_STREAM, STREAM_ID)?
        .poll_blocking(Duration::from_secs(5))?;
    let subscription = aeron
        .async_add_subscription(AERON_IPC_STREAM, STREAM_ID, Handlers::NONE, Handlers::NONE)?
        .poll_blocking(Duration::from_secs(5))?;

    // "file" contents: deterministic pseudo-random bytes
    let source: Vec<u8> = {
        let mut state = 0x9e3779b97f4a7c15u64;
        (0..FILE_SIZE)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    };
    let file_name = "sample.dat";
    let correlation_id = Aeron::nano_clock();
    println!("sending {file_name}: {FILE_SIZE} bytes in {CHUNK_SIZE}-byte chunks");

    // ── receiver state (single-threaded like FileReceiver's agent) ────────
    struct Receiver {
        correlation_id: i64,
        expected_len: u64,
        name: String,
        data: Vec<u8>,
        received: u64,
    }
    let mut receiver = Receiver {
        correlation_id: -1,
        expected_len: 0,
        name: String::new(),
        data: Vec::new(),
        received: 0,
    };
    // chunks exceed the MTU: reassembly required
    let mut assembler = AeronFragmentClosureAssembler::new()?;

    // ── sender: header, then offset-stamped chunks ────────────────────────
    // `offer_parts` gathers the parts driver-side: no per-message Vec, no copy.
    let offer = |publication: &AeronPublication, parts: &[&[u8]]| -> Result<(), AeronOfferError> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match publication.offer_parts(parts) {
                Ok(_) => return Ok(()),
                Err(e) if e.is_retryable() && Instant::now() < deadline => std::hint::spin_loop(),
                Err(e) => return Err(e),
            }
        }
    };

    let correlation_bytes = correlation_id.to_le_bytes();
    let file_len_bytes = (FILE_SIZE as u64).to_le_bytes();
    offer(
        &publication,
        &[&[TAG_HEADER], &correlation_bytes, &file_len_bytes, file_name.as_bytes()],
    )?;

    let start = Instant::now();
    let mut offset = 0usize;
    while offset < source.len() {
        let end = usize::min(offset + CHUNK_SIZE, source.len());
        let offset_bytes = (offset as u64).to_le_bytes();
        offer(
            &publication,
            &[&[TAG_CHUNK], &correlation_bytes, &offset_bytes, &source[offset..end]],
        )?;
        offset = end;

        // drain the subscription as we go (single process; a real receiver is remote)
        assembler.poll(
            &subscription,
            &mut receiver,
            |receiver, buf, _hdr| match buf[0] {
                TAG_HEADER => {
                    receiver.correlation_id = i64::from_le_bytes(buf[1..9].try_into().unwrap());
                    receiver.expected_len = u64::from_le_bytes(buf[9..17].try_into().unwrap());
                    receiver.name = String::from_utf8_lossy(&buf[17..]).to_string();
                    receiver.data = vec![0u8; receiver.expected_len as usize];
                    println!("receiving {:?}: {} bytes", receiver.name, receiver.expected_len);
                }
                TAG_CHUNK => {
                    let correlation = i64::from_le_bytes(buf[1..9].try_into().unwrap());
                    assert_eq!(correlation, receiver.correlation_id, "chunk for unknown transfer");
                    let chunk_offset = u64::from_le_bytes(buf[9..17].try_into().unwrap()) as usize;
                    let payload = &buf[17..];
                    receiver.data[chunk_offset..chunk_offset + payload.len()].copy_from_slice(payload);
                    receiver.received += payload.len() as u64;
                }
                other => panic!("unknown message tag {other}"),
            },
            64,
        )?;
    }

    // drain the tail until every byte has arrived
    let deadline = Instant::now() + Duration::from_secs(10);
    while receiver.received < FILE_SIZE as u64 && Instant::now() < deadline {
        if assembler.poll(
            &subscription,
            &mut receiver,
            |receiver, buf, _hdr| {
                if buf[0] == TAG_CHUNK {
                    let chunk_offset = u64::from_le_bytes(buf[9..17].try_into().unwrap()) as usize;
                    let payload = &buf[17..];
                    receiver.data[chunk_offset..chunk_offset + payload.len()].copy_from_slice(payload);
                    receiver.received += payload.len() as u64;
                }
            },
            64,
        )? == 0
        {
            sleep(Duration::from_millis(1));
        }
    }
    let elapsed = start.elapsed();

    assert_eq!(receiver.received, FILE_SIZE as u64, "incomplete transfer");
    assert_eq!(receiver.name, file_name);
    assert!(receiver.data == source, "received file differs from source");
    println!(
        "transferred and verified {} bytes in {elapsed:.2?} ({:.1} MB/sec)",
        FILE_SIZE,
        FILE_SIZE as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
    );

    drop(publication);
    drop(subscription);
    drop(aeron);
    Ok(())
}
