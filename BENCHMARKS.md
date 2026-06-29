**Note:** These benchmarks are environment-sensitive. Rust IPC throughput was re-measured 2026-06-27 on an Apple M1 Pro (10-core) — see [Apple M1 Pro (re-measured)](#apple-m1-pro-10-core--re-measured-2026-06-27) below. The original M1/EPYC and Java figures are retained for comparison; rerun locally for your own hardware.

# Aeron IPC Throughput Benchmarks: Java vs. rusteron (Rust)

**Note**: These benchmarks are early-stage and environment-sensitive. Interpret results with caution until verified across varied systems.

## Systems Tested

1. Apple M1 MacBook Pro  
2. AMD EPYC 7R32 (48-core)

## What Was Measured

We compared Aeron’s `EmbeddedExclusiveIpcThroughput` benchmark in Java with the Rust port at `rusteron-client/examples/embedded_exclusive_ipc_throughput.rs`.

## How to Run

### Java
```bash
just benchmark-ipc-throughput-java
````

### Rust

```bash
just run-aeron-media-driver-rust
just benchmark-ipc-throughput-rust
```

## Results

### Apple M1 MacBook Pro

**Java**: \~27–29 million msgs/sec
**Rust**: \~35–38 million msgs/sec

**Example (Rust)**:

```
Throughput: 36,859,281 msgs/sec, 1,179,496,981 bytes/sec
...
```

### Apple M1 Pro (10-core) — re-measured 2026-06-27

Rust, 32-byte IPC, SHARED client threading + DEDICATED media driver, steady-state per-second samples.

**Rust**: \~32–51 million msgs/sec (typically \~36–40M, peak \~51M)

**Example (Rust)**:

```
Throughput: 39,248,311 msgs/sec, 1,255,945,944 bytes/sec
Throughput: 50,859,457 msgs/sec, 1,627,502,615 bytes/sec
Throughput: 36,694,513 msgs/sec, 1,174,224,425 bytes/sec
...
```

(Java was not re-measured in this run; the M1 Java figure above is a reasonable reference.)

### AMD EPYC 7R32 (48-core)

**Java**: \~10.8–11.2 million msgs/sec
**Rust**: \~38–39 million msgs/sec

**Example (Rust)**:

```
Throughput: 39,360,449 msgs/sec, 1,259,534,357 bytes/sec
...
```

Rust consistently outperformed Java by \~3.5x in this benchmark.

---

## Ping Pong Benchmark (UDP, EPYC)

* Warm-up: 100,000 messages
* Main run: 10,000,000 messages (32-byte payload)
* Channels: `aeron:udp?endpoint=localhost:20123` and `:20124`
* Regular (not exclusive) publications used.

### Rust

```
avg: 9.918µs
p99: 12.799µs
max: 138.936ms
```

### Java

```
avg: 9.290µs
p99: ~12–16µs
max: 650.641ms
```

---

## Summary

| Platform             | Java (msgs/sec) | Rust (msgs/sec) | Speedup |
| -------------------- | --------------- | --------------- | ------- |
| M1 MacBook           | \~28M           | \~36–38M        | \~1.3x  |
| M1 Pro (2026-06-27)  | \~28M (ref)     | \~37M (32–51M)  | \~1.3x  |
| EPYC 7R32            | \~11M           | \~38–39M        | \~3.5x  |

* Rust's `rusteron-client` shows strong throughput advantages, especially on high-core servers.
* Ping Pong (UDP) latencies are comparable between Rust and Java.
* Using `/dev/shm` for the Aeron directory improves performance (used on EPYC).
