**WARNING:** This page has not been updated since the initial first commit; implementation, configuration, and performance characteristics have changed. Treat all results below as stale. Rerun the benchmarks with the current codebase before citing, and feel free to submit PRs with updated, reproducible benchmark data.

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

| Platform   | Java (msgs/sec) | Rust (msgs/sec) | Speedup |
| ---------- | --------------- | --------------- | ------- |
| M1 MacBook | \~28M           | \~36–38M        | \~1.3x  |
| EPYC 7R32  | \~11M           | \~38–39M        | \~3.5x  |

* Rust's `rusteron-client` shows strong throughput advantages, especially on high-core servers.
* Ping Pong (UDP) latencies are comparable between Rust and Java.
* Using `/dev/shm` for the Aeron directory improves performance (used on EPYC).
