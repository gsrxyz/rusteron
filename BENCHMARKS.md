These benchmarks are environment-sensitive; rerun them on your own hardware. Everything below was measured on 2026-10-09 on two Azure `Standard_D8s_v6` VMs (Intel Xeon Platinum 8573C, 4 cores × 2 SMT threads, 32 GiB) in one proximity placement group in North Central US, with accelerated networking (the MANA NIC). They ran Debian 13 with Linux 7.2.6 (the `trixie-backports` cloud kernel), Aeron 1.52.2 and rustc 1.95.0. UDP is measured between the two VMs, never over loopback. The archive-on-disk section comes from a later run (2026-10-10) on two `Standard_D8ds_v6` in West US 3.

# Intel D8s_v6 pair on Azure (2026-10-09)

## Summary

- **UDP between the hosts.** A 32-byte round trip took 38 µs at p50 with a pinned, dedicated driver on the stock kernel. With CPU isolation and `net.core.busy_read`, it took 31 µs at p50 and 46 µs at p99.99, against 157 µs without busy polling.
  - Putting the NIC's interrupts on the receiver's core did nearly as well (p99.99 49–57 µs). With the kernel tuned it gave the lowest max, 74 µs (62–84).
  - Without one of these two, p99.99 stayed at 120–160 µs in every kernel state. On the stock kernel neither helped; only on isolated CPUs did they cut the tail.
- UDP throughput (32-byte messages):
  - 2 MiB socket buffers and receiver window: 13 M msgs/s. Aeron's default 128 KiB gives 1.9 M.
  - MTU 9000, with Aeron's MTU at 8192: 28.5 M msgs/s.
  - Socket sizes made no difference to round-trip latency, with one message in flight.
- **IPC on one host.** The p50 was about 0.31 µs whatever is pinned.
  - Isolation cut p99.99 from 2.5 to 0.8–1.0 µs, and most reps' max to 4–7 µs.
  - The client's other threads (its conductor) must stay off the ping and pong CPUs. On them, p99.99 stayed at 3.1–3.6 µs.
- Kernel tuning beyond isolation:
  - SMT off, `idle=poll`, no watchdogs and the rest of the tuned state took 3.7 µs off the cross-host p50 (38.6 to 34.9 µs). IPC didn't change.
  - `mitigations=off` took off another 1 µs and lowered p99.99 from 156 to 121 µs, at a security cost.
- **Java archive.** Recording to tmpfs ran at 6.2 M msgs/s (1.6 GB/s, 256-byte messages). Confining the whole JVM to the housekeeping CPU cost 25%.
- Java archive on disk (two D8ds_v6, 2026-10-10):
  - At file sync level 0 the page cache absorbed bursts, and the disk set the sustained rate, at 545 MB/s on local NVMe, and about the provisioned rate on Premium SSD v2 once the disks had been in use for a while. New ones were slower.
  - A Premium SSD v2 at its included 125 MB/s took 30 s bursts of 400 MB/s at full rate once `vm.dirty_bytes` allowed 16 GiB of unwritten data. With Linux's defaults it throttled after 15 s.
  - Replays slowed to 59–246 MB/s while recording ran flat out at the disk's limit.
- Packet loss and distance (2026-10-10):
  - 0.1% UDP loss cut throughput by a third and 1% by 91%, though every lost packet was retransmitted.
  - A ping-pong that lost a packet waited about 100 ms for the driver's heartbeat.
  - Across regions (53 ms round trip), 2 MiB windows carried 18 MB/s and 16 MiB windows 119 MB/s. At 0.1% loss both fell to 10–13 MB/s.

## Method

- **Machines.** Both VMs are created and deleted by `scripts/x86-lab/lab.sh` with `LAB_PAIR=1 LAB_LOCKSTEP=1`. Each phase runs on both VMs and finishes on both before the next starts, so both hosts are always in the same kernel state.
  - The first VM (intel-a) pings and the second (intel-b) pongs.
  - SMT twins are adjacent: CPUs 0–1, 2–3, 4–5, 6–7.
- **CPU layout.**
  - CPU 0 does housekeeping alone: interrupts, the drivers' conductors and the clients' other threads.
  - Every busy-spinning thread is pinned to its own core.
  - On one host (IPC): ping on CPU 2, pong on 4; a SHARED driver, where tested, on 6.
  - Across hosts: the app thread (ping or pong) on 2, the driver's sender on 4 and its receiver on 6. The driver pins its own threads (`AERON_CONDUCTOR_CPU_AFFINITY` and the like).
- Kernel states, applied one after the other to both VMs:

| State       | What it adds                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| pinned      | the stock kernel, with the pinning above                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| isolated    | `isolcpus=nohz,domain,managed_irq,1-7 nohz_full=1-7 rcu_nocbs=1-7 irqaffinity=0`                                                                                                                                                                                                                                                                                                                                                                                                 |
| tuned       | `nosmt idle=poll rcu_nocb_poll nowatchdog nmi_watchdog=0 nosoftlockup skew_tick=1 transparent_hugepage=never audit=0`. At boot: every IRQ and the kernel workqueues on CPU 0 (re-pinned before each cross-host run); `irqbalance`, the Azure agent and the apt, man-db, fstrim and e2scrub timers stopped; KSM off; `kernel.watchdog=0`, `vm.stat_interval=120`, `kernel.numa_balancing=0`, a quiet `kernel.printk`, `rp_filter=2`; swap off. SMT off leaves CPUs 0, 2, 4 and 6. |
| tuned-nomit | tuned, plus `mitigations=off` (the CPU vulnerability files then read "Vulnerable")                                                                                                                                                                                                                                                                                                                                                                                               |

- **Aeron settings throughout.**
  - Non-sparse term buffers (`AERON_TERM_BUFFER_SPARSE_FILE=false`) and pre-touched client mappings (`AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY=true`).
  - `AERON_DIR` on 2 MiB `hugetlbfs` (`AERON_FILE_PAGE_SIZE=2097152`).
  - Across hosts, the socket profile of Adaptive's low-latency driver: `AERON_SOCKET_SO_SNDBUF`, `AERON_SOCKET_SO_RCVBUF` and `AERON_RCV_INITIAL_WINDOW_LENGTH` all 2 MiB, with `net.core.rmem_max` and `wmem_max` at 16 MiB. The "defaults" rows keep Aeron's own.
  - The dedicated driver's sender and receiver use `noop` idle; its conductor keeps the default backoff.
  - Exclusive publications; pong echoes with `try_claim`.
- **Workloads.**
  - Round trip: 32-byte messages, one in flight. IPC does 2,000,000 round trips after 200,000 of warm-up; UDP does 300,000 after 50,000. Each run stops after 20 s.
  - Throughput: 32-byte messages offered flat out. The figure is the median of five one-second samples after a one-second warm-up.
  - Each state runs 5 reps, with the variants in a different order each rep.
- **Provenance.** The run recorded commit `62ec7a0`. Before any benchmark phase, its `vm.sh` was replaced with one that also puts `/usr/sbin` on the `PATH` (`2a39f78`), so the per-run NIC counters could call `ethtool`.
- **Reading the tables.**
  - Each cell is the median across reps, with the range across reps in brackets.
  - A difference counts only when the ranges do not overlap.
  - Max is the slowest single round trip of a rep, one sample, so it varies most.
- **Validity checks.**
  - Azure can move a VM's traffic from the NIC's direct path (the VF) to its software path during host servicing. Every cross-host run recorded both hosts' VF packet counters and any "data path switched" messages. All 200 runs stayed on the VF.
  - The busy-polling runs recorded the kernel's busy-poll receive counter: about 180,000–270,000 per run on each host, so busy polling did run.
  - Azure's scheduled-events service was queried at the start of each phase, and 24 queries listed no events while 20 returned nothing.
  - The clock source was `tsc` throughout.

## UDP round trip between the two hosts (µs)

Variants:

- dedicated: sender and receiver threads, each pinned with `noop` idle.
- defaults: the same, with Aeron's default socket buffers and window.
- SHARED_NETWORK: one `noop` thread does sending and receiving; the conductor stays on CPU 0.
- SHARED: the whole driver is one `noop` thread on CPU 4.
- busy_read: dedicated, with `net.core.busy_read=50`, and on the VF `napi_defer_hard_irqs=2` and `gro_flush_timeout=200000`, on both hosts.
- IRQs on receiver core: dedicated, with the NIC queues' IRQs on CPU 6, where the receiver spins.

| state, variant                     | reps | p50                 | p99                 | p99.9               | p99.99                 | max                     |
| ---------------------------------- | ---- | ------------------- | ------------------- | ------------------- | ---------------------- | ----------------------- |
| pinned, dedicated                  | 5    | 38.34 (33.12–41.57) | 41.50 (35.84–58.05) | 52.51 (44.06–65.98) | 132.48 (117.12–156.93) | 420.86 (324.35–581.12)  |
| pinned, defaults                   | 5    | 38.17 (36.58–43.65) | 50.02 (40.96–51.26) | 53.95 (50.85–61.44) | 124.22 (71.68–164.35)  | 479.74 (179.20–872.45)  |
| pinned, SHARED_NETWORK             | 5    | 40.73 (34.98–42.14) | 45.41 (37.50–56.22) | 55.30 (47.39–62.02) | 130.30 (125.50–147.46) | 349.44 (273.66–378.62)  |
| pinned, SHARED                     | 5    | 39.74 (37.28–41.98) | 45.09 (43.33–54.91) | 57.09 (54.37–62.69) | 160.64 (158.34–179.97) | 655.87 (462.85–1256.45) |
| pinned, busy_read                  | 5    | 33.09 (31.07–34.62) | 39.30 (34.56–49.41) | 43.84 (40.93–63.42) | 147.07 (115.26–170.37) | 430.08 (275.20–607.74)  |
| pinned, IRQs on receiver core      | 5    | 33.34 (33.31–33.38) | 35.71 (35.55–36.06) | 44.73 (44.00–45.76) | 120.45 (116.86–150.14) | 726.01 (284.93–988.67)  |
| isolated, dedicated                | 5    | 38.62 (38.40–38.94) | 51.30 (50.88–53.09) | 61.09 (59.90–64.48) | 157.18 (146.69–161.79) | 575.49 (338.69–1597.44) |
| isolated, defaults                 | 5    | 38.66 (38.21–38.66) | 51.45 (51.04–51.62) | 60.67 (60.38–62.72) | 157.31 (149.89–170.50) | 449.79 (310.01–683.01)  |
| isolated, SHARED_NETWORK           | 5    | 39.81 (39.30–39.97) | 53.31 (53.02–53.76) | 63.65 (62.85–64.29) | 157.95 (156.41–178.56) | 761.34 (647.17–1935.36) |
| isolated, SHARED                   | 5    | 40.77 (39.90–40.80) | 46.37 (45.53–46.62) | 53.66 (53.12–55.23) | 156.93 (151.42–166.91) | 472.83 (333.82–919.55)  |
| isolated, busy_read                | 5    | 31.05 (30.77–31.87) | 34.05 (33.44–35.30) | 37.60 (36.06–38.46) | 45.98 (42.24–49.34)    | 230.40 (179.71–343.04)  |
| isolated, IRQs on receiver core    | 5    | 34.17 (33.82–34.17) | 37.15 (36.93–38.14) | 43.20 (41.82–54.14) | 56.58 (52.48–81.22)    | 173.82 (160.51–1132.54) |
| tuned, dedicated                   | 5    | 34.88 (34.72–35.04) | 39.36 (39.04–39.49) | 43.45 (42.91–43.94) | 156.29 (150.53–162.30) | 522.75 (481.02–1098.75) |
| tuned, defaults                    | 5    | 34.98 (34.81–35.07) | 39.30 (39.26–39.30) | 42.88 (42.72–43.23) | 147.20 (142.97–156.93) | 497.15 (431.36–532.48)  |
| tuned, SHARED_NETWORK              | 5    | 35.62 (35.62–35.65) | 39.68 (39.62–39.84) | 44.13 (43.84–44.35) | 149.76 (142.59–158.21) | 577.53 (490.75–1926.14) |
| tuned, SHARED                      | 5    | 34.91 (34.85–34.98) | 38.27 (38.14–38.46) | 44.29 (44.09–44.35) | 153.34 (147.46–166.27) | 500.99 (475.65–748.03)  |
| tuned, busy_read                   | 5    | 31.58 (30.93–32.45) | 35.01 (33.70–37.76) | 38.56 (37.05–40.90) | 43.90 (43.42–52.99)    | 120.13 (89.41–830.98)   |
| tuned, IRQs on receiver core       | 5    | 34.21 (34.14–34.30) | 36.73 (36.58–36.86) | 42.11 (41.63–42.46) | 51.77 (51.07–53.02)    | 74.24 (62.02–83.52)     |
| tuned-nomit, dedicated             | 5    | 33.85 (33.53–33.95) | 38.24 (37.98–38.37) | 43.10 (42.08–59.36) | 120.58 (113.47–123.97) | 172.41 (159.87–299.26)  |
| tuned-nomit, defaults              | 5    | 33.92 (33.57–34.14) | 38.30 (37.89–38.53) | 41.79 (40.67–42.21) | 125.25 (119.55–135.04) | 285.95 (160.77–677.38)  |
| tuned-nomit, SHARED_NETWORK        | 5    | 34.88 (34.53–34.98) | 38.59 (38.02–39.42) | 42.81 (42.08–44.16) | 130.75 (126.85–138.24) | 432.89 (185.47–1793.02) |
| tuned-nomit, SHARED                | 5    | 34.05 (33.47–34.11) | 37.18 (36.35–39.68) | 42.59 (42.11–45.02) | 126.85 (124.86–139.01) | 183.94 (157.82–489.47)  |
| tuned-nomit, busy_read             | 5    | 31.01 (30.86–31.05) | 34.05 (33.82–34.62) | 36.83 (36.54–38.40) | 49.89 (41.47–57.53)    | 406.78 (57.85–509.69)   |
| tuned-nomit, IRQs on receiver core | 5    | 32.99 (32.75–33.09) | 35.71 (34.91–36.64) | 39.62 (39.36–41.28) | 49.25 (48.06–49.76)    | 72.51 (67.20–79.36)     |

- Busy polling (`busy_read`): on isolated cores, against dedicated without it, p50 fell from about 38 to 31 µs and p99.99 from about 157 to 44–50 µs. On the stock kernel the ranges overlap.
- NIC IRQs on the receiver core:
  - With isolation, p99.99 fell to 49–57 µs.
  - In the tuned states it gave the lowest max, 72–74 µs; dedicated alone gave 172–523 µs.
  - Against busy polling, the two can't be ranked: their p99.99 and max ranges overlap.
- The tuned state cut the dedicated driver's p50 from 38.6 to 34.9 µs. `mitigations=off` cut it to 33.9 µs and its p99.99 from 156 to 121 µs.
- **Driver threading.** SHARED, SHARED_NETWORK and dedicated were within about 2 µs of each other at p50 in every state.
- **Socket buffer size.** No effect on round-trip latency.
- The lowest p50 was about 31 µs per round trip, with this NIC and the kernel network stack.

### A second pair, with busy polling and IRQs combined (West US 3, 2026-10-10)

Two `Standard_D8ds_v6` in one placement group in West US 3, isolated, with the same settings, plus one more variant, busy_read and the NIC IRQs on the receiver core together.

| isolated, variant                   | reps | p50              | p99              | p99.9            | p99.99              | max                    |
| ----------------------------------- | ---- | ---------------- | ---------------- | ---------------- | ------------------- | ---------------------- |
| dedicated                           | 5    | 45.6 (44.9–45.7) | 61.0 (60.3–62.0) | 82.3 (73.3–89.7) | 181.0 (170.2–198.5) | 1291.3 (618.0–2758.7)  |
| defaults                            | 5    | 45.5 (45.1–45.8) | 60.3 (59.8–60.8) | 78.7 (69.4–82.7) | 178.4 (164.9–190.6) | 1549.3 (312.6–14237.7) |
| SHARED_NETWORK                      | 5    | 46.7 (46.3–47.0) | 61.2 (61.1–61.9) | 80.9 (74.0–83.0) | 181.8 (178.3–189.2) | 1738.8 (916.0–6602.8)  |
| SHARED                              | 5    | 46.7 (46.3–47.1) | 55.4 (54.8–57.7) | 81.6 (79.3–88.0) | 182.3 (178.7–184.7) | 1721.3 (798.7–2168.8)  |
| busy_read                           | 5    | 37.5 (37.4–37.6) | 43.8 (43.2–44.1) | 59.4 (48.9–63.1) | 75.8 (66.9–78.9)    | 700.9 (181.0–740.9)    |
| IRQs on receiver core               | 5    | 39.0 (38.7–39.2) | 45.9 (45.2–48.8) | 62.3 (56.7–69.5) | 78.7 (73.5–82.4)    | 177.0 (100.2–184.1)    |
| busy_read and IRQs on receiver core | 5    | 39.0 (38.7–40.1) | 46.3 (45.4–46.5) | 57.4 (50.3–63.3) | 75.0 (61.1–78.5)    | 176.0 (164.7–182.7)    |

- **Compare within this pair only.** From p50 to p99.99 its figures were 5–30 µs above the North Central US pair's, and its max was hundreds of µs higher. The pairs differed in region, VM size and day.
- **The two together gained nothing over either alone.**
  - p50 matched the IRQs variant, and busy_read alone stayed the lowest.
  - p99.99 matched both.
  - The max matched the IRQs variant's 0.18 ms, against busy_read's 0.70 ms.
- **The ranking held.** In this pair, busy_read and IRQs on the receiver core cut p50 by 8.1 and 6.6 µs, and p99.99 by more than half.
- Throughput (2 MiB windows): SHARED_NETWORK gave 11.15 (10.82–11.43) M msgs/s, the same as dedicated at 11.22 (10.94–11.35). SHARED gave 9.86 (9.48–10.38), 12% less.

## UDP throughput between the two hosts (M msgs/s, 32-byte messages)

Publisher on intel-a, subscriber on intel-b, both drivers dedicated with pinned `noop` sender and receiver.

| variant                                              | pinned              | isolated            | tuned               | tuned-nomit         |
| ---------------------------------------------------- | ------------------- | ------------------- | ------------------- | ------------------- |
| 2 MiB socket buffers and window                      | 13.03 (9.48–13.63)  | 13.00 (12.66–13.05) | 12.66 (12.59–12.71) | 14.05 (13.98–14.47) |
| Aeron's defaults (128 KiB receive buffer and window) | 1.94 (1.94–1.94)    | 1.93 (1.93–1.93)    | 1.93 (1.93–1.94)    | 1.94 (1.94–1.94)    |
| 2 MiB, MTU 9000 with `AERON_MTU_LENGTH=8192`         | 28.48 (23.77–28.77) | 28.47 (28.16–28.75) | 28.73 (28.62–28.90) | 28.77 (28.44–28.80) |
| 2 MiB, 16-message io vectors and sends               | 13.65 (9.46–14.32)  | 13.89 (13.43–13.96) | 13.36 (13.26–13.39) | 14.79 (14.74–15.12) |

- **Receiver window.** With Aeron's defaults, the 128 KiB window caps a 32-byte stream at 1.9 M msgs/s. 2 MiB raised it 6.7×.
- Jumbo frames doubled throughput again.
  - Azure allows MTU 9000 only inside a VNet and directly peered VNets.
  - The run kept the default route at 1500, so traffic leaving the VNet still fit: `ip route replace <default route> mtu 1500`, then `ip link set eth0 mtu 9000`.
- **Batching.** 16-message io vectors and sends (`AERON_SENDER_IO_VECTOR_CAPACITY`, `AERON_RECEIVER_IO_VECTOR_CAPACITY`, `AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND`, default 4) added 5–7% in the isolated and tuned states. On the stock kernel the ranges overlap.

## Packet loss and distance (West US 3 and North Central US, 2026-10-10)

The West US 3 pair above, isolated, plus a third VM, a `Standard_D8s_v6` in North Central US, reached over global VNet peering.

- **Loss.** A share of the UDP packets arriving from the peer was dropped on both hosts, in nftables' input hook (`numgen random`), so neither end was told, as with loss on the wire.
- **Settings.** Both drivers dedicated, with pinned `noop` sender and receiver and 2 MiB windows.
- **Throughput.** 32-byte messages for 8 s. Each run read the publisher's driver counters (AeronStat) and both hosts' drop counts.
- **Round trips.** One 32-byte message in flight. Across regions they ran for 60 s, about 1,100 round trips, so p99.9 and above are not given there.
- **Reps.** 3 in the same zone and 2 across regions. Ranges are given where reps differed. Cells without a range are medians, or the mean of two; across regions the max lists both reps.
- **Provenance.** The run recorded commit `e2f315a`. The 1 ms status message pass was added to both hosts mid-run; it is in `83b9e2e`.

| link, loss           | throughput (M msgs/s)     | round trip p50 | p99     | p99.9   | max           | per throughput run: data packets lost, NAKs received, retransmits sent |
| -------------------- | ------------------------- | -------------- | ------- | ------- | ------------- | ---------------------------------------------------------------------- |
| same zone, none      | 11.18 (11.03–11.40)       | 45.7 µs        | 60.6 µs | 75.7 µs | 1.09 ms       | 0, 0, 0                                                                |
| same zone, 0.1%      | 7.51 (7.40–7.57)          | 46.6 µs        | 61.5 µs | 102 ms  | 201 ms        | 1,982, 2,006, 1,979                                                    |
| same zone, 1%        | 0.99 (0.99–1.01)          | 47.1 µs        | 102 ms  | 103 ms  | 204 ms        | 2,779, 2,949, 2,754                                                    |
| across regions, none | 0.56 (0.55–0.56), 18 MB/s | 53.0 ms        | 53.0 ms |         | 53.1, 53.2 ms | 0, 0, 0                                                                |
| across regions, 0.1% | 0.29 (0.26–0.32), 9 MB/s  | 53.3 ms        | 53.4 ms |         | 53.0, 208 ms  | 86, 4,334, 507                                                         |
| across regions, 1%   | 0.04 (0.04–0.04), 1 MB/s  | 53.0 ms        | 208 ms  |         | 208, 362 ms   | 130, 6,107, 714                                                        |

- Every lost data packet was NAKed and retransmitted, so loss cost time, not data.
- **In the same zone, each loss drew about one NAK and one retransmit. Across regions, each drew about 50 NAKs and 6 retransmits.**
  - The receiver repeats its NAK until the repair arrives.
  - The sender ignores a repeat for only 10 ms after it retransmits (`AERON_RETRANSMIT_UNICAST_LINGER`), a fifth of the 53 ms round trip. That would give the 6 retransmits.
- **A lost ping or echo costs about 100 ms.** In a ping-pong nothing follows the lost packet, so the receiver notices the gap only at the sender's next heartbeat. The C driver sends one after 100 ms without data (`AERON_NETWORK_PUBLICATION_HEARTBEAT_TIMEOUT_NS`, a compile-time constant).
  - At 0.1% loss that put p99.9 at 102 ms, and at 1% p99.
  - A continuous stream doesn't wait like this, as the next packet shows the gap.
- **Throughput fell a third at 0.1% loss and 91% at 1%.**
  - The receiver's periodic status message every 1 ms instead of 200 (`AERON_RCV_STATUS_MESSAGE_TIMEOUT=1000000`) changed nothing. Throughput was 7.58 and 0.99 M msgs/s. So lost status messages are not the cause.
  - What is, this run didn't find.
- **Across regions, on a clean link, the window sets throughput.** 2 MiB over a 53 ms round trip gave 18 MB/s, under half the 40 MB/s that window allows per round trip.

A later run measured wider windows across the same regions, using 16 MiB socket buffers and initial window over 64 MiB terms, with `net.core.rmem_max`/`wmem_max` at 16 MiB. Each cell is the mean of two reps, which were within 0.06 M msgs/s of each other. Its 2 MiB rows matched the run above at 18, 10 and 1 MB/s.

| across regions, variant          | no loss                 | 0.1% loss     | 1% loss      |
| -------------------------------- | ----------------------- | ------------- | ------------ |
| 2 MiB windows                    | 0.57 M msgs/s, 18 MB/s  | 0.31, 10 MB/s | 0.03, 1 MB/s |
| 16 MiB windows                   | 3.73 M msgs/s, 119 MB/s | 0.40, 13 MB/s | 0.04, 1 MB/s |
| 2 MiB, status message every 1 ms | 0.56 M msgs/s, 18 MB/s  | 0.29, 9 MB/s  | 0.04, 1 MB/s |

- 16 MiB windows carried 6.6 times as much on a clean link, still about 38% of what that window allows per round trip.
- 0.1% loss took almost all of that back, and at 1% every variant carried 1 MB/s.
- **A lost ping or echo across regions cost about 208 ms.** That is the 100 ms heartbeat plus about four one-way trips, for the gap, the NAK, the retransmit and the echo.

## IPC on one host (µs, M msgs/s)

The driver is dedicated with its default idle strategies, so no unpinned thread spins, except in the SHARED rows, where a `noop` SHARED driver has CPU 6 to itself. Rows are given per VM. The isolated, tuned and tuned-nomit states run only the variants that pin every busy thread. The kernel keeps unpinned work off isolated CPUs, so it would crowd onto CPU 0.

| state, variant                                    | intel-a p50 | p99.99              | max                  | M msgs/s | intel-b p50 | p99.99             | max                   | M msgs/s |
| ------------------------------------------------- | ----------- | ------------------- | -------------------- | -------- | ----------- | ------------------ | --------------------- | -------- |
| pinned, nothing pinned                            | 0.310       | 8.33 (2.27–9.22)    | 43.55 (24.66–293.38) | 74.5     | 0.311       | 8.78 (3.10–9.34)   | 157.95 (51.74–664.58) | 67.2     |
| pinned, ping and pong pinned                      | 0.307       | 2.45 (2.28–2.62)    | 38.24 (20.54–67.07)  | 79.2     | 0.312       | 2.75 (2.63–3.12)   | 48.77 (28.37–60.00)   | 68.2     |
| pinned, client threads on ping and pong CPUs      | 0.307       | 7.34 (6.60–7.59)    | 38.14 (21.42–52.70)  | 76.5     | 0.312       | 7.71 (7.04–7.77)   | 36.41 (30.69–62.91)   | 68.4     |
| pinned, SHARED driver                             | 0.373       | 6.16 (4.42–7.30)    | 43.81 (27.07–69.25)  | 43.5     | 0.404       | 7.08 (6.77–7.67)   | 54.69 (32.83–61.70)   | 38.9     |
| isolated, ping and pong pinned                    | 0.307       | 0.804 (0.730–0.811) | 4.51 (4.23–55.13)    | 78.7     | 0.300       | 1.04 (0.988–1.09)  | 6.63 (4.95–63.36)     | 69.4     |
| isolated, client threads on ping and pong CPUs    | 0.306       | 3.29 (2.96–4.30)    | 28.40 (22.96–65.79)  | 79.0     | 0.304       | 3.10 (3.01–3.82)   | 24.43 (21.63–31.92)   | 68.1     |
| isolated, SHARED driver                           | 0.356       | 1.24 (1.13–1.31)    | 4.72 (4.16–63.10)    | 42.1     | 0.396       | 1.43 (1.35–20.67)  | 5.59 (5.48–60.03)     | 38.7     |
| tuned, ping and pong pinned                       | 0.315       | 0.732 (0.647–0.849) | 5.21 (4.71–5.32)     | 70.1     | 0.311       | 0.974 (0.801–1.04) | 4.73 (4.65–68.61)     | 70.0     |
| tuned, client threads on ping and pong CPUs       | 0.315       | 3.21 (3.08–4.84)    | 27.18 (21.49–31.65)  | 70.3     | 0.311       | 3.35 (3.29–4.25)   | 23.95 (16.35–26.06)   | 72.7     |
| tuned, SHARED driver                              | 0.367       | 1.25 (1.15–1.66)    | 5.26 (5.03–57.95)    | 39.5     | 0.416       | 1.24 (1.16–1.34)   | 5.12 (4.88–39.84)     | 38.8     |
| tuned-nomit, ping and pong pinned                 | 0.315       | 0.686 (0.644–1.06)  | 4.41 (4.11–65.28)    | 73.7     | 0.299       | 1.06 (0.876–1.17)  | 4.47 (4.32–16.17)     | 71.7     |
| tuned-nomit, client threads on ping and pong CPUs | 0.316       | 3.46 (3.06–4.34)    | 20.96 (18.14–59.97)  | 76.0     | 0.302       | 3.56 (3.09–3.71)   | 20.83 (17.31–58.49)   | 68.9     |
| tuned-nomit, SHARED driver                        | 0.387       | 1.20 (1.16–1.30)    | 30.41 (5.12–67.71)   | 40.8     | 0.371       | 1.39 (1.10–1.44)   | 14.17 (4.42–69.06)    | 42.4     |

Across every rep without the SHARED driver, p50 was 0.297–0.317 µs, p99 0.35–0.41 µs and p99.9 0.43–0.54 µs. With it, p50 was 0.33–0.43 µs, p99 0.44–0.63 µs and p99.9 0.47–1.45 µs. The full tables, with every percentile and range, are in the run's results folder (see [How to run](#how-to-run)).

- The median didn't move with pinning or kernel state.
- Isolation cut p99.99 from 2.5–2.8 to 0.8–1.0 µs on both VMs. Most isolated reps had a max of 4–7 µs, against 20–67 µs pinned; the occasional 55–65 µs outlier keeps the max ranges overlapping.
- The client's other threads, its conductor among them, held p99.99 at 3.1–3.6 µs when they ran on the ping and pong CPUs, in every isolated or tuned state. Give them a CPU of their own.
- **A SHARED driver costs IPC.** Its single spinning thread kept p50 at 0.36–0.42 µs and halved throughput.
- Tuning beyond isolation, `mitigations=off` included, made no measurable difference to IPC.

## Java archive (intel-a)

An `ArchivingMediaDriver` (Java 21) runs with `-Xms1g -Xmx1g -XX:+AlwaysPreTouch -XX:+UseParallelGC -XX:-UsePerfData -XX:GuaranteedSafepointInterval=300000`, DEDICATED archive threading, and `AERON_DIR` and the archive directory on tmpfs, so that disk speed doesn't hide the CPUs. The recording test records 1,000,000 messages of 256 bytes over IPC, published from a thread pinned to CPU 2. The round-trip test is the IPC ping-pong above, with the ping stream recorded.

| JVM                                        | reps | published (M msgs/s) | recorded (M msgs/s) | recorded MB/s | recorded-ping round trip p50 / p99 / p99.99 / max (µs) |
| ------------------------------------------ | ---- | -------------------- | ------------------- | ------------- | ------------------------------------------------------ |
| unpinned                                   | 5    | 6.72 (6.50–6.90)     | 6.13 (5.88–6.24)    | 1569          | lost to a harness race (see below)                     |
| on CPU 0                                   | 5    | 5.11 (4.83–5.21)     | 4.62 (4.46–4.73)    | 1182          | 0.317 / 1.90 / 8.78 / 38.1                             |
| on CPU 0, archive-recorder thread on CPU 6 | 5    | 6.83 (6.59–6.93)     | 6.23 (6.00–6.27)    | 1595          | 0.322 / 1.93 / 9.10 / 43.4 (4 reps)                    |
| as above, recorder `noop` idle             | 5    | 6.86 (6.75–6.90)     | 6.23 (6.16–6.27)    | 1594          | 0.322 / 1.93 / 11.0 / 22.9 (3 reps)                    |

- The recorded ping-pong ran on the archive's embedded Java driver, not the C driver. Its p99 of about 1.9 µs, against 0.37 µs on the C driver without recording, combines the driver change with the recording, so neither cause is isolated.
- **Placement.** Confining the whole JVM to the housekeeping CPU cut recording throughput by 25%; pinning its recorder to a core of its own got it back. A busy-spinning recorder added nothing.
- Missing reps were lost to a race in the benchmark itself, not the archive. The ping started once its publication was connected, but the archive's recording subscription also connects it. So ping sometimes sent before pong had subscribed, pong missed that first message, and both waited forever. Aeron's counters confirmed it, showing the ping stream at 64 bytes, recorded by the archive, and pong's subscription joined at 64. The benchmark now waits for pong's own subscription; locally that took the recorded ping-pong from 0 of 5 to 5 of 5.

## Java archive on disk, under load (two D8ds_v6, 2026-10-10)

A separate run used two `Standard_D8ds_v6` VMs, with the same CPU as above plus a 440 GiB local NVMe disk, in zone 1 of West US 3, on the stock kernel (Linux 7.2.6) with the pinned layout. Each VM recorded to three disks in turn, each formatted xfs and mounted `noatime`:

| Disk                     | What it is                                                                 | Price                  |
| ------------------------ | -------------------------------------------------------------------------- | ---------------------- |
| local NVMe               | the VM's own disk; its data is lost when the VM stops or its host fails    | included in the VM     |
| Premium SSD v2, 125 MB/s | 256 GiB at 3,000 IOPS and 125 MB/s, the performance every v2 disk includes | capacity only          |
| Premium SSD v2, 400 MB/s | 512 GiB at 3,000 IOPS and 400 MB/s                                         | capacity plus 275 MB/s |

The VM caps its network disks together at 12,800 IOPS and 424 MB/s.

- The archive is the C media driver with a standalone Java `Archive` (Java 21, `-Xms2g -Xmx2g -XX:+AlwaysPreTouch -XX:+UseParallelGC`) attached to it, with DEDICATED archive threading. Its recorder and replayer are pinned to the two threads of one core. File sync level 0 unless stated.
- The load is `arcload`: 1 KiB messages, one exclusive publication per stream, each on its own thread, the threads spread over four CPUs.
- **Each figure is a single run.** Both hosts ran every test, so cells read intel-a / intel-b where the two differ.
- **Provenance.** The run recorded commit `c6a21be` with `vm.sh` and `lab.sh` modified. Mid-run, both hosts got the fixes now in `e2f315a`, and the whole archive phase re-ran:
  - Sixteen publishers spinning on the housekeeping CPU had starved the driver's conductor, so `arcload` now pins each thread before it adds its publication.
  - A bare `wait` had also waited for the archive.
  - The second fio pass was added at the same time.

### What each disk can do (fio, one job at a time, MB/s unless stated)

| Disk, when                      | sequential write, 1 MiB, 32 deep | sequential read | 1 MiB write + `fdatasync` | `fdatasync` after a 64 KiB write, p50 | 4 KiB random writes (IOPS) |
| ------------------------------- | -------------------------------- | --------------- | ------------------------- | ------------------------------------- | -------------------------- |
| local NVMe, new                 | 562 / 560                        | 1124 / 1089     | 559 / 558                 | 0.11 / 0.08 ms                        | 19,590 / 21,490            |
| local NVMe, 1.5 h later         | 562 / 560                        | 1125 / 1094     | 559 / 558                 | 0.03 / 0.03 ms                        | 60,191 / 59,569            |
| Premium SSD v2 125, new         | 41 / 140                         | 77 / 123        | 45 / 91                   | 0.9 / 1.5 ms                          | 1,316 / 1,916              |
| Premium SSD v2 125, 1.5 h later | 130 / 129                        | 130 / 126       | 140 / 141                 | 0.7 / 0.8 ms                          | 1,344 / 3,063              |
| Premium SSD v2 400, new         | 58 / 229                         | 76 / 244        | 59 / 198                  | 0.8 / 1.4 ms                          | 1,340 / 1,523              |
| Premium SSD v2 400, 1.5 h later | 451 / 424                        | 375 / 403       | 445 / 419                 | 0.7 / 0.8 ms                          | 1,068 / 2,860              |

- **New Premium SSD v2 disks were mostly slow.**
  - 10–25 minutes after creation, three of the four managed only 15–60% of their provisioned throughput. intel-b's 125 MB/s disk was the exception.
  - On intel-a, Azure's own per-minute metrics showed at most 50% of the disks' bandwidth and 26% of their IOPS in use, so they were not being throttled at their limits.
  - About 1.5 hours later every disk delivered what was provisioned. A new 8 GiB file then wrote at 485–501 MB/s on the 400 MB/s disks (for the 17 s it took) and at 125–138 MB/s on the 125 MB/s ones.
  - By then the archive tests had written tens of GB to each disk, and xfs reuses freed blocks. The lab also formats each disk with plain `mkfs.xfs`, which discards the whole device (256 or 512 GiB) just before the first fio pass.
  - So this run can't tell apart a cost of writing each block the first time, that discard, a warm-up after creation, or a backend that just varies.
- Random 4 KiB writes stayed below the provisioned 3,000 IOPS on intel-a's disks in both passes. The archive writes sequentially, so this did not affect it.
- A forced write waits about 0.03 ms on local NVMe and about 0.8 ms on Premium SSD v2.

### Recording on the archive's host (IPC, 4 streams flat out for 60 s, MB/s)

| Disk               | level 0, first 10 s | level 0, last 30 s | level 1, median |
| ------------------ | ------------------- | ------------------ | --------------- |
| local NVMe         | 998 / 998           | 545 / 543          | 541 / 541       |
| Premium SSD v2 125 | 592 / 666           | 125 / 127          | 56 / 112        |
| Premium SSD v2 400 | 693 / 1010          | 346 / 394          | 57 / 220        |

- At file sync level 0 the page cache takes the first seconds at 0.6–1 GB/s. Once it reaches the kernel's limit on unwritten data, the disk sets the pace. Disk writes then waited about 240 ms each, a deep write-back queue, against 1–2 ms at level 1.
- **The disk set the rate, not the stream count.** On local NVMe, 1, 4 and 16 streams all recorded at 542–552 MB/s (median). On the 400 MB/s disks, 16 streams reached 415 / 412 MB/s over the last 30 s.
- Level 1 cost nothing sustained on local NVMe, but left no burst headroom. On Premium SSD v2 it cut recording to 56–220 MB/s, as each write waits for the network disk. These level 1 runs came early, while those disks were still slow.
- SHARED archive threading recorded at 545 / 543 MB/s (4 streams, NVMe), the same as DEDICATED, since the disk was the limit.

### Bursts (file sync level 0, two 30 s bursts of 400 MB/s, 60 s apart)

400 MB/s is 4 streams of 1 KiB at 97,656 msgs/s each. The kernel's write-back limits were either Linux's default (`vm.dirty_ratio=20`, `vm.dirty_background_ratio=10`, as shares of available memory on a 32 GiB VM) or large (`vm.dirty_bytes` at 16 GiB, `vm.dirty_background_bytes` at 512 MiB). Both hosts gave the same results.

| Disk               | write-back limits | seconds held at 400 MB/s, of 30 | most data waiting to be written | written out after the second burst |
| ------------------ | ----------------- | ------------------------------- | ------------------------------- | ---------------------------------- |
| local NVMe         | default           | 30                              | 2.8 GiB                         | 36 s                               |
| local NVMe         | large             | 30                              | 0.5 GiB                         | 7 s                                |
| Premium SSD v2 125 | default           | 15, then about 155 MB/s         | 4.8–5.0 GiB                     | 43 s                               |
| Premium SSD v2 125 | large             | 30                              | 8.4 GiB                         | 74–76 s                            |
| Premium SSD v2 400 | default           | 30                              | 4.6–5.3 GiB                     | 16–19 s                            |
| Premium SSD v2 400 | large             | 30                              | 1.8–4.4 GiB                     | 5–11 s                             |

- **With Linux's defaults, the 125 MB/s disk throttled each burst after 15 s.** In the burst's 30 s the publishers sent 8.1 of their 11.7 million messages, 2.0–2.2 million of them late. 99.6% of offer attempts met back-pressure.
- **With the large limits it took both bursts at full rate.** That pattern averages 133 MB/s, though, just above the disk's 125, so about 0.75 GB more would be left over each cycle until the limit is reached again.
- More unwritten data in memory is more data a host failure loses, which file sync level 0 already accepts.

### Replays (page cache dropped first, aggregate MB/s)

| Disk               | 4 replays   | 16 replays  | 4 replays while 4 streams record flat out |
| ------------------ | ----------- | ----------- | ----------------------------------------- |
| local NVMe         | 1039 / 1027 | 1077 / 1070 | 67 / 246                                  |
| Premium SSD v2 125 | 119 / 131   | 124 / 126   | 66 / 59                                   |
| Premium SSD v2 400 | 365 / 128   | 365 / 141   | 178 / 155                                 |

- **Each replay read back a whole recording of 1.1–1.3 GB.** The 16 replays on intel-b's 400 MB/s disk completed only 5 within 120 s.
- **Replays starved while recording ran flat out at the disk's limit.** On local NVMe they fell from about 1 GB/s to 67–246 MB/s; on intel-a none of the 4 finished within 50 s. The cause was not tested. Level 0's deep write-back queue could hold the reads up, and the recorder and replayer also share one core's two threads.
- The first replayed fragment arrived 230–290 ms after the request in nearly every run, whatever the disk. That was not investigated.

### Recording across hosts (publishers on intel-a, the archive on intel-b, UDP, MB/s)

| intel-b's disk     | 4 streams | 16 streams |
| ------------------ | --------- | ---------- |
| local NVMe         | 449       | 480        |
| Premium SSD v2 125 | 128       | 127        |
| Premium SSD v2 400 | 459       | 461        |

- The NVMe and 400 MB/s disks both stopped at 450–480 MB/s, below the 545 MB/s recorded locally, so the UDP path (2 MiB windows, MTU 1500) probably set that limit. That was not explored further.
- Replays from intel-b's NVMe to intel-a over UDP ran at 345, 502 and 485 MB/s for 1, 4 and 16 replays.

### Round trip across the hosts under recording load (West US 3 pair, isolated, µs)

A later run, on the West US 3 pair isolated, with the archive on intel-b's NVMe. Four streams recorded across the hosts at a fixed share of their measured maximum (486 MB/s) while the round trip ran. One run each.

The layout differs from the round-trip tables above, so compare only the rows here with each other:

- The ping and pong share the recording's drivers. Their `AERON_DIR` is on `/dev/shm`, not hugetlbfs.
- The four publishers spin (between their send slots too) on CPUs 3, 5 and 7, the SMT twins of ping (2), the sender (4) and the receiver (6).
- On the archive host, pong runs on CPU 5, the twin of its spinning sender.

| recording load | p50  | p99  | p99.9 | p99.99 | max  |
| -------------- | ---- | ---- | ----- | ------ | ---- |
| 25% (122 MB/s) | 46.8 | 64.1 | 90.4  | 191.2  | 2503 |
| 50% (243 MB/s) | 50.2 | 73.1 | 92.9  | 219.8  | 6226 |
| 75% (364 MB/s) | 49.7 | 76.9 | 182.8 | 688.1  | 2552 |

- From 25% to 50% load, p50 rose about 3 µs and p99 about 9 µs.
- At 75%, p99.9 doubled and p99.99 more than tripled.
- The pinned run before it measured 65.7 µs at p50 at 25% load, in the same layout. It lost its 50% and 75% runs to a harness bug, now fixed. Each reused the last pong's ports, and that pong's image lingered in the archive's driver.

## Kubernetes pods (single VM, earlier run, 2026-10-09)

These pods ran in an earlier run on one `Standard_D8s_v6`, on Linux 6.12 with k3s v1.36.5, measuring IPC only.

- **Pod shape.** Each pod had a driver container (a SHARED `noop` driver, or dedicated in `st-d3-a3`) and an app container (ping and pong), sharing a `HugePages-2Mi` `emptyDir`.
- **Policies.** "def" pods ran under kubelet's default CPU manager; "st" pods ran under the static policy with `reserved-cpus=0`.
- **Caveat.** In `st-d3-a3` the driver's threads were not pinned within its 3 CPUs. rusteron's `media_driver` didn't then apply `AERON_*_CPU_AFFINITY`; it does now.

| pod                             | CPU requests, driver / app | throttled periods, driver / app | ping, pong CPUs         | p50 (µs) | p99.99 (µs) | max (µs)       | M msgs/s |
| ------------------------------- | -------------------------- | ------------------------------- | ----------------------- | -------- | ----------- | -------------- | -------- |
| def-s1-a2                       | 1 / 2                      | 133 of 571 / 28 of 512          | 2, 4                    | 0.372    | 9.35        | 1797 (30–1812) | 38.6     |
| def-s2-a3                       | 2 / 3                      | 0 / 0                           | 2, 4                    | 0.382    | 9.06        | 41.0 (32–65)   | 34.8     |
| def-s1-a2.5                     | 1 / 2.5                    | 320 of 581 / 0                  | 2, 4                    | 0.375    | 9.51        | 48.3 (32–61)   | 36.0     |
| def-s1-a2-burst (no CPU limits) | 1 / 2                      | no quota                        | 2, 4                    | 0.383    | 9.70        | 33.8 (30–69)   | 39.6     |
| st-s1-a2                        | 1 / 2                      | no quota                        | 2, 3 (one core's twins) | 0.287    | 8.00        | 19.2 (19–21)   | 50.9     |
| st-s2-a3                        | 2 / 3                      | no quota                        | 1, 4                    | 0.360    | 9.57        | 55.1 (27–62)   | 39.2     |
| st-d3-a3                        | 3 / 3                      | no quota                        | 4, 5 (one core's twins) | 0.115    | 0.405       | 52.4 (16–84)   | 62.5     |
| st-s1-a2.5                      | 1 / 2.5 (shared pool)      | no quota                        | 2, 4                    | 0.354    | 8.95        | 57.4 (30–68)   | 36.3     |

- **The default policy's quota.** Under it, a CPU limit is a quota per 100 ms.
  - A driver container limited to 1 CPU was throttled in 23–55% of periods, because its spinning thread uses the whole quota and the driver's other threads push it over. IPC doesn't go through the driver, so that throttling barely shows here. `def-s1-a2.5`, the most throttled driver, had a max of 48 µs.
  - The one pod that stalled for up to 1.8 ms, `def-s1-a2`, was also the one whose app container (ping, pong and the client's other threads in 2 CPUs) was throttled, in 28 of 512 periods. Its max ranged from 30 to 1812 µs, overlapping the other pods', so this isn't a firm difference.
  - One CPU of headroom (`def-s2-a3`), or no CPU limit (Burstable), removed all throttling.
- The static policy gave each Guaranteed container whole CPUs to itself, with no quota at all (`cpu.max` read `max`). Kubernetes 1.36 drops the quota for exclusive CPUs.
- **Placement.** The kubelet chose where ping and pong landed.
  - Ping and pong on one core's two SMT twins, with the client's other threads on a third CPU (`st-d3-a3`): 0.115 µs at p50 and 0.405 µs at p99.99.
  - On twins too, but sharing them with those threads (`st-s1-a2`): 0.287 µs.
  - On separate cores: 0.35–0.38 µs.
  - The pods weren't isolated, and in all but `st-d3-a3` p99.99 stayed near 9 µs.

### Recommended settings for Kubernetes

Each setting is marked with where its evidence comes from:

- [pod]: the pod measurements above.
- [host]: the host measurements in this file, carried over to pods.
- [untested]: neither.

The Kubernetes behaviour relied on here is documented in:

- [Control CPU Management Policies on the Node](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/): only Guaranteed containers with integer CPU requests get exclusive CPUs, and `--reserved-cpus` sets the system's.
- [Manage HugePages](https://kubernetes.io/docs/tasks/manage-hugepages/scheduling-hugepages/): pages are allocated on the node first, requests equal limits, and an `emptyDir` with `medium: HugePages-2Mi` is capped at the pod's request.
- [Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/): the baseline profile forbids `hostNetwork`, `hostIPC` and `hostPath`.

The host settings' sources are listed under [Tuning on x86-64 Linux](https://github.com/gsrxyz/rusteron/blob/main/docs/tuning_x86_64_linux.md) in the README.

#### The node

- **[pod] Huge pages.** Set `vm.nr_hugepages` in `/etc/sysctl.d` on the node before kubelet starts, or kubelet does not report them.
  - Earlier 4-vCPU runs found that 2 MiB pages for `AERON_DIR` cut Intel's IPC p99 from about 0.7 to 0.14 µs, and p99.99 from 1.05 to 0.4–0.6 µs.
  - They made no difference on AMD, and 1 GiB pages gained nothing over 2 MiB.
- **[host] Socket buffer limits.** Set `net.core.rmem_max` and `wmem_max` in `/etc/sysctl.d`, at least as large as the windows you configure. They are node-wide, not per pod, and a reboot undoes `sysctl -w`. A driver whose window exceeds its receive buffer refuses to start.
- [host] Busy reads for UDP:
  - Set `net.core.busy_read=50`, with `napi_defer_hard_irqs=2` and `gro_flush_timeout=200000` on the NIC's VF.
  - Or put the NIC's queue interrupts on the receiver's CPU. Choose one; together they gained nothing.
  - Both helped only on isolated CPUs.
- **[host] Isolation of the CPUs kubelet hands out.** Use `isolcpus=nohz,domain,managed_irq,<CPUs> nohz_full=<CPUs> rcu_nocbs=<CPUs> irqaffinity=<reserved CPUs>`. On a host, isolation cut IPC p99.99 from 2.5 to 0.8–1.0 µs.
  - **[untested] in pods.** The kernel doesn't balance load across isolated CPUs, so non-Guaranteed pods in kubelet's shared pool would pile up on them. Keep such nodes for Guaranteed pods.
- [host] A tuned kernel on top of isolation:
  - The options were `nosmt idle=poll rcu_nocb_poll nowatchdog nmi_watchdog=0 nosoftlockup skew_tick=1 transparent_hugepage=never audit=0`.
  - They cut the UDP p50 from 38.6 to 34.9 µs, and left IPC unchanged.
  - With `idle=poll`, an idle CPU spins instead of entering a C-state.
  - [untested] On AWS instance types that let the guest control C-states, AWS's `intel_idle.max_cstate=1 processor.max_cstate=1` is the milder alternative. Use one or the other (README: [On AWS](./README.md#tuning-on-x86-64-linux)).
- **[host] Archive nodes.** `vm.dirty_bytes` and `vm.dirty_background_bytes` (see the archive section) are node-wide too.

**Applying the node settings.** None of the settings above can go in a pod spec. `net.core.busy_read` isn't a namespaced sysctl, so `securityContext.sysctls` can't set it, and [Kubernetes](https://kubernetes.io/docs/tasks/administer-cluster/sysctl-cluster/) allows no `net.*` sysctls with `hostNetwork`. [untested] by platform:

- AKS:
  - [Custom node configuration](https://learn.microsoft.com/en-us/azure/aks/custom-node-configuration-reference) sets the socket buffer limits and transparent huge pages per node pool (`az aks nodepool add --linux-os-config`).
  - It doesn't cover `busy_read` or `vm.dirty_*`. Microsoft says "use a daemon set" beyond its subset.
  - It has no kernel command line field, so isolation and `idle=poll` need a custom node image.
  - Its kubelet configuration has `cpuManagerPolicy` and `topologyManagerPolicy`, but no `reservedSystemCPUs` or CPU manager policy options.
- EKS:
  - On Amazon Linux 2023, kubelet settings go in nodeadm's [`NodeConfig`](https://awslabs.github.io/amazon-eks-ami/nodeadm/doc/api/) through the [launch template's user data](https://docs.aws.amazon.com/eks/latest/userguide/launch-templates.html), as do the sysctl files.
  - Kernel options need a custom AMI.
  - Bottlerocket takes kernel options in [`settings.boot.kernel-parameters`](https://bottlerocket.dev/en/os/1.42.x/api/settings/boot/), with `reboot-to-reconcile = true`.
- OpenShift:
  - A [`PerformanceProfile`](https://docs.redhat.com/en/documentation/openshift_container_platform/4.22/html/scalability_and_performance/cnf-provisioning-low-latency-workloads) sets the isolated and reserved CPUs, huge pages and extra kernel options.
  - CRI-O's pod annotations then turn off C-states, CFS quota, and load and IRQ balancing per pod; see the commented lines in the example below. containerd has no equivalent.

#### kubelet

- **[pod] The static CPU manager.** Use `cpu-manager-policy=static`, with `reserved-cpus` set to the housekeeping CPUs, the same ones as `irqaffinity`.
  - It gives every Guaranteed container whole CPUs to itself, with no CFS quota.
  - On k3s these go in `/etc/rancher/k3s/config.yaml` as `kubelet-arg`.
  - kubelet refuses to start with a state file from another policy. Stop it, delete `/var/lib/kubelet/cpu_manager_state`, then start it.
- **[pod] Room for the pod.** Other pods' CPU requests count against the allocatable CPUs.
- **[untested] Whole cores with SMT on.** The static policy's `full-pcpus-only=true` option keeps a container's CPUs on whole cores, but rejects odd CPU requests. With `nosmt` it doesn't matter.
- [untested] Nothing on the reserved CPUs:
  - Without the [`strict-cpu-reservation`](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/) option, pods in the shared pool also run on the reserved CPUs, which take the interrupts.
  - With it, only system daemons do. It is GA from Kubernetes 1.35.
  - Turning it on needs the same `cpu_manager_state` reset.

#### Pods

- **[pod] Every container Guaranteed.** Requests equal limits, in whole CPUs.
  - Give one CPU to each busy-spinning thread, plus one for the conductor and the other threads. Every thread in a container shares its CPUs.
  - On a host, the client's conductor on the spinning CPUs held IPC p99.99 at 3.1–3.6 µs, against 0.7–1.0 µs.
- **[pod] If the static policy is not available.** Don't set a CPU limit equal to the spinning threads. Drivers limited that way were throttled in 23–55% of 100 ms periods. One more CPU, or no CPU limit, avoided it.
- **[pod] Huge pages.** Requests must equal limits.
  - Share one `emptyDir: {medium: HugePages-2Mi}` between the driver and app containers.
  - Set `AERON_PERFORM_STORAGE_CHECKS=false`, because kubelet mounts hugetlbfs without `size=`.
- **[host] Term buffers and mappings.** Set `AERON_TERM_BUFFER_SPARSE_FILE=false` on the driver and `AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY=true` on the clients.

#### Sharing `AERON_DIR`, with one driver per pod or one per node

Aeron's IPC is memory-mapped files in `AERON_DIR`. The driver and every client must see the same directory, and anything that can mount it can read and publish on every stream and command the driver. Aeron's IPC has no authentication. So decide who may mount the directory, and give the driver and clients the same uid or gid.

- **[pod] A driver in each pod.** Run the driver as a container beside the app, sharing an `emptyDir: {medium: HugePages-2Mi}` as in the example below. This was the measured layout.
  - kubelet reserves and charges the huge pages to the pod.
  - The directory goes with the pod.
  - It needs no access to the host, so it passes the Pod Security Standards' baseline and restricted profiles.
  - The cost is a driver's CPUs in every pod, which is 3 for a dedicated UDP driver.
  - The pod's own `/dev/shm` can't replace the volume, because it is usually capped at 64 MiB, while one IPC publication with Aeron's default 64 MiB terms needs about 192 MiB.
- **[untested] One driver per node, shared by several app pods (a DaemonSet, say).** The pods must share a directory on the node.
  - `hostPath` for just that directory, on the node's hugetlbfs mount, keeps the huge pages. The driver pod must request `hugepages-2Mi` covering all its log buffers, because kubelet caps each pod's huge pages at its request, and a page fault beyond that kills the process with `SIGBUS`.
    - With non-sparse term buffers the driver touches every page as it creates a log, so its clients should map pages that already exist. Whether a client pod with no huge-page request can still fault one in was not checked.
    - Size the mount (`size=`) or set `AERON_PERFORM_STORAGE_CHECKS=false`.
  - `hostPath` on the node's `/dev/shm`, or `hostIPC: true`, which mounts the host's `/dev/shm` with containerd and CRI-O. Either works, but `hostIPC` also hands the pod the host's whole IPC namespace, which Aeron doesn't use. System V shared memory, semaphores, message queues and every other file in `/dev/shm` come with it.
    - **[host] Page size.** `/dev/shm` uses 4 KiB pages unless the node sets transparent huge pages for shared memory to `always` (`/sys/kernel/mm/transparent_hugepage/shmem_enabled`). In the earlier Intel runs, that matched hugetlbfs, with an IPC p99 of 0.14 µs against 0.7 µs with 4 KiB pages.
  - Either way:
    - The baseline profile forbids `hostPath` and `hostIPC` (and `hostNetwork`), so the namespace needs the privileged profile or a policy exception.
    - Files on the node outlive the driver's pod; set `AERON_DIR_DELETE_ON_START=true` so a restarted driver clears what a crashed one left.
    - Pages in the host's tmpfs are charged to whichever container first touches them, not to a pod's request.

#### The driver container

- **[host] IPC only.** A dedicated driver with its default idle strategies spins nothing, and one CPU is enough. On a host, a SHARED `noop` driver cost IPC: p50 from 0.31 to 0.36–0.42 µs, and half the throughput.
- **[host] UDP.** Use a dedicated driver with a `noop` sender and receiver each pinned, which is 3 CPUs with the conductor.
  - Short of CPUs, SHARED_NETWORK (2 CPUs) gave the same UDP latency and throughput.
  - SHARED gave 12% less throughput.
- **[untested] Pinning inside the container, in a pod.** Inside a container the CPU numbers aren't known in advance. Set `AERON_DRIVER_CPUSET_AFFINITY=true`, and Aeron reads the `AERON_*_CPU_AFFINITY` values as indexes into the container's cpuset, with 0 for its first CPU, and so on.
  - This needs cgroup v2. rusteron's `media_driver` applies it, as `aeronmd` does.
  - It is checked in Aeron's code but not run in a pod, because in the measured pods it wasn't applied yet.

#### The app container

- **[host] Pinning.** Pin each spinning thread to one of the container's CPUs, read from `/sys/fs/cgroup/cpuset.cpus.effective`. Leave one CPU for the client's conductor, or run the client with the conductor agent invoker.
- **[pod] Placement.** The kubelet picks the CPUs.
  - Ping and pong on one core's two SMT threads, with the client's other threads on a third CPU, gave 0.115 µs at p50.
  - On separate cores they gave 0.35–0.38 µs.

#### Networking

- **[untested] UDP from a pod.** The UDP results here used the host's network stack, and a CNI overlay adds its own path. For low-latency UDP between nodes, use `hostNetwork: true` and measure it. A VF can't be handed to a pod instead, because the [SR-IOV device plugin's README](https://github.com/k8snetworkplumbingwg/sriov-network-device-plugin/blob/master/README.md) says the SR-IOV CNI "doesn't support running in a virtualized environment".
- **[host] Windows.** 2 MiB socket buffers and receiver window lifted throughput from 1.9 to 13 M msgs/s. Across regions, size the window to the round trip (16 MiB for 53 ms gave 119 MB/s).
- **[host] Jumbo frames.** MTU 9000 with `AERON_MTU_LENGTH=8192` inside the VNet doubled throughput again.
- **[host] Avoid packet loss before anything else.** 0.1% loss cost a third of the throughput, and a lost request or reply waited about 100 ms.

#### An archive

- [untested] in a pod:
  - Run the archive in its own Guaranteed container with at least 2 whole CPUs. On a host, confining the whole JVM to one CPU cost 25% of recording throughput; a core of its own for the recorder got it back.
  - Put the archive directory on a volume sized for throughput, such as Premium SSD v2 on Azure, or local NVMe through a local persistent volume only if the archive is replicated elsewhere.
  - Use file sync level 0, with the node's `vm.dirty_*` limits raised for bursts.

The pods above used a SHARED `noop` driver. This example follows the recommendations instead, with a dedicated driver in the pod for UDP on the host's network. It has not itself been run. `hostNetwork` puts it outside the baseline profile too:

```yaml
# Needs, on the node (see "The node" and "kubelet" above; none of it can go in a pod spec):
#   kernel options: isolcpus=nohz,domain,managed_irq,<CPUs> nohz_full=<CPUs> rcu_nocbs=<CPUs>
#     irqaffinity=<reserved CPUs>, and idle=poll (or intel_idle.max_cstate=1 processor.max_cstate=1
#     on AWS instance types that allow it)
#   /etc/sysctl.d: vm.nr_hugepages, net.core.rmem_max and wmem_max, net.core.busy_read=50
#   kubelet: cpuManagerPolicy static, reservedSystemCPUs = the irqaffinity CPUs
apiVersion: v1
kind: Pod
metadata:
  name: aeron-app
  # OpenShift with a PerformanceProfile only (CRI-O; not measured here), with runtimeClassName below:
  # annotations:
  #   cpu-load-balancing.crio.io: "disable"
  #   cpu-quota.crio.io: "disable"
  #   irq-load-balancing.crio.io: "disable"
  #   cpu-c-states.crio.io: "disable"
  #   cpu-freq-governor.crio.io: "performance"
spec:
  # runtimeClassName: performance-<profile name>
  hostNetwork: true # UDP between nodes on the host's stack; not measured from a pod
  containers:
    - name: driver
      image: debian:trixie-slim
      env:
        - { name: AERON_DIR, value: /aeron/driver }
        - { name: AERON_THREADING_MODE, value: DEDICATED }
        - { name: AERON_SENDER_IDLE_STRATEGY, value: noop }
        - { name: AERON_RECEIVER_IDLE_STRATEGY, value: noop }
        # indexes into the container's cpuset, not CPU numbers
        - { name: AERON_DRIVER_CPUSET_AFFINITY, value: "true" }
        - { name: AERON_CONDUCTOR_CPU_AFFINITY, value: "0" }
        - { name: AERON_SENDER_CPU_AFFINITY, value: "1" }
        - { name: AERON_RECEIVER_CPU_AFFINITY, value: "2" }
        - { name: AERON_SOCKET_SO_SNDBUF, value: 2m }
        - { name: AERON_SOCKET_SO_RCVBUF, value: 2m }
        - { name: AERON_RCV_INITIAL_WINDOW_LENGTH, value: 2m }
        - { name: AERON_FILE_PAGE_SIZE, value: "2097152" }
        - { name: AERON_PERFORM_STORAGE_CHECKS, value: "false" } # kubelet mounts hugetlbfs without size=
        - { name: AERON_TERM_BUFFER_SPARSE_FILE, value: "false" }
        # as Aeron's low-latency C driver script (the default is 4); not measured here
        # - {name: AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND, value: "2"}
      resources:
        requests: { cpu: "3", memory: 1Gi, hugepages-2Mi: 1Gi }
        limits: { cpu: "3", memory: 1Gi, hugepages-2Mi: 1Gi }
      volumeMounts:
        - { name: aeron, mountPath: /aeron }
    - name: app
      image: debian:trixie-slim
      env:
        - { name: AERON_DIR, value: /aeron/driver }
        - { name: AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY, value: "true" }
      resources:
        # two spinning threads (ping and pong), plus one CPU for the client's other threads
        requests: { cpu: "3", memory: 1Gi, hugepages-2Mi: 1Gi }
        limits: { cpu: "3", memory: 1Gi, hugepages-2Mi: 1Gi }
      volumeMounts:
        - { name: aeron, mountPath: /aeron }
  volumes:
    - name: aeron
      emptyDir: { medium: HugePages-2Mi }
```

## Hot paths: in-place header reads and cross-language LTO (2026-10-11)

Per-operation cost of the client's hot paths, from `rusteron-client/benches/hot_path.rs`, using an
embedded driver over IPC, a 32-byte payload, and header reads timed as the slope from 0 to 4
reads per fragment with every result kept alive. Each figure is the median of 3 runs of each
build, interleaved. Spreads were within about 2%. Two machines:

- Mac: Apple M4, macOS, Rust 1.95.0.
- Linux: a Debian arm64 Docker VM on the same M4, Rust 1.98.1, rustc's generic CPU. The
  absolute numbers are indicative only, but the builds ran side by side, so the differences hold.

The LTO builds link a separate crate that depends on rusteron, as an application would, with
`lto = "fat"`, `codegen-units = 1` and Aeron C compiled by clang 22 with `-flto`.

One header field read (`header.session_id()` and the others), ns per read:

| Build                                             | Mac  | Linux |
| ------------------------------------------------- | ---- | ----- |
| Through `aeron_header_values` (before)            | 3.45 | 6.96  |
| Through `aeron_header_values`, cross-language LTO | 0.51 | 1.52  |
| In place (now)                                    | 0.15 | 0.15  |
| In place, cross-language LTO                      | 0.14 | 0.13  |

Reading the frame in place is what removes the cost, on every build. Cross-language LTO inlines
`aeron_header_values` and drops its zeroing and copy, but its null checks, error path and the
wrapper's branches remain.

Publish and receive, ns per operation (Linux; header reads in place in all three):

| Build                             | `offer` | `try_claim` + `commit` | `poll` per fragment |
| --------------------------------- | ------- | ---------------------- | ------------------- |
| Aeron C built by gcc (default)    | 7.18    | 5.74                   | 1.68                |
| Aeron C built by clang 22, no LTO | 6.55    | 6.41                   | 1.65                |
| clang 22, cross-language LTO      | 5.81    | 5.56                   | 0.99                |

On Linux the whole `poll` path inlines. `aeron_subscription_poll`, `aeron_image_poll`, the
fragment trampoline and the closure become one loop, with no indirect call per fragment.
`commit` inlines anyway (LLVM cost 10); `try_claim` inlines only because this bench called it from
one place (see [Inlining every publish call](#inlining-every-publish-call)). `offer` stays a call, being too
large for LLVM's inliner, and about half of its gain comes from building Aeron with clang rather
than gcc. On the Mac only
the claim path inlined (`offer` 6.16 to 5.85 ns, `try_claim` + `commit` 5.75 to 5.29 ns, `poll`
unchanged), and macOS is not a supported setup (see below).

Other build settings made no difference beyond noise on the Mac, namely Rust `lto = "thin"` against
`"fat"`, and `panic = "abort"`. C `-flto=thin` beside Rust's fat LTO inlined nothing.

The generated `get_constants` and `get_values` getters now let Aeron fill uninitialised memory
instead of zeroing the struct first, as Aeron writes every field on success. On the Mac,
`publication.get_constants()` went from 10.62 to 8.20 ns and `subscription.get_constants()` from
6.09 to 5.26 ns. Neither is called per message.

A release build with cross-language LTO took 87 s against 39 s on the Mac, and 55 s against 42 s
on Linux.

### Inlining every publish call

LLVM inlines a call when the callee's cost, by its own model, is below a threshold, which is 250 at `-O3`,
525 for a call it estimates as hot ("locally hot": run at least 60 times as often as its function's
entry, typically inside a loop), with a 15000 bonus when the call is the only one to that function
in the whole program. The bench above called each publish function from one place,
so that bonus inlined `try_claim`. A real application calls them from several, and so does the
bench now (it publishes once through each before timing). LLVM's remarks
(`-Clink-arg=-Wl,--opt-remarks-filename,remarks.yaml -Clink-arg=-Wl,--opt-remarks-passes,inline`)
then give, on the Linux VM, for the timed loops:

| Function                                       | LLVM cost     | Inlined at the default (525) | With `-locally-hot-callsite-threshold=3000` |
| ---------------------------------------------- | ------------- | ---------------------------- | ------------------------------------------- |
| `aeron_buffer_claim_commit`                    | 10            | yes                          | yes                                         |
| `aeron_subscription_poll` / `aeron_image_poll` | 335 / 145-345 | yes                          | yes                                         |
| `aeron_exclusive_publication_try_claim`        | 585           | no, "too costly"             | yes                                         |
| `aeron_publication_try_claim`                  | 675           | no                           | yes                                         |
| `aeron_publication_offer`                      | 735           | no                           | yes                                         |
| `aeron_exclusive_publication_offer`            | 1010          | no                           | yes                                         |
| `aeron_exclusive_publication_offerv`           | 1085          | no                           | yes                                         |

Nothing else blocks them. There are no `noinline` or `cold` attributes, no feature mismatch once C targets
rustc's CPU, and no interposition, since LTO makes every symbol of the executable internal. The
cost comes from the error paths (two variadic `aeron_err_set` calls with formatted arguments),
term rotation and back-pressure status, all inlined into each function.

Raising the general threshold does not work. `-inline-threshold=325` left the loop threshold at
525, and `-inline-threshold=1000` pushed `aeron_subscription_poll` out of line. The locally hot
threshold changes only call sites LLVM estimates as hot, so it never pushes `poll` out. On the
Linux VM `poll` stays inlined, and on the Mac, where LTO alone left it a call (1.62 ns), the flag
inlines it too (0.97 ns). At a call site that is not hot, `aeron_subscription_poll` (335) is over
the normal threshold of 250 and stays a call whatever the flag. An x86 run of a downstream
application, without the flag, found `try_claim` and both polls still calls at several of its call
sites. Medians of 3 interleaved runs on the Mac (native, ns per operation):

| Operation                         | Plain | Cross-language LTO | LTO + locally hot threshold |
| --------------------------------- | ----- | ------------------ | --------------------------- |
| exclusive `offer` (32 bytes)      | 5.89  | 5.74               | 4.98                        |
| exclusive `offerv` (2 × 16 bytes) | 7.02  | 6.54               | 5.93                        |
| exclusive `try_claim` + `commit`  | 5.19  | 4.97               | 4.71                        |
| `offer`                           | 6.28  | 5.94               | 5.61                        |
| `try_claim` + `commit`            | 5.89  | 5.62               | 5.41                        |
| `poll` per fragment               | 1.61  | 1.62               | 0.97                        |

The flagged build here used a threshold of 1200. Spreads were within 2%, apart from one slow run
of it (up to 6.1 ns). A second interleaved pair, 1200 against 3000, gave the same code and the
same timings (exclusive `offer` 4.82, exclusive `try_claim` + `commit` 4.58-4.60 ns); 3000 is LLVM's own threshold for profiled hot
call sites and leaves room for Aeron's functions to grow. The binary's text grew by 16 KB. On the
Linux VM, `offer` went from 5.95 to 5.67 ns and `try_claim` + `commit` from 5.85 to 5.46 ns, with
`poll` unchanged at 1.0 ns. The exclusive publication took 18 to 23 ns per operation there in every
build, gcc included, so the VM numbers cannot show its gain.

### Turning on cross-language LTO (Linux)

Only the final binary can turn it on, because rusteron's own build settings do not reach crates that
depend on it.

1. Install clang, lld and llvm-ar with the same LLVM major version as rustc. Rust 1.98.1 uses
   LLVM 22:

   ```bash
   rustc -vV | grep LLVM                 # LLVM version: 22.1.8
   wget https://apt.llvm.org/llvm.sh
   sudo bash llvm.sh 22
   sudo apt install lld-22
   ```

   Upgrade them together with Rust. rusteron's build stops with the fix when they differ.

2. In the application's `Cargo.toml`, link Aeron statically and use fat LTO:

   ```toml
   [dependencies]
   rusteron-client = { version = "0.2", features = ["static"] }
   rusteron-media-driver = { version = "0.2", features = ["static"] }

   [profile.release]
   lto = "fat"
   codegen-units = 1
   ```

3. In the application's `.cargo/config.toml`:

   ```toml
   [target.x86_64-unknown-linux-gnu]
   linker = "clang-22"
   rustflags = ["-Clinker-plugin-lto", "-Clink-arg=-fuse-ld=lld-22", "-Ctarget-cpu=native",
                "-Clink-arg=-Wl,-mllvm,-locally-hot-callsite-threshold=3000"]

   [target.aarch64-unknown-linux-gnu]
   linker = "clang-22"
   rustflags = ["-Clinker-plugin-lto", "-Clink-arg=-fuse-ld=lld-22", "-Ctarget-cpu=native",
                "-Clink-arg=-Wl,-mllvm,-locally-hot-callsite-threshold=3000"]

   [env]
   CC = "clang-22"
   ```

   With `-Clinker-plugin-lto` set, rusteron's build script compiles Aeron C to LLVM bitcode with
   `$CC` (or `RUSTERON_CLANG`), for the CPU named by `-Ctarget-cpu`. C and Rust must target the
   same CPU, or LLVM refuses to inline across them. `CC` also covers other C dependencies, which
   the `cc` crate compiles to bitcode under the same flag. `-locally-hot-callsite-threshold`
   lets Aeron's `offer`, `offerv` and `try_claim` inline at hot call sites (see above). In a
   Dockerfile the same settings are environment variables:

   ```dockerfile
   ENV CC=clang-22
   ENV RUSTFLAGS="-C target-cpu=native -C linker-plugin-lto -C linker=clang-22 -C link-arg=-fuse-ld=lld-22 -C link-arg=-Wl,-mllvm,-locally-hot-callsite-threshold=3000"
   ```

4. Check it worked. `cargo build --release` prints
   `cross-language LTO: Aeron C compiled to bitcode by clang-22 (LLVM 22) with -flto` for each
   rusteron crate (Cargo shows build-script warnings only for path dependencies). If the
   application calls the publish functions only from hot loops, they are gone from an unstripped
   binary (this prints nothing):

   ```bash
   nm target/release/my-app | grep -E ' aeron_(exclusive_)?publication_(offerv?|try_claim)$| aeron_buffer_claim_commit$'
   ```

   A call made anywhere else (start-up, a one-off message, a retry path) keeps a separate copy of
   that function even when the hot loop inlined it; LLVM's remarks (above) show each call site as
   inlined or "too costly".

C uses full LTO (`-flto`) to match Rust's `lto = "fat"`. With Rust `lto = "thin"`, set
`RUSTERON_C_LTO=thin`. The `precompile` libraries are native code and cannot take part (the build
warns). macOS is not supported because rust-lld cannot read the macOS SDK, Apple's linker rejects
rustc's LTO flags, and clang marks Apple-silicon code with CPU features rustc does not, which
blocks inlining.

### Running the hot-path bench

```bash
cargo bench -p rusteron-client --features static --bench hot_path   # plain build
just bench-lto                                                     # cross-language LTO (Linux)
```

## What else was checked

No effect inside an Azure Hyper-V guest, or not used in Aeron's path:

- Interrupt coalescing (`ethtool -C`): MANA has no coalescing control. On AWS, [AWS recommends](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-improve-network-latency-linux.html) turning the ENA NIC's off for latency (`ethtool -C <interface> adaptive-rx off rx-usecs 0 tx-usecs 0`); not measured here.
- GRO and LRO: plain UDP sockets are never merged.
- `net.core.busy_poll`: Aeron's receiver calls `recvmmsg` directly, never `poll`, so only `busy_read` matters.
- Limiting C-states (`intel_idle.max_cstate`, `processor.max_cstate`): the Hyper-V guest has no cpuidle driver (every run recorded `none`), so there are no C-states to limit; `idle=poll` keeps idle CPUs spinning instead. `haltpoll` needs KVM. On AWS instance types that let the guest control C-states, [AWS recommends](https://docs.aws.amazon.com/linux/al2/ug/processor_state_control.html) `intel_idle.max_cstate=1 processor.max_cstate=1`; not measured here.
- `tsc=reliable`: Hyper-V already marks the TSC reliable, and `tsc` was the clock source.
- `mlockall`: there is no swap, and Aeron's memory is pre-touched.

Not done, with the reason:

- `SCHED_FIFO` for the busy-spinning threads: it gains nothing on isolated cores and risks starving kernel threads.
- NIC IRQs on a quiet core of their own: once SMT is off, a D8s_v6 has no spare core.
- `SO_BUSY_POLL` and `SO_PREFER_BUSY_POLL` per socket: Aeron's C code never sets them.
- Kernel bypass (DPDK): open-source Aeron has no DPDK transport.

## How to run

```bash
LAB_PAIR=1 LAB_LOCKSTEP=1 \
LAB_NODES="intel-a:northcentralus:Standard_D8s_v6 intel-b:northcentralus:Standard_D8s_v6" \
LAB_ARMS="impr impr-ps" LAB_EXTRAS=samples \
LAB_PHASES="bootstrap kernel build bench8 xhost8-pinned archive8 isolate8 bench8-isolated xhost8-isolated tune8 bench8-tuned xhost8-tuned tune8-nomit bench8-tuned-nomit xhost8-tuned-nomit" \
scripts/x86-lab/lab.sh
```

The settings recommended elsewhere but not yet measured, against the tuned state:

- **The kernel options.** Add `tune8x bench8-tunedx xhost8-tunedx`. That is the tuned kernel plus tuned's `network-latency` RCU options, timers kept in place, and the writeback workqueue and RCU's offload kthreads on the housekeeping CPU.
- **The send batch.** Every `xhost8-*` phase also runs `ded-threads-msgs2`, which sends at most 2 messages per sender cycle.
- **Drops.** Every `xhost8-*` phase records the ping host's UDP drop counters before and after in `drops8.txt`.

The archive under load, with three disks on each VM:

```bash
LAB_PAIR=1 LAB_LOCKSTEP=1 LAB_ZONE=1 \
LAB_DATA_DISK="PremiumV2_LRS:256:3000:125 PremiumV2_LRS:512:3000:400" \
LAB_NODES="intel-a:westus3:Standard_D8ds_v6 intel-b:westus3:Standard_D8ds_v6" \
LAB_ARMS="impr impr-ps" LAB_EXTRAS=samples \
LAB_PHASES="bootstrap kernel build disks8 diskbench8 archload8 archburst8 diskbench8-overwrite xarchload8" \
scripts/x86-lab/lab.sh
```

Loss and distance, with a third VM in another region, the round trip under recording load, and the isolated pair:

```bash
LAB_PAIR=1 LAB_LOCKSTEP=1 \
LAB_NODES="intel-a:westus3:Standard_D8ds_v6 intel-b:westus3:Standard_D8ds_v6 intel-c:northcentralus:Standard_D8s_v6" \
LAB_ARMS="impr impr-ps" LAB_EXTRAS=samples \
LAB_PHASES="bootstrap kernel build isolate8 disks8 xhost8-isolated xnet8-zone xnet8-region xarcrtt8" \
scripts/x86-lab/lab.sh
```

`lab.sh` deletes the VMs on any exit. Its header lists the options, and the phases are in `scripts/x86-lab/harness/vm.sh`. Results go to `target/x86lab/results/<time>/<vm>/after-<phase>/results/`: `bench.csv`, `xhost8-runs.csv`, and each state's kernel settings and interrupt counts.

These recipes run the samples on one machine without pinning:

```bash
just benchmark-ipc-throughput-java
just run-aeron-media-driver-rust          # terminal 1
just benchmark-ipc-throughput-rust        # terminal 2
just benchmark-embedded-ping-pong-java
just benchmark-embedded-ping-pong-rust    # with the driver from terminal 1
```
