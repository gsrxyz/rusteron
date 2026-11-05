# Tuning on x86-64 Linux

Measured on 2026-10-09 between two Azure `Standard_D8s_v6` VMs (Intel Xeon Platinum 8573C) in one proximity placement group, on Debian 13 with Linux 7.2.6. The tests covered UDP round trips and throughput between the two hosts, and IPC on each host, with 32-byte messages. Each figure is a median of 5 interleaved runs. Other NICs and clouds will differ. Tables, method and open issues are in [BENCHMARKS.md](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md#intel-d8s_v6-pair-on-azure-2026-10-09).

Every run used the following settings. Earlier measurements on smaller VMs found a benefit from each:
- `AERON_TERM_BUFFER_SPARSE_FILE=false` (driver) and `AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY=true` (clients). Without them, accessing a new publication's log causes page faults.
- `AERON_DIR` on 2 MiB `hugetlbfs` with `AERON_FILE_PAGE_SIZE=2097152`.
- `noop` idle for the driver's sender and receiver.
- Every busy-spinning thread pinned to its own core, and everything else (interrupts, the driver's conductor, the clients' other threads) on a housekeeping CPU.

| Setting | Measured effect | Cost |
|---|---|---|
| Socket buffers and receiver window: `AERON_SOCKET_SO_SNDBUF`, `AERON_SOCKET_SO_RCVBUF` and `AERON_RCV_INITIAL_WINDOW_LENGTH` at 2 MiB, with `net.core.rmem_max`/`wmem_max` raised to allow them | UDP throughput between the hosts rose from 1.9 to 13 M msgs/s. No effect on round-trip latency with one message in flight. | Memory per socket. |
| MTU 9000 inside the VNet and `AERON_MTU_LENGTH=8192` | UDP throughput rose from 13 to 28.5 M msgs/s. | Azure allows it only inside a VNet and directly peered VNets; keep the default route at 1500 so traffic leaving the VNet still fits. |
| CPU isolation: `isolcpus=nohz,domain,managed_irq,<hot CPUs> nohz_full=<same> rcu_nocbs=<same> irqaffinity=<housekeeping>` | IPC p99.99 fell from 2.5 to 0.8 to 1.0 µs. The maximum latency in most runs fell from 20 to 67 to 4 to 7 µs. UDP measurements did not change with isolation alone. | Isolated CPUs take no unpinned work, which then all crowds onto the housekeeping CPUs, so every busy thread must be pinned. |
| Socket busy reads, `net.core.busy_read=50`, with `napi_defer_hard_irqs=2` and `gro_flush_timeout=200000` on the NIC's VF, on isolated CPUs | UDP round-trip p50 fell from 38.6 to 31 µs. The p99.99 fell from 157 to 44 to 50 µs. `net.core.busy_poll` does nothing for Aeron's receiver, which calls `recvmmsg` directly. | The receiver's core polls the NIC queue itself. |
| The NIC's queue IRQs on the receiver's core, on isolated CPUs | UDP p99.99 fell from 157 to 49 to 57 µs. With the tuned kernel below, the median maximum was 74 µs, with a range of 62 to 84 µs. | Re-pin them whenever Azure re-adds the VF (host servicing), which spreads them over all CPUs again. |
| The client's other threads (its conductor) off the busy-spinning CPUs | IPC p99.99 fell from 3.1 to 3.6 to 0.7 to 1.0 µs on isolated CPUs. | Reserve a CPU for these threads or use the conductor agent invoker. |
| A tuned kernel on top of isolation: `nosmt idle=poll rcu_nocb_poll nowatchdog nmi_watchdog=0 nosoftlockup skew_tick=1 transparent_hugepage=never audit=0`, all IRQs and workqueues on the housekeeping CPU, background services stopped | UDP p50 fell from 38.6 to 34.9 µs. IPC did not change. | Disabling SMT halves the logical CPU count. `idle=poll` makes idle CPUs spin. |
| `mitigations=off` | UDP p50 fell from 34.9 to 33.9 µs and p99.99 from 156 to 121 µs. IPC did not change. | Turns off the kernel's protection against CPU side-channel attacks. |
| Driver threading | Dedicated (pinned `noop` sender and receiver), SHARED_NETWORK and SHARED were within about 2 µs of each other at UDP p50. A SHARED driver raised IPC p50 from 0.31 to 0.36 to 0.42 µs and halved throughput. | Dedicated spins two cores. |
| Across regions, socket buffers and initial window sized to the round trip: 16 MiB for 53 ms between West US 3 and North Central US, over 64 MiB terms, with `net.core.rmem_max`/`wmem_max` raised to match and kept in `/etc/sysctl.d` | Throughput rose from 18 to 119 MB/s on a link without packet loss. At 0.1% loss both carried 10 to 13 MB/s. | Memory per socket. If `rmem_max` resets (a reboot undoes `sysctl -w`), the driver refuses to start, as the window exceeds the receive buffer. |
| In Kubernetes: kubelet's static CPU manager and Guaranteed pods requesting whole CPUs | Each container gets its CPUs to itself, with no CFS quota. Under the default policy, containers limited to exactly their spinning threads' CPUs were throttled (23 to 55% of 100 ms periods for a 1-CPU driver); one CPU of headroom or no CPU limit avoided it. | See [BENCHMARKS.md](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md#recommended-settings-for-kubernetes) for the kubelet and pod settings. |

Sources for these settings:
- Aeron's settings:
  - Aeron's [Best Practices Guide](https://github.com/aeron-io/aeron/wiki/Best-Practices-Guide) gives:
    - socket receive buffers of 2 to 4 MB and a receiver window of 2 MB or more for throughput;
    - DEDICATED threading when there are cores to spare, else SHARED_NETWORK or SHARED;
    - the low-latency driver's busy-spin conductor with `noop` sender and receiver.
  - Its [Configuration Options](https://github.com/aeron-io/aeron/wiki/Configuration-Options) explain sparse term files ("can cause latency pauses"), pre-touch ("to avoid soft page faults"), the file page size for huge pages, and the C driver's CPU affinity and cpuset options.
- CPU isolation, thread pinning and IRQ affinity: Erik Rigtorp's [Low Latency Tuning Guide](https://rigtorp.se/low-latency-guide/) covers these, along with SMT off (`nosmt`), transparent huge pages off with explicit huge pages, and `mitigations=off`.
- The tuned kernel command line: SUSE's [kernel arguments for low latency and high performance](https://documentation.suse.com/suse-edge/3.1/html/edge/atip-features.html) include `isolcpus=domain,nohz,managed_irq`, `nohz_full`, `rcu_nocbs`, `rcu_nocb_poll`, `irqaffinity`, `idle=poll`, `skew_tick=1`, `nosoftlockup`, `nowatchdog` and `nmi_watchdog=0`. The [Linux kernel parameters](https://docs.kernel.org/admin-guide/kernel-parameters.html) document every one.
- Busy reads: `net.core.busy_read` and the socket buffer limits are in the [kernel's net sysctls](https://docs.kernel.org/admin-guide/sysctl/net.html). The kernel's [NAPI documentation](https://docs.kernel.org/networking/napi.html) recommends setting `napi_defer_hard_irqs` and `gro_flush_timeout` together.
- Jumbo frames on Azure: Microsoft's [Configure MTU for virtual machines in Azure](https://learn.microsoft.com/en-us/azure/virtual-network/how-to-virtual-machine-mtu) gives MTU 9000 on the MANA NIC, but only within a VNet and directly peered VNets in the same region. Gateways, global peering and the internet stay at 1500.
- Kubernetes: [Control CPU Management Policies on the Node](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/) and [Manage HugePages](https://kubernetes.io/docs/tasks/manage-hugepages/scheduling-hugepages/).
- On AWS (not measured here):
  - AWS's [Improve network latency for Linux based EC2 instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-improve-network-latency-linux.html) recommends:
    - a cluster placement group;
    - `net.core.busy_read=50`, as here. Its `net.core.busy_poll=50` does nothing for Aeron, whose receiver calls `recvmmsg` directly;
    - limiting deeper C-states;
    - turning off the ENA NIC's interrupt moderation: `sudo ethtool -C <interface> adaptive-rx off rx-usecs 0 tx-usecs 0`.
  - [Processor state control](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/processor_state_control.html) lists the instance types whose guests may limit C-states, and the fewer that may also set P-states. Support varies by type.
  - The [Amazon Linux 2 guide](https://docs.aws.amazon.com/linux/al2/ug/processor_state_control.html) keeps idle cores in C1 with the kernel options `intel_idle.max_cstate=1 processor.max_cstate=1`. For the steadiest frequency it turns off Turbo Boost with `echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo`.
  - `idle=poll`, as in the tuned kernel above, goes further: idle CPUs never enter a C-state at all. Use one or the other.
  - On these Azure VMs the C-state options do nothing. The guest has no cpuidle driver, so there are no C-states to limit. Frequency control was not checked.

The following settings have external recommendations but no measurements here. The lab's `tune8x` state and `ded-threads-msgs2` variant test the kernel and send-batch settings:
- Aeron's low-latency C driver script:
  - [`low-latency-c-media-driver`](https://github.com/aeron-io/aeron/blob/master/aeron-samples/scripts/low-latency-c-media-driver) also sets `AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND=2` (the default is 4).
  - It also sets `AERON_CONDUCTOR_IDLE_STRATEGY=spin`, which spins a whole core.
- Messages within the MTU: Aeron's [Java Programming Guide](https://github.com/aeron-io/aeron/wiki/Java-Programming-Guide) says "For the lowest latency, keep messages within the MTU size."
- Monitor stalls and packet loss:
  - Lower `AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD`, `AERON_DRIVER_SENDER_CYCLE_THRESHOLD` and `AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD`. The default is 100 ms; Aeron's benchmark driver uses 1 ms.
  - Then watch AeronStat's "work cycle exceeded threshold count" counters, and the NAK counters and `loss-report.dat` ([Monitoring and Debugging](https://github.com/aeron-io/aeron/wiki/Monitoring-and-Debugging)).
  - On the host, watch `nstat -az UdpRcvbufErrors UdpInErrors`, the drops in `/proc/net/softnet_stat` and `ethtool -S` ([Red Hat's network tuning](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/monitoring_and_managing_system_status_and_performance/tuning-the-network-performance_monitoring-and-managing-system-status-and-performance)).
- RCU, timers and kernel threads:
  - tuned's [`network-latency` profile](https://github.com/redhat-performance/tuned/blob/master/profiles/network-latency/tuned.conf) adds `rcupdate.rcu_normal_after_boot=1`, `rcutree.nohz_full_patience_delay=1000` and `kernel.timer_migration=0`.
  - Its [`cpu-partitioning` profile](https://github.com/redhat-performance/tuned/blob/master/profiles/cpu-partitioning/tuned.conf) also moves the writeback workqueue (`/sys/bus/workqueue/devices/writeback/cpumask`) to the housekeeping CPUs.
  - The kernel's [NO_HZ documentation](https://docs.kernel.org/timers/no_hz.html) leaves pinning RCU's `rcuo` kthreads to userspace.
  - The kernel's [CPU isolation guide](https://docs.kernel.org/admin-guide/cpu-isolation.html) prefers cpuset isolated partitions, which can be changed at run time, to `isolcpus`.
- Connection tracking: if any firewall rule loads conntrack, a `notrack` rule at raw priority keeps Aeron's packets out of it ([nftables wiki](https://wiki.nftables.org/wiki-nftables/index.php/Setting_packet_connection_tracking_metainformation)).
- AWS's ENA driver:
  - Its [best practices guide](https://github.com/amzn/amzn-drivers/blob/master/kernel/linux/ena/ENA_Linux_Best_Practices.rst) suggests `busy_read` and `busy_poll` of 70.
  - It suggests keeping irqbalance but banning the hot CPUs (`IRQBALANCE_BANNED_CPUS`), and running the application on the vCPU that takes its queue's IRQ.
  - [ENA Express](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-express.html) can add "tens of microseconds" to median latency, so leave it off for latency.
- Azure placement: after host maintenance, check `az ppg show --include-colocation-status` ([co-location](https://learn.microsoft.com/en-us/azure/virtual-machines/co-location)). A VM moved out of alignment needs a full deallocate and start.

Packet loss reduced throughput and delayed request/response messages:
- 0.1% UDP loss cut same-zone throughput by a third, and 1% by 91%. Every lost packet was still retransmitted.
- In request/response traffic, a lost message waits for the sender's next heartbeat, 100 ms in the C driver.
- Find and fix the loss before tuning anything else ([details](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md#packet-loss-and-distance-west-us-3-and-north-central-us-2026-10-10)).

```bash
# C media driver (a Java driver reads -D system properties instead)
export AERON_TERM_BUFFER_SPARSE_FILE=false
export AERON_SENDER_IDLE_STRATEGY=noop AERON_RECEIVER_IDLE_STRATEGY=noop
export AERON_CONDUCTOR_CPU_AFFINITY=0 AERON_SENDER_CPU_AFFINITY=4 AERON_RECEIVER_CPU_AFFINITY=6
export AERON_SOCKET_SO_SNDBUF=2m AERON_SOCKET_SO_RCVBUF=2m AERON_RCV_INITIAL_WINDOW_LENGTH=2m
export AERON_FILE_PAGE_SIZE=2097152 AERON_DIR=/mnt/huge/aeron
# every client
export AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY=true
# the host
sudo sysctl -w net.core.rmem_max=16777216 net.core.wmem_max=16777216 net.core.busy_read=50
# the NIC's VF (its netdev has eth0 as master), as measured with busy_read
vf=
for i in /sys/class/net/*; do [ "$(basename "$(readlink "$i/master")")" = eth0 ] && vf=$(basename "$i"); done
if [ -n "$vf" ]; then
  echo 2 | sudo tee /sys/class/net/$vf/napi_defer_hard_irqs
  echo 200000 | sudo tee /sys/class/net/$vf/gro_flush_timeout
else
  echo "no VF has eth0 as master: is accelerated networking on?" >&2
fi
sudo sysctl -w vm.nr_hugepages=1536
sudo mkdir -p /mnt/huge && sudo mount -t hugetlbfs -o pagesize=2M,size=2G,uid="$(id -u)" none /mnt/huge
```

## Archive disks

Measured on 2026-10-10 with a Java Archive on two Azure `Standard_D8ds_v6`, recording 1 KiB messages from up to 16 streams ([details](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md#java-archive-on-disk-under-load-two-d8ds_v6-2026-10-10)).

- Size the disk for sustained throughput.
  - At file sync level 0 (the default) the archive writes into the page cache. The page cache absorbs bursts, so the disk only needs the average write rate plus replay reads.
  - Cloud disks share one budget between reads and writes.
  - Recordings are sequential, so the included 3,000 IOPS did not limit these recordings.
- A durable network disk can support recording at its provisioned throughput. On Azure that is Premium SSD v2, with 125 MB/s included and additional throughput available separately. AWS gp3 is priced the same way but wasn't measured here.
  - The VM caps its disks as well, at 424 MB/s in total on a D8ds_v6.
  - New Premium SSD v2 disks, freshly formatted with a full-device discard (plain `mkfs.xfs`), ran well below their provisioned rate for their first 25 minutes or more. Test a fresh disk before relying on it.
- Increase the page-cache write-back limits to absorb larger bursts. Run `sudo sysctl -w vm.dirty_bytes=17179869184 vm.dirty_background_bytes=536870912`. That allows 16 GiB of unwritten data, half of the 32 GiB VM, so scale it to your RAM.
  - With these, a 125 MB/s disk took 30 s bursts of 400 MB/s at full rate; with Linux's defaults it throttled after 15 s.
  - Data still in memory is lost if the host fails, which level 0 already accepts.
  - A full 16 GiB takes over 2 minutes to write out at 125 MB/s, at shutdown too.
- Leave headroom for replays. While recording used all the disk's throughput, replays from that disk slowed from about 1 GB/s to 67 to 246 MB/s on local NVMe.
- Local NVMe had the highest sustained throughput and was included in the VM price. It sustained 545 MB/s and replayed at 1 GB/s, and `fdatasync` took 0.03 ms against 0.8 ms on Premium SSD v2. Its data is gone when the VM stops or its host fails, though, so use it only for an archive that is replicated elsewhere.
- File sync level 1 or 2 makes every write wait for the disk. This did not reduce sustained throughput on local NVMe. On Premium SSD v2 it was measured only while the new disks were still slow, at 56 to 220 MB/s, so its cost at full speed is unknown.

Sources:
- Aeron's [Archive guide](https://github.com/aeron-io/aeron/wiki/Aeron-Archive) defines the file sync levels: 0 writes to the page cache, 1 forces data to disk, 2 forces data and metadata. It also notes that the archive's "major limitation is the performance of the storage for the recordings".
- Disk performance models:
  - Microsoft's [disk types](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-types#premium-ssd-v2) say Premium SSD v2 includes 3,000 IOPS and 125 MB/s, with more throughput bought at 0.25 MB/s per IOPS. It is designed to deliver what was provisioned 99.9% of the time, and needs a zonal VM in most regions.
  - AWS's [gp3 volumes](https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html#gp3-ebs-volume-type) work the same way, with 3,000 IOPS and 125 MiB/s included, up to 2,000 MiB/s.
- The kernel's [vm sysctls](https://docs.kernel.org/admin-guide/sysctl/vm.html) describe `dirty_bytes` and `dirty_background_bytes`. Setting one clears its `dirty_*ratio` counterpart.
