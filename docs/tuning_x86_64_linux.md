# Tuning on x86-64 Linux

Settings for low-latency Aeron on x86-64 Linux, mainly on cloud VMs. Effects vary with the NIC and cloud.
Measurements are in [BENCHMARKS.md](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md#intel-d8s_v6-pair-on-azure-2026-10-09).

## Baseline

Apply these first:

- `AERON_TERM_BUFFER_SPARSE_FILE=false` (driver) and `AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY=true` (clients). Without them, first access to a new publication's log causes page faults.
- `AERON_DIR` on 2 MiB `hugetlbfs` with `AERON_FILE_PAGE_SIZE=2097152`.
- `noop` idle strategy for the driver's sender and receiver.
- Pin every busy-spinning thread to its own core. Keep everything else on a housekeeping CPU: interrupts, the driver's conductor and the clients' other threads.

## Settings

| Setting | Effect | Cost |
|---|---|---|
| `AERON_SOCKET_SO_SNDBUF`, `AERON_SOCKET_SO_RCVBUF` and `AERON_RCV_INITIAL_WINDOW_LENGTH` at 2 MiB, with `net.core.rmem_max` and `wmem_max` raised to allow them | Much higher UDP throughput. No effect on round-trip latency with one message in flight. | Memory per socket. |
| MTU 9000 on the network and `AERON_MTU_LENGTH=8192` | Higher UDP throughput. | On Azure, jumbo frames work only inside a VNet and directly peered VNets. Keep the default route at 1500 so traffic leaving the VNet still fits. |
| CPU isolation: `isolcpus=nohz,domain,managed_irq,<hot CPUs> nohz_full=<same> rcu_nocbs=<same> irqaffinity=<housekeeping>` | Lower IPC tail latency. No change to UDP on its own. | Isolated CPUs take no unpinned work, so every busy thread must be pinned. |
| `net.core.busy_read=50`, with `napi_defer_hard_irqs=2` and `gro_flush_timeout=200000` on the NIC's VF, on isolated CPUs | Lower UDP latency at all percentiles. `net.core.busy_poll` has no effect, because Aeron's receiver calls `recvmmsg` directly. | The receiver's core polls the NIC queue itself. |
| The NIC's queue IRQs on the receiver's core, on isolated CPUs | Lower UDP tail latency. | Re-pin them whenever Azure re-adds the VF (host servicing), which spreads them over all CPUs again. |
| The client's other threads (its conductor) off the busy-spinning CPUs | Lower IPC tail latency. | Reserve a CPU for these threads or use the conductor agent invoker. |
| Tuned kernel on top of isolation: `nosmt idle=poll rcu_nocb_poll nowatchdog nmi_watchdog=0 nosoftlockup skew_tick=1 transparent_hugepage=never audit=0`, all IRQs and workqueues on the housekeeping CPU, background services stopped | Slightly lower UDP median. No change to IPC. | `nosmt` halves the logical CPU count. `idle=poll` makes idle CPUs spin. |
| `mitigations=off` | Slightly lower UDP median and tail. No change to IPC. | Turns off kernel protection against CPU side-channel attacks. |
| Driver threading | Dedicated (pinned `noop` sender and receiver), `SHARED_NETWORK` and `SHARED` give similar UDP median latency. `SHARED` raises IPC latency and lowers throughput. | Dedicated spins two cores. |
| Across regions: socket buffers and initial window sized to the round trip (for example 16 MiB at 53 ms), 64 MiB terms, and `net.core.rmem_max` and `wmem_max` raised to match and kept in `/etc/sysctl.d` | Higher throughput on a long link without packet loss. | Memory per socket. If `rmem_max` resets (a reboot undoes `sysctl -w`), the driver refuses to start because the window exceeds the receive buffer. |
| In Kubernetes: the kubelet's static CPU manager and Guaranteed pods requesting whole CPUs | Each container gets its CPUs to itself with no CFS quota. Under the default policy, a container limited to exactly its spinning threads' CPUs is throttled. Give it one CPU of headroom or no CPU limit. | See [BENCHMARKS.md](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md#recommended-settings-for-kubernetes) for the kubelet and pod settings. |

## Example setup

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

## Packet loss

Fix packet loss before tuning anything else. Every lost packet is retransmitted, but loss cuts throughput sharply. In request/response traffic, a lost message waits for the sender's next heartbeat, 100 ms in the C driver. See [the measurements](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md#packet-loss-and-distance-west-us-3-and-north-central-us-2026-10-10).

To find stalls and loss:

- Lower `AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD`, `AERON_DRIVER_SENDER_CYCLE_THRESHOLD` and `AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD` from the 100 ms default. Aeron's benchmark driver uses 1 ms.
- Watch AeronStat's "work cycle exceeded threshold count" counters, the NAK counters and `loss-report.dat` ([Monitoring and Debugging](https://github.com/aeron-io/aeron/wiki/Monitoring-and-Debugging)).
- On the host, watch `nstat -az UdpRcvbufErrors UdpInErrors`, the drops in `/proc/net/softnet_stat` and `ethtool -S` ([Red Hat's network tuning](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/monitoring_and_managing_system_status_and_performance/tuning-the-network-performance_monitoring-and-managing-system-status-and-performance)).

## Other settings to consider

- Aeron's [`low-latency-c-media-driver`](https://github.com/aeron-io/aeron/blob/master/aeron-samples/scripts/low-latency-c-media-driver) script also sets `AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND=2` (the default is 4) and `AERON_CONDUCTOR_IDLE_STRATEGY=spin`, which spins a whole core.
- Keep messages within the MTU for the lowest latency.
- tuned's [`network-latency` profile](https://github.com/redhat-performance/tuned/blob/master/profiles/network-latency/tuned.conf) adds `rcupdate.rcu_normal_after_boot=1`, `rcutree.nohz_full_patience_delay=1000` and `kernel.timer_migration=0`. Its [`cpu-partitioning` profile](https://github.com/redhat-performance/tuned/blob/master/profiles/cpu-partitioning/tuned.conf) also moves the writeback workqueue (`/sys/bus/workqueue/devices/writeback/cpumask`) to the housekeeping CPUs.
- The kernel's [CPU isolation guide](https://docs.kernel.org/admin-guide/cpu-isolation.html) prefers cpuset isolated partitions, which you can change at run time, to `isolcpus`.
- If any firewall rule loads conntrack, add a `notrack` rule at raw priority to keep Aeron's packets out of it ([nftables wiki](https://wiki.nftables.org/wiki-nftables/index.php/Setting_packet_connection_tracking_metainformation)).
- On Azure, after host maintenance, run `az ppg show --include-colocation-status` ([co-location](https://learn.microsoft.com/en-us/azure/virtual-machines/co-location)). A VM moved out of alignment needs a full deallocate and start.

### AWS

- Use a cluster placement group.
- Set `net.core.busy_read=50`. `net.core.busy_poll=50` does nothing for Aeron.
- Turn off the ENA NIC's interrupt moderation: `sudo ethtool -C <interface> adaptive-rx off rx-usecs 0 tx-usecs 0`.
- Limit deeper C-states where the instance type allows it ([processor state control](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/processor_state_control.html)). On Amazon Linux 2, use `intel_idle.max_cstate=1 processor.max_cstate=1`. `idle=poll` goes further, so use one or the other.
- For a steadier frequency, turn off Turbo Boost with `echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo`.
- The ENA driver's [best practices guide](https://github.com/amzn/amzn-drivers/blob/master/kernel/linux/ena/ENA_Linux_Best_Practices.rst) suggests keeping irqbalance but banning the hot CPUs (`IRQBALANCE_BANNED_CPUS`), and running the application on the vCPU that takes its queue's IRQ.
- Leave [ENA Express](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-express.html) off for latency. It can add tens of microseconds to the median.

On Azure VMs the C-state options do nothing, because the guest has no cpuidle driver.

## Archive disks

These points are based on a Java Archive recording 1 KiB messages from up to 16 streams ([details](https://github.com/gsrxyz/rusteron/blob/main/BENCHMARKS.md#java-archive-on-disk-under-load-two-d8ds_v6-2026-10-10)).

- Size the disk for sustained throughput. At file sync level 0 (the default) the archive writes into the page cache, which absorbs bursts. The disk needs the average write rate plus replay reads.
- Cloud disks share one budget between reads and writes. Recordings are sequential, so IOPS limits rarely matter.
- A durable network disk can sustain recording at its provisioned throughput. On Azure that is Premium SSD v2, with 125 MB/s included and more available separately. AWS gp3 is priced the same way. The VM also caps its disks in total.
- Test a new Premium SSD v2 disk before relying on it. Freshly formatted disks (plain `mkfs.xfs` discards the whole device) ran well below their provisioned rate for 25 minutes or more.
- Raise the page-cache write-back limits to absorb larger bursts: `sudo sysctl -w vm.dirty_bytes=17179869184 vm.dirty_background_bytes=536870912`. This allows 16 GiB of unwritten data, so scale it to your RAM. Data still in memory is lost if the host fails, which level 0 already accepts. A full 16 GiB takes over 2 minutes to write out at 125 MB/s, at shutdown too.
- Leave headroom for replays. Replays slow down while recording uses all of a disk's throughput.
- Local NVMe has the highest sustained throughput and is included in the VM price. Its data is lost when the VM stops or its host fails, so use it only for an archive that is replicated elsewhere.
- File sync level 1 or 2 makes every write wait for the disk.

## Sources

- Aeron's [Best Practices Guide](https://github.com/aeron-io/aeron/wiki/Best-Practices-Guide) and [Configuration Options](https://github.com/aeron-io/aeron/wiki/Configuration-Options).
- Aeron's [Archive guide](https://github.com/aeron-io/aeron/wiki/Aeron-Archive) defines the file sync levels: 0 writes to the page cache, 1 forces data to disk, 2 forces data and metadata.
- Erik Rigtorp's [Low Latency Tuning Guide](https://rigtorp.se/low-latency-guide/) covers CPU isolation, thread pinning, IRQ affinity, `nosmt`, transparent huge pages and `mitigations=off`.
- SUSE's [kernel arguments for low latency and high performance](https://documentation.suse.com/suse-edge/3.1/html/edge/atip-features.html) and the [Linux kernel parameters](https://docs.kernel.org/admin-guide/kernel-parameters.html).
- [Kernel net sysctls](https://docs.kernel.org/admin-guide/sysctl/net.html) and [NAPI documentation](https://docs.kernel.org/networking/napi.html), which recommends setting `napi_defer_hard_irqs` and `gro_flush_timeout` together.
- [Configure MTU for virtual machines in Azure](https://learn.microsoft.com/en-us/azure/virtual-network/how-to-virtual-machine-mtu).
- Kubernetes: [CPU management policies](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/) and [HugePages](https://kubernetes.io/docs/tasks/manage-hugepages/scheduling-hugepages/).
- AWS: [Improve network latency for Linux based EC2 instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ena-improve-network-latency-linux.html) and the [Amazon Linux 2 processor state guide](https://docs.aws.amazon.com/linux/al2/ug/processor_state_control.html).
- Disk models: Azure [disk types](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-types#premium-ssd-v2), AWS [gp3 volumes](https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html#gp3-ebs-volume-type) and the kernel's [vm sysctls](https://docs.kernel.org/admin-guide/sysctl/vm.html).
