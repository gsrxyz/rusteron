#!/usr/bin/env python3
"""Turns the output of Aeron's Java samples and rusteron's examples into bench.csv rows,
in the harness's formats:
  rtt,<label>,udp,p50,p90,p99,p99.9,p99.99,max,mean,samples   (ns)
  tput,<label>,ipc,<median msgs/s>,<min>,<max>

parse_samples.py <java-pp|rust-pp|java-tput|rust-tput> <output file> <label> [skip]
skip: leading throughput intervals to drop (warm-up and pinning), default 4."""
import re
import statistics
import sys


def duration_ns(text):
    m = re.fullmatch(r"([\d.]+)(ns|µs|us|ms|s)", text.strip())
    value, unit = float(m.group(1)), m.group(2)
    return value * {"ns": 1, "µs": 1e3, "us": 1e3, "ms": 1e6, "s": 1e9}[unit]


def java_pp(text):
    # HdrHistogram percentile table, values in µs: value percentile count 1/(1-p)
    rows = [(float(a), float(b), int(c)) for a, b, c in
            re.findall(r"^\s*([\d.]+)\s+([01]\.\d+)\s+(\d+)", text, re.M)]
    if not rows:
        raise ValueError("no percentile table")
    def at(q):
        return next(v for v, p, _ in rows if p >= q) * 1e3
    mean = float(re.search(r"Mean\s*=\s*([\d.]+)", text).group(1)) * 1e3
    peak = float(re.search(r"Max\s*=\s*([\d.]+)", text).group(1)) * 1e3
    return [at(0.5), at(0.9), at(0.99), at(0.999), at(0.9999), peak, mean, rows[-1][2]]


def rust_pp(text):
    def get(name):
        return duration_ns(re.search(rf"^{re.escape(name)}: (\S+)", text, re.M).group(1))
    samples = int(re.search(r"# of samples: (\d+)", text).group(1))
    return [get("50th percentile"), 0, get("99th percentile"), get("99.9th percentile"),
            get("99.99th percentile"), get("max"), get("avg"), samples]


def java_tput(text):
    return [int(n.replace(",", "")) / (int(ms) / 1000)
            for ms, n in re.findall(r"Duration (\d+)ms - ([\d,]+) messages", text)]


def rust_tput(text):
    return [float(n) for n in re.findall(r"rate: (\d+) msg/s", text)]


def main():
    kind, path, label = sys.argv[1:4]
    skip = int(sys.argv[4]) if len(sys.argv) > 4 else 4
    text = open(path, errors="replace").read()
    if kind.endswith("-pp"):
        values = (java_pp if kind == "java-pp" else rust_pp)(text)
        print(f"rtt,{label},udp," + ",".join(f"{v:.0f}" for v in values))
    else:
        rates = (java_tput if kind == "java-tput" else rust_tput)(text)
        # the last interval is cut short by the stop signal
        rates = rates[skip:-1]
        if not rates:
            raise ValueError("no full throughput intervals")
        print(f"tput,{label},ipc,{statistics.median(rates):.0f},{min(rates):.0f},{max(rates):.0f}")


if __name__ == "__main__":
    main()
