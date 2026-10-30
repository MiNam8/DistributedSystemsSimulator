#!/usr/bin/env python3
"""Plot bench/results.csv: configuration comparison and the concurrency sweep.

    python3 analysis/plot_bench.py
"""
import csv
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "bench" / "results.csv"
OUT = ROOT / "analysis"


def load():
    with open(CSV) as f:
        return list(csv.DictReader(f))


def plot_configs(rows):
    """Throughput and p50 latency per configuration, at fixed concurrency."""
    rows = [r for r in rows if not r["config"].startswith("sweep-")]
    if not rows:
        return
    labels = [r["config"] for r in rows]
    tput = [float(r["throughput"]) for r in rows]
    p50 = [float(r["p50_ms"]) for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    colors = ["#c0392b", "#27ae60", "#7f8c8d"]

    ax1.bar(labels, tput, color=colors[: len(labels)])
    ax1.set_ylabel("throughput (ops/s)")
    ax1.set_title("Throughput by configuration (3 nodes, 64 clients)")
    for i, v in enumerate(tput):
        ax1.text(i, v, f"{v:,.0f}", ha="center", va="bottom", fontsize=9)
    ax1.tick_params(axis="x", rotation=15)

    ax2.bar(labels, p50, color=colors[: len(labels)])
    ax2.set_ylabel("p50 latency (ms)")
    ax2.set_title("Median latency by configuration")
    for i, v in enumerate(p50):
        ax2.text(i, v, f"{v:.1f}", ha="center", va="bottom", fontsize=9)
    ax2.tick_params(axis="x", rotation=15)

    fig.tight_layout()
    fig.savefig(OUT / "throughput_configs.png", dpi=130)
    print("wrote analysis/throughput_configs.png")


def plot_sweep(rows):
    """Throughput and latency against offered concurrency."""
    rows = [r for r in rows if r["config"].startswith("sweep-")]
    if not rows:
        return
    rows.sort(key=lambda r: int(r["clients"]))
    clients = [int(r["clients"]) for r in rows]
    tput = [float(r["throughput"]) for r in rows]
    p50 = [float(r["p50_ms"]) for r in rows]
    p99 = [float(r["p99_ms"]) for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))

    ax1.plot(clients, tput, "o-", color="#27ae60", linewidth=2)
    ax1.set_xscale("log", base=2)
    ax1.set_xlabel("concurrent clients")
    ax1.set_ylabel("throughput (ops/s)")
    ax1.set_title("Throughput scales with offered concurrency")
    ax1.grid(alpha=0.3)

    ax2.plot(clients, p50, "o-", label="p50", color="#2980b9", linewidth=2)
    ax2.plot(clients, p99, "s--", label="p99", color="#c0392b", linewidth=2)
    ax2.set_xscale("log", base=2)
    ax2.set_xlabel("concurrent clients")
    ax2.set_ylabel("latency (ms)")
    ax2.set_title("Latency under load (closed-loop)")
    ax2.legend()
    ax2.grid(alpha=0.3)

    fig.tight_layout()
    fig.savefig(OUT / "throughput_sweep.png", dpi=130)
    print("wrote analysis/throughput_sweep.png")


def plot_reads():
    """The three read paths, at one client (latency) and at 64 (throughput)."""
    path = ROOT / "bench" / "reads.csv"
    if not path.exists():
        return
    with open(path) as f:
        rows = list(csv.DictReader(f))

    order = ["log", "readindex", "lease"]
    pretty = {"log": "through log", "readindex": "ReadIndex", "lease": "leader lease"}
    conc = {m: None for m in order}
    single = {m: None for m in order}
    for r in rows:
        for m in order:
            if r["config"] == f"read-{m}":
                conc[m] = r
            elif r["config"] == f"read-{m}-c1":
                single[m] = r
    if not all(conc.values()) or not all(single.values()):
        return

    labels = [pretty[m] for m in order]
    colors = ["#c0392b", "#2980b9", "#27ae60"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))

    tput = [float(conc[m]["throughput"]) for m in order]
    ax1.bar(labels, tput, color=colors)
    ax1.set_ylabel("throughput (ops/s)")
    ax1.set_title("Read throughput, 64 concurrent clients")
    for i, v in enumerate(tput):
        ax1.text(i, v, f"{v:,.0f}", ha="center", va="bottom", fontsize=9)

    lat = [float(single[m]["p50_ms"]) for m in order]
    ax2.bar(labels, lat, color=colors)
    ax2.set_ylabel("p50 latency (ms)")
    ax2.set_title("Read latency, single client (no queueing)")
    for i, v in enumerate(lat):
        ax2.text(i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=9)

    fig.tight_layout()
    fig.savefig(OUT / "read_paths.png", dpi=130)
    print("wrote analysis/read_paths.png")


def plot_failover():
    """Failover latency against election timeout, with and without pre-vote."""
    path = ROOT / "bench" / "failover.csv"
    if not path.exists():
        return
    with open(path) as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return

    modes = {}
    for r in rows:
        modes.setdefault(r["mode"], []).append(r)
    for rs in modes.values():
        rs.sort(key=lambda r: int(r["election_timeout_ms"]))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    colors = {"plain": "#c0392b", "pre-vote": "#27ae60"}

    for mode, rs in modes.items():
        t = [int(r["election_timeout_ms"]) for r in rs]
        c = colors.get(mode, "#7f8c8d")
        ax1.plot(t, [int(r["p50_ms"]) for r in rs], "o-", color=c, label=f"{mode} p50", linewidth=2)
        ax1.plot(t, [int(r["p99_ms"]) for r in rs], "s--", color=c, label=f"{mode} p99", alpha=0.7)
        ax2.plot(t, [float(r["spurious_per_run"]) for r in rs], "o-", color=c, label=mode, linewidth=2)

    ax1.set_xlabel("election timeout (ms)")
    ax1.set_ylabel("time to a new leader (ms)")
    ax1.set_title("Failover latency, 500 leader kills per point")
    ax1.legend(fontsize=8)
    ax1.grid(alpha=0.3)

    ax2.set_xlabel("election timeout (ms)")
    ax2.set_ylabel("unnecessary elections per run")
    ax2.set_title("Cost of an aggressive timeout (healthy cluster, lossy link)")
    ax2.legend(fontsize=8)
    ax2.grid(alpha=0.3)

    fig.tight_layout()
    fig.savefig(OUT / "failover.png", dpi=130)
    print("wrote analysis/failover.png")


def main():
    if CSV.exists():
        rows = load()
        plot_configs(rows)
        plot_sweep(rows)
    plot_reads()
    plot_failover()


if __name__ == "__main__":
    main()
