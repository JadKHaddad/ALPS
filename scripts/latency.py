"""
Usage: python latency.py <data.json> <output.png>

Plots latency per event over trigger time (left) and latency distribution histogram (right).
"""

import json
import sys
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

if len(sys.argv) < 3:
    print("Usage: python latency.py <data.json> <output.png>")
    sys.exit(1)


# ── helpers ───────────────────────────────────────────────────────────────────


def parse_ts(ts):
    ts = ts.rstrip("Z")
    if "." in ts:
        base, frac = ts.split(".")
        ts = f"{base}.{frac[:6]}"
    return datetime.fromisoformat(ts)


def load_events(path):
    with open(path, "r") as f:
        data = json.load(f)

    events = []
    for d in data:
        trigger = parse_ts(d["original"]["triggertime"])
        arrived = parse_ts(d["arrived_at"])
        latency_ms = (arrived - trigger).total_seconds() * 1000
        events.append((trigger, latency_ms))

    events.sort(key=lambda x: x[0])
    trigger_times, latencies = zip(*events)
    latencies = np.array(latencies)

    stats = {
        "avg": np.mean(latencies),
        "median": np.median(latencies),
        "std": np.std(latencies),
        "p95": np.percentile(latencies, 95),
        "p99": np.percentile(latencies, 99),
        "min": np.min(latencies),
        "max": np.max(latencies),
        "n": len(latencies),
    }

    return trigger_times, latencies, stats


# ── load ──────────────────────────────────────────────────────────────────────

trigger_times, latencies, s = load_events(sys.argv[1])

# ── plot ──────────────────────────────────────────────────────────────────────

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle(
    "Event Latency Statistics (arrived_at − triggertime)",
    fontsize=14,
    fontweight="bold",
)

# ── left: latency over trigger time ──────────────────────────────────────────

ax1.plot(
    trigger_times,
    latencies,
    "o-",
    color="#4C72B0",
    alpha=0.6,
    markersize=4,
    linewidth=1,
    label="Latency",
)
ax1.axhline(
    s["avg"],
    color="red",
    linestyle="--",
    linewidth=1.5,
    label=f"Mean:   {s['avg']:.3f} ms",
)
ax1.axhline(
    s["median"],
    color="green",
    linestyle="--",
    linewidth=1.5,
    label=f"Median: {s['median']:.3f} ms",
)
ax1.axhline(
    s["p95"],
    color="orange",
    linestyle=":",
    linewidth=1.5,
    label=f"P95:    {s['p95']:.3f} ms",
)
ax1.axhline(
    s["p99"],
    color="purple",
    linestyle=":",
    linewidth=1.5,
    label=f"P99:    {s['p99']:.3f} ms",
)
ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
ax1.set_xlabel("Trigger Time")
ax1.set_ylabel("Latency (ms)")
ax1.set_title("Latency per Event")
ax1.legend(fontsize=9)
ax1.grid(True, linestyle="--", alpha=0.4)

# ── right: histogram ──────────────────────────────────────────────────────────

ax2.hist(latencies, bins=20, color="#4C72B0", alpha=0.7, edgecolor="white")
ax2.axvline(
    s["avg"],
    color="red",
    linestyle="--",
    linewidth=1.5,
    label=f"Mean:   {s['avg']:.3f} ms",
)
ax2.axvline(
    s["median"],
    color="green",
    linestyle="--",
    linewidth=1.5,
    label=f"Median: {s['median']:.3f} ms",
)
ax2.axvline(
    s["p95"],
    color="orange",
    linestyle=":",
    linewidth=1.5,
    label=f"P95:    {s['p95']:.3f} ms",
)
ax2.axvline(
    s["p99"],
    color="purple",
    linestyle=":",
    linewidth=1.5,
    label=f"P99:    {s['p99']:.3f} ms",
)
ax2.set_xlabel("Latency (ms)")
ax2.set_ylabel("Count")
ax2.set_title("Latency Distribution")
ax2.legend(fontsize=9)
ax2.grid(True, linestyle="--", alpha=0.4)

# ── footer ────────────────────────────────────────────────────────────────────

summary = (
    f"n={s['n']}  |  Min: {s['min']:.3f} ms  |  "
    f"Max: {s['max']:.3f} ms  |  Std: {s['std']:.3f} ms"
)
fig.text(0.5, 0.01, summary, ha="center", fontsize=10, color="gray")

fig.autofmt_xdate(rotation=30)
plt.tight_layout(rect=(0, 0.04, 1, 1))
plt.savefig(sys.argv[2], dpi=150, bbox_inches="tight")
print(f"Saved {sys.argv[2]}")
