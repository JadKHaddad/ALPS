"""
Usage: python latency.py <data.json> <output_line.png> <output_hist.png> [y_max]

Plots latency per event over trigger time and latency distribution histogram
into two separate output files.

Optional 4th argument y_max sets the maximum value of the y-axis on the
"Latenz pro Ereignis" line plot (e.g. 100000 for 100000 ms). If omitted,
matplotlib auto-scales the axis as before.
"""

import json
import sys
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

if len(sys.argv) < 4:
    print(
        "Usage: python latency.py <data.json> <output_line.png> <output_hist.png> [y_max]"
    )
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

    t0 = trigger_times[0]
    elapsed_seconds = np.array([(t - t0).total_seconds() for t in trigger_times])

    return elapsed_seconds, latencies, stats


# ── load ──────────────────────────────────────────────────────────────────────

trigger_times, latencies, s = load_events(sys.argv[1])
out_line, out_hist = sys.argv[2], sys.argv[3]

y_max = None
if len(sys.argv) > 4:
    try:
        y_max = float(sys.argv[4])
    except ValueError:
        print(f"Invalid y_max value: {sys.argv[4]!r} (must be a number)")
        sys.exit(1)

footer = (
    f"Ereignisse: {s['n']}  |  Min: {s['min']:.3f} ms  |  "
    f"Max: {s['max']:.3f} ms  |  Std: {s['std']:.3f} ms"
)

# ── plot 1: Latenz über Triggerzeit ───────────────────────────────────────────

fig1, ax1 = plt.subplots(figsize=(10, 5))

ax1.plot(
    trigger_times,
    latencies,
    "o-",
    color="#4C72B0",
    alpha=0.6,
    markersize=4,
    linewidth=1,
    label="Latenz",
)
ax1.axhline(
    s["avg"],
    color="red",
    linestyle="--",
    linewidth=1.5,
    label=f"Mittelwert: {s['avg']:.3f} ms",
)
ax1.axhline(
    s["median"],
    color="green",
    linestyle="--",
    linewidth=1.5,
    label=f"Median:     {s['median']:.3f} ms",
)
ax1.axhline(
    s["p95"],
    color="orange",
    linestyle=":",
    linewidth=1.5,
    label=f"P95:        {s['p95']:.3f} ms",
)
ax1.axhline(
    s["p99"],
    color="purple",
    linestyle=":",
    linewidth=1.5,
    label=f"P99:        {s['p99']:.3f} ms",
)
ax1.set_xlabel("Triggerzeit (Sekunden seit Beginn)")
ax1.set_ylabel("Latenz (ms)")
ax1.set_title("Latenz pro Ereignis")
if y_max is not None:
    ax1.set_ylim(0, y_max)
ax1.legend(fontsize=9)
ax1.grid(True, linestyle="--", alpha=0.4)
fig1.text(0.5, 0.01, footer, ha="center", fontsize=10, color="gray")
plt.tight_layout(rect=(0, 0.04, 1, 1))
plt.savefig(out_line, dpi=150, bbox_inches="tight")
print(f"Saved: {out_line}")
plt.close(fig1)

# ── plot 2: Histogramm ────────────────────────────────────────────────────────

fig2, ax2 = plt.subplots(figsize=(10, 5))

ax2.hist(latencies, bins=20, color="#4C72B0", alpha=0.7, edgecolor="white")
ax2.axvline(
    s["avg"],
    color="red",
    linestyle="--",
    linewidth=1.5,
    label=f"Mittelwert: {s['avg']:.3f} ms",
)
ax2.axvline(
    s["median"],
    color="green",
    linestyle="--",
    linewidth=1.5,
    label=f"Median:     {s['median']:.3f} ms",
)
ax2.axvline(
    s["p95"],
    color="orange",
    linestyle=":",
    linewidth=1.5,
    label=f"P95:        {s['p95']:.3f} ms",
)
ax2.axvline(
    s["p99"],
    color="purple",
    linestyle=":",
    linewidth=1.5,
    label=f"P99:        {s['p99']:.3f} ms",
)
ax2.set_xlabel("Latenz (ms)")
ax2.set_ylabel("Anzahl")
ax2.set_title("Latenzverteilung")
ax2.legend(fontsize=9)
ax2.grid(True, linestyle="--", alpha=0.4)
fig2.text(0.5, 0.01, footer, ha="center", fontsize=10, color="gray")
plt.tight_layout(rect=(0, 0.04, 1, 1))
plt.savefig(out_hist, dpi=150, bbox_inches="tight")
print(f"Saved: {out_hist}")
plt.close(fig2)
