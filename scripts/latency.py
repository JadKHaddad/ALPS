import json
import sys
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

if len(sys.argv) < 2:
    print("Usage: python plot.py <data.json>")
    sys.exit(1)

with open(sys.argv[1], "r") as f:
    data = json.load(f)


def parse_ts(ts):
    ts = ts.rstrip("Z")
    if "." in ts:
        base, frac = ts.split(".")
        frac = frac[:6]
        ts = f"{base}.{frac}"
    return datetime.fromisoformat(ts)


# Compute latency in milliseconds for each event, then sort by trigger time
events = []
for d in data:
    trigger = parse_ts(d["original"]["triggertime"])
    arrived = parse_ts(d["arrived_at"])
    latency_ms = (arrived - trigger).total_seconds() * 1000
    events.append((trigger, latency_ms))

events.sort(key=lambda x: x[0])
trigger_times, latencies = zip(*events)
latencies = np.array(latencies)

avg = np.mean(latencies)
median = np.median(latencies)
std = np.std(latencies)
p95 = np.percentile(latencies, 95)
p99 = np.percentile(latencies, 99)
mn = np.min(latencies)
mx = np.max(latencies)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle(
    "Event Latency Statistics (arrived_at − triggertime)",
    fontsize=14,
    fontweight="bold",
)

# --- Left: latency over trigger time ---
ax1 = axes[0]
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
    avg, color="red", linestyle="--", linewidth=1.5, label=f"Mean:   {avg:.3f} ms"
)
ax1.axhline(
    median,
    color="green",
    linestyle="--",
    linewidth=1.5,
    label=f"Median: {median:.3f} ms",
)
ax1.axhline(
    p95, color="orange", linestyle=":", linewidth=1.5, label=f"P95:    {p95:.3f} ms"
)
ax1.axhline(
    p99, color="purple", linestyle=":", linewidth=1.5, label=f"P99:    {p99:.3f} ms"
)
ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
ax1.set_xlabel("Trigger Time")
ax1.set_ylabel("Latency (ms)")
ax1.set_title("Latency per Event")
ax1.legend(fontsize=9)
ax1.grid(True, linestyle="--", alpha=0.4)
fig.autofmt_xdate(rotation=30)

# --- Right: histogram ---
ax2 = axes[1]
ax2.hist(latencies, bins=20, color="#4C72B0", alpha=0.7, edgecolor="white")
ax2.axvline(
    avg, color="red", linestyle="--", linewidth=1.5, label=f"Mean:   {avg:.3f} ms"
)
ax2.axvline(
    median,
    color="green",
    linestyle="--",
    linewidth=1.5,
    label=f"Median: {median:.3f} ms",
)
ax2.axvline(
    p95, color="orange", linestyle=":", linewidth=1.5, label=f"P95:    {p95:.3f} ms"
)
ax2.axvline(
    p99, color="purple", linestyle=":", linewidth=1.5, label=f"P99:    {p99:.3f} ms"
)
ax2.set_xlabel("Latency (ms)")
ax2.set_ylabel("Count")
ax2.set_title("Latency Distribution")
ax2.legend(fontsize=9)
ax2.grid(True, linestyle="--", alpha=0.4)

# Summary text box
summary = (
    f"n={len(latencies)}  |  Min: {mn:.3f} ms  |  "
    f"Max: {mx:.3f} ms  |  Std: {std:.3f} ms"
)
fig.text(0.5, 0.01, summary, ha="center", fontsize=10, color="gray")

plt.tight_layout(rect=(0, 0.04, 1, 1))

plt.savefig("png/latency_stats.png", dpi=150, bbox_inches="tight")
print("Saved png/latency_stats.png")
