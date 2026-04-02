"""
Usage: python latency_plot.py <data.json> <output.png>

Plots latency per event over trigger time.
"""

import sys
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from latency_common import load_events

if len(sys.argv) < 3:
    print("Usage: python latency_plot.py <data.json> <output.png>")
    sys.exit(1)

trigger_times, latencies, s = load_events(sys.argv[1])

fig, ax = plt.subplots(figsize=(10, 5))
fig.suptitle(
    "Event Latency over Trigger Time",
    fontsize=14,
    fontweight="bold",
)

ax.plot(
    trigger_times,
    latencies,
    "o-",
    color="#4C72B0",
    alpha=0.6,
    markersize=4,
    linewidth=1,
    label="Latency",
)
ax.axhline(
    s["avg"],
    color="red",
    linestyle="--",
    linewidth=1.5,
    label=f"Mean:   {s['avg']:.3f} ms",
)
ax.axhline(
    s["median"],
    color="green",
    linestyle="--",
    linewidth=1.5,
    label=f"Median: {s['median']:.3f} ms",
)
ax.axhline(
    s["p95"],
    color="orange",
    linestyle=":",
    linewidth=1.5,
    label=f"P95:    {s['p95']:.3f} ms",
)
ax.axhline(
    s["p99"],
    color="purple",
    linestyle=":",
    linewidth=1.5,
    label=f"P99:    {s['p99']:.3f} ms",
)

ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
ax.set_xlabel("Trigger Time")
ax.set_ylabel("Latency (ms)")
ax.set_title("Latency per Event")
ax.legend(fontsize=9)
ax.grid(True, linestyle="--", alpha=0.4)
fig.autofmt_xdate(rotation=30)

summary = (
    f"n={s['n']}  |  Min: {s['min']:.3f} ms  |  "
    f"Max: {s['max']:.3f} ms  |  Std: {s['std']:.3f} ms"
)
fig.text(0.5, 0.01, summary, ha="center", fontsize=10, color="gray")

plt.tight_layout(rect=(0, 0.04, 1, 1))
plt.savefig(sys.argv[2], dpi=150, bbox_inches="tight")
print(f"Saved {sys.argv[2]}")
