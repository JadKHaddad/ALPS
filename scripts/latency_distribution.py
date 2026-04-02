"""
Usage: python latency_distribution.py <data.json> <output.png>

Plots a histogram of latency distribution.
"""

import sys
import matplotlib.pyplot as plt

from latency_common import load_events

if len(sys.argv) < 3:
    print("Usage: python latency_distribution.py <data.json> <output.png>")
    sys.exit(1)

_, latencies, s = load_events(sys.argv[1])

fig, ax = plt.subplots(figsize=(8, 5))
fig.suptitle(
    "Event Latency Distribution",
    fontsize=14,
    fontweight="bold",
)

ax.hist(latencies, bins=20, color="#4C72B0", alpha=0.7, edgecolor="white")
ax.axvline(
    s["avg"],
    color="red",
    linestyle="--",
    linewidth=1.5,
    label=f"Mean:   {s['avg']:.3f} ms",
)
ax.axvline(
    s["median"],
    color="green",
    linestyle="--",
    linewidth=1.5,
    label=f"Median: {s['median']:.3f} ms",
)
ax.axvline(
    s["p95"],
    color="orange",
    linestyle=":",
    linewidth=1.5,
    label=f"P95:    {s['p95']:.3f} ms",
)
ax.axvline(
    s["p99"],
    color="purple",
    linestyle=":",
    linewidth=1.5,
    label=f"P99:    {s['p99']:.3f} ms",
)

ax.set_xlabel("Latency (ms)")
ax.set_ylabel("Count")
ax.set_title("Latency Distribution")
ax.legend(fontsize=9)
ax.grid(True, linestyle="--", alpha=0.4)

summary = (
    f"n={s['n']}  |  Min: {s['min']:.3f} ms  |  "
    f"Max: {s['max']:.3f} ms  |  Std: {s['std']:.3f} ms"
)
fig.text(0.5, 0.01, summary, ha="center", fontsize=10, color="gray")

plt.tight_layout(rect=(0, 0.04, 1, 1))
plt.savefig(sys.argv[2], dpi=150, bbox_inches="tight")
print(f"Saved {sys.argv[2]}")
