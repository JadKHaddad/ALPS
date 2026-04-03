"""
Usage: python tcp_drop_duplicate_plot.py <server_events.json> <tcp_drops.json> <output.png>

Correlates TCP proxy connection drops with duplicated event deliveries.

Duplicate definition (mirrors remove_duplication() in Rust):
    Two records with the same (device_id, event_id) in the server event log.

Causality model (probabilistic, no consumer ID available):
    For each duplicate pair (first_arrived, second_arrived), the drop that caused
    the redelivery is the latest drop timestamp that falls inside the window
    [first_arrived_at, second_arrived_at].

    Rationale:
      - Consumer picked up the event and was processing it around first_arrived_at
      - The TCP drop interrupted the ack → message requeued
      - A second consumer redelivered it → second_arrived_at
      - Therefore the causal drop must lie between the two arrivals

    If no drop falls in that window the duplicate is flagged as "unexplained"
    (could be a network hiccup not captured in the drop log).
"""

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.lines as mlines
import numpy as np

if len(sys.argv) < 4:
    print(
        "Usage: python tcp_drop_duplicate.py <server_events.json> <tcp_drops.json> <output.png>"
    )
    sys.exit(1)

server_file, drops_file, out_file = sys.argv[1:]


# ── helpers ───────────────────────────────────────────────────────────────────


def parse_ts(ts: str) -> datetime:
    ts = ts.rstrip("Z")
    if "." in ts:
        base, frac = ts.split(".")
        ts = f"{base}.{frac[:6]}"
    return datetime.fromisoformat(ts)


def load_json(path):
    with open(path) as f:
        return json.load(f)


# ── load ──────────────────────────────────────────────────────────────────────

raw_events = load_json(server_file)
raw_drops = load_json(drops_file)

last_arrived = max((parse_ts(e["arrived_at"]) for e in raw_events), default=None)
drop_times = sorted(
    [
        parse_ts(d["dropped_at"])
        for d in raw_drops
        if last_arrived is None or parse_ts(d["dropped_at"]) <= last_arrived
    ]
)

# ── find duplicates ───────────────────────────────────────────────────────────
# Group by (device_id, event_id), sort arrivals within each group.
# Any group with >1 arrival is a duplicate set.

groups: dict[tuple, list[datetime]] = defaultdict(list)
for e in raw_events:
    key = (e["device_id"], e["event_id"])
    groups[key].append(parse_ts(e["arrived_at"]))

for key in groups:
    groups[key].sort()

dup_keys = [k for k, v in groups.items() if len(v) > 1]

# ── match each duplicate pair to a causal drop ───────────────────────────────
# For groups with N arrivals there are N-1 redelivery pairs:
#   (arrivals[i], arrivals[i+1]) for i in 0..N-2


class DupPair:
    def __init__(self, key, first, second, causal_drop):
        self.device_id = key[0]
        self.event_id = key[1]
        self.first = first  # datetime of first arrival
        self.second = second  # datetime of redelivery
        self.drop = causal_drop  # datetime | None


dup_pairs: list[DupPair] = []

for key in dup_keys:
    arrivals = groups[key]
    for i in range(len(arrivals) - 1):
        first = arrivals[i]
        second = arrivals[i + 1]
        # Latest drop that falls strictly inside [first, second]
        causal = None
        for dt in reversed(drop_times):
            if first <= dt <= second:
                causal = dt
                break
        dup_pairs.append(DupPair(key, first, second, causal))

explained = [p for p in dup_pairs if p.drop is not None]
unexplained = [p for p in dup_pairs if p.drop is None]

# ── per-drop: how many duplicates did it cause? ───────────────────────────────
drop_dup_count: dict[datetime, int] = defaultdict(int)
for p in explained:
    drop_dup_count[p.drop] += 1

# ── redelivery lag: time from causal drop to second arrival ───────────────────
# Measures how quickly the broker requeues + a second consumer redelivers
# after a drop. Only defined for explained pairs.
redelivery_lags_ms = [
    (p.second - p.drop).total_seconds() * 1000  # type: ignore[operator]
    for p in explained
]

# ── rolling duplicate rate (over time, bucketed) ──────────────────────────────
# Bucket all arrivals into 500 ms bins and compute dup fraction per bin.
all_arrivals = sorted(parse_ts(e["arrived_at"]) for e in raw_events)
if all_arrivals:
    t_start = all_arrivals[0]
    t_end = all_arrivals[-1]
    bin_ms = 500
    n_bins = max(1, int((t_end - t_start).total_seconds() * 1000 / bin_ms) + 1)

    bin_total = np.zeros(n_bins)
    bin_dup = np.zeros(n_bins)

    for key, arrivals in groups.items():
        for idx, arr in enumerate(arrivals):
            b = int((arr - t_start).total_seconds() * 1000 / bin_ms)
            b = min(b, n_bins - 1)
            bin_total[b] += 1
            if idx > 0:
                bin_dup[b] += 1

    bin_centers = [
        t_start + timedelta(milliseconds=bin_ms * (i + 0.5)) for i in range(n_bins)
    ]
    with np.errstate(invalid="ignore", divide="ignore"):
        dup_rate = np.where(bin_total > 0, bin_dup / bin_total * 100, np.nan)
else:
    bin_centers = []
    dup_rate = np.array([])

# ── stats ─────────────────────────────────────────────────────────────────────
n_events = len(raw_events)
n_dup_events = sum(len(v) - 1 for v in groups.values() if len(v) > 1)
n_drops = len(drop_times)
n_explained = len(explained)
explain_pct = 100 * n_explained / max(len(dup_pairs), 1)

# ── plot ──────────────────────────────────────────────────────────────────────

fig, (ax1, ax3) = plt.subplots(2, 1, figsize=(15, 9), sharex=True)
axes = [ax1, ax3]

fig.suptitle(
    "TCP Proxy Drop ↔ Duplicate Event Correlation",
    fontsize=15,
    fontweight="bold",
    y=0.98,
)

date_fmt = mdates.DateFormatter("%H:%M:%S")
C = {
    "drop": "#C44E52",
    "dup": "#DD8452",
    "unexplained": "#937860",
    "ok": "#4C72B0",
    "rate": "#8172B3",
    "link": "#C44E52",
}

# ── panel 1: timeline — drops (top lane) and duplicate pairs (bottom lane) ────
ax1 = axes[0]
ax1.set_title("Timeline: TCP Drops and Duplicate Redeliveries")

DROP_Y = 1.0
DUP_Y = 0.0
LANE_H = 0.35

ax1.set_ylim(-0.6, 1.6)
ax1.set_yticks([DUP_Y, DROP_Y])
ax1.set_yticklabels(["Duplicates", "TCP Drops"])

for dt in drop_times:
    count = drop_dup_count.get(dt, 0)
    color = C["drop"] if count > 0 else "#888888"
    ax1.plot(
        [dt, dt],
        [DROP_Y - LANE_H / 2, DROP_Y + LANE_H / 2],
        color=color,
        linewidth=2.5,
        solid_capstyle="round",
        zorder=3,
    )
    if count > 0:
        ax1.text(
            dt,
            DROP_Y + LANE_H / 2 + 0.04,
            str(count),
            ha="center",
            va="bottom",
            fontsize=7,
            color=C["drop"],
        )

for p in dup_pairs:
    color = C["dup"] if p.drop else C["unexplained"]
    ax1.plot(
        [p.first, p.second],
        [DUP_Y, DUP_Y],
        color=color,
        linewidth=4,
        solid_capstyle="round",
        alpha=0.75,
        zorder=3,
    )
    ax1.scatter([p.first], [DUP_Y], color=color, s=30, zorder=4)

for p in explained:
    mid_x = p.first + (p.second - p.first) / 2
    ax1.annotate(
        "",
        xy=(mid_x, DUP_Y + LANE_H / 2 + 0.02),
        xytext=(p.drop, DROP_Y - LANE_H / 2 - 0.02),
        arrowprops=dict(
            arrowstyle="-|>",
            color=C["link"],
            alpha=0.35,
            lw=1.0,
            connectionstyle="arc3,rad=0.15",
        ),
        zorder=2,
    )

legend_handles = [
    mlines.Line2D(
        [], [], color=C["drop"], linewidth=2.5, label="TCP drop (caused ≥1 dup)"
    ),
    mlines.Line2D(
        [], [], color="#888888", linewidth=2.5, label="TCP drop (no matched dup)"
    ),
    mlines.Line2D(
        [], [], color=C["dup"], linewidth=4, label="Duplicate pair (explained)"
    ),
    mlines.Line2D(
        [],
        [],
        color=C["unexplained"],
        linewidth=4,
        label="Duplicate pair (unexplained)",
    ),
]
ax1.legend(handles=legend_handles, fontsize=8, loc="upper right")
ax1.grid(True, axis="x", linestyle="--", alpha=0.3)
ax1.xaxis.set_major_formatter(date_fmt)

# ── panel 3: rolling duplicate rate + drop markers ────────────────────────────
ax3 = axes[1]
ax3.set_title("Duplicate Rate Over Time with Drop Markers")

if len(bin_centers) > 1:
    valid = ~np.isnan(dup_rate)
    ax3.plot(
        np.array(bin_centers)[valid],
        dup_rate[valid],
        color=C["rate"],
        linewidth=2,
        label=f"Dup rate ({bin_ms} ms bins)",  # type: ignore
    )
    ax3.fill_between(
        np.array(bin_centers)[valid], dup_rate[valid], alpha=0.15, color=C["rate"]
    )

first_drop_label = True
for dt in drop_times:
    label = "TCP drop" if first_drop_label else None
    ax3.axvline(
        dt, color=C["drop"], linestyle="--", linewidth=1.0, alpha=0.6, label=label
    )
    first_drop_label = False

ax3.set_ylabel("Duplicate arrivals (%)")
ax3.set_xlabel("Time")
ax3.set_ylim(bottom=0)
ax3.legend(fontsize=9)
ax3.grid(True, linestyle="--", alpha=0.3)
ax3.xaxis.set_major_formatter(date_fmt)
fig.autofmt_xdate(rotation=30)

# ── summary footer ────────────────────────────────────────────────────────────
summary = (
    f"Total arrivals: {n_events}  |  "
    f"Duplicate extra copies: {n_dup_events}  |  "
    f"TCP drops: {n_drops}  |  "
    f"Dup pairs explained by a drop: {n_explained}/{len(dup_pairs)} ({explain_pct:.0f}%)"
)
fig.text(0.5, 0.01, summary, ha="center", fontsize=10, color="gray")

plt.tight_layout(rect=(0, 0.03, 1, 0.97))
plt.savefig(out_file, dpi=150, bbox_inches="tight")
print(f"Saved {out_file}")
