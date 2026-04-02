"""
Usage: python failure_order_plot.py <device_events.json> <server_events.json> <failures.json> <output.png>

Plots a timeline showing:
  - Which events arrived out of order, and by how much
  - Which events failed (and when)
  - Rolling out-of-order rate overlaid with failure markers
  - Per-event latency coloured by status

"Out of order" mirrors validate_order() in the Rust server:
  Events are walked in arrival order. Per device_id, we track the last seen event_id.
  A violation occurs when event.event_id < last_seen_event_id for that device.
  The severity is (last_seen_event_id - event.event_id): how far back the event_id jumped.
"""

import json
import sys
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
from datetime import datetime

if len(sys.argv) < 5:
    print(
        "Usage: python failure_order.py <device_events.json> <server_events.json> <failures.json> <output.png>"
    )
    sys.exit(1)

device_file, server_file, failures_file, out_file = sys.argv[1:]


# ── helpers ──────────────────────────────────────────────────────────────────


def parse_ts(ts):
    if ts is None:
        return None
    ts = ts.rstrip("Z")
    if "." in ts:
        base, frac = ts.split(".")
        ts = f"{base}.{frac[:6]}"
    return datetime.fromisoformat(ts)


def load_json(path):
    with open(path) as f:
        return json.load(f)


# ── load data ─────────────────────────────────────────────────────────────────

device_events = load_json(device_file)  # sent by device; arrived_at may be null
server_events = load_json(server_file)  # successfully received
failures_raw = load_json(failures_file)  # failed processing


# ── build lookup: event_id → failed_at ───────────────────────────────────────

failure_map = {}  # event_id -> failed_at datetime
for f in failures_raw:
    eid = f["event"]["event_id"]
    failure_map[eid] = parse_ts(f["failed_at"])


# ── build per-event records from server events (have both trigger + arrival) ──

server_by_id = {e["event_id"]: e for e in server_events}

# Walk events in arrival order — this is what validate_order() does.
arrived_events = [e for e in server_events if e.get("arrived_at")]
arrived_events.sort(key=lambda e: parse_ts(e["arrived_at"]) or datetime.min)

# Per-device last-seen event_id, mirroring:
#   if event.event_id < last[device_id] → violation
last_seen: dict[str, int] = {}
ooo_severity: dict[int, int] = {}  # event_id → how far back it jumped (0 = in order)

for e in arrived_events:
    eid = e["event_id"]
    device_id = e["device_id"]
    prev = last_seen.get(device_id)
    if prev is not None and eid < prev:
        ooo_severity[eid] = prev - eid  # positive = violation magnitude
    else:
        ooo_severity[eid] = 0
    last_seen[device_id] = eid

# Build records (sorted by arrival time, matching the order violations were detected)
records = []
for e in arrived_events:
    eid = e["event_id"]
    trig_ts = parse_ts(e["original"]["triggertime"])
    arr_ts = parse_ts(e["arrived_at"])
    latency = (arr_ts - trig_ts).total_seconds() * 1000  # type: ignore[operator]
    severity = ooo_severity[eid]
    failed = eid in failure_map
    records.append(
        {
            "event_id": eid,
            "device_id": e["device_id"],
            "trig_ts": trig_ts,
            "arr_ts": arr_ts,
            "latency_ms": latency,
            "ooo_severity": severity,  # 0 = in order, >0 = violation magnitude
            "out_of_order": severity > 0,
            "failed": failed,
            "failed_at": failure_map.get(eid),
        }
    )

# Also collect failures whose event never appeared in server_events
orphan_failures = [
    (failure_map[eid], eid) for eid in failure_map if eid not in server_by_id
]

# ── derived series ────────────────────────────────────────────────────────────

trig_times = [
    r["arr_ts"] for r in records
]  # x-axis = arrival time (matches detection order)
severities = [r["ooo_severity"] for r in records]
latencies = [r["latency_ms"] for r in records]
ooo_flags = [r["out_of_order"] for r in records]

# Rolling out-of-order rate (window = 10 events)
window = max(1, min(10, len(records) // 5))
ooo_arr = np.array(ooo_flags, dtype=float)
rolling_ooo = np.convolve(ooo_arr, np.ones(window) / window, mode="same")

n_total = len(records)
n_ooo = sum(ooo_flags)
n_failed = len(failure_map)
n_orphan = len(orphan_failures)

# ── plot ──────────────────────────────────────────────────────────────────────

fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
fig.suptitle(
    "Event Pipeline Health: Out-of-Order Arrival vs Failures",
    fontsize=15,
    fontweight="bold",
    y=0.98,
)

date_fmt = mdates.DateFormatter("%H:%M:%S")
colors = {
    "ok": "#4C72B0",
    "ooo": "#DD8452",
    "failed": "#C44E52",
    "rate": "#937860",
    "latency": "#55A868",
}

# ── subplot 1: OOO violation severity ────────────────────────────────────────
ax1 = axes[0]

for r in records:
    if r["failed"]:
        ax1.axvline(r["arr_ts"], color=colors["failed"], alpha=0.15, linewidth=6)

ax1.bar(
    trig_times,
    severities,
    width=0.0003,
    color=[colors["ooo"] if o else colors["ok"] for o in ooo_flags],
    alpha=0.8,
    label="OOO severity (prev_event_id − event_id for violations, 0 = in order)",
)

failed_ts = [r["arr_ts"] for r in records if r["failed"]]
failed_sev = [r["ooo_severity"] + 0.3 for r in records if r["failed"]]
ax1.scatter(
    failed_ts,
    failed_sev,
    marker="x",
    color=colors["failed"],
    s=80,
    linewidths=2,
    zorder=5,
    label="Failed event",
)

ax1.set_ylabel("event_id jump (violation magnitude)")
ax1.set_title(
    "Out-of-Order Violation Severity per Event  (per-device, in arrival order)"
)
ax1.legend(fontsize=9)
ax1.grid(True, linestyle="--", alpha=0.3)

# ── subplot 2: rolling OOO rate + failure events ──────────────────────────────
ax2 = axes[1]

ax2.plot(
    trig_times,
    rolling_ooo * 100,
    color=colors["rate"],
    linewidth=2,
    label=f"Rolling OOO rate (window={window})",
)
ax2.fill_between(trig_times, rolling_ooo * 100, alpha=0.15, color=colors["rate"])

# Vertical lines for failures (including orphans)
first_fail = True
for r in records:
    if r["failed"]:
        label = "Failure" if first_fail else None
        ax2.axvline(
            r["arr_ts"],
            color=colors["failed"],
            linestyle="--",
            linewidth=1.2,
            alpha=0.7,
            label=label,
        )
        first_fail = False

for fat, _ in orphan_failures:
    ax2.axvline(fat, color=colors["failed"], linestyle=":", linewidth=1, alpha=0.5)

ax2.set_ylabel("OOO rate (%)")
ax2.set_title("Rolling Out-of-Order Rate with Failure Markers")
ax2.set_ylim(0, 105)
ax2.legend(fontsize=9)
ax2.grid(True, linestyle="--", alpha=0.3)

# ── subplot 3: latency coloured by OOO / failed ───────────────────────────────
ax3 = axes[2]


def point_color(r):
    if r["failed"]:
        return colors["failed"]
    if r["out_of_order"]:
        return colors["ooo"]
    return colors["ok"]


point_colors = [point_color(r) for r in records]

ax3.scatter(trig_times, latencies, c=point_colors, s=20, alpha=0.75, zorder=3)
ax3.set_ylabel("Latency (ms)")
ax3.set_xlabel("Arrival Time")
ax3.set_title("Latency per Event  (colour = in-order / out-of-order / failed)")
ax3.grid(True, linestyle="--", alpha=0.3)

legend_handles = [
    mpatches.Patch(color=colors["ok"], label="In order"),
    mpatches.Patch(color=colors["ooo"], label="Out of order"),
    mpatches.Patch(color=colors["failed"], label="Failed"),
]
ax3.legend(handles=legend_handles, fontsize=9)
ax3.xaxis.set_major_formatter(date_fmt)
fig.autofmt_xdate(rotation=30)

# ── summary footer ────────────────────────────────────────────────────────────
summary = (
    f"Total arrived: {n_total}  |  "
    f"Out-of-order: {n_ooo} ({100 * n_ooo / max(n_total, 1):.1f}%)  |  "
    f"Failures: {n_failed}  |  "
    f"Orphan failures (never arrived): {n_orphan}"
)
fig.text(0.5, 0.01, summary, ha="center", fontsize=10, color="gray")

plt.tight_layout(rect=(0, 0.03, 1, 0.97))
plt.savefig(out_file, dpi=150, bbox_inches="tight")
print(f"Saved {out_file}")
