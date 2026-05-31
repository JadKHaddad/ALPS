"""
Usage: python failure_order.py <device_events.json> <server_events.json> <failures.json> <output.png>

Plots a timeline showing:
  - Per-event latency coloured by status (in-order / out-of-order / failed)

"Out of order" mirrors validate_order() in the Rust server:
  Events are walked in arrival order. Per device_id, we track the last seen event_id.
  A violation occurs when event.event_id < last_seen_event_id for that device.
  The severity is (last_seen_event_id - event.event_id): how far back the event_id jumped.
"""

import json
import sys
import numpy as np
import matplotlib.pyplot as plt
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

arr_times = [
    r["arr_ts"] for r in records
]  # x-axis = arrival time (matches detection order)
latencies = [r["latency_ms"] for r in records]

# Normalize x-axis: seconds elapsed since first arrival
if arr_times:
    t0 = min(arr_times)
    elapsed_seconds = [(t - t0).total_seconds() for t in arr_times]
else:
    elapsed_seconds = []

n_total = len(records)
n_ooo = sum(r["out_of_order"] for r in records)
n_failed = len(failure_map)
n_orphan = len(orphan_failures)

# Events the device sent that never appeared in server events at all
server_keys = {(e["device_id"], e["event_id"]) for e in server_events}
n_never_arrived = sum(
    1 for e in device_events if (e["device_id"], e["event_id"]) not in server_keys
)

# ── plot ──────────────────────────────────────────────────────────────────────

colors = {
    "ok": "#4C72B0",
    "ooo": "#F5C518",
    "failed": "#C44E52",
}

fig, ax = plt.subplots(figsize=(14, 5))


def point_color(r):
    if r["failed"]:
        return colors["failed"]
    if r["out_of_order"]:
        return colors["ooo"]
    return colors["ok"]


point_colors = [point_color(r) for r in records]

ax.scatter(elapsed_seconds, latencies, c=point_colors, s=20, alpha=0.75, zorder=3)
ax.set_ylabel("Latenz (ms)")
ax.set_xlabel("Ankunftszeit (Sekunden seit Beginn)")
ax.grid(True, linestyle="--", alpha=0.3)

legend_handles = [
    mpatches.Patch(color=colors["ok"], label="In Reihenfolge"),
    mpatches.Patch(color=colors["ooo"], label="Außer Reihenfolge"),
    mpatches.Patch(color=colors["failed"], label="Fehlgeschlagen"),
]
ax.legend(handles=legend_handles, fontsize=9)

# ── summary footer ────────────────────────────────────────────────────────────
summary = (
    f"Gesamt angekommen: {n_total}  |  "
    f"Nie angekommen: {n_never_arrived}  |  "
    f"Außer Reihenfolge: {n_ooo} ({100 * n_ooo / max(n_total, 1):.1f}%)  |  "
    f"Fehler: {n_failed}"
)
fig.text(0.5, 0.01, summary, ha="center", fontsize=10, color="gray")

plt.tight_layout(rect=(0, 0.05, 1, 1))
plt.savefig(out_file, dpi=150, bbox_inches="tight")
print(f"Saved: {out_file}")
