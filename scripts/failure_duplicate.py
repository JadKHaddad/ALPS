"""
Usage: python failure_duplicate.py <device_events.json> <server_events.json> <failures.json> <output.png>

Plots a timeline showing:
  - Per-event latency coloured by status (unique / duplicated / failed)

Duplicate definition:
  Two or more server records with the same (device_id, event_id). The first
  arrival is treated as unique; every later arrival for the same key is treated
  as duplicated.
"""

import json
import sys
from collections import defaultdict
from datetime import datetime

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

if len(sys.argv) < 5:
    print(
        "Usage: python failure_duplicate.py <device_events.json> <server_events.json> <failures.json> <output.png>"
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


# ── detect duplicate arrivals ────────────────────────────────────────────────

arrived_events = [e for e in server_events if e.get("arrived_at")]
arrived_events.sort(key=lambda e: parse_ts(e["arrived_at"]) or datetime.min)

seen_counts: dict[tuple, int] = defaultdict(int)

records = []
for e in arrived_events:
    eid = e["event_id"]
    key = (e["device_id"], eid)
    seen_counts[key] += 1

    trig_ts = parse_ts(e["original"]["triggertime"])
    arr_ts = parse_ts(e["arrived_at"])
    latency = (arr_ts - trig_ts).total_seconds() * 1000  # type: ignore[operator]
    duplicated = seen_counts[key] > 1
    failed = eid in failure_map

    records.append(
        {
            "event_id": eid,
            "device_id": e["device_id"],
            "trig_ts": trig_ts,
            "arr_ts": arr_ts,
            "latency_ms": latency,
            "duplicated": duplicated,
            "failed": failed,
            "failed_at": failure_map.get(eid),
        }
    )


# ── derived series ────────────────────────────────────────────────────────────

arr_times = [r["arr_ts"] for r in records]
latencies = [r["latency_ms"] for r in records]

# Normalize x-axis: seconds elapsed since first arrival
if arr_times:
    t0 = min(arr_times)
    elapsed_seconds = [(t - t0).total_seconds() for t in arr_times]
else:
    elapsed_seconds = []

n_total = len(records)
n_duplicated = sum(r["duplicated"] for r in records)
n_unique = n_total - n_duplicated
n_failed = len(failure_map)

# Events the device sent that never appeared in server events at all
server_keys = {(e["device_id"], e["event_id"]) for e in server_events}
n_never_arrived = sum(
    1 for e in device_events if (e["device_id"], e["event_id"]) not in server_keys
)


# ── plot ──────────────────────────────────────────────────────────────────────

colors = {
    "unique": "#4C72B0",
    "duplicated": "#F5C518",
    "failed": "#C44E52",
}

fig, ax = plt.subplots(figsize=(14, 5))


def point_color(r):
    if r["failed"]:
        return colors["failed"]
    if r["duplicated"]:
        return colors["duplicated"]
    return colors["unique"]


point_colors = [point_color(r) for r in records]

ax.scatter(elapsed_seconds, latencies, c=point_colors, s=20, alpha=0.75, zorder=3)
ax.set_ylabel("Latenz (ms)")
ax.set_xlabel("Ankunftszeit (Sekunden seit Beginn)")
ax.grid(True, linestyle="--", alpha=0.3)

legend_handles = [
    mpatches.Patch(color=colors["unique"], label="Eindeutig"),
    mpatches.Patch(color=colors["duplicated"], label="Dupliziert"),
    mpatches.Patch(color=colors["failed"], label="Fehlgeschlagen"),
]
ax.legend(handles=legend_handles, fontsize=9)

# ── summary footer ────────────────────────────────────────────────────────────
summary = (
    f"Gesamt angekommen: {n_total}  |  "
    f"Eindeutig: {n_unique}  |  "
    f"Dupliziert: {n_duplicated} ({100 * n_duplicated / max(n_total, 1):.1f}%)  |  "
    f"Nie angekommen: {n_never_arrived}  |  "
    f"Fehlgeschlagen: {n_failed}"
)
fig.text(0.5, 0.01, summary, ha="center", fontsize=10, color="gray")

plt.tight_layout(rect=(0, 0.05, 1, 1))
plt.savefig(out_file, dpi=150, bbox_inches="tight")
print(f"Saved: {out_file}")
