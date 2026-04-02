import json
import numpy as np
from datetime import datetime


def parse_ts(ts):
    ts = ts.rstrip("Z")
    if "." in ts:
        base, frac = ts.split(".")
        frac = frac[:6]
        ts = f"{base}.{frac}"
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
