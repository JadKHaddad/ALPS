#!/usr/bin/env python3
"""
Compute ALP 2 response times (time ALP 2 waits for SRV 2) from consumer-*-results.json.

A response time is the difference between
    ReceivedAndSentAndWaitingForResponse  ->  SuccessBeforeAck
or
    ReceivedAndSentAndWaitingForResponse  ->  FailBeforeNack
for the same event in the same queue of the same consumer file.

Usage:
    python process_response_latency.py [BASE_DIR] [--percentile 80] [--csv out.csv]

BASE_DIR defaults to the current directory and must contain the simulation-<i> folders.
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

WAITING = "ReceivedAndSentAndWaitingForResponse"
RESPONSE_STATUSES = {"SuccessBeforeAck", "FailBeforeNack"}


def parse_ns(ts: str) -> int:
    """Parse an RFC3339 timestamp with up to nanosecond precision into integer nanoseconds."""
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1]
    elif ts.endswith("+00:00"):
        ts = ts[:-6]
    else:
        raise ValueError(f"Unsupported timezone in timestamp: {ts}")
    main, _, frac = ts.partition(".")
    dt = datetime.strptime(main, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    frac_ns = int((frac + "000000000")[:9]) if frac else 0
    return int(dt.timestamp()) * 1_000_000_000 + frac_ns


def percentile(sorted_values, p: float) -> float:
    """Linear-interpolation percentile (same as numpy's default)."""
    if not sorted_values:
        return float("nan")
    k = (len(sorted_values) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


def event_key(entry: dict):
    """Identify an event like ALP-Dedup does: (device_id, triggertime), scoped by queue."""
    ev = entry["event"]
    triggertime = (ev.get("original") or {}).get("triggertime")
    return (entry.get("queue"), ev.get("device_id"), triggertime)


def process_consumer_file(path: str):
    """Return (durations_ms, n_success, n_fail, n_unmatched_waiting, n_orphan_responses)."""
    with open(path, "r", encoding="utf-8") as f:
        entries = json.load(f)

    # Sort by time so pairing is correct even if the file is not strictly ordered.
    entries.sort(key=lambda e: parse_ns(e["happened_at"]))

    pending = {}  # event_key -> start ns
    durations_ms = []
    n_success = n_fail = n_orphan = 0
    n_unmatched = 0

    for e in entries:
        status = e["status"]
        key = event_key(e)
        t = parse_ns(e["happened_at"])

        if status == WAITING:
            if key in pending:
                # A new send without a response to the previous one (e.g. connection dropped).
                n_unmatched += 1
            pending[key] = t
        elif status in RESPONSE_STATUSES:
            start = pending.pop(key, None)
            if start is None:
                n_orphan += 1
                continue
            durations_ms.append((t - start) / 1e6)
            if status == "SuccessBeforeAck":
                n_success += 1
            else:
                n_fail += 1
        # SuccessAfterAck / FailAfterNack are ignored.

    n_unmatched += len(pending)
    return durations_ms, n_success, n_fail, n_unmatched, n_orphan


def run_number(path: str) -> int:
    m = re.search(r"simulation-(\d+)$", path.rstrip("/"))
    return int(m.group(1)) if m else -1


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("base_dir", nargs="?", default=".")
    parser.add_argument("--percentile", type=float, default=80.0)
    parser.add_argument(
        "--csv", help="Optional path to write per-run statistics as CSV"
    )
    args = parser.parse_args()
    p = args.percentile

    run_dirs = sorted(
        (
            d
            for d in glob.glob(os.path.join(args.base_dir, "simulation-*"))
            if os.path.isdir(d) and run_number(d) >= 0
        ),
        key=run_number,
    )
    if not run_dirs:
        sys.exit(f"No simulation-<i> folders found in {os.path.abspath(args.base_dir)}")

    all_durations = []
    rows = []
    totals = {"success": 0, "fail": 0, "unmatched": 0, "orphan": 0}

    for run_dir in run_dirs:
        files = sorted(
            glob.glob(os.path.join(run_dir, "mount", "consumer-*-results.json"))
        )
        # Exclude proxy result files such as consumer-amqp-proxy-0-results.json.
        files = [f for f in files if re.search(r"consumer-\d+-results\.json$", f)]
        if not files:
            print(f"warning: no consumer results in {run_dir}", file=sys.stderr)
            continue

        run_durations = []
        s = fl = um = orph = 0
        for f in files:
            d, a, b, c, o = process_consumer_file(f)
            run_durations.extend(d)
            s += a
            fl += b
            um += c
            orph += o

        run_durations.sort()
        all_durations.extend(run_durations)
        totals["success"] += s
        totals["fail"] += fl
        totals["unmatched"] += um
        totals["orphan"] += orph

        rows.append(
            {
                "run": run_number(run_dir),
                "n": len(run_durations),
                "success": s,
                "fail": fl,
                "unmatched_waiting": um,
                "orphan_responses": orph,
                "mean_ms": sum(run_durations) / len(run_durations)
                if run_durations
                else float("nan"),
                "p50_ms": percentile(run_durations, 50),
                f"p{p:g}_ms": percentile(run_durations, p),
                "p99_ms": percentile(run_durations, 99),
                "max_ms": run_durations[-1] if run_durations else float("nan"),
            }
        )

    # Per-run table
    pk = f"p{p:g}_ms"
    print(
        f"{'run':>4} {'n':>6} {'fail':>5} {'unm':>4} {'mean':>9} {'p50':>9} {pk[:-3]:>9} {'p99':>9} {'max':>9}   (ms)"
    )
    for r in rows:
        print(
            f"{r['run']:>4} {r['n']:>6} {r['fail']:>5} {r['unmatched_waiting']:>4} "
            f"{r['mean_ms']:>9.2f} {r['p50_ms']:>9.2f} {r[pk]:>9.2f} {r['p99_ms']:>9.2f} {r['max_ms']:>9.2f}"
        )

    # Overall
    all_durations.sort()
    per_run_p = sorted(r[pk] for r in rows if r["n"])
    print()
    print(
        f"Runs processed:            {len(rows)}"
        + ("  (note: expected 59)" if len(rows) != 59 else "")
    )
    print(
        f"Response times total:      {len(all_durations)}  "
        f"(success {totals['success']}, fail {totals['fail']}, "
        f"unmatched waiting {totals['unmatched']}, orphan responses {totals['orphan']})"
    )
    if all_durations:
        print(
            f"Pooled mean:               {sum(all_durations) / len(all_durations):.3f} ms"
        )
        print(f"Pooled p50:                {percentile(all_durations, 50):.3f} ms")
        print(
            f"Pooled p{p:g}:                {percentile(all_durations, p):.3f} ms   <- candidate value"
        )
        print(f"Pooled p99:                {percentile(all_durations, 99):.3f} ms")
        print(
            f"Per-run p{p:g}: min {per_run_p[0]:.3f} / median {percentile(per_run_p, 50):.3f} "
            f"/ max {per_run_p[-1]:.3f} ms"
        )

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"Per-run statistics written to {args.csv}")


if __name__ == "__main__":
    main()

# python3 process_response_latency.py . --csv response-times.csv
