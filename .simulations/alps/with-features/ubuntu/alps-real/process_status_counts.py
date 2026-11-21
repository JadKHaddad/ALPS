#!/usr/bin/env python3
"""
Count Received / Success / Fail events in the ALP 2 consumer result files.

    Received = ReceivedAndSentAndWaitingForResponse (requests sent, incl. retries)
    Success  = SuccessBeforeAck
    Fail     = FailBeforeNack
All other statuses (SuccessAfterAck, FailAfterNack, ...) are ignored.

Percentages are relative to Received:
    Success % = Success / Received * 100
    Fail %    = Fail    / Received * 100

Reads mount/consumer-<i>-results.json of every simulation-<i> folder and prints
the counts per run and for all runs together.

Usage:
    python process_status_counts.py [DIR] [--per-consumer] [--csv out.csv]

DIR is either a folder containing simulation-<i> folders, or a single simulation-<i> folder.
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
from collections import Counter

# JSON status -> short name
STATUSES = {
    "ReceivedAndSentAndWaitingForResponse": "Received",
    "SuccessBeforeAck": "Success",
    "FailBeforeNack": "Fail",
}
CONSUMER_FILE_RE = re.compile(r"^consumer-(?P<i>\d+)-results\.json$")


def run_number(path: str) -> int:
    m = re.search(r"simulation-(\d+)$", os.path.normpath(path))
    return int(m.group(1)) if m else 0


def find_runs(base: str):
    if os.path.isdir(os.path.join(base, "mount")):
        return [base]
    runs = [
        d
        for d in glob.glob(os.path.join(base, "simulation-*"))
        if os.path.isdir(os.path.join(d, "mount"))
    ]
    return sorted(runs, key=run_number)


def count_file(path: str) -> Counter:
    with open(path, "r", encoding="utf-8") as f:
        entries = json.load(f)
    cnt = Counter()
    for e in entries:
        name = STATUSES.get(e.get("status"))
        if name:
            cnt[name] += 1
    return cnt


def pct(part: int, whole: int) -> float:
    return part / whole * 100 if whole else 0.0


def print_table(title, rows, label_header):
    """rows: list of (label, Counter). A TOTAL row is appended."""
    width = max([len(label_header), len("TOTAL")] + [len(r[0]) for r in rows])
    header = (
        f"{label_header:<{width}}  {'Received':>9}  {'Success':>9}  {'Fail':>9}  "
        f"{'Success %':>10}  {'Fail %':>8}"
    )

    def line(label, c):
        return (
            f"{label:<{width}}  {c['Received']:>9}  {c['Success']:>9}  {c['Fail']:>9}  "
            f"{pct(c['Success'], c['Received']):>9.1f}%  {pct(c['Fail'], c['Received']):>7.1f}%"
        )

    total = Counter()
    print(title)
    print(header)
    for label, c in rows:
        total.update(c)
        print(line(label, c))
    print("-" * len(header))
    print(line("TOTAL", total))
    print()


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("dir", nargs="?", default=".")
    ap.add_argument(
        "--per-consumer",
        action="store_true",
        help="also print the counts of every consumer within each run",
    )
    ap.add_argument("--csv", help="write one row per run and consumer to this CSV file")
    args = ap.parse_args()

    runs = find_runs(args.dir)
    if not runs:
        sys.exit(f"No simulation folders found in {os.path.abspath(args.dir)}")

    per_run = []  # (run number, Counter)
    per_consumer = []  # (run number, consumer i, Counter)
    for run in runs:
        run_cnt = Counter()
        files = glob.glob(os.path.join(run, "mount", "consumer-*-results.json"))
        for f in files:
            m = CONSUMER_FILE_RE.match(os.path.basename(f))
            if not m:
                continue
            cnt = count_file(f)
            per_consumer.append((run_number(run), int(m.group("i")), cnt))
            run_cnt.update(cnt)
        if not files:
            print(f"warning: no consumer results in {run}", file=sys.stderr)
        per_run.append((run_number(run), run_cnt))

    print(f"Runs processed: {len(runs)}\n")
    print_table("Per run:", [(f"run {n}", c) for n, c in per_run], "run")

    per_consumer.sort(key=lambda x: (x[0], x[1]))
    if args.per_consumer:
        print_table(
            "Per run and consumer:",
            [(f"run {n} consumer {i}", c) for n, i, c in per_consumer],
            "run / consumer",
        )

    print(
        "  Success % = Success / Received, Fail % = Fail / Received "
        "(Received counts every request sent, incl. retries)"
    )

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "run",
                    "consumer",
                    "received",
                    "success",
                    "fail",
                    "success_pct",
                    "fail_pct",
                ]
            )
            for n, i, c in per_consumer:
                w.writerow(
                    [
                        n,
                        i,
                        c["Received"],
                        c["Success"],
                        c["Fail"],
                        f"{pct(c['Success'], c['Received']):.2f}",
                        f"{pct(c['Fail'], c['Received']):.2f}",
                    ]
                )
        print(f"\nPer-run/consumer counts written to {args.csv}")


if __name__ == "__main__":
    main()

# python3 process_status_counts.py . --per-consumer --csv status-counts.csv
