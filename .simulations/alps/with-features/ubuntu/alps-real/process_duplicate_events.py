#!/usr/bin/env python3
"""
Count how many "Duplicate event" lines appear in each stdout log.

Reads every logs/stdout-run-<n>.log, e.g.
    logs/stdout-run-1.log
    logs/stdout-run-60.log

Usage:
    python process_duplicate_events.py [DIR] [--pattern TEXT] [--csv out.csv]

DIR is either the folder that contains the logs/ folder, or the logs/ folder itself.
"""

import argparse
import csv
import glob
import os
import re
import statistics
import sys

FILE_RE = re.compile(r"^stdout-run-(?P<run>\d+)\.log$")


def find_logs_dir(base: str) -> str:
    sub = os.path.join(base, "logs")
    if os.path.isdir(sub):
        return sub
    return base


def find_logs(logs_dir: str):
    logs = []
    for path in glob.glob(os.path.join(logs_dir, "stdout-run-*.log")):
        m = FILE_RE.match(os.path.basename(path))
        if m:
            logs.append((int(m.group("run")), path))
    return sorted(logs)


def count_in_file(path: str, pattern: str) -> int:
    n = 0
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            n += line.count(pattern)
    return n


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("dir", nargs="?", default=".")
    ap.add_argument(
        "--pattern",
        default="Duplicate event",
        help='text to count (default: "Duplicate event")',
    )
    ap.add_argument("--csv", help="write one row per run to this CSV file")
    args = ap.parse_args()

    logs_dir = find_logs_dir(args.dir)
    logs = find_logs(logs_dir)
    if not logs:
        sys.exit(f"No stdout-run-*.log files found in {os.path.abspath(logs_dir)}")

    rows = []
    print(f"{'run':>5} {'duplicates':>11}")
    for run, path in logs:
        c = count_in_file(path, args.pattern)
        rows.append({"run": run, "duplicates": c})
        print(f"{run:>5} {c:>11}")

    counts = [r["duplicates"] for r in rows]
    n = len(counts)
    with_dup = sum(1 for c in counts if c > 0)
    total = sum(counts)

    print()
    print(f"Runs processed:        {n}")
    print(f"Runs with duplicates:  {with_dup}/{n} ({with_dup / n * 100:.1f}%)")
    print(f"Total duplicates:      {total}")
    print(f"Mean per run:          {total / n:.2f}")
    print(f"Median per run:        {statistics.median(counts):.1f}")
    print(f"Min / max per run:     {min(counts)} / {max(counts)}")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["run", "duplicates"])
            w.writeheader()
            w.writerows(rows)
        print(f"\nPer-run counts written to {args.csv}")


if __name__ == "__main__":
    main()


# python3 process_duplicate_events.py                                # from alps-real/
# python3 process_duplicate_events.py . --csv duplicate-events.csv
