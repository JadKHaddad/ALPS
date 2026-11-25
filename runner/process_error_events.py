#!/usr/bin/env python3
"""
Count how often TimedOut, ConnectionReset and IncompleteMessage appear in each stdout log.

Reads every logs/stdout-run-<n>.log, e.g.
    logs/stdout-run-1.log
    logs/stdout-run-60.log

Percentages:
    per run / overall table: share of each pattern among all matches of that row
    runs with hit:           runs with at least one match of the pattern / runs processed

Usage:
    python process_error_events.py [DIR] [--pattern TEXT ...] [--csv out.csv]

DIR is either the folder that contains the logs/ folder, or the logs/ folder itself.
--pattern can be given several times to replace the default patterns.
"""

import argparse
import csv
import glob
import os
import re
import statistics
import sys

DEFAULT_PATTERNS = ["TimedOut", "ConnectionReset", "IncompleteMessage"]
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


def count_in_file(path: str, patterns) -> dict:
    counts = {p: 0 for p in patterns}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            for p in patterns:
                counts[p] += line.count(p)
    return counts


def pct(part: int, whole: int) -> float:
    return part / whole * 100 if whole else 0.0


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("dir", nargs="?", default=".")
    ap.add_argument(
        "--pattern",
        action="append",
        help=f"text to count, repeatable (default: {', '.join(DEFAULT_PATTERNS)})",
    )
    ap.add_argument("--csv", help="write one row per run to this CSV file")
    args = ap.parse_args()
    patterns = args.pattern or DEFAULT_PATTERNS

    logs_dir = find_logs_dir(args.dir)
    logs = find_logs(logs_dir)
    if not logs:
        sys.exit(f"No stdout-run-*.log files found in {os.path.abspath(logs_dir)}")

    w = max(len(p) for p in patterns) + 8  # room for "count (xx.x%)"

    def cell(count, total):
        return f"{count} ({pct(count, total):.1f}%)"

    rows = []
    header = f"{'run':>5} " + " ".join(f"{p:>{w}}" for p in patterns) + f" {'total':>8}"
    print(header)
    for run, path in logs:
        c = count_in_file(path, patterns)
        total = sum(c.values())
        rows.append({"run": run, **c, "total": total})
        print(
            f"{run:>5} "
            + " ".join(f"{cell(c[p], total):>{w}}" for p in patterns)
            + f" {total:>8}"
        )

    n = len(rows)
    sums = {p: sum(r[p] for r in rows) for p in patterns}
    grand = sum(sums.values())

    print("-" * len(header))
    print(
        f"{'ALL':>5} "
        + " ".join(f"{cell(sums[p], grand):>{w}}" for p in patterns)
        + f" {grand:>8}"
    )

    print(f"\nRuns processed: {n}\n")
    print(
        f"{'pattern':<20} {'total':>8} {'share':>8} {'runs w/ hit':>16} "
        f"{'mean/run':>9} {'median':>7} {'min':>5} {'max':>5}"
    )
    for p in patterns:
        vals = [r[p] for r in rows]
        hit = sum(1 for v in vals if v > 0)
        print(
            f"{p:<20} {sums[p]:>8} {pct(sums[p], grand):>7.1f}% "
            f"{f'{hit}/{n} ({pct(hit, n):.1f}%)':>16} "
            f"{sums[p] / n:>9.2f} {statistics.median(vals):>7.1f} {min(vals):>5} {max(vals):>5}"
        )
    any_hit = sum(1 for r in rows if r["total"] > 0)
    print(f"\nRuns with at least one match: {any_hit}/{n} ({pct(any_hit, n):.1f}%)")

    if args.csv:
        fields = ["run"] + patterns + ["total"] + [f"{p}_pct" for p in patterns]
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            wr = csv.DictWriter(f, fieldnames=fields)
            wr.writeheader()
            for r in rows:
                out = dict(r)
                for p in patterns:
                    out[f"{p}_pct"] = f"{pct(r[p], r['total']):.2f}"
                wr.writerow(out)
        print(f"\nPer-run counts written to {args.csv}")


if __name__ == "__main__":
    main()


# python3 process_error_events.py                                # from alps-real/
# python3 process_error_events.py . --csv error-events.csv
