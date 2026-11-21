#!/usr/bin/env python3
"""
Count accepted, dropped and refused connections per TCP proxy and per consumer index i.

Reads every mount/*-proxy-<i>-results.json, e.g.
    consumer-amqp-proxy-0-results.json
    consumer-server-proxy-1-results.json
    server-tcp-proxy-2-results.json

Each file is a list of objects with exactly one key: accepted_at, dropped_at or refused_at.

Percentages:
    refused %  = refused / (accepted + refused)   share of connection attempts the proxy refused
    dropped %  = dropped / accepted                share of accepted connections the proxy cut
    closed %   = (accepted - dropped) / accepted   share of accepted connections closed normally

Usage:
    python process_proxy_results.py [DIR] [--per-run] [--csv out.csv]

DIR is either a folder containing simulation-<i> folders, or a single simulation-<i> folder.
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict

FILE_RE = re.compile(r"^(?P<proxy>.+-proxy)-(?P<i>\d+)-results\.json$")
EVENTS = ["accepted", "dropped", "refused"]
PCTS = ["refused %", "dropped %", "closed %"]


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
    c = Counter()
    for e in entries:
        for key in e:
            c[key[:-3] if key.endswith("_at") else key] += 1
    return c


def percentages(c: Counter) -> dict:
    attempts = c["accepted"] + c["refused"]
    acc = c["accepted"]
    return {
        "refused %": c["refused"] / attempts * 100 if attempts else 0.0,
        "dropped %": c["dropped"] / acc * 100 if acc else 0.0,
        "closed %": (acc - c["dropped"]) / acc * 100 if acc else 0.0,
    }


def fmt_row(label, c: Counter, width=34):
    others = {k: v for k, v in c.items() if k not in EVENTS}
    extra = (
        ("  other: " + ", ".join(f"{k}={v}" for k, v in sorted(others.items())))
        if others
        else ""
    )
    p = percentages(c)
    return (
        f"{label:<{width}} "
        + " ".join(f"{c[e]:>9}" for e in EVENTS)
        + " "
        + " ".join(f"{p[k]:>9.1f}%" for k in PCTS)
        + extra
    )


def header(title, width=34):
    return (
        f"{title:<{width}} "
        + " ".join(f"{e:>9}" for e in EVENTS)
        + " "
        + " ".join(f"{k:>10}" for k in PCTS)
    )


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("dir", nargs="?", default=".")
    ap.add_argument(
        "--per-run",
        action="store_true",
        help="also print the counts of every single run",
    )
    ap.add_argument("--csv", help="write one row per run, proxy and i to this CSV file")
    args = ap.parse_args()

    runs = find_runs(args.dir)
    if not runs:
        sys.exit(f"No simulation folders found in {os.path.abspath(args.dir)}")

    by_proxy_i = defaultdict(Counter)  # (proxy, i) -> counts over all runs
    by_proxy = defaultdict(Counter)  # proxy -> counts over all runs and i
    by_i = defaultdict(Counter)  # i -> counts over all runs and proxies
    runs_with_drop = defaultdict(int)  # (proxy, i) -> number of runs with >= 1 drop
    csv_rows = []

    for run in runs:
        files = sorted(glob.glob(os.path.join(run, "mount", "*-proxy-*-results.json")))
        run_counts = {}
        for f in files:
            m = FILE_RE.match(os.path.basename(f))
            if not m:
                continue
            proxy, i = m.group("proxy"), int(m.group("i"))
            c = count_file(f)
            run_counts[(proxy, i)] = c
            by_proxy_i[(proxy, i)] += c
            by_proxy[proxy] += c
            by_i[i] += c
            if c["dropped"] > 0:
                runs_with_drop[(proxy, i)] += 1
            p = percentages(c)
            csv_rows.append(
                {
                    "run": run_number(run),
                    "proxy": proxy,
                    "i": i,
                    **{e: c[e] for e in EVENTS},
                    "refused_pct": round(p["refused %"], 2),
                    "dropped_pct": round(p["dropped %"], 2),
                    "closed_pct": round(p["closed %"], 2),
                }
            )

        if args.per_run:
            print(header(f"run {run_number(run)}"))
            for (proxy, i), c in sorted(run_counts.items()):
                print(fmt_row(f"  {proxy}-{i}", c))
            print()

    n = len(runs)
    print(f"Runs processed: {n}\n")

    print(header("Per proxy and i (sum over runs)") + f" {'runs w/ drop':>13}")
    for (proxy, i), c in sorted(by_proxy_i.items()):
        print(fmt_row(f"  {proxy}-{i}", c) + f" {runs_with_drop[(proxy, i)]:>8}/{n}")
    print()

    print(header("Per proxy type (sum over runs and i)"))
    for proxy, c in sorted(by_proxy.items()):
        print(fmt_row(f"  {proxy}", c))
    print()

    print(header("Per consumer i (sum over runs and proxies)"))
    for i, c in sorted(by_i.items()):
        print(fmt_row(f"  i = {i}", c))
    print()

    if n > 1:
        print(
            f"{'Per proxy type, mean per run':<34} "
            + " ".join(f"{e:>9}" for e in EVENTS)
        )
        for proxy, c in sorted(by_proxy.items()):
            print(f"  {proxy:<32} " + " ".join(f"{c[e] / n:>9.1f}" for e in EVENTS))
        print()

    print("  refused % = refused / (accepted + refused)")
    print("  dropped % = dropped / accepted")
    print("  closed %  = (accepted - dropped) / accepted   (closed normally)")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
            w.writeheader()
            w.writerows(csv_rows)
        print(f"\nPer-run counts written to {args.csv}")


if __name__ == "__main__":
    main()


# python3 process_proxy_results.py simulation-1               # one run
# python3 process_proxy_results.py . --per-run --csv proxy-counts.csv   # all runs
