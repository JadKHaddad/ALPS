#!/usr/bin/env python3
"""
Count TCP drops that happened while an ALP 2 consumer was waiting for a response from SRV 2.

Waiting window of a request (from consumer-<i>-results.json, same queue and same event):
    start = ReceivedAndSentAndWaitingForResponse
    end   = SuccessBeforeAck or FailBeforeNack
If no response was logged (e.g. the consumer lost its connection while waiting),
the window is closed at the next logged entry of the same queue in that consumer
file and is reported as "open" (no response).

Mapping between proxies and consumers (by index i):
    consumer-amqp-proxy-<i>    -> consumer-<i>   (ALP 2 <-> broker)
    consumer-server-proxy-<i>  -> consumer-<i>   (ALP 2 <-> SRV 2 PROXY)
    server-tcp-proxy-<i>       -> consumer-<i>   (SRV 2 PROXY <-> server)
Change PROXY_TO_CONSUMER below if your setup maps them differently.

A drop at time t counts as "during waiting" if start <= t <= end for at least one
waiting window of the mapped consumer. A drop of the AMQP proxy during waiting is
the critical constellation F.

All containers run on the same host, so their timestamps share one clock.

Usage:
    python process_drops_during_waiting.py [DIR] [--per-run] [--csv out.csv]

DIR is either a folder containing simulation-<i> folders, or a single simulation-<i> folder.
"""

import argparse
import bisect
import csv
import glob
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

WAITING = "ReceivedAndSentAndWaitingForResponse"
RESPONSES = {"SuccessBeforeAck", "FailBeforeNack"}
PROXY_FILE_RE = re.compile(r"^(?P<proxy>.+-proxy)-(?P<i>\d+)-results\.json$")
CONSUMER_FILE_RE = re.compile(r"^consumer-(?P<i>\d+)-results\.json$")


def PROXY_TO_CONSUMER(proxy: str, i: int) -> int:
    return i


def parse_ns(ts: str) -> int:
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1]
    elif ts.endswith("+00:00"):
        ts = ts[:-6]
    main, _, frac = ts.partition(".")
    dt = datetime.strptime(main, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1_000_000_000 + (
        int((frac + "000000000")[:9]) if frac else 0
    )


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


def load_windows(path: str):
    """Return (windows, attempts, fails).

    windows:  list of (start_ns, end_ns, has_response) waiting windows, sorted by start
    attempts: number of ReceivedAndSentAndWaitingForResponse entries (requests sent, incl. retries)
    fails:    number of FailBeforeNack entries
    """
    with open(path, "r", encoding="utf-8") as f:
        entries = json.load(f)
    for e in entries:
        e["_t"] = parse_ns(e["happened_at"])
    entries.sort(key=lambda e: e["_t"])

    queue_times = defaultdict(list)  # queue -> sorted timestamps of all its entries
    for e in entries:
        queue_times[e.get("queue")].append(e["_t"])
    last_t = entries[-1]["_t"] if entries else 0

    def next_in_queue(queue, t):
        times = queue_times[queue]
        k = bisect.bisect_right(times, t)
        return times[k] if k < len(times) else last_t

    windows = []
    pending = {}
    for e in entries:
        ev = e["event"]
        key = (
            e.get("queue"),
            ev.get("device_id"),
            (ev.get("original") or {}).get("triggertime"),
        )
        if e["status"] == WAITING:
            if key in pending:  # resent without a response to the previous send
                s = pending[key]
                windows.append((s, next_in_queue(key[0], s), False))
            pending[key] = e["_t"]
        elif e["status"] in RESPONSES:
            s = pending.pop(key, None)
            if s is not None:
                windows.append((s, e["_t"], True))
    for key, s in pending.items():
        windows.append((s, next_in_queue(key[0], s), False))

    windows.sort()
    attempts = sum(1 for e in entries if e["status"] == WAITING)
    fails = sum(1 for e in entries if e["status"] == "FailBeforeNack")
    return windows, attempts, fails


def load_drops(path: str):
    with open(path, "r", encoding="utf-8") as f:
        entries = json.load(f)
    return sorted(parse_ns(e["dropped_at"]) for e in entries if "dropped_at" in e)


class WindowIndex:
    """Find waiting windows that contain a given time."""

    def __init__(self, windows):
        self.windows = windows
        self.starts = [w[0] for w in windows]
        self.max_len = max((w[1] - w[0] for w in windows), default=0)

    def containing(self, t):
        hits = []
        k = bisect.bisect_right(self.starts, t) - 1
        while k >= 0 and self.windows[k][0] >= t - self.max_len:
            s, e, resp = self.windows[k]
            if s <= t <= e:
                hits.append(self.windows[k])
            k -= 1
        return hits


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("dir", nargs="?", default=".")
    ap.add_argument(
        "--per-run", action="store_true", help="print the counts of every run"
    )
    ap.add_argument("--csv", help="write one row per run, proxy and i to this CSV file")
    args = ap.parse_args()

    runs = find_runs(args.dir)
    if not runs:
        sys.exit(f"No simulation folders found in {os.path.abspath(args.dir)}")

    totals = defaultdict(lambda: defaultdict(int))  # proxy type -> metric -> value
    runs_with_hit = defaultdict(int)  # proxy type -> runs with >= 1 drop during waiting
    total_attempts = total_fails = 0
    csv_rows = []

    for run in runs:
        mount = os.path.join(run, "mount")
        indexes, attempts, fails = {}, {}, {}
        for f in glob.glob(os.path.join(mount, "consumer-*-results.json")):
            m = CONSUMER_FILE_RE.match(os.path.basename(f))
            if m:
                ci = int(m.group("i"))
                windows, attempts[ci], fails[ci] = load_windows(f)
                indexes[ci] = WindowIndex(windows)
        total_attempts += sum(attempts.values())
        total_fails += sum(fails.values())

        run_hit = defaultdict(int)
        rows = []
        for f in sorted(glob.glob(os.path.join(mount, "*-proxy-*-results.json"))):
            m = PROXY_FILE_RE.match(os.path.basename(f))
            if not m:
                continue
            proxy, i = m.group("proxy"), int(m.group("i"))
            idx = indexes.get(PROXY_TO_CONSUMER(proxy, i))
            if idx is None:
                print(
                    f"warning: no consumer results for {os.path.basename(f)} in {run}",
                    file=sys.stderr,
                )
                continue

            drops = load_drops(f)
            during = during_open = 0
            for t in drops:
                hits = idx.containing(t)
                if hits:
                    during += 1
                    if not any(h[2] for h in hits):
                        during_open += 1

            row = {
                "run": run_number(run),
                "proxy": proxy,
                "i": i,
                "drops": len(drops),
                "during_waiting": during,
                "during_waiting_no_response": during_open,
                "attempts": attempts[PROXY_TO_CONSUMER(proxy, i)],
            }
            rows.append(row)
            run_hit[proxy] += during

        # Attempts that reach a proxy: the server proxy only sees attempts that
        # were not already cut by the consumer-server-proxy of the same consumer.
        srv_hits = {
            r["i"]: r["during_waiting"]
            for r in rows
            if r["proxy"] == "consumer-server-proxy"
        }
        for r in rows:
            reached = r["attempts"]
            if r["proxy"] == "server-tcp-proxy":
                reached -= srv_hits.get(r["i"], 0)
            r["reached"] = reached
            r["rate_of_attempts"] = (
                r["during_waiting"] / r["attempts"] if r["attempts"] else 0.0
            )
            r["rate_of_reached"] = r["during_waiting"] / reached if reached else 0.0
            csv_rows.append(r)
            t = totals[r["proxy"]]
            for k in (
                "drops",
                "during_waiting",
                "during_waiting_no_response",
                "attempts",
                "reached",
            ):
                t[k] += r[k]

        for proxy, hits in run_hit.items():
            if hits > 0:
                runs_with_hit[proxy] += 1

        if args.per_run:
            ra, rf = sum(attempts.values()), sum(fails.values())
            print(
                f"run {run_number(run)}: attempts {ra}, fails {rf} "
                f"({rf / ra * 100 if ra else 0:.1f}%)"
            )
            for r in rows:
                print(
                    f"  {r['proxy'] + '-' + str(r['i']):<26} drops {r['drops']:>6}  "
                    f"during waiting {r['during_waiting']:>6}  (no response {r['during_waiting_no_response']})  "
                    f"of attempts {r['rate_of_attempts'] * 100:5.1f}%  of reached {r['rate_of_reached'] * 100:5.1f}%"
                )
            print()

    n = len(runs)
    print(f"Runs processed: {n}")
    print(
        f"Attempts (requests sent incl. retries): {total_attempts}, "
        f"fails: {total_fails} ({total_fails / total_attempts * 100 if total_attempts else 0:.1f}%)\n"
    )
    print(
        f"{'proxy type':<24} {'drops':>8} {'during wait':>12} {'of drops':>9} "
        f"{'of attempts':>12} {'of reached':>11} {'no resp':>8} {'runs w/ hit':>12}"
    )
    for proxy in sorted(totals):
        t = totals[proxy]
        of_drops = t["during_waiting"] / t["drops"] * 100 if t["drops"] else 0.0
        of_att = t["during_waiting"] / t["attempts"] * 100 if t["attempts"] else 0.0
        of_reached = t["during_waiting"] / t["reached"] * 100 if t["reached"] else 0.0
        print(
            f"{proxy:<24} {t['drops']:>8} {t['during_waiting']:>12} {of_drops:>8.1f}% "
            f"{of_att:>11.1f}% {of_reached:>10.1f}% {t['during_waiting_no_response']:>8} "
            f"{runs_with_hit[proxy]:>8}/{n}"
        )
    print(
        "\n  of drops    = drops during waiting / all drops of the proxy"
        "\n  of attempts = drops during waiting / all requests sent by the consumer"
        "\n  of reached  = drops during waiting / requests that reached this proxy"
        " (server-tcp-proxy: minus those already cut by consumer-server-proxy)"
    )

    amqp = [p for p in totals if "amqp" in p]
    if amqp:
        f_total = sum(totals[p]["during_waiting"] for p in amqp)
        f_runs = sum(runs_with_hit[p] for p in amqp)
        print(
            f"\nCritical constellation F (AMQP drop while waiting): {f_total} times, "
            f"in {f_runs} of {n} runs"
        )

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
            w.writeheader()
            w.writerows(csv_rows)
        print(f"\nPer-run counts written to {args.csv}")


if __name__ == "__main__":
    main()

# python3 process_drops_during_waiting.py . --per-run --csv drops-waiting.csv
