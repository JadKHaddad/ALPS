"""
check.py

Checks the Phase-1 "qualifying condition" for the ALPS Exactly-Once-Delivery
experiment (see sub-sub:exp-alps-phase-1):

    "Die TCP-Verbindung zwischen dem Broker und ALP2 wird getrennt, nachdem
    ALP2 die Alarmmeldung an SRV2 weitergeleitet hat und auf dessen Antwort
    wartet."

For every simulation-<n> run, this compares each consumer's event-status log
(consumer-<i>-results.json) against the matching AMQP-proxy drop log
(consumer-amqp-proxy-<i>-results.json). A run "qualifies" (i.e. is not
excluded from the 59-run evaluation) if, for at least one consumer index i
and at least one alarm event, a `dropped_at` timestamp in the proxy log
falls inside the "waiting window":

    [ time of "ReceivedAndSentAndWaitingForResponse",
      time of the NEXT status entry for that same event (or +inf if none) )

Usage:
    python3 check.py /path/to/root_folder
    python3 check.py /path/to/root_folder --out report.csv -v
    python3 check.py /path/to/root_folder --max-index 58

Notes / extension points:
  * This only implements the *qualifying* condition. Whether a qualifying
    run then PASSES Phase 1 additionally requires N_loss == 0 and
    N_dup == 0, which would need to be read from device-results.json /
    duplicate-events.json (not covered here, but plugged in easily -
    see `evaluate_pass_fail_placeholder`).
  * Logic is deliberately generic (keys off the NEXT log entry for the
    same event rather than hardcoding "SuccessBeforeAck") so it also works
    unchanged for Phase 3, which has multiple consumer/proxy index pairs
    (0, 1, 2, ...) per run.
  * --max-index limits the scan to simulation-<n> directories whose
    numeric index n is <= the given value, so you can check a subset
    (e.g. the first 59 runs) without touching every simulation-<n> dir
    present under root.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------
# Timestamps look like "2026-09-18T17:36:10.046870401Z" - nanosecond
# precision with a trailing "Z". Python's datetime only supports up to
# microsecond precision, so we truncate the fractional part to 6 digits
# before parsing.
_TS_RE = re.compile(
    r"^(?P<base>.+T\d{2}:\d{2}:\d{2})(?:\.(?P<frac>\d+))?(?P<tz>Z|[+-]\d{2}:\d{2})$"
)


def parse_ts(ts: str) -> datetime:
    m = _TS_RE.match(ts.strip())
    if not m:
        raise ValueError(f"Unrecognized timestamp format: {ts!r}")
    base = m.group("base")
    frac = m.group("frac") or ""
    tz = m.group("tz")
    frac = (frac + "000000")[:6]  # pad/truncate to microseconds
    tz_str = "+00:00" if tz == "Z" else tz
    return datetime.fromisoformat(f"{base}.{frac}{tz_str}")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class WaitingWindow:
    device_id: str
    event_id: int
    start: datetime
    end: datetime | None  # None => open-ended (still waiting at end of log)

    def contains(self, ts: datetime) -> bool:
        if ts < self.start:
            return False
        if self.end is None:
            return True
        return ts < self.end


@dataclass
class SimulationResult:
    name: str
    qualifies: bool = False
    matched_events: list = field(default_factory=list)  # list of dicts for reporting
    consumer_indices_found: list = field(default_factory=list)
    proxy_indices_found: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------
CONSUMER_RE = re.compile(r"^consumer-(\d+)-results\.json$")
PROXY_RE = re.compile(r"^consumer-amqp-proxy-(\d+)-results\.json$")


def load_json(path: Path) -> list:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_waiting_windows(consumer_entries: list) -> list[WaitingWindow]:
    """Group consumer status entries by (device_id, event_id), sort by time,
    and build a waiting window for every ReceivedAndSentAndWaitingForResponse
    entry, closing it at the next entry for that same event (or leaving it
    open if none)."""
    by_event: dict[tuple[str, int], list[dict]] = {}
    for entry in consumer_entries:
        ev = entry["event"]
        key = (ev["device_id"], ev["event_id"])
        by_event.setdefault(key, []).append(entry)

    windows: list[WaitingWindow] = []
    for (device_id, event_id), entries in by_event.items():
        entries_sorted = sorted(entries, key=lambda e: parse_ts(e["happened_at"]))
        for i, entry in enumerate(entries_sorted):
            if entry["status"] == "ReceivedAndSentAndWaitingForResponse":
                start = parse_ts(entry["happened_at"])
                end = None
                if i + 1 < len(entries_sorted):
                    end = parse_ts(entries_sorted[i + 1]["happened_at"])
                windows.append(WaitingWindow(device_id, event_id, start, end))
    return windows


def check_simulation(sim_dir: Path) -> SimulationResult:
    result = SimulationResult(name=sim_dir.name)
    mount_dir = sim_dir / "mount"
    if not mount_dir.is_dir():
        result.warnings.append(f"No 'mount' subdirectory in {sim_dir}")
        return result

    consumer_files: dict[int, Path] = {}
    proxy_files: dict[int, Path] = {}
    for p in mount_dir.iterdir():
        if not p.is_file():
            continue
        m_proxy = PROXY_RE.match(p.name)
        if m_proxy:
            proxy_files[int(m_proxy.group(1))] = p
            continue  # must check proxy pattern first: it also starts with "consumer-"
        m_consumer = CONSUMER_RE.match(p.name)
        if m_consumer:
            consumer_files[int(m_consumer.group(1))] = p

    result.consumer_indices_found = sorted(consumer_files)
    result.proxy_indices_found = sorted(proxy_files)

    common_indices = sorted(set(consumer_files) & set(proxy_files))
    missing_pairs = sorted(set(consumer_files) ^ set(proxy_files))
    if missing_pairs:
        result.warnings.append(
            f"Unmatched consumer/proxy indices (no pair): {missing_pairs}"
        )

    for idx in common_indices:
        try:
            consumer_entries = load_json(consumer_files[idx])
            proxy_entries = load_json(proxy_files[idx])
        except (json.JSONDecodeError, OSError) as e:
            result.warnings.append(f"Failed to read index {idx}: {e}")
            continue

        windows = build_waiting_windows(consumer_entries)
        drop_times = [
            parse_ts(e["dropped_at"]) for e in proxy_entries if "dropped_at" in e
        ]

        for window in windows:
            for dt in drop_times:
                if window.contains(dt):
                    result.qualifies = True
                    result.matched_events.append(
                        {
                            "consumer_index": idx,
                            "device_id": window.device_id,
                            "event_id": window.event_id,
                            "window_start": window.start.isoformat(),
                            "window_end": window.end.isoformat()
                            if window.end
                            else None,
                            "dropped_at": dt.isoformat(),
                        }
                    )
                    # Don't break: collect all matches for reporting, a run
                    # only needs >=1 to qualify but more matches are useful
                    # diagnostic info.

    return result


def find_simulation_dirs(root: Path, max_index: int | None = None) -> list[Path]:
    """Return simulation-<n> directories under root, sorted naturally.

    If max_index is given, only directories whose numeric index n is
    <= max_index are included (e.g. max_index=58 keeps simulation-0 ..
    simulation-58, and skips simulation-59, simulation-60, ...).
    """
    dirs = []
    for p in sorted(root.iterdir(), key=lambda x: _natural_key(x.name)):
        m = re.match(r"^simulation-(\d+)$", p.name)
        if not (p.is_dir() and m):
            continue
        if max_index is not None and int(m.group(1)) > max_index:
            continue
        dirs.append(p)
    return dirs


def _natural_key(s: str):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", s)]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "root", type=Path, help="Folder containing the simulation-<n> directories"
    )
    parser.add_argument(
        "--out", type=Path, default=None, help="Optional CSV report path"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Print per-event matches"
    )
    parser.add_argument(
        "--target",
        type=int,
        default=59,
        help="Number of qualifying runs needed (Success-Run-Theorem n; default: 59)",
    )
    parser.add_argument(
        "--max-index",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Only scan simulation-<n> directories with n <= N "
            "(e.g. --max-index 58 scans simulation-0 .. simulation-58 and "
            "skips any higher-numbered runs found under root). "
            "Default: scan all simulation-<n> dirs found."
        ),
    )
    args = parser.parse_args()

    if not args.root.is_dir():
        print(f"Error: {args.root} is not a directory", file=sys.stderr)
        return 1

    sim_dirs = find_simulation_dirs(args.root, max_index=args.max_index)
    if not sim_dirs:
        scope = f" with index <= {args.max_index}" if args.max_index is not None else ""
        print(
            f"No simulation-<n> directories{scope} found under {args.root}",
            file=sys.stderr,
        )
        return 1

    if args.max_index is not None:
        print(f"Scanning simulation-<n> dirs with n <= {args.max_index} only.")

    results = [check_simulation(d) for d in sim_dirs]

    qualified = [r for r in results if r.qualifies]
    not_qualified = [r for r in results if not r.qualifies]
    num_qualified = len(qualified)
    still_needed = max(0, args.target - num_qualified)

    print(f"Scanned {len(results)} simulation runs.")
    print(f"  Qualifying (must be included in evaluation): {num_qualified}")
    print(f"  Not qualifying (excluded per thesis text):    {len(not_qualified)}")
    print()
    if still_needed > 0:
        print(
            f">>> {num_qualified}/{args.target} qualifying runs so far. "
            f"You still need {still_needed} more qualifying run(s)."
        )
    else:
        print(
            f">>> {num_qualified}/{args.target} qualifying runs reached. "
            f"No further runs needed."
        )

    if args.verbose and not_qualified:
        print("\nExcluded runs:")
        for r in not_qualified:
            extra = f" [{'; '.join(r.warnings)}]" if r.warnings else ""
            print(f"  - {r.name}{extra}")

    any_warnings = [r for r in results if r.warnings]
    if any_warnings:
        print("\nRuns with warnings (missing files / index mismatches):")
        for r in any_warnings:
            print(f"  - {r.name}: {'; '.join(r.warnings)}")

    if args.verbose:
        print("\nMatched events per qualifying run:")
        for r in qualified:
            print(f"  {r.name}:")
            for m in r.matched_events:
                print(
                    f"    idx={m['consumer_index']} device={m['device_id']} "
                    f"event={m['event_id']} window=[{m['window_start']}, {m['window_end']}) "
                    f"drop={m['dropped_at']}"
                )

    if args.out:
        with args.out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "simulation",
                    "qualifies",
                    "num_matched_events",
                    "consumer_indices_found",
                    "proxy_indices_found",
                    "warnings",
                ]
            )
            for r in results:
                writer.writerow(
                    [
                        r.name,
                        r.qualifies,
                        len(r.matched_events),
                        ";".join(map(str, r.consumer_indices_found)),
                        ";".join(map(str, r.proxy_indices_found)),
                        " | ".join(r.warnings),
                    ]
                )
        print(f"\nCSV report written to {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# python3 check.py . --out report.csv --target 59
# python3 check.py . --out report.csv --target 59 --max-index 99
