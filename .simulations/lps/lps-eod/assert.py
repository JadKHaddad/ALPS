"""
Check rates-results.json in every simulation-*/mount/ folder.

Run from the folder that contains the simulation-* directories.

Mode 1 - counts: exact match on event counts (pass any of the three):
    python assert.py counts --total-events 3 --unique-events 1 --duplicate-events 2

Mode 2 - rate: duplication_rate must be >= the given minimum:
    python assert.py rate --min-duplication-rate 0.5

Mode 3 - avg: print the average duplication_rate across all simulations:
    python assert.py avg

Exit code is 0 if every simulation passes (or, for avg, if at least one
rate could be read), 1 otherwise.
"""

import argparse
import json
import re
import sys
from pathlib import Path

COUNT_FIELDS = ["total_events", "unique_events", "duplicate_events"]


def sim_number(path: Path) -> int:
    m = re.search(r"simulation-(\d+)$", path.name)
    return int(m.group(1)) if m else -1


def check_counts(data: dict, args) -> list:
    mismatches = []
    for field in COUNT_FIELDS:
        exp = getattr(args, field)
        if exp is None:
            continue
        actual = data.get(field)
        if actual != exp:
            mismatches.append(f"{field}: expected {exp}, got {actual}")
    return mismatches


def check_rate(data: dict, args) -> list:
    actual = data.get("duplication_rate")
    if actual is None:
        return ["duplication_rate missing"]
    if actual < args.min_duplication_rate:
        return [
            f"duplication_rate: expected >= {args.min_duplication_rate}, got {actual}"
        ]
    return []


def parse_args():
    parser = argparse.ArgumentParser(
        description="Assert values in rates-results.json across simulations."
    )
    parser.add_argument(
        "--dir",
        default=".",
        help="folder containing simulation-* dirs (default: current dir)",
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    p_counts = sub.add_parser("counts", help="exact match on event counts")
    p_counts.add_argument(
        "--total-events", dest="total_events", type=int, help="expected total_events"
    )
    p_counts.add_argument(
        "--unique-events", dest="unique_events", type=int, help="expected unique_events"
    )
    p_counts.add_argument(
        "--duplicate-events",
        dest="duplicate_events",
        type=int,
        help="expected duplicate_events",
    )

    p_rate = sub.add_parser("rate", help="duplication_rate >= minimum")
    p_rate.add_argument(
        "--min-duplication-rate",
        dest="min_duplication_rate",
        type=float,
        required=True,
        help="minimum allowed duplication_rate (e.g. 0.5)",
    )

    sub.add_parser("avg", help="print the average duplication_rate")

    args = parser.parse_args()
    if args.mode == "counts" and all(getattr(args, f) is None for f in COUNT_FIELDS):
        p_counts.error(
            "pass at least one of --total-events, --unique-events, --duplicate-events"
        )
    return args


def load_rates(sim: Path):
    """Return (data, error_message). Exactly one of them is None."""
    rates_file = sim / "mount" / "rates-results.json"
    if not rates_file.is_file():
        return None, f"{rates_file} not found"
    try:
        return json.loads(rates_file.read_text()), None
    except (json.JSONDecodeError, OSError) as e:
        return None, f"could not read {rates_file}: {e}"


def run_avg(sim_dirs: list) -> int:
    rates = []
    for sim in sim_dirs:
        data, err = load_rates(sim)
        if err:
            print(f"[SKIP] {sim.name}: {err}")
            continue
        rate = data.get("duplication_rate")
        if rate is None:
            print(f"[SKIP] {sim.name}: duplication_rate missing")
            continue
        rates.append(rate)
        print(f"       {sim.name}: {rate}")

    if not rates:
        print("\nNo duplication_rate values found")
        return 1

    avg = sum(rates) / len(rates)
    print(
        f"\nAverage duplication_rate over {len(rates)}/{len(sim_dirs)} simulations: {avg}"
    )
    return 0


def main() -> int:
    args = parse_args()

    sim_dirs = sorted(
        (p for p in Path(args.dir).glob("simulation-*") if p.is_dir()),
        key=sim_number,
    )
    if not sim_dirs:
        print(f"No simulation-* directories found in {Path(args.dir).resolve()}")
        return 1

    if args.mode == "avg":
        return run_avg(sim_dirs)

    check = check_counts if args.mode == "counts" else check_rate

    failed = 0
    for sim in sim_dirs:
        data, err = load_rates(sim)
        if err:
            print(f"[FAIL] {sim.name}: {err}")
            failed += 1
            continue

        problems = check(data, args)
        if problems:
            print(f"[FAIL] {sim.name}: " + "; ".join(problems))
            failed += 1
        else:
            print(f"[ OK ] {sim.name}")

    total = len(sim_dirs)
    print(f"\n{total - failed}/{total} simulations passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

# python3 assert.py counts --total-events 3 --unique-events 1 --duplicate-events 2
# python3 assert.py rate --min-duplication-rate 0.0
# python3 assert.py avg
