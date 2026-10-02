"""
check.py

Checks the delivery-guarantee outcome recorded in each run's
mount/rates-results.json. A run "passes" if

    duplication_rate  == 0.0
    out_of_order_rate == 0.0
    missing_rate      == 0.0

(i.e. N_dup == 0, N_ooo == 0 and N_loss == 0 for that run, matching the
pass criteria described for Phase 1 / Phase 2 / Phase 3 in the thesis).

Usage:
    python3 check.py /path/to/root_folder
    python3 check.py /path/to/root_folder --out report.csv -v
    python3 check.py /path/to/root_folder --target 59

Combine this with check.py (the Phase-1 qualifying-condition check) if you
need "qualifies AND passes" - both scripts write a CSV keyed by simulation
name, so the two reports can be merged on that column.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

RATE_KEYS = ("duplication_rate", "out_of_order_rate", "missing_rate")


@dataclass
class SimulationRates:
    name: str
    passed: bool = False
    rates: dict = field(default_factory=dict)
    counts: dict = field(default_factory=dict)
    warnings: list = field(default_factory=list)


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def check_simulation(sim_dir: Path) -> SimulationRates:
    result = SimulationRates(name=sim_dir.name)
    rates_path = sim_dir / "mount" / "rates-results.json"

    if not rates_path.is_file():
        result.warnings.append(f"Missing {rates_path}")
        return result

    try:
        data = load_json(rates_path)
    except (json.JSONDecodeError, OSError) as e:
        result.warnings.append(f"Failed to read {rates_path}: {e}")
        return result

    missing_keys = [k for k in RATE_KEYS if k not in data]
    if missing_keys:
        result.warnings.append(f"Missing key(s) in rates-results.json: {missing_keys}")
        return result

    result.rates = {k: data[k] for k in RATE_KEYS}
    result.counts = {
        k: data[k]
        for k in (
            "total_events",
            "unique_events",
            "duplicate_events",
            "out_of_order_events",
            "missing_events",
        )
        if k in data
    }
    result.passed = all(result.rates[k] == 0.0 for k in RATE_KEYS)
    return result


def find_simulation_dirs(root: Path) -> list[Path]:
    dirs = []
    for p in sorted(root.iterdir(), key=lambda x: _natural_key(x.name)):
        if p.is_dir() and re.match(r"^simulation-\d+$", p.name):
            dirs.append(p)
    return dirs


def _natural_key(s: str):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", s)]


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
        "-v", "--verbose", action="store_true", help="Print per-run rates"
    )
    parser.add_argument(
        "--target",
        type=int,
        default=59,
        help="Number of passing runs needed (Success-Run-Theorem n; default: 59)",
    )
    args = parser.parse_args()

    if not args.root.is_dir():
        print(f"Error: {args.root} is not a directory", file=sys.stderr)
        return 1

    sim_dirs = find_simulation_dirs(args.root)
    if not sim_dirs:
        print(f"No simulation-<n> directories found under {args.root}", file=sys.stderr)
        return 1

    results = [check_simulation(d) for d in sim_dirs]

    passed = [r for r in results if r.passed]
    failed = [r for r in results if not r.passed and not r.warnings]
    broken = [r for r in results if r.warnings]
    num_passed = len(passed)
    still_needed = max(0, args.target - num_passed)

    print(f"Scanned {len(results)} simulation runs.")
    print(f"  Passed (duplication/out-of-order/missing all 0.0): {num_passed}")
    print(f"  Failed (a delivery guarantee was violated):        {len(failed)}")
    print(f"  Unreadable / missing rates-results.json:           {len(broken)}")
    print()
    if still_needed > 0:
        print(
            f">>> {num_passed}/{args.target} passing runs so far. "
            f"You still need {still_needed} more passing run(s)."
        )
    else:
        print(
            f">>> {num_passed}/{args.target} passing runs reached. No further runs needed."
        )

    if failed:
        print("\nFailed runs (violation detected):")
        for r in failed:
            bad = {k: v for k, v in r.rates.items() if v != 0.0}
            print(f"  - {r.name}: {bad}")

    if broken:
        print("\nRuns that could not be evaluated:")
        for r in broken:
            print(f"  - {r.name}: {'; '.join(r.warnings)}")

    if args.verbose:
        print("\nAll rates:")
        for r in results:
            if r.rates:
                print(f"  {r.name}: {r.rates}  counts={r.counts}")

    if args.out:
        with args.out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "simulation",
                    "passed",
                    "duplication_rate",
                    "out_of_order_rate",
                    "missing_rate",
                    "total_events",
                    "warnings",
                ]
            )
            for r in results:
                writer.writerow(
                    [
                        r.name,
                        r.passed,
                        r.rates.get("duplication_rate", ""),
                        r.rates.get("out_of_order_rate", ""),
                        r.rates.get("missing_rate", ""),
                        r.counts.get("total_events", ""),
                        " | ".join(r.warnings),
                    ]
                )
        print(f"\nCSV report written to {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# python3 runner/check.py simulations/alps/alps-eod --out simulations/alps/alps-eod.csv -v
# python3 runner/check.py simulations/alps/alps-iod --out simulations/alps/alps-iod.csv -v
# python3 runner/check.py simulations/alps/alps-real --out simulations/alps/alps-real.csv -v
