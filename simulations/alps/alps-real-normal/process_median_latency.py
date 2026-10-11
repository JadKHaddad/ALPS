"""
Print the median of process_median_latency_ms across all simulation-*/mount/rates-results.json.

Run from the folder that contains the simulation-* directories:
    python3 process_median_latency.py

Optionally pick another numeric field:
    python3 process_median_latency.py --field median_latency_ms

Exit code is 0 if at least one value was read, 1 otherwise.
"""

import argparse
import json
import re
import statistics
import sys
from pathlib import Path


def sim_number(path: Path) -> int:
    m = re.search(r"simulation-(\d+)$", path.name)
    return int(m.group(1)) if m else -1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Median of a latency field across all simulations."
    )
    parser.add_argument(
        "--field",
        default="process_median_latency_ms",
        help="field in rates-results.json (default: process_median_latency_ms)",
    )
    parser.add_argument(
        "--dir",
        default=".",
        help="folder containing simulation-* dirs (default: current dir)",
    )
    args = parser.parse_args()

    sim_dirs = sorted(
        (p for p in Path(args.dir).glob("simulation-*") if p.is_dir()),
        key=sim_number,
    )
    if not sim_dirs:
        print(f"No simulation-* directories found in {Path(args.dir).resolve()}")
        return 1

    values = []
    for sim in sim_dirs:
        rates_file = sim / "mount" / "rates-results.json"
        if not rates_file.is_file():
            print(f"[SKIP] {sim.name}: {rates_file} not found")
            continue
        try:
            data = json.loads(rates_file.read_text())
        except (json.JSONDecodeError, OSError) as e:
            print(f"[SKIP] {sim.name}: could not read {rates_file}: {e}")
            continue
        value = data.get(args.field)
        if value is None:
            print(f"[SKIP] {sim.name}: {args.field} missing")
            continue
        values.append(value)
        print(f"       {sim.name}: {value:.3f}")

    if not values:
        print(f"\nNo {args.field} values found")
        return 1

    print(
        f"\nMedian {args.field} over {len(values)}/{len(sim_dirs)} simulations: "
        f"{statistics.median(values):.3f}"
    )
    print(f"(min {min(values):.3f}, max {max(values):.3f})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

# python3 process_median_latency.py
# python3 process_median_latency.py --field process_median_latency_ms
