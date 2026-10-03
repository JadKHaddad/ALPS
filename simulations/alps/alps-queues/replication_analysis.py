"""
replication_analysis.py

Applies Jain's (1991) confidence-interval / relative-precision method for
independent replications to a folder of simulation runs, and always
determines a "representative run" -- the run whose value for a chosen
metric lies closest to the aggregate mean across all replications, useful
for picking which single run to use for detailed/illustrative plots.

Expected layout:

    <root>/
      simulation-6/mount/rates-results.json
      simulation-7/mount/rates-results.json
      simulation-8/mount/rates-results.json
      ...

Each rates-results.json is expected to contain numeric fields such as:
    {
      "median_latency_ms": 71540.0,
      "average_latency_ms": 71147.911,
      "p99_latency_ms": 140375.0,
      "duplication_rate": 0.0,
      "out_of_order_rate": 0.0,
      "missing_rate": 0.0
    }

For each requested metric, the script:
  1. Collects one value per simulation run (= one i.i.d. replication).
  2. Computes sample mean, sample standard deviation.
  3. Computes the (1 - alpha) confidence interval half-width using the
     t-distribution.
  4. Computes achieved relative precision r = half_width / mean.
  5. Compares against your target relative precision and, if not yet
     reached, reports how many total replications (n_required) you'd need,
     following Jain's n_required = n0 * (r_achieved / r_target)^2 rule.

It also always computes a representative run: the run whose value for
--representative-metric is closest to the overall mean of that metric.


Usage:
    python replication_analysis.py --dir /path/to/folder \
        --metrics average_latency_ms,p99_latency_ms \
        --confidence 0.95 --precision 0.10
"""

import argparse
import csv
import json
import math
import statistics
import sys
from pathlib import Path

from scipy import stats


def t_critical(confidence: float, df: int) -> float:
    """Two-sided critical value for the given confidence level and degrees of freedom."""
    alpha = 1 - confidence
    return float(stats.t.ppf(1 - alpha / 2, df))


def discover_runs(root: Path, pattern: str, json_rel_path: str):
    """Find rates-results.json files under root/<pattern>/<json_rel_path>."""
    run_dirs = sorted(
        (p for p in root.glob(pattern) if p.is_dir()),
        key=lambda p: p.name,
    )
    runs = []
    for run_dir in run_dirs:
        json_path = run_dir / json_rel_path
        if not json_path.is_file():
            sys.stderr.write(
                f"WARNING: {json_path} not found, skipping {run_dir.name}\n"
            )
            continue
        try:
            with open(json_path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            sys.stderr.write(f"WARNING: could not read {json_path}: {e}, skipping\n")
            continue
        runs.append((run_dir.name, data))
    return runs


def analyze_metric(
    name: str, values: list[float], confidence: float, target_precision: float
):
    n = len(values)
    result = {
        "metric": name,
        "n": n,
        "values": values,
    }

    if n < 2:
        result["error"] = "need at least 2 replications to compute a standard deviation"
        return result

    mean = statistics.mean(values)
    stdev = statistics.stdev(values)  # sample stdev, n-1 denominator
    result["mean"] = mean
    result["stdev"] = stdev

    if mean == 0:
        result["error"] = "mean is 0, relative precision is undefined for this metric"
        return result

    t_val = t_critical(confidence, df=n - 1)
    half_width = t_val * stdev / math.sqrt(n)
    r_achieved = half_width / abs(mean)

    result["t_value"] = t_val
    result["half_width"] = half_width
    result["ci_low"] = mean - half_width
    result["ci_high"] = mean + half_width
    result["r_achieved"] = r_achieved
    result["r_target"] = target_precision

    if r_achieved <= target_precision:
        result["sufficient"] = True
        result["n_required"] = n
        result["additional_runs_needed"] = 0
    else:
        n_required = math.ceil(n * (r_achieved / target_precision) ** 2)
        result["sufficient"] = False
        result["n_required"] = n_required
        result["additional_runs_needed"] = n_required - n

    return result


def pick_representative_run(runs: list[tuple[str, dict]], metric: str):
    """
    Find the run whose value for `metric` is closest to the mean across
    all runs. Always computed -- this is what you should use to pick a
    single run for detailed/illustrative plots (e.g. per-event scatter
    plots), while aggregate stats (mean +/- CI) are used for summary plots.
    """
    values = {}
    for name, data in runs:
        val = data.get(metric)
        if isinstance(val, (int, float)):
            values[name] = float(val)

    if not values:
        return {
            "representative_metric": metric,
            "error": f"metric '{metric}' not found (or not numeric) in any run",
        }

    mean = sum(values.values()) / len(values)
    rep_name = min(values, key=lambda r: abs(values[r] - mean))
    rep_value = values[rep_name]
    abs_diff = abs(rep_value - mean)
    rel_diff_pct = (abs_diff / mean * 100) if mean != 0 else None

    return {
        "representative_metric": metric,
        "representative_run": rep_name,
        "representative_value": rep_value,
        "overall_mean": mean,
        "abs_diff_from_mean": abs_diff,
        "rel_diff_from_mean_pct": rel_diff_pct,
        "all_run_values": values,
    }


def print_report(results: list[dict], confidence: float, representative: dict):
    conf_pct = int(round(confidence * 100))
    for res in results:
        print(f"\n=== {res['metric']} ===")
        if "error" in res:
            print(f"  n = {res['n']}")
            print(f"  SKIPPED: {res['error']}")
            continue

        print(f"  n              = {res['n']}")
        print(f"  mean           = {res['mean']:.4f}")
        print(f"  stdev          = {res['stdev']:.4f}")
        print(
            f"  {conf_pct}% CI       = [{res['ci_low']:.4f}, {res['ci_high']:.4f}]  "
            f"(mean ± {res['half_width']:.4f})"
        )
        print(
            f"  achieved prec. = {res['r_achieved'] * 100:.2f}%  "
            f"(target: {res['r_target'] * 100:.2f}%)"
        )

        if res["sufficient"]:
            print(
                f"  VERDICT        = SUFFICIENT ({res['n']} replications already meet the target)"
            )
        else:
            print("  VERDICT        = NEED MORE RUNS")
            print(
                f"  n_required     = {res['n_required']}  "
                f"(run {res['additional_runs_needed']} more replications)"
            )

    print("\n=== Representative run ===")
    if "error" in representative:
        print(f"  SKIPPED: {representative['error']}")
    else:
        print(f"  metric         = {representative['representative_metric']}")
        print(f"  run            = {representative['representative_run']}")
        print(f"  value          = {representative['representative_value']:.4f}")
        print(f"  overall mean   = {representative['overall_mean']:.4f}")
        rel = representative["rel_diff_from_mean_pct"]
        rel_str = f"{rel:.2f}%" if rel is not None else "n/a"
        print(
            f"  diff from mean = {representative['abs_diff_from_mean']:.4f} ({rel_str})"
        )
        print("  -> use this run's raw output files for detailed/illustrative plots")


def write_csv(results: list[dict], path: str):
    fieldnames = [
        "metric",
        "n",
        "mean",
        "stdev",
        "t_value",
        "half_width",
        "ci_low",
        "ci_high",
        "r_achieved",
        "r_target",
        "sufficient",
        "n_required",
        "additional_runs_needed",
        "error",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for res in results:
            row = {k: res.get(k, "") for k in fieldnames}
            writer.writerow(row)
    print(f"CSV written to {path}")


def write_json(
    results: list[dict],
    representative: dict,
    run_names: list[str],
    args: argparse.Namespace,
    path: str,
):
    payload = {
        "run_dir": str(args.dir),
        "pattern": args.pattern,
        "json_path": args.json_path,
        "confidence": args.confidence,
        "target_precision": args.precision,
        "runs": run_names,
        "n_runs": len(run_names),
        "metrics": results,
        "representative": representative,
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"JSON results written to {path}")


def main():
    parser = argparse.ArgumentParser(
        description="Apply Jain's (1991) replication/precision method to simulation run folders.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--dir",
        "-d",
        required=True,
        type=Path,
        help="Root folder containing simulation-* subfolders",
    )
    parser.add_argument(
        "--pattern",
        default="simulation-*",
        help="Glob pattern (relative to --dir) matching each run's folder",
    )
    parser.add_argument(
        "--json-path",
        default="mount/rates-results.json",
        help="Path to the results JSON file relative to each run folder",
    )
    parser.add_argument(
        "--metrics",
        default="average_latency_ms,p99_latency_ms,median_latency_ms",
        help="Comma-separated list of JSON fields to analyze",
    )
    parser.add_argument(
        "--confidence",
        type=float,
        default=0.95,
        help="Confidence level for the interval (e.g. 0.90, 0.95, 0.99)",
    )
    parser.add_argument(
        "--precision",
        type=float,
        default=0.10,
        help="Target relative precision, e.g. 0.10 = within +/-10%% of the mean",
    )
    parser.add_argument(
        "--representative-metric",
        default="average_latency_ms",
        help="Metric used to pick the representative run (closest to the mean)",
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="Optional path to also write per-metric results as CSV",
    )
    parser.add_argument(
        "--json",
        default=None,
        help="Optional path to write the full results + representative run as JSON",
    )
    args = parser.parse_args()

    if not args.dir.is_dir():
        raise SystemExit(f"--dir {args.dir} is not a directory")

    runs = discover_runs(args.dir, args.pattern, args.json_path)
    if not runs:
        raise SystemExit(
            f"No runs found matching '{args.pattern}' with a readable "
            f"'{args.json_path}' under {args.dir}"
        )

    run_names = [name for name, _ in runs]
    print(f"Found {len(runs)} run(s): {', '.join(run_names)}")

    metrics = [m.strip() for m in args.metrics.split(",") if m.strip()]

    results = []
    for metric in metrics:
        values = []
        for run_name, data in runs:
            if metric not in data:
                sys.stderr.write(
                    f"WARNING: '{metric}' not found in {run_name}, skipping this run for this metric\n"
                )
                continue
            val = data[metric]
            if not isinstance(val, (int, float)):
                sys.stderr.write(
                    f"WARNING: '{metric}' in {run_name} is not numeric, skipping\n"
                )
                continue
            values.append(float(val))
        results.append(analyze_metric(metric, values, args.confidence, args.precision))

    representative = pick_representative_run(runs, args.representative_metric)

    print_report(results, args.confidence, representative)

    if args.csv:
        write_csv(results, args.csv)

    if args.json:
        write_json(results, representative, run_names, args, args.json)


if __name__ == "__main__":
    main()

# python3 simulations/alps/alps-queues/replication_analysis.py --confidence 0.95 --precision 0.10 --dir simulations/alps/alps-queues/1-queue --json simulations/alps/alps-queues/1-queue-summary.json
# python3 simulations/alps/alps-queues/replication_analysis.py --confidence 0.95 --precision 0.10 --dir simulations/alps/alps-queues/3-queues --json simulations/alps/alps-queues/3-queues-summary.json
# python3 simulations/alps/alps-queues/replication_analysis.py --confidence 0.95 --precision 0.10 --dir simulations/alps/alps-queues/6-queues --json simulations/alps/alps-queues/6-queues-summary.json
# python3 simulations/alps/alps-queues/replication_analysis.py --confidence 0.95 --precision 0.10 --dir simulations/alps/alps-queues/12-queues --json simulations/alps/alps-queues/12-queues-summary.json
# python3 simulations/alps/alps-queues/replication_analysis.py --confidence 0.95 --precision 0.10 --dir simulations/alps/alps-queues/24-queues --json simulations/alps/alps-queues/24-queues-summary.json
# python3 simulations/alps/alps-queues/replication_analysis.py --confidence 0.95 --precision 0.10 --dir simulations/alps/alps-queues/48-queues --json simulations/alps/alps-queues/48-queues-summary.json
# python3 simulations/alps/alps-queues/replication_analysis.py --confidence 0.95 --precision 0.10 --dir simulations/alps/alps-queues/50-queues --json simulations/alps/alps-queues/50-queues-summary.json
# python3 simulations/alps/alps-queues/replication_analysis.py --confidence 0.95 --precision 0.10 --dir simulations/alps/alps-queues/96-queues --json simulations/alps/alps-queues/96-queues-summary.json
