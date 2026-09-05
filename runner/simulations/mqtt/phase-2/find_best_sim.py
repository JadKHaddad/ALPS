import os
import json
import glob
import math
import statistics


def main():
    data = {}
    file_pattern = os.path.join("simulation-*", "mount", "rates-results.json")

    total_found = 0
    for filepath in glob.glob(file_pattern):
        total_found += 1
        sim_name = filepath.split(os.sep)[0]
        with open(filepath, "r") as f:
            try:
                sim_data = json.load(f)

                if sim_data.get("missing_rate", 0) > 0:
                    continue

                data[sim_name] = sim_data
            except json.JSONDecodeError:
                print(f"Error reading {filepath}")

    print(
        f"Found {total_found} simulations. Filtered down to {len(data)} with a missing_rate of 0.\n"
    )

    if not data:
        print("No simulations matched the criteria (all had missing_rate > 0).")
        return

    all_metrics = ["median_latency_ms", "duplication_rate", "out_of_order_rate"]
    rates_metrics = ["duplication_rate", "out_of_order_rate"]

    mins = {m: min(d[m] for d in data.values()) for m in all_metrics}
    maxs = {m: max(d[m] for d in data.values()) for m in all_metrics}

    normalized_data = {}
    for sim, vals in data.items():
        normalized_data[sim] = {}
        for m in all_metrics:
            range_val = maxs[m] - mins[m]
            if range_val == 0:
                normalized_data[sim][m] = 0.0
            else:
                normalized_data[sim][m] = (vals[m] - mins[m]) / range_val

    medians = {}
    for m in all_metrics:
        sorted_vals = sorted(nd[m] for nd in normalized_data.values())
        mid = len(sorted_vals) // 2
        medians[m] = (sorted_vals[mid] + sorted_vals[~mid]) / 2.0

    distances_to_all_median = {}
    for sim, nd in normalized_data.items():
        dist = math.sqrt(sum((nd[m] - medians[m]) ** 2 for m in all_metrics))
        distances_to_all_median[sim] = dist
    rep_all_metrics = min(distances_to_all_median, key=distances_to_all_median.get)

    distances_to_rates_median = {}
    for sim, nd in normalized_data.items():
        # Only use duplication_rate and out_of_order_rate for the distance calculation
        dist = math.sqrt(sum((nd[m] - medians[m]) ** 2 for m in rates_metrics))
        distances_to_rates_median[sim] = dist
    rep_rates_only = min(distances_to_rates_median, key=distances_to_rates_median.get)

    performance_scores = {}
    for sim, nd in normalized_data.items():
        performance_scores[sim] = sum(nd[m] for m in all_metrics)
    best_performer = min(performance_scores, key=performance_scores.get)

    all_latencies = [d["median_latency_ms"] for d in data.values()]
    overall_median_latency = statistics.median(all_latencies)

    diff_all = data[rep_all_metrics]["median_latency_ms"] - overall_median_latency
    diff_rates = data[rep_rates_only]["median_latency_ms"] - overall_median_latency

    print(f"Overall Median Latency (All Valid Sims): {overall_median_latency:.2f} ms")
    print("=" * 60)

    print(f"MOST REPRESENTATIVE (All 3 Metrics): {rep_all_metrics}")
    print(f"Difference from Overall Median Latency: {diff_all:+.2f} ms")
    print(json.dumps(data[rep_all_metrics], indent=2))
    print("-" * 60)

    print(f"MOST REPRESENTATIVE (Rates Only): {rep_rates_only}")
    print(" (Based purely on typical duplication and out-of-order rates)")
    print(f"Difference from Overall Median Latency: {diff_rates:+.2f} ms")
    print(json.dumps(data[rep_rates_only], indent=2))
    print("-" * 60)

    print(f"BEST PERFORMING (Lowest Latency & Errors): {best_performer}")
    print(json.dumps(data[best_performer], indent=2))


if __name__ == "__main__":
    main()
