import json
import os
import statistics
import subprocess

import yaml

BASE_CONFIG_FILE = "mqtt-simulation-config.yaml"
SUMMARY_FILE = "simulation-summary.json"
DELTA_THRESHOLD = 1000  # Acceptable delta (in ms) here
MAX_RUNS = 50  # Failsafe to prevent infinite loops
INITIAL_RUNS = 3  # Number of initial runs before checking convergence


def run_simulation(run_index, cwd):
    """Generates a config for the current run and executes the Docker container."""
    simulation_dir = f"simulation-mqtt-{run_index}"
    run_config_file = f"mqtt-simulation-config-run-{run_index}.yaml"

    with open(BASE_CONFIG_FILE, "r") as f:
        config = yaml.safe_load(f)

    config["simulation_dir"] = simulation_dir

    with open(run_config_file, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

    print(
        f"\n[{run_index}] Starting simulation into directory: {simulation_dir} (Docker output suppressed)..."
    )

    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        "/var/run/docker.sock:/var/run/docker.sock",
        "-v",
        f"{cwd}:/app",
        "jadkhaddad/lps-rs:0.1.0",
        "mqtt-simulation-runner",
        "--config-file",
        run_config_file,
        "--root-abs-path",
        cwd,
    ]

    subprocess.run(
        cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    return simulation_dir, run_config_file


def read_latency(simulation_dir):
    """Reads the median_latency_ms from the results file."""

    results_path = os.path.join(simulation_dir, "mount", "rates-results.json")

    if not os.path.exists(results_path):
        raise FileNotFoundError(f"Expected results file not found: {results_path}")

    with open(results_path, "r") as f:
        results = json.load(f)

    return results["median_latency_ms"]


def main():
    cwd = os.getcwd()
    run_data = []  # Will store tuples of (run_index, latency)

    for i in range(1, MAX_RUNS + 1):
        try:
            sim_dir, temp_config = run_simulation(i, cwd)
            current_latency = read_latency(sim_dir)

            print(f"[{i}] Run complete. Median Latency: {current_latency} ms")

            past_latencies = [data[1] for data in run_data]

            run_data.append((i, current_latency))

            if i > INITIAL_RUNS:
                baseline_median = statistics.median(past_latencies)
                diff = abs(current_latency - baseline_median)

                print(
                    f"[{i}] Checking convergence: |{current_latency} - {baseline_median}| = {diff:.2f} ms"
                )

                if diff <= DELTA_THRESHOLD:
                    print(
                        f"\nConvergence reached! The latency difference ({diff:.2f} ms) is under the threshold of {DELTA_THRESHOLD} ms."
                    )
                    break
                else:
                    print(
                        f"Difference ({diff:.2f} ms) > {DELTA_THRESHOLD} ms. Continuing simulations..."
                    )

            # Clean up the temporary config file
            if os.path.exists(temp_config):
                os.remove(temp_config)

        except subprocess.CalledProcessError as e:
            print(f"Docker execution failed on run {i}.")
            break
        except Exception as e:
            print(f"An error occurred on run {i}: {e}")
            break

    if run_data:
        all_latencies = [data[1] for data in run_data]
        overall_median = statistics.median(all_latencies)

        best_run = min(run_data, key=lambda x: abs(x[1] - overall_median))
        best_index = best_run[0]
        best_latency = best_run[1]
        best_dir = f"simulation-mqtt-{best_index}"

        print("\n" + "=" * 50)
        print("SIMULATION SUMMARY")
        print("=" * 50)
        print(f"Total runs executed: {len(run_data)}")
        print(f"Overall Median Latency: {overall_median:.2f} ms")
        print(f"Closest run to overall median: Run {best_index} ({best_latency} ms)")
        print(f"Use directory: {best_dir} for your plots.")
        print("=" * 50)

        summary_data = {
            "total_runs": len(run_data),
            "overall_median_latency_ms": overall_median,
            "closest_run_index": best_index,
            "closest_run_latency_ms": best_latency,
            "target_directory": best_dir,
        }

        with open(SUMMARY_FILE, "w") as f:
            json.dump(summary_data, f, indent=4)

        print(f"Summary saved to {SUMMARY_FILE}")


if __name__ == "__main__":
    main()
