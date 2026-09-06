import argparse
import json
import os
import statistics
import subprocess

import yaml


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run MQTT/AMQP simulations until average latency converges."
    )
    parser.add_argument(
        "--protocol",
        type=str,
        choices=["mqtt", "amqp"],
        default="mqtt",
        help="The protocol to simulate: 'mqtt' or 'amqp' (default: mqtt)",
    )
    parser.add_argument(
        "--base-config",
        type=str,
        default="simulation-config.yaml",
        help="Path to the base configuration file (default: simulation-config.yaml)",
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default=".",
        help="Base directory to place simulation outputs and configs (default: current directory)",
    )
    parser.add_argument(
        "--summary-file",
        type=str,
        default="simulation-summary.json",
        help="Path for the output summary file (default: simulation-summary.json)",
    )
    parser.add_argument(
        "--delta-threshold",
        type=float,
        default=1000.0,
        help=(
            "Convergence threshold in ms, applied to the standard error of the "
            "mean (SEM) of all tracked run latencies (default: 1000). This is "
            "NOT compared against a single new sample; it reflects how tightly "
            "the estimate of the true mean has converged."
        ),
    )
    parser.add_argument(
        "--consecutive-required",
        type=int,
        default=3,
        help=(
            "Number of consecutive runs where SEM must stay under the "
            "threshold before declaring convergence (default: 3). Prevents "
            "stopping on a single lucky/noisy sample."
        ),
    )
    parser.add_argument(
        "--outlier-zscore",
        type=float,
        default=2.0,
        help=(
            "If a new run's latency is more than this many standard deviations "
            "from the mean of prior runs, flag it as an outlier in the logs "
            "(default: 2.0). Purely informational; does not affect convergence."
        ),
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=50,
        help="Failsafe to prevent infinite loops (default: 50)",
    )
    parser.add_argument(
        "--initial-runs",
        type=int,
        default=5,
        help="Number of initial runs before checking convergence (default: 5)",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=1,
        help="The index to start/resume the simulation from (default: 1)",
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        default=None,
        help="Directory to place stdout and stderr logs for each run. (default: suppressed)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Don't launch any Docker containers. Instead, read latencies from "
            "already-existing simulation-N directories under --base-dir and run "
            "the exact same convergence checks against them. Useful for "
            "re-analyzing a batch of runs you already have on disk, or for "
            "tuning --delta-threshold / --consecutive-required without paying "
            "for new simulations."
        ),
    )
    return parser.parse_args()


def run_simulation(run_index, cwd, base_dir, base_config_file, protocol, log_dir):
    """Generates a config for the current run and executes the Docker container."""
    simulation_dir = os.path.join(base_dir, f"simulation-{run_index}")
    run_config_file = os.path.join(base_dir, f"simulation-config-run-{run_index}.yaml")

    with open(base_config_file, "r") as f:
        config = yaml.safe_load(f)

    # Use the path containing the base_dir so the runner saves outputs there
    config["simulation_dir"] = simulation_dir

    with open(run_config_file, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

    stdout_target = subprocess.DEVNULL
    stderr_target = subprocess.DEVNULL
    log_msg = "(Docker output suppressed)"

    if log_dir:
        stdout_file = os.path.join(log_dir, f"stdout-run-{run_index}.log")
        stderr_file = os.path.join(log_dir, f"stderr-run-{run_index}.log")
        # Open in write mode since each run gets its own unique file
        stdout_target = open(stdout_file, "w")
        stderr_target = open(stderr_file, "w")
        log_msg = f"(Logs saved to {log_dir}/std{{out,err}}-run-{run_index}.log)"

    print(
        f"\n[{run_index}] Starting {protocol.upper()} simulation into directory: {simulation_dir} {log_msg}..."
    )

    runner_executable = (
        "mqtt-simulation-runner" if protocol == "mqtt" else "simulation-runner"
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
        runner_executable,
        "--config-file",
        run_config_file,
        "--root-abs-path",
        cwd,
    ]

    try:
        subprocess.run(cmd, check=True, stdout=stdout_target, stderr=stderr_target)
    finally:
        # Ensure file descriptors are safely closed after the run
        if stdout_target is not subprocess.DEVNULL:
            stdout_target.close()
        if stderr_target is not subprocess.DEVNULL:
            stderr_target.close()

    return simulation_dir, run_config_file


def read_latency(simulation_dir):
    """Reads the average_latency_ms from the results file."""
    results_path = os.path.join(simulation_dir, "mount", "rates-results.json")

    if not os.path.exists(results_path):
        raise FileNotFoundError(f"Expected results file not found: {results_path}")

    with open(results_path, "r") as f:
        results = json.load(f)

    return results["average_latency_ms"]


def compute_stats(latencies):
    """Returns (mean, stdev, sem) for a list of latencies.

    stdev/sem are 0.0 / inf respectively when there isn't enough data to
    compute a sample standard deviation.
    """
    n = len(latencies)
    mean = statistics.mean(latencies)
    stdev = statistics.stdev(latencies) if n > 1 else 0.0
    sem = (stdev / (n**0.5)) if n > 1 else float("inf")
    return mean, stdev, sem


def check_outlier(current_latency, past_latencies, zscore_threshold):
    """Flags (prints a warning about) current_latency if it's far from the
    distribution of past_latencies. Purely informational."""
    if len(past_latencies) < 2:
        return
    mean, stdev, _ = compute_stats(past_latencies)
    if stdev == 0:
        return
    z = abs(current_latency - mean) / stdev
    if z > zscore_threshold:
        print(
            f"  ⚠ Outlier warning: this run is {z:.2f} std devs from the prior "
            f"mean ({mean:.2f} ms, stdev {stdev:.2f} ms). Consider investigating "
            f"this run rather than letting it feed the convergence average."
        )


def main():
    args = parse_args()
    cwd = os.getcwd()
    run_data = []  # Will store tuples of (run_index, latency)
    consecutive_converged = 0

    # Ensure the base directory exists
    os.makedirs(args.base_dir, exist_ok=True)

    # Ensure the log directory exists if specified
    if args.log_dir:
        os.makedirs(args.log_dir, exist_ok=True)

    if args.start_index > 1:
        print(f"Resuming from index {args.start_index}. Loading previous run data...")
        for i in range(1, args.start_index):
            sim_dir = os.path.join(args.base_dir, f"simulation-{i}")
            try:
                latency = read_latency(sim_dir)
                run_data.append((i, latency))
                print(f"  Loaded run {i} latency: {latency} ms")
            except FileNotFoundError:
                print(f"  Warning: No prior data found for run {i} at {sim_dir}")

    if args.dry_run:
        print(
            f"\n[DRY RUN] Skipping Docker execution. Reading existing simulation "
            f"directories under '{args.base_dir}' instead.\n"
        )

    for i in range(args.start_index, args.max_runs + 1):
        try:
            if args.dry_run:
                sim_dir = os.path.join(args.base_dir, f"simulation-{i}")
                try:
                    current_latency = read_latency(sim_dir)
                except FileNotFoundError:
                    print(
                        f"[DRY RUN] No results found for run {i} at {sim_dir} "
                        "(stopping here; this is likely the end of your existing runs)."
                    )
                    break
                print(
                    f"[{i}] Loaded existing result from {sim_dir}. "
                    f"Average Latency: {current_latency} ms"
                )
            else:
                sim_dir, _ = run_simulation(
                    i, cwd, args.base_dir, args.base_config, args.protocol, args.log_dir
                )
                current_latency = read_latency(sim_dir)

                print(f"[{i}] Run complete. Average Latency: {current_latency} ms")

            past_latencies = [data[1] for data in run_data]

            # Informational only: flag runs that look statistically unusual
            # compared to everything seen so far, so you can investigate
            # bimodal/anomalous behavior instead of averaging over it.
            check_outlier(current_latency, past_latencies, args.outlier_zscore)

            run_data.append((i, current_latency))
            all_latencies = [data[1] for data in run_data]

            if len(all_latencies) >= args.initial_runs:
                mean, stdev, sem = compute_stats(all_latencies)

                print(
                    f"[{i}] Checking convergence: mean={mean:.2f} ms, "
                    f"stdev={stdev:.2f} ms, SEM={sem:.2f} ms "
                    f"(threshold {args.delta_threshold} ms)"
                )

                if sem <= args.delta_threshold:
                    consecutive_converged += 1
                    print(
                        f"  SEM under threshold ({consecutive_converged}/"
                        f"{args.consecutive_required} consecutive runs)."
                    )
                    if consecutive_converged >= args.consecutive_required:
                        print(
                            f"\nConvergence reached! SEM ({sem:.2f} ms) has stayed "
                            f"under the threshold of {args.delta_threshold} ms for "
                            f"{args.consecutive_required} consecutive runs."
                        )
                        break
                else:
                    if consecutive_converged > 0:
                        print(
                            "  SEM back above threshold; resetting consecutive count."
                        )
                    consecutive_converged = 0
                    print(
                        f"SEM ({sem:.2f} ms) > {args.delta_threshold} ms. "
                        "Continuing simulations..."
                    )

        except subprocess.CalledProcessError:
            print(f"Docker execution failed on run {i}.")
            break
        except Exception as e:
            print(f"An error occurred on run {i}: {e}")
            break

    if run_data:
        all_latencies = [data[1] for data in run_data]
        overall_average = statistics.mean(all_latencies)
        overall_stdev = (
            statistics.stdev(all_latencies) if len(all_latencies) > 1 else 0.0
        )
        overall_sem = (
            overall_stdev / (len(all_latencies) ** 0.5)
            if len(all_latencies) > 1
            else float("inf")
        )

        best_run = min(run_data, key=lambda x: abs(x[1] - overall_average))
        best_index = best_run[0]
        best_latency = best_run[1]
        best_dir = os.path.join(args.base_dir, f"simulation-{best_index}")

        print("\n" + "=" * 50)
        print("SIMULATION SUMMARY" + (" (DRY RUN)" if args.dry_run else ""))
        print("=" * 50)
        print(f"Protocol: {args.protocol.upper()}")
        print(f"Total runs tracked: {len(run_data)}")
        print(f"Overall Average Latency: {overall_average:.2f} ms")
        print(f"Overall Stdev: {overall_stdev:.2f} ms")
        print(f"Overall SEM: {overall_sem:.2f} ms")
        print(f"Closest run to overall average: Run {best_index} ({best_latency} ms)")
        print(f"Use directory: {best_dir} for your plots.")
        print("=" * 50)

        summary_data = {
            "protocol": args.protocol,
            "dry_run": args.dry_run,
            "total_runs": len(run_data),
            "overall_average_latency_ms": overall_average,
            "overall_stdev_latency_ms": overall_stdev,
            "overall_sem_latency_ms": overall_sem,
            "closest_run_index": best_index,
            "closest_run_latency_ms": best_latency,
            "target_directory": best_dir,
        }

        with open(args.summary_file, "w") as f:
            json.dump(summary_data, f, indent=4)

        print(f"Summary saved to {args.summary_file}")


if __name__ == "__main__":
    main()
