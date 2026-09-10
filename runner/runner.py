import argparse
import os
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
        "--runs",
        type=int,
        default=50,
        help="Number of simulation runs to perform (default: 50)",
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

    return parser.parse_args()


def run_simulation(run_index, cwd, base_dir, base_config_file, protocol, log_dir):
    """Generates a config for the current run and executes the Docker container."""

    simulation_dir = os.path.join(base_dir, f"simulation-{run_index}")
    run_config_file = os.path.join(base_dir, f"simulation-config-run-{run_index}.yaml")

    with open(base_config_file, "r") as f:
        config = yaml.safe_load(f)

    config["simulation_dir"] = simulation_dir

    with open(run_config_file, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

    stdout_target = subprocess.DEVNULL
    stderr_target = subprocess.DEVNULL

    log_msg = "(Docker output suppressed)"

    if log_dir:
        stdout_file = os.path.join(log_dir, f"stdout-run-{run_index}.log")
        stderr_file = os.path.join(log_dir, f"stderr-run-{run_index}.log")

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
        if stdout_target is not subprocess.DEVNULL:
            stdout_target.close()
        if stderr_target is not subprocess.DEVNULL:
            stderr_target.close()

    return simulation_dir, run_config_file


def main():
    args = parse_args()
    cwd = os.getcwd()

    os.makedirs(args.base_dir, exist_ok=True)

    if args.log_dir:
        os.makedirs(args.log_dir, exist_ok=True)

    if args.start_index > 1:
        print(f"Resuming from index {args.start_index}...")

    for i in range(args.start_index, args.runs + 1):
        print(f"Running simulation {i}/{args.runs}...")

        try:
            _ = run_simulation(
                i, cwd, args.base_dir, args.base_config, args.protocol, args.log_dir
            )

            print(f"[{i}] Run complete.")

        except subprocess.CalledProcessError:
            print(f"Docker execution failed on run {i}.")

            break
        except Exception as e:
            print(f"An error occurred on run {i}: {e}")

            break


if __name__ == "__main__":
    main()

# python3 runner.py --protocol "amqp" --runs 10 --base-dir "simulation" --base-config "dev-simulation-config.yaml" --log-dir "simulation/logs"
