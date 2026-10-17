import argparse
import os
import subprocess
import time
from datetime import datetime

import yaml


def log(msg="", leading_newline=False):
    """Print a message prefixed with the current local time."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = "\n" if leading_newline else ""
    print(f"{prefix}[{ts}] {msg}", flush=True)


def format_duration(seconds):
    """Format a duration in seconds as H:MM:SS."""
    seconds = int(round(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}"


def parse_args():
    parser = argparse.ArgumentParser(description="Run MQTT/AMQP simulations.")
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
    parser.add_argument(
        "--sleep",
        type=float,
        default=0,
        help="Seconds to sleep between runs, e.g. 5 (default: 0, no sleep)",
    )

    args = parser.parse_args()

    if args.sleep < 0:
        parser.error("--sleep must be >= 0")

    return args


def stop_process(process, timeout=10):
    """Terminate a running process, escalating to kill if it doesn't exit in time."""
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


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

    log(
        f"[{run_index}] Starting {protocol.upper()} simulation into directory: {simulation_dir} {log_msg}...",
        leading_newline=True,
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

    process = None
    try:
        process = subprocess.Popen(cmd, stdout=stdout_target, stderr=stderr_target)
        returncode = process.wait()

        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, cmd)
    except BaseException:
        # Covers KeyboardInterrupt too: don't leave the docker client running.
        if process is not None:
            stop_process(process)
        raise
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
        log(f"Resuming from index {args.start_index}...")

    if args.sleep > 0:
        log(f"Sleeping {args.sleep:g}s between runs.")

    total_start = time.monotonic()

    for i in range(args.start_index, args.runs + 1):
        log(f"Running simulation {i}/{args.runs}...")
        run_start = time.monotonic()

        try:
            _ = run_simulation(
                i, cwd, args.base_dir, args.base_config, args.protocol, args.log_dir
            )

            elapsed = format_duration(time.monotonic() - run_start)
            log(f"[{i}] Run complete (took {elapsed}).")

        except KeyboardInterrupt:
            log(
                f"Interrupted on run {i}. Resume with --start-index {i}.",
                leading_newline=True,
            )

            break
        except subprocess.CalledProcessError as e:
            log(f"Docker execution failed on run {i} (exit code {e.returncode}).")

            break
        except Exception as e:
            log(f"An error occurred on run {i}: {e}")

            break

        # Sleep between runs, but not after the last one
        if args.sleep > 0 and i < args.runs:
            log(f"[{i}] Sleeping for {args.sleep:g}s before run {i + 1}...")
            time.sleep(args.sleep)

    log(f"Finished. Total elapsed: {format_duration(time.monotonic() - total_start)}.")


if __name__ == "__main__":
    main()

# python3 runner.py --protocol "amqp" --runs 10 --base-dir "simulation" --base-config "dev-simulation-config.yaml" --log-dir "simulation/logs" --sleep 5
