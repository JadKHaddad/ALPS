#!/usr/bin/env python3

"""
Runs runner/runner.py once for every queue folder in
simulations/alps/alps-queues/, one after another.

Run it from the LPS root directory (the folder that contains runner/):
    cd ./LPS
    python3 simulations/alps/alps-queues/run.py
"""

import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ---- Settings ---------------------------------------------------------------
QUEUES_DIR = Path("simulations/alps/alps-queues")
RUNNER = Path("runner/runner.py")
PROTOCOL = "amqp"
START_INDEX = 9
RUNS = 12
STOP_ON_ERROR = False  # set True to abort if one folder fails
# -----------------------------------------------------------------------------


def queue_number(folder: Path) -> int:
    """Extract the leading number so folders sort as 1, 3, 6, 12... not 1, 12, 24..."""
    match = re.match(r"(\d+)", folder.name)
    return int(match.group(1)) if match else 10**9


def fmt_duration(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:d}h {m:02d}m {s:02d}s"


def main() -> None:
    if not RUNNER.exists():
        sys.exit(
            f"[ERROR] {RUNNER} not found. Run this script from the LPS root directory."
        )
    if not QUEUES_DIR.is_dir():
        sys.exit(f"[ERROR] {QUEUES_DIR} not found.")

    folders = sorted((p for p in QUEUES_DIR.iterdir() if p.is_dir()), key=queue_number)
    if not folders:
        sys.exit(f"[ERROR] No folders found in {QUEUES_DIR}")

    print(f"Found {len(folders)} folders: {', '.join(f.name for f in folders)}\n")

    results = []
    total_start = time.time()

    for i, folder in enumerate(folders, start=1):
        config = folder / "simulation-config.yaml"
        log_dir = folder / "logs"

        print("=" * 70)
        print(
            f"[{i}/{len(folders)}] {folder.name}   (started {datetime.now():%H:%M:%S})"
        )
        print("=" * 70)

        if not config.exists():
            print(f"[SKIP] No simulation-config.yaml in {folder}\n")
            results.append((folder.name, "SKIPPED", 0.0))
            continue

        log_dir.mkdir(parents=True, exist_ok=True)

        cmd = [
            sys.executable,
            str(RUNNER),
            "--protocol",
            PROTOCOL,
            "--start-index",
            str(START_INDEX),
            "--runs",
            str(RUNS),
            "--base-dir",
            str(folder),
            "--base-config",
            str(config),
            # "--log-dir",
            # str(log_dir),
        ]
        print(
            "Running:",
            " ".join(f'"{c}"' if " " in c else c for c in cmd),
            "\n",
            flush=True,
        )

        start = time.time()
        try:
            # Output of runner.py streams straight to the terminal
            ret = subprocess.run(cmd).returncode
        except KeyboardInterrupt:
            print("\n[INTERRUPTED] Stopped by user.")
            results.append((folder.name, "INTERRUPTED", time.time() - start))
            break
        elapsed = time.time() - start

        status = "OK" if ret == 0 else f"FAILED (exit {ret})"
        results.append((folder.name, status, elapsed))
        print(f"\n[{status}] {folder.name} finished in {fmt_duration(elapsed)}\n")

        if ret != 0 and STOP_ON_ERROR:
            print("Stopping because STOP_ON_ERROR is True.")
            break

    # ---- Summary ----
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for name, status, elapsed in results:
        print(f"  {name:<12} {status:<20} {fmt_duration(elapsed)}")
    print(f"\nTotal time: {fmt_duration(time.time() - total_start)}")


if __name__ == "__main__":
    main()
