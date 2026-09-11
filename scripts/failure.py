"""
failure.py — combined out-of-order / duplicate-arrival analysis.

Classifies arrived server events into 4 columns and plots
relative arrival time (seconds since start) per status as a seaborn violin plot.

Usage:
    python failure.py DEVICE_EVENTS SERVER_EVENTS OUTPUT
        [--figsize WIDTH HEIGHT] [--usetex]

Output must be a .pdf or .png file.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

Event = dict[str, Any]
Record = dict[str, Any]

CATEGORY_ORDER = ["Eindeutig", "Dupliziert", "In Reihenfolge", "Außer Reihenfolge"]
CATEGORY_COLORS = {
    "Eindeutig": "#4C72B0",  # Blue
    "Dupliziert": "#F5C518",  # Yellow
    "In Reihenfolge": "#55A868",  # Green
    "Außer Reihenfolge": "#C44E52",  # Red
}

SUPPORTED_FORMATS = {".pdf", ".png"}


def parse_ts(ts: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp, truncating fractional seconds to µs."""
    if ts is None:
        return None
    ts = ts.rstrip("Z")
    if "." in ts:
        base, frac = ts.split(".")
        ts = f"{base}.{frac[:6]}"
    return datetime.fromisoformat(ts)


def load_json(path: str) -> Any:
    with open(path) as f:
        return json.load(f)


def remove_duplication(server_events: list[Event]) -> tuple[list[Event], list[Event]]:
    """Mirrors Event::remove_duplication in the Rust validator."""
    seen_keys: set[tuple[str, int]] = set()
    unique_events: list[Event] = []
    duplicate_events: list[Event] = []
    for e in server_events:
        key = (e["device_id"], e["event_id"])
        if key in seen_keys:
            duplicate_events.append(e)
        else:
            seen_keys.add(key)
            unique_events.append(e)
    return unique_events, duplicate_events


def compute_order_violations(unique_events: list[Event]) -> dict[tuple[str, int], bool]:
    """Mirrors Event::validate_order in the Rust validator."""
    last_seen: dict[str, int] = {}
    out_of_order: dict[tuple[str, int], bool] = {}
    for e in unique_events:
        eid, device_id = e["event_id"], e["device_id"]
        prev = last_seen.get(device_id)

        # Exact match to rust validator logic: out of order IF event_id < prev.
        out_of_order[(device_id, eid)] = prev is not None and eid < prev
        last_seen[device_id] = eid

    return out_of_order


def build_records(server_events: list[Event]) -> list[Record]:
    """
    Builds records for plotting. Yield unique events TWICE:
    once for overall "Eindeutig", and once for specific order status.
    """
    # Find the very first arrival time to serve as t=0
    valid_times = []
    for e in server_events:
        if e.get("arrived_at"):
            ts = parse_ts(e["arrived_at"])
            if ts:
                valid_times.append(ts)

    if not valid_times:
        return []

    start_time = min(valid_times)

    unique_events, duplicate_events = remove_duplication(server_events)
    ooo_map = compute_order_violations(unique_events)

    records = []
    for e in unique_events:
        if not e.get("arrived_at"):
            continue
        key = (e["device_id"], e["event_id"])
        out_of_order = ooo_map.get(key, False)

        arr_ts = parse_ts(e["arrived_at"])
        # Calculate seconds since the very first event
        relative_sec = (arr_ts - start_time).total_seconds()

        # 1. Add to the overall "Eindeutig" (Unique) column
        records.append(
            {
                "event_id": e["event_id"],
                "device_id": e["device_id"],
                "time_sec": relative_sec,
                "category": "Eindeutig",
            }
        )

        # 2. Add to the specific order column
        records.append(
            {
                "event_id": e["event_id"],
                "device_id": e["device_id"],
                "time_sec": relative_sec,
                "category": "Außer Reihenfolge" if out_of_order else "In Reihenfolge",
            }
        )

    for e in duplicate_events:
        if not e.get("arrived_at"):
            continue

        arr_ts = parse_ts(e["arrived_at"])
        relative_sec = (arr_ts - start_time).total_seconds()

        records.append(
            {
                "event_id": e["event_id"],
                "device_id": e["device_id"],
                "time_sec": relative_sec,
                "category": "Dupliziert",
            }
        )

    return records


def configure_style(use_tex: bool) -> None:
    sns.set_style("whitegrid")

    rc_params = {
        "font.family": "serif",
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
    }

    if use_tex:
        rc_params.update(
            {
                "text.usetex": True,
                "font.serif": ["Computer Modern Roman"],
                "axes.formatter.use_mathtext": True,
                "text.latex.preamble": r"\usepackage{lmodern} \usepackage[T1]{fontenc}",
            }
        )
    else:
        rc_params.update(
            {
                "text.usetex": False,
                "font.serif": ["Times New Roman", "DejaVu Serif"],
            }
        )

    plt.rcParams.update(rc_params)


def plot(
    ax: plt.Axes, records: list[Record], actual_total_arrived: int, use_tex: bool
) -> None:
    df = pd.DataFrame(records)
    counts = df["category"].value_counts()
    unique_count = counts.get("Eindeutig", 0)

    # Two conceptually separate pairs share this plot:
    #   - "Eindeutig" vs "Dupliziert" (both as a fraction of ALL arrived events)
    #   - "In Reihenfolge" vs "Außer Reihenfolge" (both as a fraction of UNIQUE events)
    # We want each pair's two violin widths to literally add up to a full slot
    # (i.e. "merging" them back together would reconstitute 100%), not just be
    # proportional to each other. That means each violin needs its own explicit
    # `width` -- base_width * (its share of the pair's total) -- rather than
    # relying on density_norm="count", which only scales the *smaller* violin
    # down relative to the largest one (so the pair doesn't sum to 100%).
    # Since `width` is a single value per violinplot() call, each category is
    # drawn with its own call, at its own fixed x position via `order`.
    base_width = 0.8  # seaborn's own default slot width
    pair_denom = {
        "Eindeutig": actual_total_arrived,
        "Dupliziert": actual_total_arrived,
        "In Reihenfolge": unique_count,
        "Außer Reihenfolge": unique_count,
    }
    for cat in CATEGORY_ORDER:
        cat_count = counts.get(cat, 0)
        denom = pair_denom[cat]
        share = (cat_count / denom) if denom > 0 else 0.0
        sns.violinplot(
            data=df[df["category"] == cat],
            x="category",
            y="time_sec",
            order=CATEGORY_ORDER,
            color=CATEGORY_COLORS[cat],
            width=base_width * share,
            legend=False,
            ax=ax,
        )

    ax.set_ylabel("Sekunden seit Beginn")

    legend = ax.get_legend()
    if legend is not None:
        legend.remove()

    summary_parts = []
    for cat in CATEGORY_ORDER:
        count = counts.get(cat, 0)
        if cat in ["In Reihenfolge", "Außer Reihenfolge"]:
            pct = (count / unique_count * 100) if unique_count > 0 else 0.0
        else:
            pct = (
                (count / actual_total_arrived * 100)
                if actual_total_arrived > 0
                else 0.0
            )

        if use_tex:
            summary_parts.append(f"\\textbf{{{cat}}}: {count} ({pct:.1f}\\%)")
        else:
            summary_parts.append(f"{cat}: {count} ({pct:.1f}%)")

    summary = " | ".join(summary_parts)

    if use_tex:
        title_str = f"\\textbf{{Gesamt}}: {actual_total_arrived} | {summary}"
    else:
        title_str = f"Gesamt: {actual_total_arrived} | {summary}"

    # Use the x-axis label as a bottom-placed title so it automatically
    # respects bounding boxes and padding in matplotlib.
    ax.set_xlabel(title_str, fontsize=10, labelpad=15)


def save_figure(fig: plt.Figure, out_file: str, dpi: int = 300) -> None:
    """Save the figure, inferring the output format (pdf/png) from the extension."""
    fmt = os.path.splitext(out_file)[1].lower().lstrip(".")
    save_kwargs: dict[str, Any] = {"format": fmt, "bbox_inches": "tight"}
    # DPI only matters for raster formats like PNG; PDF is vector-based
    # but passing dpi is harmless for it too, so keep the call uniform.
    if fmt == "png":
        save_kwargs["dpi"] = dpi
    fig.savefig(out_file, **save_kwargs)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot per-event arrival time as a seaborn violin plot with 4 status columns.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example:\n"
            "  python failure.py device.json server.json out.pdf --usetex\n"
            "  python failure.py device.json server.json out.png"
        ),
    )
    parser.add_argument("device_events", help="Path to device_events.json")
    parser.add_argument("server_events", help="Path to server_events.json")
    parser.add_argument(
        "output", help="Output plot path (must end in .pdf or .png)"
    )
    parser.add_argument(
        "--figsize",
        type=float,
        nargs=2,
        metavar=("WIDTH", "HEIGHT"),
        default=(12, 6),
        help="Figure size in inches (default: 12 6)",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="DPI used when saving raster (.png) output (default: 300; ignored for .pdf)",
    )
    parser.add_argument(
        "--usetex",
        action="store_true",
        help="Use LaTeX for text rendering (requires TeX Live installed)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv if argv is not None else sys.argv[1:])

    ext = os.path.splitext(args.output)[1].lower()
    if ext not in SUPPORTED_FORMATS:
        supported = ", ".join(sorted(SUPPORTED_FORMATS))
        print(
            f"error: output file must end in one of [{supported}], got '{args.output}'",
            file=sys.stderr,
        )
        sys.exit(1)

    configure_style(args.usetex)

    server_events = load_json(args.server_events)
    actual_total_arrived = sum(1 for e in server_events if e.get("arrived_at"))
    records = build_records(server_events)

    fig, ax = plt.subplots(figsize=args.figsize)
    plot(ax, records, actual_total_arrived, args.usetex)
    plt.tight_layout()

    save_figure(fig, args.output, dpi=args.dpi)
    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
# python3 failure.py simulation/device-results.json simulation/server-results.json simulation/failure.pdf --usetex
# python3 failure.py simulation/device-results.json simulation/server-results.json simulation/failure.png
