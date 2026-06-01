#!/usr/bin/env python3
"""Plot throughput vs date for the last N days at a chosen concurrency.

Reads gh-pages/data.json (the same file the page uses) and writes a static PNG
that can be posted to Slack.

Usage:
  plot_throughput_history.py --data-json PATH --out PATH \
                             [--target-concurrency N] [--days N]
"""

import argparse
import json
import math
import pathlib
import sys
from datetime import datetime

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-json", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--target-concurrency", type=int, default=64)
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()

    data_path = pathlib.Path(args.data_json)
    if not data_path.exists() or data_path.stat().st_size == 0:
        print(f"{data_path} is missing or empty; nothing to plot.")
        return 0

    try:
        data = json.loads(data_path.read_text())
    except json.JSONDecodeError as exc:
        print(f"Invalid JSON in {data_path}: {exc}")
        return 0
    if not isinstance(data, list) or not data:
        print("No entries in data.json; nothing to plot.")
        return 0

    data.sort(key=lambda e: e.get("date", ""))
    data = data[-args.days :]

    dates: list = []
    values: list = []
    for entry in data:
        date_str = entry.get("date")
        if not date_str:
            continue
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        row = next(
            (
                r
                for r in entry.get("results", [])
                if r.get("concurrency") == args.target_concurrency
            ),
            None,
        )
        tp = row.get("throughput") if row else None
        dates.append(d)
        values.append(tp if tp is not None else math.nan)

    if not dates:
        print("No plottable points.")
        return 0

    plt.figure(figsize=(10, 4.5))
    plt.plot(dates, values, marker="o", linewidth=2)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.gca().xaxis.set_major_locator(
        mdates.DayLocator(interval=max(1, len(dates) // 10))
    )
    plt.xticks(rotation=30, ha="right")
    plt.xlabel("Date (UTC)")
    plt.ylabel("Throughput (ops/s)")
    plt.title(
        f"Daily benchmark - concurrency {args.target_concurrency} "
        f"(last {args.days} days)"
    )
    plt.grid(True, alpha=0.3)
    plt.ylim(bottom=0)
    plt.tight_layout()
    plt.savefig(args.out, dpi=150)
    print(f"Wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
