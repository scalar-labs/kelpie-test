#!/usr/bin/env python3
"""Flag the latest throughput as an anomaly if it falls outside the 3-sigma band
of the preceding days at a chosen concurrency.

Reads the same history file the dashboard uses (data.json / dl_data.json) which
is a list of ``{"date": "YYYY-MM-DD", "results": [{"concurrency": N,
"throughput": X, ...}]}`` entries. The most recent ``--days`` entries *before*
``--date`` form the baseline; today's value is compared against
``mean +/- sigma * stdev`` of that baseline.

Outputs key=value pairs to ``$GITHUB_OUTPUT`` (when set) and a human-readable
summary to stdout. The exit code is always 0 so the workflow controls behaviour
through the ``anomaly`` output.

Usage:
  check_throughput_anomaly.py --data-json PATH --date YYYY-MM-DD \
      [--target-concurrency 16] [--days 14] [--sigma 3.0]
"""

import argparse
import json
import os
import pathlib
import statistics
import sys


def _throughput_at(entry, concurrency):
    for r in entry.get("results", []):
        if r.get("concurrency") == concurrency:
            return r.get("throughput")
    return None


def _emit(outputs):
    """Write key=value lines to $GITHUB_OUTPUT (if set) and echo them."""
    lines = [f"{k}={v}" for k, v in outputs.items()]
    gh_output = os.environ.get("GITHUB_OUTPUT")
    if gh_output:
        with open(gh_output, "a") as f:
            f.write("\n".join(lines) + "\n")
    for line in lines:
        print(line)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-json", required=True)
    parser.add_argument("--date", required=True, help="The latest run's date (UTC).")
    parser.add_argument("--target-concurrency", type=int, default=16)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--sigma", type=float, default=3.0)
    args = parser.parse_args()

    outputs = {
        "anomaly": "false",
        "direction": "none",
        "target_concurrency": args.target_concurrency,
    }

    data_path = pathlib.Path(args.data_json)
    if not data_path.exists() or data_path.stat().st_size == 0:
        outputs["reason"] = "history file missing or empty"
        _emit(outputs)
        return 0

    try:
        data = json.loads(data_path.read_text())
    except json.JSONDecodeError as exc:
        outputs["reason"] = f"invalid JSON: {exc}"
        _emit(outputs)
        return 0
    if not isinstance(data, list) or not data:
        outputs["reason"] = "no entries in history"
        _emit(outputs)
        return 0

    data.sort(key=lambda e: e.get("date", ""))

    latest = next((e for e in data if e.get("date") == args.date), None)
    latest_tp = _throughput_at(latest, args.target_concurrency) if latest else None
    if latest_tp is None:
        outputs["reason"] = (
            f"no throughput for {args.date} at concurrency {args.target_concurrency}"
        )
        _emit(outputs)
        return 0

    # Baseline: the most recent --days entries strictly before today that have a
    # throughput value at the target concurrency.
    baseline = []
    for e in data:
        if e.get("date", "") >= args.date:
            continue
        tp = _throughput_at(e, args.target_concurrency)
        if tp is not None:
            baseline.append(tp)
    baseline = baseline[-args.days :]

    outputs["latest"] = round(latest_tp, 2)
    outputs["baseline_n"] = len(baseline)

    if len(baseline) < 2:
        outputs["reason"] = "not enough baseline days (need >= 2)"
        _emit(outputs)
        return 0

    mean = statistics.fmean(baseline)
    sd = statistics.stdev(baseline)
    outputs["mean"] = round(mean, 2)
    outputs["sigma"] = round(sd, 2)

    if sd == 0:
        outputs["reason"] = "baseline standard deviation is zero"
        _emit(outputs)
        return 0

    lower = mean - args.sigma * sd
    upper = mean + args.sigma * sd
    outputs["lower"] = round(lower, 2)
    outputs["upper"] = round(upper, 2)
    outputs["deviation_sigma"] = round((latest_tp - mean) / sd, 2)

    if latest_tp < lower:
        outputs["anomaly"] = "true"
        outputs["direction"] = "low"
    elif latest_tp > upper:
        outputs["anomaly"] = "true"
        outputs["direction"] = "high"

    _emit(outputs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
