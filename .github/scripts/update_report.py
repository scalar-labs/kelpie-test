#!/usr/bin/env python3
"""Append (or replace) today's benchmark summary into gh-pages/data.json.

Usage:
  update_report.py --csv PATH --date YYYY-MM-DD --pages-dir PATH \
                   [--out-name FILENAME]

`--out-name` is the JSON file written inside --pages-dir (default data.json);
pass e.g. dl_data.json to keep a second benchmark's history alongside the
default one in the same directory.

If the CSV is missing or empty, an entry with an empty results list is written
for the date, so the chart can show the gap on failed days.

The page (index.html) is maintained as a checked-in asset on the gh-pages
branch and is not touched by this script.
"""

import argparse
import csv
import json
import pathlib
import sys

NUMERIC_FIELDS = (
    "throughput",
    "succeeded",
    "failed",
    "mean_ms",
    "sd_ms",
    "p50_ms",
    "p90_ms",
    "p99_ms",
    "max_ms",
)


def _to_number(value):
    if value is None or value == "":
        return None
    try:
        if any(c in value for c in ".eE"):
            return float(value)
        return int(value)
    except ValueError:
        return None


def read_results(csv_path: pathlib.Path):
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return []
    results = []
    with csv_path.open() as f:
        for row in csv.DictReader(f):
            entry = {"concurrency": _to_number(row.get("concurrency"))}
            for field in NUMERIC_FIELDS:
                entry[field] = _to_number(row.get(field))
            if entry["concurrency"] is not None:
                results.append(entry)
    return results


def upsert_entry(data, date, results):
    data = [e for e in data if e.get("date") != date]
    data.append({"date": date, "results": results})
    data.sort(key=lambda e: e["date"])
    return data


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--pages-dir", required=True)
    parser.add_argument("--out-name", default="data.json")
    args = parser.parse_args()

    pages_dir = pathlib.Path(args.pages_dir)
    data_path = pages_dir / args.out_name

    if data_path.exists() and data_path.stat().st_size > 0:
        try:
            data = json.loads(data_path.read_text())
            if not isinstance(data, list):
                data = []
        except json.JSONDecodeError:
            data = []
    else:
        data = []

    results = read_results(pathlib.Path(args.csv))
    data = upsert_entry(data, args.date, results)
    data_path.write_text(json.dumps(data, indent=2) + "\n")

    print(f"Wrote {data_path} with {len(data)} day(s); today has {len(results)} row(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
