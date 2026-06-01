#!/usr/bin/env python3
"""Append (or replace) today's verification results into gh-pages/verification.json.

Usage:
  update_verification.py --results-dir PATH --date YYYY-MM-DD --pages-dir PATH

The results directory is expected to contain `result-<test>.txt` files (one per
matrix shard). Each file holds a single line `<test-name>:<status>` where status
is one of `success`, `failure`, or `timeout` — the same format the verification
workflow emits. `timeout` is recorded as TIMEOUT; anything else for a known test
is treated as FAIL.
"""

import argparse
import json
import pathlib
import sys


TESTS = ("phantom-write", "write-skew")


def _normalize_status(raw: str) -> str:
    s = (raw or "").strip().lower()
    if s == "success":
        return "PASS"
    if s == "timeout":
        return "TIMEOUT"
    return "FAIL"


def read_results(results_dir: pathlib.Path) -> dict:
    tests = {}
    for path in sorted(results_dir.glob("result-*.txt")):
        try:
            line = path.read_text().strip().splitlines()[0]
        except (OSError, IndexError):
            continue
        name, _, status = line.partition(":")
        name = name.strip()
        if not name:
            continue
        tests[name] = _normalize_status(status)
    return tests


def upsert_entry(data: list, date: str, tests: dict) -> list:
    data = [e for e in data if e.get("date") != date]
    data.append({"date": date, "tests": tests})
    data.sort(key=lambda e: e["date"])
    return data


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--pages-dir", required=True)
    args = parser.parse_args()

    pages_dir = pathlib.Path(args.pages_dir)
    data_path = pages_dir / "verification.json"

    if data_path.exists() and data_path.stat().st_size > 0:
        try:
            data = json.loads(data_path.read_text())
            if not isinstance(data, list):
                data = []
        except json.JSONDecodeError:
            data = []
    else:
        data = []

    tests = read_results(pathlib.Path(args.results_dir))
    # Fill in missing tests as FAIL so the dashboard does not silently hide a
    # shard that crashed before recording its result.
    for name in TESTS:
        tests.setdefault(name, "FAIL")

    data = upsert_entry(data, args.date, tests)
    data_path.write_text(json.dumps(data, indent=2) + "\n")

    print(f"Wrote {data_path} with {len(data)} day(s); today: {tests}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
