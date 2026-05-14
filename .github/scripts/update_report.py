#!/usr/bin/env python3
"""Append (or replace) today's benchmark summary into gh-pages/data.json.

Usage:
  update_report.py --csv PATH --date YYYY-MM-DD --pages-dir PATH \
                   [--target-concurrency N]

If the CSV is missing or empty, an entry with an empty results list is written
for the date, so the chart can show the gap on failed days.

Also (re)writes <pages-dir>/index.html — a static page that renders the last
14 days of throughput at the chosen concurrency using Chart.js from a CDN.
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


INDEX_HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Daily DB benchmark</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           margin: 2rem auto; max-width: 900px; padding: 0 1rem; color: #222; }
    h1 { font-size: 1.4rem; }
    .meta { color: #666; font-size: 0.9rem; margin-bottom: 1.5rem; }
    canvas { max-width: 100%; }
  </style>
</head>
<body>
  <h1>Daily DB benchmark &mdash; throughput at concurrency __TARGET__</h1>
  <p class="meta">Last 14 days. Gaps indicate a failed or missing run.</p>
  <canvas id="chart" height="120"></canvas>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"
          integrity="sha384-9nhczxUqK87bcKHh20fSQcTGD4qq5GhayNYSYWqwBkINBhOfQLg/P5HG5lF1urn4"
          crossorigin="anonymous"
          referrerpolicy="no-referrer"></script>
  <script>
    const TARGET_CONCURRENCY = __TARGET__;
    fetch('data.json', { cache: 'no-store' })
      .then(r => r.json())
      .then(data => {
        const sorted = data.slice().sort((a, b) => a.date.localeCompare(b.date));
        const last = sorted.slice(-14);
        const labels = last.map(e => e.date);
        const values = last.map(e => {
          const row = (e.results || []).find(r => r.concurrency === TARGET_CONCURRENCY);
          return row && row.throughput != null ? row.throughput : null;
        });
        new Chart(document.getElementById('chart'), {
          type: 'line',
          data: {
            labels,
            datasets: [{
              label: 'Throughput (ops/s)',
              data: values,
              borderWidth: 2,
              spanGaps: false,
              tension: 0.2,
            }],
          },
          options: {
            scales: {
              y: { beginAtZero: true, title: { display: true, text: 'Throughput (ops/s)' } },
              x: { title: { display: true, text: 'Date (UTC)' } },
            },
          },
        });
      });
  </script>
</body>
</html>
"""


def write_index(pages_dir: pathlib.Path, target_concurrency: int) -> None:
    (pages_dir / "index.html").write_text(
        INDEX_HTML_TEMPLATE.replace("__TARGET__", str(target_concurrency))
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--pages-dir", required=True)
    parser.add_argument("--target-concurrency", type=int, default=64)
    args = parser.parse_args()

    pages_dir = pathlib.Path(args.pages_dir)
    pages_dir.mkdir(parents=True, exist_ok=True)
    data_path = pages_dir / "data.json"

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

    write_index(pages_dir, args.target_concurrency)

    print(f"Wrote {data_path} with {len(data)} day(s); today has {len(results)} row(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
