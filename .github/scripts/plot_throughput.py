#!/usr/bin/env python3
"""Plot throughput vs concurrency from a benchmark summary CSV.

Usage:
  plot_throughput.py [INPUT_CSV] [OUTPUT_PNG]

Defaults:
  INPUT_CSV  = results/summary.csv
  OUTPUT_PNG = results/throughput-vs-concurrency.png

The CSV must contain at least the columns `concurrency` and `throughput`.
"""

import csv
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main() -> int:
    input_csv = sys.argv[1] if len(sys.argv) > 1 else "results/summary.csv"
    output_png = (
        sys.argv[2] if len(sys.argv) > 2 else "results/throughput-vs-concurrency.png"
    )

    xs: list[int] = []
    ys: list[float] = []
    with open(input_csv) as f:
        for row in csv.DictReader(f):
            try:
                xs.append(int(row["concurrency"]))
                ys.append(float(row["throughput"]))
            except (ValueError, KeyError, TypeError):
                continue

    if not xs:
        print(f"No data points found in {input_csv}; nothing to plot.")
        return 0

    plt.figure(figsize=(8, 5))
    plt.plot(xs, ys, marker="o", linewidth=2)
    plt.xscale("log", base=2)
    plt.xticks(xs, [str(x) for x in xs])
    plt.xlabel("Concurrency")
    plt.ylabel("Throughput (ops/s)")
    plt.title("Throughput vs Concurrency")
    plt.grid(True, which="both", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_png, dpi=150)
    print(f"Wrote {output_png}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
