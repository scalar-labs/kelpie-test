# Daily Test Types

What each scheduled Kelpie CI job checks. For schedules and how CI runs, see [test-environment-and-setup.md](./test-environment-and-setup.md). For failure handling, see [daily-test-checks.md](./daily-test-checks.md).

---

## Daily DB verification (ScalarDB on Cassandra)

Workflow: [`daily-db-verification.yml`](../.github/workflows/daily-db-verification.yml).  
Environment: three Cassandra nodes + client container. Isolation: `SERIALIZABLE`. Fault injection: `CassandraKiller` (`kelpie --inject`) randomly kills and restarts Cassandra nodes. Run length is about 900 seconds.

| Test | Config | Schema | Modules (prep / run / check) | What it verifies |
|------|--------|--------|------------------------------|------------------|
| **phantom-write** | `phantom-write-config.toml` | `tx_sensor.json` | `SensorPreparer` / `SensorProcessor` / `SensorChecker` | Concurrent sensor clients scan a timestamp partition and append the next revision. The checker fails if any revision is duplicated — a phantom-write / lost-serializability signal — even while Cassandra nodes are killed. |
| **write-skew** | `write-skew-config.toml` | `tx_transfer.json` | `TransferPreparer` / `WriteSkewTransferProcessor` / `WriteSkewTransferChecker` | Concurrent transfers debit one account type only after reading that account’s total across types, then check that totals never go negative and that balance/version invariants hold under the same Cassandra faults. |

---

## Daily DB benchmark (ScalarDB on Postgres)

Workflow: [`daily-db-benchmark.yml`](../.github/workflows/daily-db-benchmark.yml).  
Config: `scalardb-test/benchmark-config.toml`. Storage: ScalarDB JDBC → Postgres.  
Workload: payment-style **transfer** (`TransferPreparer` / `TransferProcessor` / `TransferReporter`) — random money moves between accounts.

| Aspect | Detail |
|--------|--------|
| **What it measures** | Throughput and latency of the transfer workload (not a correctness/anomaly checker like verification). |
| **Concurrency sweep** | `1, 2, 4, 8, 16, 32` (schema reset between levels). |
| **Duration (defaults)** | `run_for_sec` 200, `ramp_for_sec` 10. |
| **Anomaly alert** | Compares concurrency **16** throughput to a 14-day 3σ band; Slack + Jira only when that fires. |

---

## Daily DL benchmark (ScalarDL)

Workflow: [`daily-dl-benchmark.yml`](../.github/workflows/daily-dl-benchmark.yml).  
Config: `scalardl-test/benchmark-config.toml`. Environment: `scalardl-samples` ledger (+ Postgres).  
Workload: bank **transfer** via ScalarDL contracts (`BatchCreate` populate, `Transfer` execute) — same style of money moves as the DB benchmark, through the ledger.

| Aspect | Detail |
|--------|--------|
| **What it measures** | Throughput and latency of ScalarDL transfers. |
| **How it runs** | `kelpie --only-pre` once, then a concurrency sweep with `--except-pre`. |
| **Concurrency sweep** | `1, 2, 4, 8, 16, 32`. |
| **Duration (defaults)** | `run_for_sec` 200, `ramp_for_sec` 10. |
| **Anomaly alert** | Same idea as DB benchmark: concurrency **16**, 14-day 3σ band. |

---

## Not covered by daily CI in this repo

| Item | Note |
|------|------|
| ScalarDL Kelpie **verification** | `scalardl-test/verification-config.toml` exists but has no daily workflow here. |
| `jdbc-test` | Manual / local YCSB-style JDBC tests; no daily workflow. |
