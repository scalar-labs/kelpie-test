# Test Environment and Setup

Daily Kelpie tests run on GitHub-hosted `ubuntu-latest` runners. Each job creates a Docker environment and tears it down at the end. There is no Terraform or Azure VM retain step.

Module usage notes remain in [`scalardb-test/README.md`](../scalardb-test/README.md) and [`jdbc-test/README.md`](../jdbc-test/README.md).

---

## Daily workflows

| Workflow | File | Schedule (UTC) | Environment |
|----------|------|----------------|-------------|
| Daily DB verification | [`.github/workflows/daily-db-verification.yml`](../.github/workflows/daily-db-verification.yml) | `0 10 * * *` | Cassandra ×3 + client (`scalardb-test/docker`) |
| Daily DB benchmark | [`.github/workflows/daily-db-benchmark.yml`](../.github/workflows/daily-db-benchmark.yml) | `0 15 * * *` | Postgres 18 on the runner |
| Daily DL benchmark | [`.github/workflows/daily-dl-benchmark.yml`](../.github/workflows/daily-dl-benchmark.yml) | `0 18 * * *` | `scalardl-samples` ledger + Postgres |

All three also support `workflow_dispatch`. Slack, Jira, and GitHub Pages run only on `schedule`.

There is no ScalarDL Kelpie **verification** workflow in this repo (`scalardl-test/verification-config.toml` is for local / other consumers). `jdbc-test` has no CI.

---

## ScalarDB verification (Docker Cassandra)

Defined in [`scalardb-test/docker/docker-compose.yml`](../scalardb-test/docker/docker-compose.yml): Cassandra 5 nodes with sshd (for CassandraKiller) plus a client image with Kelpie.

CI steps:

1. JDK 21, `./gradlew shadowJar` (GitHub Packages via `CR_PAT`).
2. Docker Buildx bake of `scalardb-test/docker` (GHA cache).
3. `./make-key.sh` then `docker compose up -d --wait`.
4. `scalardb-schema-loader:4.0.0-SNAPSHOT` on the compose network (`--coordinator`, RF=3).
5. `docker compose exec -T client kelpie --config <toml> --inject`.
6. Copy Cassandra logs; upload artifacts; `docker compose down -v`.

**Matrix** (`fail-fast: false`, job timeout 60 min):

| Cell | Config | Schema |
|------|--------|--------|
| `phantom-write` | `phantom-write-config.toml` | `tx_sensor.json` |
| `write-skew` | `write-skew-config.toml` | `tx_transfer.json` |

Isolation is `SERIALIZABLE`; run is about 900s with CassandraKiller (`--inject`).

**Artifacts:** `verification-logs-<name>` (kelpie + Cassandra logs), `result-<name>`.

**Notify:** Slack webhook summary; Jira on Kelpie non-success; gh-pages `verification.json`.

---

## ScalarDB benchmark (Postgres)

1. Build shadow jar; start `postgres:18-alpine` on port 5432.
2. Download Kelpie **1.2.4** (cached); pull schema-loader `4.0.0-SNAPSHOT`.
3. Patch `run_for_sec` / `ramp_for_sec` in `scalardb-test/benchmark-config.toml` (defaults 200 / 10).
4. For each concurrency in `1,2,4,8,16,32`: delete/recreate `tx_transfer.json` schema, run `kelpie --config benchmark-config.toml`.
5. Write `summary.csv`, plot throughput vs concurrency, upload `benchmark-results`.
6. On schedule: update gh-pages `data.json` and `throughput-vs-date.png`; Slack report to `eng-benchmark`; 3σ anomaly check at concurrency 16 over 14 days.

A failed Kelpie invocation in the sweep is logged and **does not `exit 1`**. Jira is created only when the anomaly checker flags concurrency 16.

`workflow_dispatch` can override Kelpie version, schema-loader tag, durations, and concurrencies.

---

## ScalarDL benchmark (scalardl-samples)

1. Checkout `scalar-labs/scalardl-samples`; build `scalardl-test` shadow jar.
2. Patch sample compose: mount `ledger.properties` (not `.tmpl`) and disable the JVM SecurityManager for JDBC.
3. Start ledger compose with `SCALARDL_VERSION` default `4.0.0-SNAPSHOT`.
4. Kelpie `--only-pre`, then concurrency sweep with `--except-pre` using `scalardl-test/benchmark-config.toml`.
5. Same summarize / plot / Pages / Slack / anomaly / Jira pattern as DB bench (`dl_data.json`, `dl_throughput-vs-date.png`). Artifact: `dl-benchmark-results` (includes compose service logs on teardown).

---

## Reproduce locally

**Verification** (from `scalardb-test/`):

```sh
./gradlew shadowJar
cd docker
./make-key.sh
docker compose build
docker compose up -d --wait
# Load schema with scalardb-schema-loader (Cassandra contact_points cassandra1,cassandra2,cassandra3)
docker compose exec -T client kelpie --config phantom-write-config.toml --inject
docker compose down -v
```

Configs and compose comments in `scalardb-test/docker/docker-compose.yml` describe schema loading. Download `verification-logs-*` from the failed run to compare `kelpie.log` and node logs.

**DB benchmark:** run Postgres locally, apply `schema/tx_transfer.json` with the schema-loader, then `${KELPIE}/bin/kelpie --config scalardb-test/benchmark-config.toml`.

**DL benchmark:** start `scalardl-samples/postgres` ledger compose (apply the same compose workarounds as CI if using 4.0.0-SNAPSHOT), then kelpie `--only-pre` / `--except-pre` with `scalardl-test/benchmark-config.toml`.

---

## Secrets (names only)

| Secret | Used for |
|--------|----------|
| `CR_PAT` | GitHub Packages (shadowJar) and GHCR (schema-loader / images) |
| `SLACK_WEBHOOK_URL` | Verification summary |
| `SLACK_TOKEN` | Benchmark bot posts to `eng-benchmark` |
| `JIRA_AUTH`, `JIRA_ASSIGNEE_ID` | Tickets in `DLT` |
| `GITHUB_TOKEN` | Pages, artifact download, GHCR for DL bench |

Do not put secret values in tickets or this document.

Jepsen daily tests: [scalar-jepsen test environment and setup](https://github.com/scalar-labs/scalar-jepsen/blob/master/docs/test-environment-and-setup.md).
