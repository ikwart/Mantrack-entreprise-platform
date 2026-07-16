# Local Orchestration: n8n as a RAM-Conscious Substitute for Airflow

`airflow/dags/mantrac_pipeline_dag.py` remains the **production-intended orchestrator** — fully built, documented, and preserved untouched in this repo. `n8n/mantrac_pipeline_workflow.json` is a **local substitute** for machines that can't spare the ~1.4GB Airflow's webserver + scheduler + separate metadata database need, on top of everything else Docker is already running.

Both call the exact same underlying scripts, in the exact same order. **Nothing about the pipeline's logic, correctness, or the bugs we found and fixed this session changes based on which one triggers it** — orchestration is a thin scheduling layer on top of scripts that were validated independently of either tool.

## Why n8n needs to run natively, not in Docker

If n8n itself lived in a Docker container, its Execute Command nodes would only reach *inside* that container — not your host's Python/PySpark installation. That would either force installing Java+PySpark inside the n8n container too (recreating the exact ~1.4GB+ footprint problem this swap exists to avoid), or require Docker-socket-mounting tricks into other containers — the same fragile pattern deliberately avoided when building the Airflow image in the first place. Running n8n natively via `npx n8n` sidesteps both problems: its Execute Command nodes run directly on your host shell, where native Python/PySpark already live.

## A better RAM picture than initially estimated

While building this, a useful fact fell out: `spark/jobs/*.py` never sets `spark.master` to a cluster URL — so running them via plain `python3 script.py` (exactly how every Spark job in this project was validated during development) already runs Spark in **local mode automatically**. That means the local n8n path needs **zero Spark Docker containers at all** — `spark-master`/`spark-worker` simply aren't invoked.

Revised comparison:

| | Airflow path | n8n path |
|---|---|---|
| Postgres | `postgres-dw` + `postgres-airflow` | `postgres-dw` only |
| Kafka + Zookeeper | Docker (when running the DAG) | Docker (only when running the telemetry step) |
| Spark | Docker (`spark-master` + `spark-worker`) | **Not needed** - runs natively as a local Python process |
| Orchestrator | Docker (webserver + scheduler + init) | Native (`npx n8n`, ~200-300MB, outside Docker's allocation entirely) |

Realistic Docker footprint on the n8n path: just `postgres-dw` (~150-250MB) plus Kafka+Zookeeper only when that specific step runs (~850MB with the heap trims already in `docker-compose.yml`) — comfortably under a 3.8GB ceiling with real headroom, versus the ~4.2GB the full simultaneous Airflow path would need.

## Setup

```bash
# One-time
npm install -g n8n
pip install pyspark  # native, for the two Spark steps

# Start only what this workflow needs (not the full docker-compose stack)
docker-compose up -d postgres-dw
docker-compose up -d kafka zookeeper   # only when running step 1

# Run n8n natively
npx n8n
```

Open `http://localhost:5678`, import `n8n/mantrac_pipeline_workflow.json` (Workflows → Import from File), and set the `MANTRAC_ETL_PASSWORD` environment variable n8n needs for the telemetry-consumer node (matches `mantrac_etl`'s password in `sql/governance.sql`).

Note the connection details in the workflow differ from the Airflow DAG's: `localhost:5433` and `localhost:9092` (Docker's host-exposed ports), not `postgres-dw:5432`/`kafka:29092` (Docker's internal network hostnames) — because these commands run on your host now, not inside the Docker network.

## What's actually validated here, stated plainly

**Tested for real**: the exact command sequence this workflow runs — `apply_triage_rules.py`, `resolve_parts_demand.py`, and `convert_signals_to_workorders.py` were run with native relative paths (`--sql-dir sql`) against the real project structure, confirmed working correctly (same output as every other time these scripts have been run in this project).

**Not tested**: n8n itself. Attempted a real install in this sandbox (`npm install -g n8n`) to test the actual workflow import/execution — it failed with a `403 Forbidden` fetching a dependency (`xlsx` via `cdn.sheetjs.com`), a domain not reachable from this sandbox's restricted network. That's a sandbox limitation, not a flaw in the workflow design — `npm install -g n8n` should succeed normally on a machine with full internet access. **Confirm the import and a manual execution work the first time you set this up** — the workflow JSON's structure was validated (well-formed, correct node chain, all connections present), but the n8n engine itself running it end-to-end has not been.

## What you lose with this swap, honestly

The claim shifts from "I operated Airflow" to "I designed and validated an Airflow DAG, and ran the equivalent daily pipeline locally via n8n for a documented hardware-constraint reason." That's a different claim, not a weaker one, but worth knowing exactly what you can honestly say happened if asked. n8n workflows are also JSON, not Python — less naturally git-diffable or reviewable in a code walkthrough than the Airflow DAG file.

## Upgrading RAM later

Nothing here touches `airflow/` at all — the Dockerfile, DAG, and `docker-compose.yml` service definitions sit unused but fully intact. Docker only consumes resources for services actually started, so a defined-but-unused Airflow service costs nothing at rest. The day you have more RAM: `docker-compose up airflow-init && docker-compose up -d` brings it fully to life exactly as originally designed, no rework needed.
