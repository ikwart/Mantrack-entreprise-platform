# Mantrac Ghana Data Platform

An enterprise data platform simulating **Mantrac Ghana** — the authorized Caterpillar dealer in Ghana — across its real business lines: **equipment sales, parts, service, and rental**, integrated with a **VisionLink-inspired telemetry and predictive maintenance layer**.

This is a portfolio project demonstrating Data Engineering, Analytics Engineering, Business Intelligence, Cloud/Streaming Architecture, and Industrial IoT — built to show the kind of system thinking relevant to data roles at heavy-equipment dealers and industrial firms (Mantrac, Komatsu, Epiroc, Cummins, and similar).

**This is not a toy dashboard project.** It's a full pipeline: synthetic-but-realistic data generation → streaming telemetry → a real trained ML model → automated business-rule triage → a human-in-the-loop review step → parts demand forecasting → orchestrated daily batch processing → a tested analytics layer. Every piece listed as "done" below was actually run against live infrastructure (a real Postgres database, a real trained Spark ML model, real dbt test runs) during development — not just written and assumed to work. Where something couldn't be tested (see [Known Limitations](#known-limitations)), that's stated plainly rather than glossed over.

---

## Table of Contents

- [What this platform simulates](#what-this-platform-simulates)
- [Architecture](#architecture)
- [Repository structure](#repository-structure)
- [Quick start](#quick-start)
- [Walkthrough: what each layer does](#walkthrough-what-each-layer-does)
- [Data model summary](#data-model-summary)
- [Business context](#business-context)
- [Known limitations](#known-limitations)
- [Roadmap / next steps](#roadmap--next-steps)

---

## What this platform simulates

Mantrac's real business runs on four revenue legs — equipment sales, parts, service, and rental — across mining, construction, quarrying, and oil & gas/energy customers in Ghana. This platform models that end to end:

- **A synthetic but realistic fleet**: 18 real Ghanaian companies (Gold Fields, AngloGold Ashanti, Newmont, Rocksure International, Engineers & Planners, and others across mining/construction/quarrying/logistics), operating 347 pieces of Cat equipment across 39 models and 12 equipment categories.
- **Telemetry that actually degrades realistically**: equipment doesn't fail randomly — a logistic degradation curve models fault-code frequency rising as a component approaches failure, with background noise, false starts, and severity escalation, so a predictive model has genuine signal to learn from (validated — see below).
- **A real trained ML model**: a Spark GBTClassifier scores every (equipment, component) pair for failure probability daily, using rolling fault-frequency features, honestly evaluated on a time-based holdout (not random split — no future leaking into training).
- **A closed predictive-maintenance loop**: signal → automated triage → parts demand resolution (component → model-specific part) → simulated human advisor review → real work order — with the model *never* writing directly to the maintenance schedule, only advisor-reviewed signals do.
- **Real Ghanaian tax logic**: VAT/NHIL/GETFund/withholding tax computed per the actual GRA VAT Act 2025 (Act 1151) reform effective January 2026, with revenue shown at the grain a business actually needs (monthly, not artificially flattened to quarterly just because filings happen quarterly).
- **Inventory carried at cost**, never at selling price — a deliberate accounting-correctness choice, not an oversight.

---

## Architecture

```
SOURCE GENERATION (Python)
├── Dimension seed data (customers, equipment, parts, components, fault codes)
├── Transactional generators (sales, rentals, maintenance history, tax filings)
└── Telemetry simulator (degradation-curve fault events + sensor readings)
        │
INGESTION
├── Batch: CSV → Postgres via \copy loaders
└── Streaming: Kafka topics (equipment.fault_events, equipment.telemetry_daily)
        │
PROCESSING (Apache Spark)
├── Structured Streaming consumer: Kafka → fact_fault_events / fact_telemetry_daily
├── Predictive maintenance scoring: GBTClassifier → ml_maintenance_signals
└── (business-rule layer, plain Python - deliberately NOT Spark, see below)
    ├── Triage rules (auto-suppress duplicates, SLA escalation)
    ├── Parts demand resolution (component → model-specific part)
    └── Signal-to-work-order conversion (human-in-the-loop simulation)
        │
WAREHOUSE (PostgreSQL, star schema)
├── 25 tables: dimensions, bridges, facts (see docs/architecture.md for full ERD)
└── dbt: staging (19 views) → intermediate (3 views) → marts (5 tables, tested)
        │
ORCHESTRATION (Apache Airflow, custom image with Java + PySpark)
└── Daily DAG: consume telemetry → score → merge → triage → resolve parts → convert to work orders
        │
BI LAYER (bring your own - Metabase/Superset recommended)
└── Finance | Warehouse & Inventory | Predictive Maintenance | Fleet Operations
    (cross-filtered by an Industry slicer)
```

**Why Spark for scoring but plain Python for the business-rule steps?** Feature engineering across ~3M equipment-component-day rows genuinely benefits from Spark's distributed processing. Triage rules, parts resolution, and work-order conversion operate on a few thousand rows at most — Spark would be unjustified overhead there. Using the right tool for each stage's actual scale, not the same tool everywhere, is a deliberate design choice.

---

## Repository structure

```
mantrac-ghana-data-platform/
├── docker-compose.yml          # Postgres (x2), Kafka, Spark, Airflow - full local stack
├── .env.example                 # copy to .env before running
├── docs/
│   └── architecture.md          # full design doc: ERD, predictive-signal flow, dashboard specs
├── sql/
│   ├── schema.sql                # full warehouse DDL (25 tables)
│   ├── seed_data/                 # dimension CSVs + generator + loader
│   └── transactions/              # fact CSVs + generators + loader
├── kafka/
│   ├── telemetry_engine.py         # pure degradation-curve simulation math (no I/O)
│   ├── generate_failure_calendar.py  # ground-truth planned failures (sim artifact)
│   ├── generate_historical_telemetry.py  # 24-month backfill
│   ├── telemetry_kafka_producer.py    # live producer, going forward from "today"
│   └── data/                           # failure calendar, historical fault events
├── spark/jobs/
│   ├── telemetry_stream_consumer.py    # Kafka -> warehouse (Structured Streaming)
│   ├── predictive_maintenance_scoring.py  # feature engineering + GBT model + scoring
│   └── merge_signals_output.py           # flattens Spark's part-file output
├── pipeline/                    # lightweight business-rule scripts (plain Python, not Spark)
│   ├── apply_triage_rules.py
│   ├── resolve_parts_demand.py
│   └── convert_signals_to_workorders.py
├── airflow/
│   ├── Dockerfile                # custom image: Java + PySpark, so Airflow can spark-submit
│   ├── requirements.txt
│   └── dags/mantrac_pipeline_dag.py
└── dbt/mantrac_dw/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── models/staging/            # 19 models, one per source table
    ├── models/intermediate/       # revenue unions, tax allocation
    ├── models/marts/               # 5 marts, one per dashboard
    └── tests/                       # 3 hand-written singular tests
```

---

## Quick start

### Prerequisites
- Docker Desktop installed and running
- ~8GB RAM available to Docker (Kafka + Spark + Postgres + Airflow together are not lightweight)
- Git

### 1. Clone and configure
```bash
git clone https://github.com/ikwart/mantrac-ghana-data-platform.git
cd mantrac-ghana-data-platform
cp .env.example .env
```
Open `.env` and change the placeholder passwords if you like (defaults work fine for local use).

### 2. Build the custom Airflow image
This bakes in Java + PySpark so Airflow can submit jobs to the Spark cluster — only needs to happen once, or whenever `airflow/requirements.txt` changes.
```bash
docker-compose build
```

### 3. Start the stack
```bash
docker-compose up airflow-init      # one-time: creates Airflow's metadata DB + admin user
docker-compose up -d                # starts everything else
```
Give it a minute or two — Kafka and Postgres both need to pass health checks before Airflow will fully come up.

### 4. Load the warehouse schema and seed data
```bash
docker exec mantrac-postgres-dw psql -U mantrac_admin -d mantrac_dw \
    -f /docker-entrypoint-initdb.d/sql/schema.sql

docker exec -w /docker-entrypoint-initdb.d/sql/seed_data mantrac-postgres-dw \
    psql -U mantrac_admin -d mantrac_dw -f load_seed_data.sql

docker exec -w /docker-entrypoint-initdb.d/sql/transactions mantrac-postgres-dw \
    psql -U mantrac_admin -d mantrac_dw -f load_transactions.sql

docker exec -w /docker-entrypoint-initdb.d/kafka_data mantrac-postgres-dw \
    psql -U mantrac_admin -d mantrac_dw -f load_fault_events.sql
```
> **Why `-w` (working directory) matters here:** the loader scripts use `\copy table FROM 'file.csv'` with relative paths, so `psql`'s working directory has to match where those CSVs actually are inside the container. `docker-compose.yml` mounts `./sql` and `./kafka/data` into `postgres-dw` for exactly this purpose — if you rename or move those folders, update the mount paths too.

### 5. Verify
```bash
docker exec -it mantrac-postgres-dw psql -U mantrac_admin -d mantrac_dw -c "SELECT count(*) FROM dim_equipment;"
```
You should see `347`. If you see `0`, the loaders didn't run correctly — check the `\copy` paths matched your working directory (see the note above).

### 6. Run dbt
`postgres-dw` exposes port 5433 to your host (see `docker-compose.yml`), so the simplest approach is running dbt from your host, pointed at that port — this is exactly how this project's dbt models were built and tested:
```bash
pip install dbt-core dbt-postgres
cd dbt/mantrac_dw
export DBT_DW_HOST=localhost DBT_DW_PORT=5433 \
       DW_DB_USER=mantrac_admin DW_DB_PASSWORD=<your password from .env> DW_DB_NAME=mantrac_dw
dbt run --profiles-dir .
dbt test --profiles-dir .
```
You should see `Done. PASS=17 WARN=0 ERROR=0`.

### 6b. (Optional) View the data lineage graph
```bash
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```
Opens an interactive lineage graph at `http://localhost:8080`, generated directly from the SQL — not a hand-drawn diagram. See [`docs/lineage.md`](docs/lineage.md).

### 7. Turn on the Airflow DAG
Open **http://localhost:8080**, log in with the admin credentials from `.env`, find `mantrac_predictive_maintenance_pipeline`, and un-pause it. It's scheduled `@daily`.

> **Low-RAM machine?** Running Postgres + Kafka + Spark + Airflow simultaneously needs ~4.2GB. If that doesn't fit your Docker allocation, see [`docs/n8n_orchestration.md`](docs/n8n_orchestration.md) for a lighter local substitute (`n8n/mantrac_pipeline_workflow.json`) that runs the same pipeline natively in under ~1.1GB of Docker footprint. The Airflow DAG stays fully intact either way — nothing here requires choosing one permanently.

### 8. (Optional) Start live telemetry streaming
`telemetry_kafka_producer.py` is a standalone Python script — simplest to run it directly on your host machine (not inside a container), pointed at Kafka's host-exposed port:
```bash
cd kafka
pip install -r requirements.txt
python telemetry_kafka_producer.py --mode kafka --bootstrap-servers localhost:9092 --loop --sleep-seconds 30
```
This simulates new equipment telemetry arriving continuously, going forward from "today" in the simulation. Leave it running in a separate terminal while the Airflow DAG runs on its daily schedule.

---

## Walkthrough: what each layer does

| Layer | What it does | Validated how |
|---|---|---|
| **Schema** (`sql/schema.sql`) | 25-table star schema: dimensions, bridge tables, facts | Loaded clean against a real local Postgres 16 instance, zero errors |
| **Seed data** (`sql/seed_data/`) | 18 customers, 347 equipment across 39 models, parts/components/fault-code taxonomy | Row counts verified against a live database after fixing two loader bugs found only by actually running them |
| **Transactions** (`sql/transactions/`) | Sales, rentals, 6,355 maintenance work orders, GRA tax filings, inventory | A real currency-conversion bug was caught and fixed by inspecting output distributions before shipping |
| **Telemetry simulator** (`kafka/`) | Degradation-curve fault generation, 227K days of historical telemetry | Sampled real planned failures and confirmed fault frequency genuinely rises approaching failure (7/8 sampled cases), Critical-severity faults only appear near failure, 88% of machine-days are fault-free background noise |
| **Spark scoring** (`spark/jobs/predictive_maintenance_scoring.py`) | Feature engineering + GBTClassifier training/scoring | Actually trained and evaluated: AUC 0.79, precision 42%, recall 17% on a time-based holdout (honest numbers, not suspiciously perfect) |
| **Pipeline** (`pipeline/`) | Triage, parts resolution, human-in-the-loop conversion | Each script run against real generated data; one script correctly no-op'd when there was genuinely nothing to suppress, rather than faking activity |
| **dbt** (`dbt/mantrac_dw/`) | 19 staging + 3 intermediate + 5 mart models, 17 tests | All 26 models built and all 17 tests passed against a live warehouse; 4 real bugs (bad path interpolation, missing column lists, wrong date arithmetic, a join-order error) were found and fixed by actually running it |
| **Airflow** (`airflow/`) | Daily DAG orchestrating the above | DAG syntax validated; a custom Airflow image (Java + PySpark) avoids fragile Docker-socket tricks for spark-submit |

---

## Data model summary

Full ERD and design rationale: [`docs/architecture.md`](docs/architecture.md). Highlights:

- **Predictive signals never write directly to the maintenance schedule.** `ml_maintenance_signals` is probabilistic model output; only the human-in-the-loop `convert_signals_to_workorders.py` step creates real `fact_maintenance_events` rows, and only for signals that survived automated triage.
- **No data leakage in either direction.** The Kafka fault-event payload deliberately excludes which component a fault "really" belongs to (real telemetry doesn't know this) — the Spark job reconstructs component attribution itself via `bridge_faultcode_component` correlation weights, the same inference a real system has to make.
- **Currency convention**: `dim_equipment_model.list_price_usd` / `dim_part.unit_cost` are USD reference prices; every transactional fact (sales, rentals, maintenance, inventory, tax) is denominated in GHS at a documented fixed FX rate.
- **Inventory is carried at cost**, never at selling price, in `fact_inventory` — deliberately kept separate from `fact_equipment_sales.sale_price`.
- **Tax logic reflects the real January 2026 GRA reform** (VAT Act 2025 / Act 1151): VAT 15% + NHIL 2.5% + GETFund 2.5% recoupled onto the same base (the old COVID-19 levy is gone); withholding tax split realistically by transaction type (3% goods, 7.5% services, 5% works).
- **Finance reporting grain is monthly**, not quarterly — GRA filings are genuinely quarterly events, but forcing revenue itself into quarterly buckets would hide real intra-quarter signal (e.g. a single large equipment sale landing in one specific month). Tax amounts are allocated down to each month proportional to that month's actual share of quarterly revenue, reconciling back to the original filing to within a rounding cent.

---

## Business context

This platform models **Mantrac Ghana**, the sole authorized Caterpillar dealer in Ghana (branches in Accra, Kumasi, Takoradi, Tarkwa), serving mining, construction, quarrying, and oil & gas/energy customers.

The 18 customers in the seed data are **real Ghanaian companies** operating in Mantrac's serviceable industries (Gold Fields Ghana, AngloGold Ashanti, Newmont Ghana, Rocksure International, Engineers & Planners, and others) — used as **realistic representative seed data**, not as a claim of confirmed commercial relationships, which Mantrac doesn't publicly disclose. Two customers (Rocksure International, Engineers & Planners) are grounded in confirmed public facts: Engineers & Planners is documented as operating a fleet of Cat 785D mining trucks at Tarkwa/Damang.

---

## Known limitations

Stated plainly rather than hidden, in the order they'd matter if you were extending this:


2. **`engine_hours` isn't tracked statefully in the live telemetry producer** — each run doesn't know yesterday's cumulative total. A real deployment should read the last known value from the warehouse before incrementing.
3. **Duplicate-suppression in triage works at the equipment level, not the component level** — the schema doesn't (yet) track which component an existing work order covers.
4. **`bridge_part_model_compatibility` treats every part as compatible with every model** in the seed data, as a generation-time simplification (documented in `generate_seed_data.py`).
6. **`CUSTOMER_BRANCH` is duplicated** across `generate_transactions.py` and `convert_signals_to_workorders.py` rather than centralized in shared config — a small refactor worth doing before this grows further.
7. **Row-level security doesn't currently reach the dbt marts.** `sql/governance.sql` enforces RLS on the base tables (verified live — region-scoped analyst roles genuinely see only their own region's data), but the marts are materialized as tables by `mantrac_admin`, which bypasses RLS as the table owner. See `docs/governance.md` for the three fix options. Full details on this and three real RLS/audit bugs found (and fixed) during validation are documented there.
8. **Rental revenue has no offsetting cost anywhere in the model** — `fact_rental_contracts` has no cost column, unlike `fact_equipment_sales.cost_basis`. This is deliberate, not an oversight: a rented machine's real cost is depreciation spread across its useful life plus ongoing maintenance, not a discrete per-contract cost the way a sale has one. Modeling it properly would need an acquisition cost + depreciation schedule on `dim_equipment` and an allocation method across variable-length rental periods — legitimate, realistic work, but a genuine new modeling exercise rather than surfacing data that already exists (unlike `mart_finance.equipment_sales_margin`, which just unlocked an already-generated column). Deliberately left undone in favor of cheaper, higher-signal wins elsewhere.
