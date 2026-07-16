# Mantrac Ghana Data Platform — Process Flow

This walks through the platform end to end, in the order data actually moves through it. Each stage names what happens, what feeds it, what it produces, and which files do the work — useful as a reference for yourself, and as the narrative to walk an interviewer through.

---

## 1. Data Simulation

**What happens:** Since there's no real Mantrac data to work with, the platform generates its own — but not randomly. Every generator is designed to produce data with genuine internal structure (realistic fleet sizing per customer type, degradation curves that actually rise before failure, tax math that matches real GRA rules) so that everything downstream — the ML model, the dbt tests, the dashboard — has something real to work against instead of noise.

**Three layers of simulation, run in order:**

| Step | Script | Produces |
|---|---|---|
| Dimension seed data | `sql/seed_data/generate_seed_data.py` | 18 customers, 347 equipment across 39 models, parts/components/fault-code taxonomy |
| Transactional history | `sql/transactions/generate_transactions.py` + `generate_inventory_snapshot.py` | Sales, rentals, 6,355 maintenance work orders, GRA tax filings, inventory snapshot |
| Telemetry | `kafka/generate_failure_calendar.py` + `generate_historical_telemetry.py` | 24 months of daily fault events and sensor readings, driven by a logistic degradation curve so failure risk genuinely rises approaching a planned failure |

**Output:** A set of CSV files — nothing is in the warehouse yet at this point, it's all just generated flat files.

---

## 2. Batch Ingestion

**What happens:** The generated CSVs get loaded into the actual warehouse — a real PostgreSQL database running the 25-table star schema defined in `sql/schema.sql`.

**Order matters here:** dimensions first (customers, equipment, parts), then transactions (which reference those dimensions via foreign keys), then telemetry.

```
sql/schema.sql               → creates all 25 tables
sql/seed_data/load_seed_data.sql     → loads dimensions
sql/transactions/load_transactions.sql  → loads facts (sales, rentals, maintenance, tax, telemetry)
kafka/data/load_fault_events.sql     → loads event-level fault history
```

**Output:** A fully populated warehouse — this is the "cold start" state. Everything from here forward either reads from this warehouse or writes back into it.

---

## 3. Streaming Ingestion (Kafka)

**What happens:** The historical backfill above only gets you to "today." From here forward, new telemetry needs to arrive continuously, the way real equipment telemetry (Cat VisionLink-style) actually would.

```
kafka/telemetry_kafka_producer.py   → simulates new fault events + daily sensor
                                       readings, publishes to two Kafka topics:
                                       equipment.fault_events
                                       equipment.telemetry_daily

spark/jobs/telemetry_stream_consumer.py  → Spark Structured Streaming job that
                                             reads those topics and writes into
                                             fact_fault_events / fact_telemetry_daily
```

**Design note worth remembering:** the consumer runs with `trigger(availableNow=True)` — it processes whatever's queued, then stops, rather than running forever. That's deliberate: this platform's orchestration is a daily batch DAG, not a fleet of always-on services, so the consumer is built to fit that model rather than fight it.

**Also worth remembering:** the Kafka message payload deliberately does *not* include which component a fault "really" belongs to. Real telemetry doesn't know that — a fault code is just a fault code. Component attribution is inferred downstream, in step 4, the same way a real system has to.

**Output:** `fact_fault_events` and `fact_telemetry_daily` stay current.

---

## 4. Processing (Spark)

**What happens:** This is where prediction actually occurs. A Spark job engineers features from the telemetry and trains/scores a model.

```
spark/jobs/predictive_maintenance_scoring.py
```

**What it does, in order:**
1. Reads fault events — only `(equipment_id, event_date, fault_code, severity)`, never the internal component label
2. Fans each fault code out to its *candidate* components via `bridge_faultcode_component`, weighted by correlation — this is the inference step that stands in for what a real system has to figure out
3. Builds rolling 7/14/30-day features per (equipment, component): weighted fault score, critical fault count, days since last maintenance, machine age, engine hours
4. Trains a GBTClassifier, evaluated on a **time-based holdout** (never random — training only ever sees the past)
5. Scores the current day for the whole fleet, applies triage thresholds, writes `ml_maintenance_signals`

**Output:** `ml_maintenance_signals` — one row per (equipment, component) with a failure probability and a status (`New` / `Watch` / `Action Recommended`).

---

## 5. The Predictive-to-Work-Order Pipeline

**What happens:** A raw model probability is not the same thing as a scheduled service visit. This stage is the business-rule layer that sits between the model and reality — deliberately plain Python, not Spark, since it operates on a few thousand rows, not millions.

```
pipeline/apply_triage_rules.py           → auto-suppresses duplicate signals,
                                             escalates for Platinum-tier customers
pipeline/resolve_parts_demand.py          → resolves predicted component → actual
                                             model-specific part, logs catalog gaps
pipeline/convert_signals_to_workorders.py → simulates a service advisor: converts
                                             surviving signals into real work orders,
                                             dismisses the rest with a reason code
```

**The rule that matters most here:** the model never writes to `fact_maintenance_events` directly. Only step 3 of this pipeline does, and only for signals that survived triage and got a (simulated) human sign-off. Signals are probabilistic; work orders are committed capacity — keeping those separate is what makes the whole predictive-maintenance story auditable.

**Output:** New rows in `fact_maintenance_events` (type = `Predictive`, linked back to the originating signal), plus `parts_demand_forecast` rows telling the warehouse dashboard what's coming.

---

## 6. Orchestration (Airflow)

**What happens:** Steps 3 through 5 don't run themselves — a daily Airflow DAG wires them together in order.

```
airflow/dags/mantrac_pipeline_dag.py

consume_telemetry_stream → score_predictive_maintenance → merge_signals_output
    → apply_triage_rules → resolve_parts_demand → convert_signals_to_workorders
```

Runs `@daily`. A custom Airflow image (`airflow/Dockerfile`) bakes in Java + PySpark so it can submit jobs to the Spark cluster as a normal network client — no Docker-socket tricks.

**Output:** The warehouse advances by one simulated day, fully automatically, every time this DAG runs.

---

## 7. Transformation (dbt)

**What happens:** The warehouse's raw tables are transactional, not analysis-ready. dbt reshapes them into the four dashboard-ready marts.

```
dbt/mantrac_dw/models/staging/       → 19 models, one clean view per source table
dbt/mantrac_dw/models/intermediate/  → revenue unioned across streams, GRA tax
                                        allocated from quarterly filings down to
                                        monthly grain
dbt/mantrac_dw/models/marts/         → 5 marts:
                                          mart_finance
                                          mart_warehouse_inventory
                                          mart_predictive_maintenance
                                          mart_fleet_operations
                                          mart_maintenance_type_ratio
```

17 tests run against these marts (`dbt test`) — not_null/unique checks plus hand-written singular tests (e.g. failure probabilities must fall in [0,1]).

**Output:** Five queryable tables, each shaped exactly for one dashboard, sitting in the `analytics_marts` schema.

---

## 8. Presentation (Dashboard)

**What happens:** A BI tool (Metabase/Superset recommended, or the static HTML snapshot already in `dashboards/`) queries the marts directly.

- **Finance** — revenue by stream, monthly grain, GRA tax detail, filterable by the Industry slicer
- **Warehouse & Inventory** — stock value at cost, reorder-point risk, predictive-driven demand (dealer-wide, not industry-filtered — it's shared stock)
- **Predictive Maintenance** — the signal funnel, ranked risk list, predictive-vs-reactive trend
- **Fleet Operations** — utilization, MTBF/MTTR, owned-vs-rented, geographic rollups

**Output:** The thing a Mantrac ops manager would actually look at every morning.

---

## The loop, visualized

```
 SIMULATE ──▶ LOAD (batch) ──▶ WAREHOUSE ◀── STREAM (Kafka + Spark consumer)
                                    │
                                    ▼
                          SCORE (Spark ML model)
                                    │
                                    ▼
                    TRIAGE ──▶ RESOLVE PARTS ──▶ CONVERT TO WORK ORDER
                                    │                       │
                                    └───────────────────────┘
                                    ▼
                              WAREHOUSE (updated)
                                    │
                                    ▼
                          dbt (staging → marts)
                                    │
                                    ▼
                               DASHBOARD
```

Everything above the "WAREHOUSE (updated)" line runs once daily, orchestrated by Airflow. The warehouse is always the single source of truth that every other layer reads from or writes back into — nothing talks to anything else directly.
