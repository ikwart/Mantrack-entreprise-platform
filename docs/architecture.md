# Mantrac Ghana Enterprise Data Platform
### Architecture & Design Document

---

## 1. Project Narrative

This platform simulates an end-to-end enterprise data ecosystem for **Mantrac Ghana**, the sole authorized Caterpillar dealer in Ghana, operating across Accra (HQ), Kumasi, Takoradi, and Tarkwa, with a Component Rebuild Center in Takoradi. Mantrac's real business spans four revenue legs — **equipment sales, parts, service, and rental** — across mining, construction, oil & gas, and power sectors.

The platform is not a reporting dashboard bolted onto sample data. It models how a Cat dealership actually runs: transactional business operations (sales, parts, tax compliance) integrated with a telemetry/predictive-maintenance layer inspired by **Cat VisionLink**, where equipment condition data drives service scheduling, which in turn drives parts demand — a closed loop across departments, not four disconnected dashboards.

**Target audience for this build:** demonstrates Data Engineering, Analytics Engineering, Cloud/Streaming Architecture, and Industrial IoT competency for data engineering / data analyst roles at firms like Mantrac, Komatsu, Epiroc, and similar heavy-equipment dealers.

---

## 2. Business Context

### 2.1 Revenue model (real Mantrac structure)
| Stream | Description |
|---|---|
| Equipment Sales | New/used Cat machine sales |
| Parts | Genuine Cat parts distribution |
| Service | Scheduled + reactive maintenance, Customer Value Agreements (CVAs) |
| Rental | Equipment rental to customers who don't purchase outright |

### 2.2 Customer segments & seed companies

**Mining — Owner/Operators**
| Company | Notes |
|---|---|
| Gold Fields Ghana (Tarkwa) | Confirmed Rocksure/E&P client site |
| AngloGold Ashanti (Obuasi) | Gold mining |
| Newmont Ghana (Ahafo/Akyem) | Gold mining |
| Golden Star Resources (Wassa/Bogoso) | Gold mining |
| Asanko Gold Mine | Gold mining |
| Ghana Manganese Company (Nsuta) | Manganese mining |
| Ghana Bauxite Company | Bauxite mining |

**Mining — Contract Miners** *(operate equipment on behalf of mine owners — distinct customer type, larger fleets, higher utilization)*
| Company | Notes |
|---|---|
| Rocksure International | 150+ heavy mining/drilling units; Load & Haul, Drill & Blast, Equipment Rental; active at Tarkwa, Damang, Asanko, Nsuta |
| Engineers & Planners (E&P) | Ghana's largest indigenous mining/construction contractor; confirmed Cat 785D fleet at Tarkwa/Damang |

**Construction & Infrastructure**
| Company | Notes |
|---|---|
| Contracta Construction Ghana | Roads/infrastructure |
| Micheletti & Co. Ghana | General contracting |
| Consar Limited | Construction/real estate |
| Vanhein Building & Civil Engineering | Civil engineering |

**Quarrying & Aggregates**
| Company | Notes |
|---|---|
| Justmoh Construction & Quarry | Aggregates |
| Rocklove Company Limited | Quarrying |

**Oil & Gas / Energy / Logistics**
| Company | Notes |
|---|---|
| Ghana Ports and Harbours Authority (Takoradi) | Material handling |
| Bui Power Authority | Power/hydro construction |
| Zoomlion Ghana | Municipal/waste fleet |

> Note: These are real Ghanaian firms operating in Mantrac's serviceable industries. Actual contractual relationships are not publicly disclosed (dealer confidentiality); this list is used as **realistic representative seed data**, and the portfolio README should frame it that way rather than as confirmed client rosters.

### 2.3 Customer attributes
- Customer Type: Owner-Operator | Contract Miner | Construction Contractor | Quarry Operator | Logistics/Energy
- Industry: Mining | Construction | Oil & Gas | Energy/Power | Quarrying | Logistics
- Region: Greater Accra | Ashanti | Western | Eastern | Central | Bono

---

## 3. Platform Architecture

```
SOURCE SYSTEMS (simulated)
├── ERP-style transactional generator (sales, rentals, work orders, parts, GRA tax lines)
└── IoT telemetry simulator (engine hours, fuel, fault codes, GPS, sensor drift)
        │
INGESTION
├── Batch: ERP tables → Airflow-orchestrated extracts (daily)
└── Streaming: Telemetry → Kafka topics (per-equipment fault/sensor events)
        │
PROCESSING (Apache Spark)
├── Streaming job: telemetry aggregation, fault-code frequency, rolling health features
├── Batch job: predictive maintenance scoring → ml_maintenance_signals
├── Batch job: parts demand forecasting (baseline + signal-driven)
└── Batch job: GRA-compliant tax reporting (VAT, NHIL, GETFund, withholding)
        │
WAREHOUSE (PostgreSQL, star schema)
├── dbt: staging → intermediate → marts
├── Facts: sales, rentals, maintenance events, telemetry, signals, tax filings, inventory
└── Dimensions: customer, industry, equipment (category/model/instance), part, component,
                fault code, technician, site, date
        │
ORCHESTRATION (Apache Airflow)
└── DAG: Spark jobs → triage rules → dbt run → data quality checks
        │
BI LAYER
└── Finance | Warehouse & Inventory | Predictive Maintenance | Fleet Operations
    (cross-filtered by Industry slicer)
```

---

## 4. Data Model

### 4.1 Dimensions

**`dim_customer`**
Customer ID, Customer Code, Customer Name, Customer Type, Industry ID (FK), Region, Site(s), Contract Tier (SLA level)

**`dim_industry`**
Industry ID, Industry Name (Mining, Construction, Oil & Gas, Energy/Power, Quarrying, Logistics)

**`dim_equipment_category`**
Category ID, Category Name (Excavator, Motor Grader, Off-Highway Truck, Hydraulic Mining Shovel, Dragline, Dozer, Compactor, Asphalt Paver, Backhoe Loader, Wheel Loader, Skid Steer Loader, Road Reclaimer), Primary Application (Mining/Construction/Both)

**`dim_equipment_model`**
Model ID, Category ID (FK), Model Name (e.g. Cat 785D, Cat D11, Cat 320 GC, Cat 140 Grader), Engine Series, List Price (for sales fact, not inventory valuation)

**`dim_equipment`** (physical machine instance)
Equipment ID, Model ID (FK), Serial Number, Customer ID (FK, current owner/lessee), Site/Location, Install Date, Ownership Type (Owned/Rented), Status (Active/Idle/Decommissioned)

**`dim_component`** (failure taxonomy — shared vocabulary between telemetry and parts)
Component ID, Component Name, System Category (Hydraulics, Engine, Undercarriage, Powertrain, Electrical)

**`dim_fault_code`**
Fault Code, Description, Severity (Info/Warning/Critical), System Category

**`dim_part`**
Part ID, Part Number, Part Name, Part Category (incl. "Engine/Powertrain Component"), Compatible Model ID(s), **Unit Cost** (inventory valuation — never selling price), Supplier ID, Lead Time Days

**`dim_technician`**
Technician ID, Name, Branch (Accra/Kumasi/Takoradi/Tarkwa), Certification Level

**`dim_site`**
Site ID, Site Name, Customer ID (FK), Region, GPS Coordinates

**`dim_date`** — standard date dimension

### 4.2 Bridge tables

**`bridge_component_part`**
Component ID (FK), Part Category, Typical Quantity, Is Primary (bool)

**`bridge_faultcode_component`**
Fault Code (FK), Component ID (FK), Correlation Weight (0–1), Is Direct Indicator (bool)

### 4.3 Fact tables

**`fact_equipment_sales`** — grain: one row per sale
Sale ID, Equipment ID, Customer ID, Sale Date, Sale Price, Cost Basis, Margin, Salesperson

**`fact_rental_contracts`** — grain: one row per rental period
Rental ID, Equipment ID, Customer ID, Start Date, End Date, Rate Type (Daily/Monthly), Rate, Total Billed, Actual Usage Hours (from telemetry, where available)

**`fact_maintenance_events`** — grain: one row per work order
Work Order ID, Equipment ID, Customer ID, Technician ID, Maintenance Type (Preventive/Predictive/Corrective/Inspection), Priority, Source Signal ID (FK, nullable), Scheduled Date, Actual Start, Actual Completion, Downtime Hours, SLA Target Hours, Parts Used, Labor Hours, Total Cost, Warranty Flag, Status

**`fact_telemetry_daily`** — grain: one row per equipment per day (aggregated from streaming)
Equipment ID, Date, Engine Hours, Fault Codes Emitted (array/count by severity), Sensor Readings (avg/max), Utilization Hours

**`fact_inventory`** — grain: one row per part per warehouse location per snapshot date
Part ID, Warehouse/Branch, Quantity on Hand, **Unit Cost**, Total Inventory Value (= Qty × Unit Cost), Reorder Point, Days of Supply

**`fact_tax_filings`** — grain: one row per filing period per customer transaction
Filing ID, Customer ID, Period, VAT, NHIL, GETFund, Withholding Tax, Filing Status

**`ml_maintenance_signals`** — grain: one row per equipment per scoring run
Signal ID, Equipment ID, Scoring Date, Predicted Component ID, Failure Probability, Model Version, Recommended Action Window, Contributing Fault Codes, Signal Status (New/Watch/Action Recommended/Reviewed/Converted/Dismissed/Expired)

**`parts_demand_forecast`** — grain: one row per signal-part resolution
Forecast ID, Signal ID (FK), Equipment ID, Part ID, Expected Qty, Is Primary, Needed By Date, Demand Source (Predictive Signal / Historical Baseline)

**`signal_outcomes`** — grain: one row per resolved signal (feedback loop)
Signal ID (FK), Actual Outcome (Confirmed Failure/False Positive), Lead Time Delivered (days), Resulting Work Order ID

**`parts_mapping_gaps`** — logs unresolved component→part lookups (catalog coverage metric)

---

## 5. Predictive Signal → Work Order Flow

```
fact_telemetry_daily
     │ (fault code frequency, weighted via bridge_faultcode_component)
     ▼
Spark scoring job → ml_maintenance_signals (Failure Probability per Equipment × Component)
     │
     ▼
Triage rules (automated):
  <0.4          → stays New
  0.4–0.7       → Watch
  ≥0.7          → Action Recommended (+ SLA-based escalation)
     │
     ▼
emit_parts_demand_signal → resolves Component → Part Category → Part Number
     (via bridge_component_part + dim_part, scoped to Equipment's Model ID)
     → writes parts_demand_forecast
     │
     ▼
Human-in-the-loop review (service advisor):
  Convert  → fact_maintenance_events (Maintenance Type = Predictive, source_signal_id set)
  Dismiss  → signal_status = Dismissed + reason
  Merge    → combine with nearby signal into one visit
     │
     ▼
On work order completion → signal_outcomes (actual vs predicted, lead time delivered)
```

**Key design principle:** the ML model never writes directly to `fact_maintenance_events`. Signals are probabilistic; work orders are committed capacity. The separation is what makes the model auditable and the metrics (precision, lead time) meaningful.

---

## 6. Telemetry Simulation Logic (summary)

- **Failure calendar generated first** (ground truth): 2–4 planned failures/machine/year, weighted toward high-wear components, varying by machine age/usage.
- **Degradation curves** run backward from each failure using a logistic ramp:
  `P(fault) = P_base + (P_max - P_base) / (1 + exp(-k * (progress - midpoint)))`
  — flat early, sharp knee late in the lead window. Parameters vary by component (fast/sharp for hydraulic pump & fuel injector; slow/late-knee for undercarriage & final drive).
- **Severity mix** shifts toward Critical as `progress → 1`.
- **Background noise**: all machines emit low-rate, mostly-Info faults continuously — required so the model has real negative examples.
- **False starts**: partial ramps that resolve without failure — produces realistic (not artificially perfect) precision/recall.
- **Signals generated by running the actual scoring job against simulated telemetry** — never hand-scripted to match the failure calendar, so results are genuinely derived.

---

## 7. Dashboard Specifications

All four dashboards share a global **Industry slicer** (Mining / Construction / Oil & Gas / Energy / Quarrying / Logistics) that cross-filters every visual via `Customer ID → Industry ID`.

### 7.1 Finance
- Revenue by stream: Equipment Sales | Parts | Service | Rental
- Gross margin by segment and by industry
- GRA compliance status: VAT, NHIL, GETFund, withholding — by filing period
- Accounts receivable aging
- Revenue by industry (slicer-driven): total sales, client count, tax contribution per industry

### 7.2 Warehouse & Inventory
- Inventory value **at cost** (Qty × Unit Cost) — never selling price
- Parts fill rate, inventory turnover, days of supply
- Stockout incidents, dead stock value
- Predictive-signal-driven demand vs historical baseline demand
- Parts-at-risk callout: upcoming predictive work orders blocked by understocked primary parts
- Catalog coverage gap metric (from `parts_mapping_gaps`)

### 7.3 Predictive Maintenance
- Ranked failure probability list by machine
- Predictive vs Reactive maintenance ratio (leading indicator of fleet health)
- Model precision/recall, average lead time delivered (from `signal_outcomes`)
- Fault code trend by component/system
- Signals by status funnel (New → Watch → Action Recommended → Converted/Dismissed)

### 7.4 Fleet Operations
- Machine utilization %, uptime/availability %
- MTBF, MTTR (from `fact_maintenance_events` timing fields)
- Idle time %, fuel efficiency trend
- Owned vs Rented fleet split, rental utilization (billed vs actual usage hours)
- Geographic distribution of active fleet by site

---

## 8. Technology Stack

| Layer | Technology |
|---|---|
| Streaming ingestion | Kafka |
| Batch orchestration | Apache Airflow |
| Processing | Apache Spark |
| Transformation | dbt |
| Warehouse | PostgreSQL |
| BI | Metabase or Superset (self-hosted, screenshot-able) |
| Containerization | Docker Compose |

---

## 9. Build Sequence

1. Finish current build: Postgres schema, Spark in Docker Compose, 3-job Airflow DAG
2. Add dbt on top of existing warehouse schema (staging → marts)
3. Add Kafka + telemetry streaming simulator (with degradation curve logic from §6)
4. Build predictive maintenance scoring job + signal-to-work-order pipeline (§5)
5. Build parts demand resolution logic (component → part mapping)
6. Build GRA tax reporting job
7. Dashboards last: Finance → Warehouse → Predictive Maintenance → Fleet Operations
8. README + architecture diagram + final GitHub push (ikwart)

---

## 10. Open Items for Next Session

- [ ] Finalize equipment model list (3–4 models per category, ~25–35 total)
- [ ] Assign specific customers → equipment mix (fleet size by segment)
- [ ] Draft full DDL for all tables above
- [ ] Define dbt mart layer (which marts feed which dashboard)
- [ ] Decide BI tool (Metabase vs Superset) and connection setup
