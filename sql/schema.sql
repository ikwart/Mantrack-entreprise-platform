-- ============================================================================
-- Mantrac Ghana Enterprise Data Platform
-- PostgreSQL Warehouse Schema (star schema: dimensions, bridges, facts)
-- ============================================================================

-- ============================================================================
-- SECTION 1: DIMENSION TABLES
-- ============================================================================

CREATE TABLE dim_industry (
    industry_id     SERIAL PRIMARY KEY,
    industry_name   VARCHAR(50) NOT NULL UNIQUE
    -- Mining, Construction, Oil & Gas, Energy/Power, Quarrying, Logistics
);

CREATE TABLE dim_customer (
    customer_id     SERIAL PRIMARY KEY,
    customer_code   VARCHAR(20) NOT NULL UNIQUE,
    customer_name   VARCHAR(150) NOT NULL,
    customer_type   VARCHAR(30) NOT NULL
        CHECK (customer_type IN ('Owner-Operator', 'Contract Miner', 'Construction Contractor',
                                  'Quarry Operator', 'Logistics/Energy')),
    industry_id     INTEGER NOT NULL REFERENCES dim_industry(industry_id),
    region          VARCHAR(50) NOT NULL,
    contract_tier   VARCHAR(20) DEFAULT 'Standard'
        CHECK (contract_tier IN ('Standard', 'Silver', 'Gold', 'Platinum')),
    created_at      TIMESTAMP DEFAULT now()
);

CREATE TABLE dim_site (
    site_id         SERIAL PRIMARY KEY,
    site_name       VARCHAR(150) NOT NULL,
    customer_id     INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    region          VARCHAR(50) NOT NULL,
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6)
);

CREATE TABLE dim_equipment_category (
    category_id         SERIAL PRIMARY KEY,
    category_name       VARCHAR(50) NOT NULL UNIQUE,
    -- Excavator, Motor Grader, Off-Highway Truck, Hydraulic Mining Shovel,
    -- Dragline, Dozer, Compactor, Asphalt Paver, Backhoe Loader,
    -- Wheel Loader, Skid Steer Loader, Road Reclaimer
    primary_application VARCHAR(20) NOT NULL
        CHECK (primary_application IN ('Mining', 'Construction', 'Both'))
);

CREATE TABLE dim_equipment_model (
    model_id        SERIAL PRIMARY KEY,
    category_id     INTEGER NOT NULL REFERENCES dim_equipment_category(category_id),
    model_name      VARCHAR(50) NOT NULL,       -- e.g. 'Cat 785D', 'Cat D11', 'Cat 320 GC'
    engine_series   VARCHAR(50),
    list_price_usd  NUMERIC(14,2),              -- reference sale price, used only in sales fact
    UNIQUE (category_id, model_name)
);

CREATE TABLE dim_equipment (
    equipment_id    SERIAL PRIMARY KEY,
    model_id        INTEGER NOT NULL REFERENCES dim_equipment_model(model_id),
    serial_number   VARCHAR(50) NOT NULL UNIQUE,
    customer_id     INTEGER REFERENCES dim_customer(customer_id),   -- nullable: may sit in Mantrac stock
    site_id         INTEGER REFERENCES dim_site(site_id),
    install_date    DATE,
    ownership_type  VARCHAR(10) NOT NULL DEFAULT 'Owned'
        CHECK (ownership_type IN ('Owned', 'Rented')),
    status          VARCHAR(20) NOT NULL DEFAULT 'Active'
        CHECK (status IN ('Active', 'Idle', 'Decommissioned', 'In Stock'))
);

CREATE TABLE dim_component (
    component_id    SERIAL PRIMARY KEY,
    component_name  VARCHAR(80) NOT NULL UNIQUE,
    -- e.g. 'Hydraulic Pump', 'Engine Coolant System', 'Undercarriage/Track',
    --      'Final Drive', 'Fuel Injector', 'Engine - Fuel System'
    system_category VARCHAR(30) NOT NULL
        CHECK (system_category IN ('Hydraulics', 'Engine', 'Undercarriage', 'Powertrain', 'Electrical'))
);

CREATE TABLE dim_fault_code (
    fault_code      VARCHAR(15) PRIMARY KEY,
    description     VARCHAR(200) NOT NULL,
    severity        VARCHAR(10) NOT NULL
        CHECK (severity IN ('Info', 'Warning', 'Critical')),
    system_category VARCHAR(30) NOT NULL
);

CREATE TABLE dim_part (
    part_id         SERIAL PRIMARY KEY,
    part_number     VARCHAR(30) NOT NULL UNIQUE,
    part_name       VARCHAR(150) NOT NULL,
    part_category   VARCHAR(50) NOT NULL,
    -- e.g. 'Engine/Powertrain Component', 'Hydraulic Seal Kit', 'Filter', 'Undercarriage Wear Part'
    unit_cost       NUMERIC(12,2) NOT NULL,     -- INVENTORY VALUATION -- never selling price
    supplier_id     INTEGER,
    lead_time_days  INTEGER DEFAULT 7
);

CREATE TABLE dim_technician (
    technician_id       SERIAL PRIMARY KEY,
    technician_name     VARCHAR(100) NOT NULL,
    branch              VARCHAR(30) NOT NULL
        CHECK (branch IN ('Accra', 'Kumasi', 'Takoradi', 'Tarkwa')),
    certification_level VARCHAR(20) DEFAULT 'Certified'
);

CREATE TABLE dim_date (
    date_id         DATE PRIMARY KEY,
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    day             INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

-- ============================================================================
-- SECTION 2: BRIDGE TABLES
-- ============================================================================

CREATE TABLE bridge_part_model_compatibility (
    part_id         INTEGER NOT NULL REFERENCES dim_part(part_id),
    model_id        INTEGER NOT NULL REFERENCES dim_equipment_model(model_id),
    PRIMARY KEY (part_id, model_id)
);

CREATE TABLE bridge_component_part (
    component_id    INTEGER NOT NULL REFERENCES dim_component(component_id),
    part_category   VARCHAR(50) NOT NULL,
    typical_qty     INTEGER NOT NULL DEFAULT 1,
    is_primary      BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (component_id, part_category)
);

CREATE TABLE bridge_faultcode_component (
    fault_code          VARCHAR(15) NOT NULL REFERENCES dim_fault_code(fault_code),
    component_id        INTEGER NOT NULL REFERENCES dim_component(component_id),
    correlation_weight  NUMERIC(4,3) NOT NULL CHECK (correlation_weight BETWEEN 0 AND 1),
    is_direct_indicator BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (fault_code, component_id)
);

-- ============================================================================
-- SECTION 3: FACT TABLES
-- ============================================================================

-- --- Sales & Rental (revenue streams) ------------------------------------

CREATE TABLE fact_equipment_sales (
    sale_id         SERIAL PRIMARY KEY,
    equipment_id    INTEGER NOT NULL REFERENCES dim_equipment(equipment_id),
    customer_id     INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    sale_date       DATE NOT NULL REFERENCES dim_date(date_id),
    sale_price      NUMERIC(14,2) NOT NULL,
    cost_basis      NUMERIC(14,2) NOT NULL,
    margin          NUMERIC(14,2) GENERATED ALWAYS AS (sale_price - cost_basis) STORED,
    salesperson     VARCHAR(100)
);

CREATE TABLE fact_rental_contracts (
    rental_id           SERIAL PRIMARY KEY,
    equipment_id        INTEGER NOT NULL REFERENCES dim_equipment(equipment_id),
    customer_id         INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    start_date          DATE NOT NULL,
    end_date            DATE,
    rate_type           VARCHAR(10) NOT NULL CHECK (rate_type IN ('Daily', 'Monthly')),
    rate                NUMERIC(12,2) NOT NULL,
    total_billed         NUMERIC(14,2) NOT NULL DEFAULT 0,
    actual_usage_hours  NUMERIC(10,1)              -- from telemetry, nullable if not equipped
);

-- --- Telemetry -------------------------------------------------------------

CREATE TABLE fact_telemetry_daily (
    equipment_id            INTEGER NOT NULL REFERENCES dim_equipment(equipment_id),
    date_id                 DATE NOT NULL REFERENCES dim_date(date_id),
    engine_hours            NUMERIC(6,1),
    utilization_hours       NUMERIC(5,1),
    fault_count_info        INTEGER NOT NULL DEFAULT 0,
    fault_count_warning     INTEGER NOT NULL DEFAULT 0,
    fault_count_critical    INTEGER NOT NULL DEFAULT 0,
    avg_sensor_reading      NUMERIC(10,3),
    max_sensor_reading      NUMERIC(10,3),
    PRIMARY KEY (equipment_id, date_id)
);

-- --- Predictive maintenance pipeline ---------------------------------------

CREATE TABLE ml_maintenance_signals (
    signal_id                  SERIAL PRIMARY KEY,
    equipment_id                INTEGER NOT NULL REFERENCES dim_equipment(equipment_id),
    scoring_date                DATE NOT NULL,
    predicted_component_id      INTEGER NOT NULL REFERENCES dim_component(component_id),
    failure_probability         NUMERIC(5,4) NOT NULL CHECK (failure_probability BETWEEN 0 AND 1),
    model_version                VARCHAR(20) NOT NULL,
    recommended_action_window_days INTEGER,
    contributing_fault_codes    TEXT[],           -- array of fault codes
    signal_status                VARCHAR(20) NOT NULL DEFAULT 'New'
        CHECK (signal_status IN ('New', 'Watch', 'Action Recommended',
                                  'Reviewed', 'Converted', 'Dismissed', 'Expired')),
    dismiss_reason               VARCHAR(100),
    created_at                   TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_signals_equipment_status ON ml_maintenance_signals (equipment_id, signal_status);

-- --- Maintenance events / work orders ---------------------------------------

CREATE TABLE fact_maintenance_events (
    work_order_id       SERIAL PRIMARY KEY,
    equipment_id         INTEGER NOT NULL REFERENCES dim_equipment(equipment_id),
    customer_id          INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    technician_id         INTEGER REFERENCES dim_technician(technician_id),
    maintenance_type      VARCHAR(15) NOT NULL
        CHECK (maintenance_type IN ('Preventive', 'Predictive', 'Corrective', 'Inspection')),
    priority               VARCHAR(10) NOT NULL DEFAULT 'Routine'
        CHECK (priority IN ('Critical', 'High', 'Routine')),
    source_signal_id       INTEGER REFERENCES ml_maintenance_signals(signal_id),  -- nullable
    scheduled_date          DATE NOT NULL,
    actual_start             TIMESTAMP,
    actual_completion        TIMESTAMP,
    downtime_hours           NUMERIC(6,1),
    sla_target_hours         NUMERIC(6,1),
    labor_hours               NUMERIC(6,1),
    total_cost                NUMERIC(12,2),
    warranty_flag              BOOLEAN NOT NULL DEFAULT false,
    status                     VARCHAR(15) NOT NULL DEFAULT 'Scheduled'
        CHECK (status IN ('Scheduled', 'In Progress', 'Completed', 'Overdue')),
    root_cause                 VARCHAR(200),
    follow_up_required          BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE fact_maintenance_parts_used (
    work_order_id   INTEGER NOT NULL REFERENCES fact_maintenance_events(work_order_id),
    part_id         INTEGER NOT NULL REFERENCES dim_part(part_id),
    qty_used        INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (work_order_id, part_id)
);

-- --- Parts demand & inventory -----------------------------------------------

CREATE TABLE parts_demand_forecast (
    forecast_id     SERIAL PRIMARY KEY,
    signal_id       INTEGER REFERENCES ml_maintenance_signals(signal_id),  -- nullable if baseline demand
    equipment_id    INTEGER NOT NULL REFERENCES dim_equipment(equipment_id),
    part_id         INTEGER NOT NULL REFERENCES dim_part(part_id),
    expected_qty    INTEGER NOT NULL DEFAULT 1,
    is_primary      BOOLEAN NOT NULL DEFAULT true,
    needed_by_date  DATE,
    demand_source   VARCHAR(25) NOT NULL
        CHECK (demand_source IN ('Predictive Signal', 'Historical Baseline'))
);

CREATE TABLE parts_mapping_gaps (
    gap_id          SERIAL PRIMARY KEY,
    signal_id       INTEGER REFERENCES ml_maintenance_signals(signal_id),
    component_id    INTEGER REFERENCES dim_component(component_id),
    model_id        INTEGER REFERENCES dim_equipment_model(model_id),
    logged_at       TIMESTAMP DEFAULT now(),
    resolved        BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE fact_inventory (
    part_id             INTEGER NOT NULL REFERENCES dim_part(part_id),
    branch              VARCHAR(30) NOT NULL
        CHECK (branch IN ('Accra', 'Kumasi', 'Takoradi', 'Tarkwa')),
    snapshot_date       DATE NOT NULL,
    qty_on_hand         INTEGER NOT NULL DEFAULT 0,
    unit_cost           NUMERIC(12,2) NOT NULL,          -- carried at cost
    total_value          NUMERIC(14,2) GENERATED ALWAYS AS (qty_on_hand * unit_cost) STORED,
    reorder_point         INTEGER NOT NULL DEFAULT 0,
    days_of_supply         NUMERIC(6,1),
    PRIMARY KEY (part_id, branch, snapshot_date)
);

-- --- Model feedback loop -----------------------------------------------------

CREATE TABLE signal_outcomes (
    signal_id               INTEGER PRIMARY KEY REFERENCES ml_maintenance_signals(signal_id),
    actual_outcome           VARCHAR(20) NOT NULL
        CHECK (actual_outcome IN ('Confirmed Failure', 'False Positive')),
    lead_time_delivered_days INTEGER,               -- failure_date - signal_created_date
    resulting_work_order_id  INTEGER REFERENCES fact_maintenance_events(work_order_id)
);

-- --- Finance / tax -------------------------------------------------------------

CREATE TABLE fact_tax_filings (
    filing_id           SERIAL PRIMARY KEY,
    customer_id         INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    period_start         DATE NOT NULL,
    period_end            DATE NOT NULL,
    vat_amount             NUMERIC(14,2) NOT NULL DEFAULT 0,
    nhil_amount             NUMERIC(14,2) NOT NULL DEFAULT 0,
    getfund_amount           NUMERIC(14,2) NOT NULL DEFAULT 0,
    withholding_tax_amount   NUMERIC(14,2) NOT NULL DEFAULT 0,
    filing_status             VARCHAR(15) NOT NULL DEFAULT 'Pending'
        CHECK (filing_status IN ('Pending', 'Filed', 'Overdue'))
);

-- --- Raw fault events (event-level, feeds Spark feature engineering) ------
-- Added when building the Kafka streaming consumer: fact_telemetry_daily
-- alone (aggregate counts) isn't enough for the predictive scoring job's
-- fault-code-level feature engineering (bridge_faultcode_component
-- correlation weighting needs individual fault codes, not just a daily
-- count). This is what the streaming consumer appends to in production;
-- the historical backfill loads into it too, from the same generator.
-- Deliberately has NO component_id column - see the data-leakage note in
-- kafka/telemetry_kafka_producer.py and spark/jobs/predictive_maintenance_scoring.py.

CREATE TABLE fact_fault_events (
    event_id        SERIAL PRIMARY KEY,
    equipment_id    INTEGER NOT NULL REFERENCES dim_equipment(equipment_id),
    event_date      DATE NOT NULL,
    fault_code      VARCHAR(15) NOT NULL REFERENCES dim_fault_code(fault_code),
    severity        VARCHAR(10) NOT NULL
        CHECK (severity IN ('Info', 'Warning', 'Critical')),
    ingested_at     TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_fault_events_equipment_date ON fact_fault_events (equipment_id, event_date);

-- ============================================================================
-- SECTION 4: INDEXES (beyond PK/FK auto-indexes)
-- ============================================================================

CREATE INDEX idx_equipment_customer ON dim_equipment (customer_id);
CREATE INDEX idx_equipment_model ON dim_equipment (model_id);
CREATE INDEX idx_customer_industry ON dim_customer (industry_id);
CREATE INDEX idx_workorder_equipment ON fact_maintenance_events (equipment_id);
CREATE INDEX idx_workorder_scheduled_date ON fact_maintenance_events (scheduled_date);
CREATE INDEX idx_telemetry_date ON fact_telemetry_daily (date_id);
CREATE INDEX idx_sales_date ON fact_equipment_sales (sale_date);
CREATE INDEX idx_rental_customer ON fact_rental_contracts (customer_id);
CREATE INDEX idx_inventory_branch ON fact_inventory (branch, snapshot_date);
CREATE INDEX idx_tax_customer_period ON fact_tax_filings (customer_id, period_start);

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
