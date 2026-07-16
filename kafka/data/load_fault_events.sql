-- ============================================================================
-- Loads kafka/data/telemetry_fault_events.csv into fact_fault_events.
-- Run from within kafka/data/ (relative \copy path).
--
-- The source CSV also carries a component_id column (the simulator's
-- internal ground truth - see the leakage note in telemetry_kafka_producer.py
-- and predictive_maintenance_scoring.py). It is intentionally NOT loaded
-- into the warehouse - fact_fault_events only ever holds what real telemetry
-- actually reports: equipment_id, event_date, fault_code, severity. A
-- staging table is used because \copy needs a column list matching the CSV
-- exactly; you can't just tell it to skip a column.
--
-- Usage: cd kafka/data && psql -h <host> -U <user> -d mantrac_dw -f load_fault_events.sql
-- ============================================================================

CREATE TEMP TABLE staging_fault_events (
    event_id INTEGER, equipment_id INTEGER, event_date DATE,
    fault_code VARCHAR(15), severity VARCHAR(10), component_id INTEGER
);

\copy staging_fault_events FROM 'telemetry_fault_events.csv' WITH (FORMAT csv, HEADER true);

INSERT INTO fact_fault_events (equipment_id, event_date, fault_code, severity)
SELECT equipment_id, event_date, fault_code, severity FROM staging_fault_events;

DROP TABLE staging_fault_events;

SELECT setval(pg_get_serial_sequence('fact_fault_events', 'event_id'), (SELECT MAX(event_id) FROM fact_fault_events));
