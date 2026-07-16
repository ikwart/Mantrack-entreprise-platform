-- ============================================================================
-- Loads seed_data/*.csv into the warehouse schema (run after schema.sql)
-- Usage: cd sql/seed_data && psql -h localhost -U <user> -d mantrac_dw -f load_seed_data.sql
--
-- NOTE: run this FROM WITHIN the seed_data/ directory - the \copy paths below
-- are relative. Every \copy specifies an explicit column list matching the
-- CSV header exactly, so any column the table has that the CSV doesn't
-- (e.g. created_at, supplier_id, latitude/longitude) correctly falls back to
-- its DEFAULT/NULL instead of erroring with "missing data for column X".
-- ============================================================================

\copy dim_industry (industry_id, industry_name) FROM 'dim_industry.csv' WITH (FORMAT csv, HEADER true);

\copy dim_customer (customer_id, customer_code, customer_name, customer_type, industry_id, region, contract_tier) FROM 'dim_customer.csv' WITH (FORMAT csv, HEADER true);

\copy dim_site (site_id, site_name, customer_id, region, latitude, longitude) FROM 'dim_site.csv' WITH (FORMAT csv, HEADER true);

\copy dim_equipment_category (category_id, category_name, primary_application) FROM 'dim_equipment_category.csv' WITH (FORMAT csv, HEADER true);

\copy dim_equipment_model (model_id, category_id, model_name, engine_series, list_price_usd) FROM 'dim_equipment_model.csv' WITH (FORMAT csv, HEADER true);

\copy dim_equipment (equipment_id, model_id, serial_number, customer_id, site_id, install_date, ownership_type, status) FROM 'dim_equipment.csv' WITH (FORMAT csv, HEADER true);

\copy dim_technician (technician_id, technician_name, branch, certification_level) FROM 'dim_technician.csv' WITH (FORMAT csv, HEADER true);

\copy dim_component (component_id, component_name, system_category) FROM 'dim_component.csv' WITH (FORMAT csv, HEADER true);

\copy dim_fault_code (fault_code, description, severity, system_category) FROM 'dim_fault_code.csv' WITH (FORMAT csv, HEADER true);

\copy bridge_faultcode_component (fault_code, component_id, correlation_weight, is_direct_indicator) FROM 'bridge_faultcode_component.csv' WITH (FORMAT csv, HEADER true);

\copy dim_part (part_id, part_number, part_name, part_category, unit_cost, lead_time_days) FROM 'dim_part.csv' WITH (FORMAT csv, HEADER true);

\copy bridge_component_part (component_id, part_category, typical_qty, is_primary) FROM 'bridge_component_part.csv' WITH (FORMAT csv, HEADER true);

\copy bridge_part_model_compatibility (part_id, model_id) FROM 'bridge_part_model_compatibility.csv' WITH (FORMAT csv, HEADER true);

-- Reset sequence counters so future application inserts don't collide with seed IDs
SELECT setval(pg_get_serial_sequence('dim_industry', 'industry_id'), (SELECT MAX(industry_id) FROM dim_industry));
SELECT setval(pg_get_serial_sequence('dim_customer', 'customer_id'), (SELECT MAX(customer_id) FROM dim_customer));
SELECT setval(pg_get_serial_sequence('dim_site', 'site_id'), (SELECT MAX(site_id) FROM dim_site));
SELECT setval(pg_get_serial_sequence('dim_equipment_category', 'category_id'), (SELECT MAX(category_id) FROM dim_equipment_category));
SELECT setval(pg_get_serial_sequence('dim_equipment_model', 'model_id'), (SELECT MAX(model_id) FROM dim_equipment_model));
SELECT setval(pg_get_serial_sequence('dim_equipment', 'equipment_id'), (SELECT MAX(equipment_id) FROM dim_equipment));
SELECT setval(pg_get_serial_sequence('dim_technician', 'technician_id'), (SELECT MAX(technician_id) FROM dim_technician));
SELECT setval(pg_get_serial_sequence('dim_component', 'component_id'), (SELECT MAX(component_id) FROM dim_component));
SELECT setval(pg_get_serial_sequence('dim_part', 'part_id'), (SELECT MAX(part_id) FROM dim_part));

-- ============================================================================
-- dim_date: generate a full calendar spine (2016-01-01 through 2027-12-31)
-- covers the install_date range used by dim_equipment plus forecast headroom
-- ============================================================================
INSERT INTO dim_date (date_id, year, quarter, month, month_name, day, day_of_week, is_weekend)
SELECT
    d::date,
    EXTRACT(YEAR FROM d)::int,
    EXTRACT(QUARTER FROM d)::int,
    EXTRACT(MONTH FROM d)::int,
    TO_CHAR(d, 'Month'),
    EXTRACT(DAY FROM d)::int,
    EXTRACT(ISODOW FROM d)::int,
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2016-01-01'::date, '2027-12-31'::date, interval '1 day') AS d
ON CONFLICT (date_id) DO NOTHING;
