-- ============================================================================
-- Mantrac Ghana Data Platform - Governance Layer
-- RBAC + Row-Level Security + Audit Logging
--
-- Run AFTER schema.sql and the seed/transaction loaders.
-- Usage: psql -h <host> -U mantrac_admin -d mantrac_dw -f governance.sql
--
-- Design summary:
--   Roles (least privilege, not superuser):
--     mantrac_etl              - read/write on all base tables (generators,
--                                 Spark jobs, pipeline scripts). No DDL rights.
--     mantrac_analyst          - read-only, all regions (HQ/exec analyst)
--     mantrac_analyst_western  - read-only, Western region only (branch demo)
--     mantrac_analyst_accra    - read-only, Greater Accra region only (branch demo)
--
--   Row-Level Security: region-scoped analyst roles see only customers (and
--   their sales/tax records) in their assigned region, enforced by Postgres
--   itself - not by application-layer filtering that a bug could bypass.
--
--   Audit log: every INSERT/UPDATE/DELETE on financially/operationally
--   sensitive tables is captured in security.audit_log - who, when, what
--   changed, before/after values.
--
-- IMPORTANT CAVEAT (documented here deliberately, not hidden):
--   RLS is applied on the PUBLIC schema base tables. dbt's marts
--   (analytics_marts.*) are materialized as TABLEs - a one-time snapshot
--   computed by mantrac_admin (bypassing RLS as the table owner during the
--   dbt run). That means RLS does NOT automatically carry through to a BI
--   tool querying the marts directly. For RLS to protect the BI layer too,
--   either (a) materialize RLS-sensitive marts as VIEWs instead of TABLEs so
--   they re-evaluate per querying user, or (b) keep region-scoped users
--   querying the RLS-protected base tables directly rather than the marts.
--   This script demonstrates RLS correctly at the base-table layer; wiring
--   it through to materialized marts is a follow-up, not done here.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS security;

-- ============================================================================
-- SECTION 1: ROLES
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mantrac_etl') THEN
        CREATE ROLE mantrac_etl LOGIN PASSWORD 'change_me_etl';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mantrac_analyst') THEN
        CREATE ROLE mantrac_analyst LOGIN PASSWORD 'change_me_analyst';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mantrac_analyst_western') THEN
        CREATE ROLE mantrac_analyst_western LOGIN PASSWORD 'change_me_western';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mantrac_analyst_accra') THEN
        CREATE ROLE mantrac_analyst_accra LOGIN PASSWORD 'change_me_accra';
    END IF;
END $$;

-- mantrac_etl: read/write on existing + FUTURE tables in public, no DDL
GRANT USAGE ON SCHEMA public TO mantrac_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO mantrac_etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO mantrac_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO mantrac_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO mantrac_etl;

-- mantrac_analyst (+ region-scoped variants): read-only on the RLS-protected
-- base tables used in this demo. A broader deployment would instead point
-- these at analytics_marts/analytics_staging (see caveat above) once those
-- are RLS-aware.
GRANT USAGE ON SCHEMA public TO mantrac_analyst, mantrac_analyst_western, mantrac_analyst_accra;
GRANT SELECT ON dim_customer, dim_industry, dim_site, fact_equipment_sales, fact_tax_filings
    TO mantrac_analyst, mantrac_analyst_western, mantrac_analyst_accra;

-- Grants on the actual dbt-built schemas - this is what a real BI tool (or
-- the LLM query agent, see llm/) queries day to day, not the raw public
-- tables above (those grants exist mainly for the RLS demonstration).
-- NOTE: as documented in docs/governance.md, RLS on the public-schema base
-- tables does NOT carry through to these materialized marts - anyone
-- granted access here sees ALL regions, not just their own. Only
-- mantrac_analyst (the unscoped HQ role) is granted here for that reason;
-- the region-scoped demo roles are deliberately NOT given mart access, so
-- they can't accidentally bypass their own RLS restriction through the
-- marts instead of the base tables.
GRANT USAGE ON SCHEMA analytics_marts, analytics_staging TO mantrac_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_marts TO mantrac_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_staging TO mantrac_analyst;

-- ============================================================================
-- SECTION 2: ROW-LEVEL SECURITY
-- ============================================================================

CREATE TABLE IF NOT EXISTS security.user_region_access (
    db_role     text NOT NULL,
    region      text NOT NULL,
    PRIMARY KEY (db_role, region)
);

-- mantrac_analyst (HQ/exec) sees every region explicitly - no special-case
-- "sees all" bypass logic, just enumerate every region so the same policy
-- predicate works uniformly for every role.
INSERT INTO security.user_region_access (db_role, region) VALUES
    ('mantrac_analyst', 'Western'), ('mantrac_analyst', 'Ashanti'),
    ('mantrac_analyst', 'Greater Accra'), ('mantrac_analyst', 'Eastern'),
    ('mantrac_analyst', 'Ahafo'), ('mantrac_analyst', 'Bono'),
    ('mantrac_analyst_western', 'Western'),
    ('mantrac_analyst_accra', 'Greater Accra')
ON CONFLICT DO NOTHING;

-- RLS policies below run their subqueries AS THE QUERYING USER, not as a
-- security-definer - so the analyst roles need direct SELECT on this table,
-- or every policy check would fail with "permission denied" rather than
-- silently restricting rows. Found by actually testing as each role, not by
-- reading the SQL - this is exactly the kind of bug that looks fine on paper.
GRANT USAGE ON SCHEMA security TO mantrac_analyst, mantrac_analyst_western, mantrac_analyst_accra;
GRANT SELECT ON security.user_region_access TO mantrac_analyst, mantrac_analyst_western, mantrac_analyst_accra;

ALTER TABLE dim_customer ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS customer_region_policy ON dim_customer;
CREATE POLICY customer_region_policy ON dim_customer
    FOR SELECT
    USING (
        region IN (SELECT region FROM security.user_region_access WHERE db_role = current_user)
    );

-- mantrac_etl is NOT the table owner (mantrac_admin is), so RLS applies to
-- it too - it is not automatically exempt the way an owner is. Without this
-- policy, ETL writes fail outright once RLS is enabled, since Postgres
-- denies any command with no matching policy for that operation. Found by
-- actually testing an UPDATE as mantrac_etl, not by reading the SQL - this
-- is exactly the kind of RLS gotcha that's easy to miss on paper.
DROP POLICY IF EXISTS etl_full_access_customer ON dim_customer;
CREATE POLICY etl_full_access_customer ON dim_customer
    FOR ALL TO mantrac_etl USING (true) WITH CHECK (true);

ALTER TABLE fact_equipment_sales ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS sales_region_policy ON fact_equipment_sales;
CREATE POLICY sales_region_policy ON fact_equipment_sales
    FOR SELECT
    USING (customer_id IN (SELECT customer_id FROM dim_customer));
    -- relies on dim_customer's own RLS policy applying inside this subquery -
    -- whatever customers THIS user can see in dim_customer is exactly the
    -- set their sales records are scoped to. One policy, reused correctly,
    -- rather than duplicating the region logic per table.

DROP POLICY IF EXISTS etl_full_access_sales ON fact_equipment_sales;
CREATE POLICY etl_full_access_sales ON fact_equipment_sales
    FOR ALL TO mantrac_etl USING (true) WITH CHECK (true);

ALTER TABLE fact_tax_filings ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tax_region_policy ON fact_tax_filings;
CREATE POLICY tax_region_policy ON fact_tax_filings
    FOR SELECT
    USING (customer_id IN (SELECT customer_id FROM dim_customer));

DROP POLICY IF EXISTS etl_full_access_tax ON fact_tax_filings;
CREATE POLICY etl_full_access_tax ON fact_tax_filings
    FOR ALL TO mantrac_etl USING (true) WITH CHECK (true);

-- mantrac_admin remains the table owner and bypasses RLS entirely by
-- default (standard Postgres behavior) - correct here since schema
-- management and full-table operations (loaders, migrations) run as admin.

-- ============================================================================
-- SECTION 3: AUDIT LOGGING
-- ============================================================================

CREATE TABLE IF NOT EXISTS security.audit_log (
    audit_id    bigserial PRIMARY KEY,
    table_name  text NOT NULL,
    operation   text NOT NULL,
    db_user     text NOT NULL,
    changed_at  timestamptz NOT NULL DEFAULT now(),
    old_data    jsonb,
    new_data    jsonb
);

-- IMPORTANT: this function is SECURITY DEFINER (needed so roles with no
-- direct INSERT grant on security.audit_log can still have their actions
-- logged). Inside a SECURITY DEFINER function, current_user resolves to the
-- FUNCTION OWNER, not the actual caller - using current_user here would
-- make every audit entry falsely say "mantrac_admin" regardless of who
-- really did it, silently defeating the entire point of an audit trail.
-- session_user correctly reflects the actual connecting role. Caught by
-- testing an UPDATE as mantrac_etl and finding the log blamed mantrac_admin
-- instead - exactly the kind of bug that looks fine until you check who it
-- actually says did it.
CREATE OR REPLACE FUNCTION security.audit_trigger_fn() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO security.audit_log (table_name, operation, db_user, old_data)
        VALUES (TG_TABLE_NAME, TG_OP, session_user, to_jsonb(OLD));
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO security.audit_log (table_name, operation, db_user, old_data, new_data)
        VALUES (TG_TABLE_NAME, TG_OP, session_user, to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSE -- INSERT
        INSERT INTO security.audit_log (table_name, operation, db_user, new_data)
        VALUES (TG_TABLE_NAME, TG_OP, session_user, to_jsonb(NEW));
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Applied to financially/operationally sensitive tables. Extendable to any
-- other table by repeating this pattern - deliberately not applied
-- platform-wide, since auditing every dimension table (e.g. dim_fault_code)
-- would just add noise without real governance value.
DROP TRIGGER IF EXISTS audit_fact_tax_filings ON fact_tax_filings;
CREATE TRIGGER audit_fact_tax_filings
    AFTER INSERT OR UPDATE OR DELETE ON fact_tax_filings
    FOR EACH ROW EXECUTE FUNCTION security.audit_trigger_fn();

DROP TRIGGER IF EXISTS audit_fact_equipment_sales ON fact_equipment_sales;
CREATE TRIGGER audit_fact_equipment_sales
    AFTER INSERT OR UPDATE OR DELETE ON fact_equipment_sales
    FOR EACH ROW EXECUTE FUNCTION security.audit_trigger_fn();

DROP TRIGGER IF EXISTS audit_dim_customer ON dim_customer;
CREATE TRIGGER audit_dim_customer
    AFTER INSERT OR UPDATE OR DELETE ON dim_customer
    FOR EACH ROW EXECUTE FUNCTION security.audit_trigger_fn();

DROP TRIGGER IF EXISTS audit_ml_maintenance_signals ON ml_maintenance_signals;
CREATE TRIGGER audit_ml_maintenance_signals
    AFTER INSERT OR UPDATE OR DELETE ON ml_maintenance_signals
    FOR EACH ROW EXECUTE FUNCTION security.audit_trigger_fn();

-- audit_log itself: only admin can read it. Analysts and even the ETL role
-- should not be able to read or tamper with the audit trail.
REVOKE ALL ON security.audit_log FROM PUBLIC;
GRANT SELECT ON security.audit_log TO mantrac_admin;

-- ============================================================================
-- SECTION 4: COLUMN-LEVEL ENCRYPTION (pgcrypto) - PATTERN DEMONSTRATION
--
-- This dataset doesn't contain genuine PII (no SSNs, personal contact info,
-- etc. - technician names are the closest thing, and they're synthetic).
-- Included anyway to demonstrate the pattern a regulated deployment with
-- real personal data would need: column-level encryption at rest, not just
-- relying on disk/volume encryption. Applied here to a small demo table
-- rather than retrofitted onto real columns, to avoid implying our
-- synthetic technician names are sensitive when they aren't.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS security.encryption_pattern_demo (
    demo_id         serial PRIMARY KEY,
    label           text,
    sensitive_value bytea  -- encrypted at the column level, not just at rest via disk encryption
);

-- Example usage (not run automatically - shown as the pattern to follow):
-- INSERT INTO security.encryption_pattern_demo (label, sensitive_value)
--     VALUES ('example', pgp_sym_encrypt('sensitive text here', 'encryption-key-from-secrets-manager'));
-- SELECT label, pgp_sym_decrypt(sensitive_value, 'encryption-key-from-secrets-manager')
--     FROM security.encryption_pattern_demo;
--
-- In production, the encryption key would come from a secrets manager
-- (AWS Secrets Manager / Azure Key Vault / GCP Secret Manager), never
-- hardcoded in SQL or application code.

-- ============================================================================
-- SECTION 5: CONNECTION-LEVEL ENCRYPTION (documented, not executable here)
--
-- In-transit encryption: enforced via pg_hba.conf requiring `hostssl`
-- entries (reject plaintext `host` connections) and a valid server
-- certificate - a local Docker Postgres instance doesn't typically have a
-- signed cert configured, so this isn't demonstrated executably in this
-- repo, but the docker-compose Postgres services should have
-- POSTGRES_INITDB_ARGS or a mounted server.crt/server.key for any
-- deployment handling real data, with sslmode=require (or verify-full) on
-- every client connection string.
--
-- At-rest encryption: in a real cloud deployment this is normally handled
-- by the managed database service (RDS/Cloud SQL/Azure Database encryption
-- at rest, enabled by default on most managed offerings) rather than
-- application-level Postgres config - noted here so the governance story is
-- complete, not because there's a local command to run for it.
-- ============================================================================
