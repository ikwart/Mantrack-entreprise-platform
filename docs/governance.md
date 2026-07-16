# Governance: RBAC, Row-Level Security, and Audit Logging

`sql/governance.sql` adds a real, tested governance layer on top of the warehouse — roles, row-level security, and audit logging. Run it after `schema.sql` and the seed/transaction loaders:

```bash
psql -h <host> -U mantrac_admin -d mantrac_dw -f sql/governance.sql
```

It's idempotent — safe to rerun.

---

## Roles

| Role | Purpose | Access |
|---|---|---|
| `mantrac_admin` | Schema owner, migrations, full control | Superuser-equivalent, owns all tables, bypasses RLS by default (standard Postgres owner behavior) |
| `mantrac_etl` | Generators, Spark jobs, pipeline scripts | Read/write on all base tables, no DDL rights, no superuser |
| `mantrac_analyst` | HQ/exec analyst | Read-only, all regions |
| `mantrac_analyst_western` | Branch-scoped demo | Read-only, Western region only |
| `mantrac_analyst_accra` | Branch-scoped demo | Read-only, Greater Accra region only |

None of the non-admin roles are superuser — least privilege throughout.

## Row-Level Security

`dim_customer`, `fact_equipment_sales`, and `fact_tax_filings` have RLS enabled. A customer's `region` column drives visibility: `security.user_region_access` maps each role to the regions it's allowed to see, and the region-scoped analyst roles genuinely only see their own region's rows — verified live:

```
mantrac_analyst (HQ)          -> 18 customers (all)
mantrac_analyst_western       -> 5 customers  (Western only)
mantrac_analyst_accra         -> 7 customers  (Greater Accra only)

fact_equipment_sales:
mantrac_analyst_western       -> 90 of 335 sales rows
mantrac_analyst_accra         -> 145 of 335 sales rows
```

The `fact_equipment_sales`/`fact_tax_filings` policies don't duplicate the region logic — they reuse `dim_customer`'s own RLS by subquerying `customer_id IN (SELECT customer_id FROM dim_customer)`. Whatever customers a role can see in `dim_customer` is exactly the set their sales/tax records are scoped to.

## Audit Logging

`security.audit_log` captures every INSERT/UPDATE/DELETE on `fact_tax_filings`, `fact_equipment_sales`, `dim_customer`, and `ml_maintenance_signals` — who, when, and the full before/after row as JSON. Only `mantrac_admin` can read it; not even `mantrac_etl` can see or tamper with the trail it's subject to.

## Column-level encryption (pattern demo)

`security.encryption_pattern_demo` shows the `pgcrypto` pattern (`pgp_sym_encrypt`/`pgp_sym_decrypt`) for column-level encryption at rest. This dataset has no genuine PII to justify applying it to a real column — technician names are synthetic — so it's demonstrated on a standalone table rather than retrofitted onto data that doesn't actually need it. In a real deployment handling personal data, the encryption key would come from a secrets manager (AWS Secrets Manager / Azure Key Vault / GCP Secret Manager), never hardcoded in SQL.

Connection-level (in-transit) and at-rest encryption are documented in the script's comments rather than demonstrated executably — they depend on server certificates and managed-database configuration that don't apply cleanly to a local Docker Postgres instance.

---

## Three real bugs this surfaced (found by actually testing, not by reading the SQL)

Worth keeping as a record, since each is a genuinely instructive governance gotcha:

1. **RLS policy subqueries run as the querying user, not as a privileged context.** The region-scoped analyst roles initially got `permission denied for table user_region_access` on every query — the policy predicate references that table, but the roles had never been granted `SELECT` on it. A policy can be logically correct and still fail outright without the matching grants on everything the policy predicate touches.

2. **`mantrac_etl` is not a table owner, so it IS subject to RLS.** It's easy to assume "the ETL role probably bypasses this like admin does" — it doesn't. Only the actual table *owner* bypasses RLS by default. Without an explicit `FOR ALL TO mantrac_etl USING (true)` policy, every ETL write failed once RLS was enabled, because Postgres denies any command with no matching policy for that operation.

3. **The audit log initially blamed the wrong user for every action.** The trigger function is `SECURITY DEFINER` (needed so roles without direct `INSERT` rights on `security.audit_log` can still be logged) — but inside a `SECURITY DEFINER` function, `current_user` resolves to the function's *owner*, not the actual caller. Every audit entry was silently recording `mantrac_admin` regardless of who really performed the action, which defeats the entire purpose of an audit trail. Fixed by using `session_user` instead, which correctly reflects the real connecting role — confirmed by testing an UPDATE as `mantrac_etl` and checking the log actually said so.

None of these three would have been caught by reading the SQL and judging it "looks right" — each one only surfaced by connecting as each role and actually running the operations the role is supposed to perform (and the ones it's supposed to be blocked from).

---

## Known gap: RLS doesn't currently reach the dbt marts

RLS is applied on the `public` schema base tables. The dbt marts in `analytics_marts` are materialized as **tables** — a one-time snapshot computed by `mantrac_admin` during `dbt run`, which bypasses RLS as the table owner. That means a BI tool querying `analytics_marts.mart_finance` directly would see everything, regardless of RLS on the underlying `dim_customer`/`fact_equipment_sales`.

To close this gap:
- **Option A**: materialize RLS-sensitive marts as **views** instead of tables, so they re-evaluate per querying user
- **Option B**: keep region-scoped BI users querying the RLS-protected base tables directly rather than the marts
- **Option C**: parameterize marts by role/region at the BI layer instead of the warehouse layer

Not implemented here — documented as the honest next step rather than silently assumed to already work.
