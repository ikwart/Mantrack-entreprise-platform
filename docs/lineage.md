# Data Lineage

The dbt project generates a real, interactive lineage graph directly from the SQL in `models/` — not a hand-drawn diagram that can drift out of sync with the actual code.

## Generate and view it

```bash
cd dbt/mantrac_dw
export DBT_DW_HOST=localhost DBT_DW_PORT=5433 \
       DW_DB_USER=mantrac_admin DW_DB_PASSWORD=<your password> DW_DB_NAME=mantrac_dw
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

Opens at `http://localhost:8080` (dbt's default docs port). Click any model to see its full lineage — everything upstream that feeds it, everything downstream that depends on it — plus column-level descriptions, tests, and the compiled SQL.

`target/` (where the generated site lives) is gitignored deliberately — it's a build artifact regenerated on demand, not something to commit and let go stale.

## What's actually in the graph

44 nodes total: 25 sources, 19 staging models, 3 intermediate models, 5 marts. Every staging and intermediate model has a real description (see `models/staging/_staging__schema.yml` and `models/intermediate/_intermediate__schema.yml`) — not just the marts, which is what makes the generated docs actually useful to read rather than a bare unlabeled DAG.

As a concrete example, `mart_finance`'s real dependency chain, straight from the manifest dbt generates from the SQL itself:

```
mart_finance
  ├── int_revenue_by_stream_month
  │     ├── stg_equipment_sales
  │     ├── stg_rental_contracts
  │     └── stg_maintenance_events
  ├── int_tax_allocated_to_month
  │     ├── stg_tax_filings
  │     └── int_revenue_by_stream_month
  ├── stg_customers
  └── stg_industries
```

That's exactly the monthly-revenue-plus-allocated-quarterly-tax design from `docs/architecture.md` — the lineage graph is proof the implementation actually matches the design, not just a claim about it.

## Why this matters beyond "nice to have"

For a warehouse this size, lineage answers the two questions that actually matter in practice:

- **"If I change `stg_tax_filings`, what breaks?"** — the graph shows every downstream model that would be affected, before you make the change, not after.
- **"Where does this number in the dashboard actually come from?"** — trace `mart_finance.total_output_tax_allocated` backward through `int_tax_allocated_to_month` to `stg_tax_filings` to the raw `fact_tax_filings` table, with the transformation logic visible at every step.
