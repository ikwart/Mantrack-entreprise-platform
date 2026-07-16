# LLM / Agentic Layer: Natural-Language Warehouse Queries

`llm/query_agent.py` lets you ask plain-English questions about the fleet, finances, predictive maintenance, or inventory, and get a real answer backed by real SQL against the dbt marts.

```bash
cd llm
pip install -r requirements.txt
export ANTHROPIC_API_KEY=<your key>
export DW_DB_PASSWORD=<mantrac_analyst's password, from governance.sql>
python query_agent.py "How many Mining machines are at Action Recommended risk right now?"
```

## How it actually works

1. **Schema introspection, live, every run** (`schema_introspection.py`) — queries Postgres's own catalog (`information_schema` + `pg_description`) for every table/column in `analytics_marts` and `analytics_staging`, including real descriptions. Those descriptions are the exact same ones written in the dbt `schema.yml` files — `dbt_project.yml`'s `persist_docs` config writes them as genuine `COMMENT ON TABLE/COLUMN` statements when you run `dbt run`. The lineage docs (`docs/lineage.md`) and this agent read from the same source of truth; they can't silently drift apart.

2. **The agent connects as `mantrac_analyst`** — the same read-only role from the governance layer (`sql/governance.sql`), not a separate credential invented for this layer. Whatever that role can and can't see is exactly what the agent can and can't see.

3. **Two independent layers of write protection**: `mantrac_analyst` has no `INSERT`/`UPDATE`/`DELETE` grants at the database level — a destructive query would fail there regardless. The agent *also* rejects any non-`SELECT` statement before it's ever sent to Postgres. Neither layer depends on the other; that's deliberate defense in depth, not redundancy for its own sake.

4. **Genuinely multi-step** — the agent can run more than one query per question through a normal Claude tool-use loop (capped at 5 turns as a safety bound), not a fixed single-shot prompt-to-SQL pipeline.

## What's validated, and what isn't

Tested for real against the live warehouse:
- Schema introspection returns real, current, accurate table/column descriptions (and caught a real bug in the process — see below)
- SQL execution against `mantrac_analyst` returns real query results (confirmed against actual data: Mining industry revenue ~GHS 8.86B)
- Both layers of write-protection independently block a destructive query before it can do anything

**Not tested**: the actual Claude API call and tool-use loop. This sandbox has no Anthropic API key available, so the full agentic loop (question → tool call → SQL → result → answer) has not been run end to end. Confirm it works the first time you run this with your own key — everything except the model call itself has already been proven correct.

## A real bug this surfaced, worth knowing about

Building this caught a stale table comment: `mart_finance` still said "one row per customer per **quarter**" in its dbt description, left over from before we redesigned it to monthly grain earlier in this project. That's exactly the kind of thing that would have quietly fed the LLM agent wrong context about the data's grain — potentially causing it to write incorrect aggregations (e.g., double-counting revenue by summing quarterly figures that were actually already monthly). Fixed the description and reran `dbt run` to persist the correction. A second gap surfaced alongside it: `mantrac_analyst` had been *discussed* as having mart access in the governance docs, but the grant had never actually been executed — schema introspection came back completely empty until that was fixed too. Both bugs were only visible by actually running the introspection against live Postgres, not by reading the SQL and judging it correct.

**A third, more consequential bug surfaced right after fixing the first two**: granting `mantrac_analyst` access and then re-running `dbt run` broke access again immediately. Table-materialized dbt models are dropped and recreated on every run — which silently wipes any manually-applied `GRANT` along with the old table. Without dbt's native `grants:` config (now set in `dbt_project.yml` for the marts), *every single future `dbt run` in production would have re-locked the LLM agent and any BI tool out of the data*, requiring someone to notice and manually re-grant access each time. Fixed by using dbt's built-in grants management instead of a manual one-time `GRANT` statement — confirmed by rebuilding the mart twice in a row and proving access survived both times, not just the first.

## Grain matters for this to work reliably

`mart_finance` is customer-*month* grain, not customer grain — `COUNT(*)` counts customer-months, not distinct customers. This is exactly why the schema descriptions matter: without knowing the grain, a text-to-SQL agent (or a human) can silently miscount. The system prompt includes each mart's grain explicitly in its description for this reason.
