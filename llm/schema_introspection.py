"""
schema_introspection.py

Builds the schema context the LLM agent uses to write SQL - pulled LIVE from
Postgres's own catalog (information_schema + pg_description), not from a
hand-maintained string that could drift out of sync with the real schema.

The descriptions being introspected here are the same ones written in
dbt/mantrac_dw/models/*/. dbt's `persist_docs` config (dbt_project.yml)
writes them as real Postgres COMMENT ON TABLE/COLUMN statements when you run
`dbt run` - so this module and the dbt lineage docs are reading the exact
same source of truth, not two things that could disagree.

Only introspects analytics_marts and analytics_staging - the agent should
reason about the curated, documented layer, not raw operational tables it
was never given descriptions for.
"""

import psycopg2

SCHEMAS_TO_INTROSPECT = ["analytics_marts", "analytics_staging"]


def get_schema_context(conn_params: dict) -> str:
    """Returns a formatted string describing every table/column in scope,
    including real descriptions where they exist - suitable for dropping
    straight into an LLM system prompt."""

    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    c.table_schema,
                    c.table_name,
                    obj_description(
                        (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass
                    ) AS table_comment,
                    c.column_name,
                    c.data_type,
                    col_description(
                        (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass,
                        c.ordinal_position
                    ) AS column_comment
                FROM information_schema.columns c
                WHERE c.table_schema = ANY(%s)
                ORDER BY c.table_schema, c.table_name, c.ordinal_position;
            """, (SCHEMAS_TO_INTROSPECT,))
            rows = cur.fetchall()
    finally:
        conn.close()

    tables = {}
    for schema, table, table_comment, col, dtype, col_comment in rows:
        key = f"{schema}.{table}"
        if key not in tables:
            tables[key] = {"comment": table_comment, "columns": []}
        tables[key]["columns"].append((col, dtype, col_comment))

    lines = []
    for table_name, info in tables.items():
        lines.append(f"\nTABLE {table_name}")
        if info["comment"]:
            lines.append(f"  Description: {info['comment']}")
        for col, dtype, col_comment in info["columns"]:
            suffix = f" - {col_comment}" if col_comment else ""
            lines.append(f"  - {col} ({dtype}){suffix}")

    return "\n".join(lines)


if __name__ == "__main__":
    # Quick standalone check: run this file directly to print what the agent
    # would actually see - useful for confirming descriptions look right
    # before wiring up the LLM call.
    import os
    conn_params = {
        "host": os.environ.get("DBT_DW_HOST", "localhost"),
        "port": os.environ.get("DBT_DW_PORT", "5432"),
        "user": os.environ.get("DW_DB_USER", "mantrac_analyst"),
        "password": os.environ.get("DW_DB_PASSWORD", "change_me_analyst"),
        "dbname": os.environ.get("DW_DB_NAME", "mantrac_dw"),
    }
    print(get_schema_context(conn_params))
