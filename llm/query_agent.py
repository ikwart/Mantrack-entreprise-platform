"""
query_agent.py

Natural-language query agent for the Mantrac Ghana warehouse. Ask a question
in plain English ("How many Mining machines are at Action Recommended risk
right now?"), the agent writes SQL grounded in the REAL schema (see
schema_introspection.py), runs it, and answers conversationally.

Design choices worth knowing:

  1. GROUNDED, NOT GUESSING: the system prompt is built live from Postgres's
     own catalog every time the agent starts - the same descriptions dbt
     persisted via `persist_docs`. If a column gets renamed or a mart's
     grain changes, the agent's context updates automatically the next time
     someone runs `dbt run` - it can't drift out of sync with a stale
     hand-written schema doc the way a hardcoded prompt would.

  2. CONNECTS AS mantrac_analyst, NOT admin: reuses the read-only role from
     the governance layer (sql/governance.sql) rather than inventing a
     separate credential/permission model for the LLM layer. Whatever that
     role can and can't see, the agent can and can't see - governance and
     the agentic layer reinforce each other instead of being two unrelated
     concerns bolted together.

  3. DEFENSE IN DEPTH ON SQL SAFETY: mantrac_analyst is already a read-only
     Postgres role (no INSERT/UPDATE/DELETE grants), so a destructive query
     would fail at the database level regardless. This script ALSO rejects
     any non-SELECT statement before ever sending it to Postgres - not
     because the DB grant isn't sufficient on its own, but because a second,
     independent check at the application layer is exactly the kind of
     defense-in-depth a regulated deployment should have, not either/or.

  4. MULTI-STEP (genuinely agentic): the agent can run more than one query
     per question if it decides it needs to - e.g. looking up a customer_id
     before filtering by it - via a normal Claude tool-use loop, not a
     single fixed prompt-to-SQL-to-answer pipeline.

KNOWN LIMITATION: this has NOT been tested against a live Anthropic API call
in development - this sandbox has no API key available. The schema
introspection and SQL execution/safety-check logic (everything except the
actual model call) WAS tested against the live warehouse. Confirm the full
loop works the first time you run this with your own ANTHROPIC_API_KEY.

Usage:
  export ANTHROPIC_API_KEY=<your key>
  export DW_DB_PASSWORD=<mantrac_analyst's password>
  python query_agent.py "How many Mining machines are at Action Recommended risk?"
"""

import argparse
import os
import sys

import anthropic
import psycopg2

from schema_introspection import get_schema_context

MODEL = "claude-sonnet-4-5"
MAX_TURNS = 5  # cap the agentic loop - a well-scoped warehouse question
                # shouldn't need more than a couple of query steps; this is
                # a safety bound against a runaway loop, not a tuned limit


def get_conn_params():
    return {
        "host": os.environ.get("DBT_DW_HOST", "localhost"),
        "port": os.environ.get("DBT_DW_PORT", "5432"),
        "user": os.environ.get("DW_DB_USER", "mantrac_analyst"),
        "password": os.environ.get("DW_DB_PASSWORD", "change_me_analyst"),
        "dbname": os.environ.get("DW_DB_NAME", "mantrac_dw"),
    }


def is_safe_select(sql: str) -> bool:
    """Application-layer defense in depth - see design note #3 above. The
    database role's own read-only grants are the real enforcement; this is
    a second, independent check, not a replacement for it."""
    normalized = sql.strip().lower()
    if not normalized.startswith("select"):
        return False
    forbidden = ["insert ", "update ", "delete ", "drop ", "alter ", "truncate ",
                 "grant ", "revoke ", "create ", ";--", "/*"]
    return not any(f in normalized for f in forbidden)


def execute_sql(sql: str, conn_params: dict) -> str:
    if not is_safe_select(sql):
        return "ERROR: only single SELECT statements are permitted."
    try:
        conn = psycopg2.connect(**conn_params)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchmany(200)  # cap result size returned to the model
                if not rows:
                    return "Query returned no rows."
                lines = [", ".join(columns)]
                for row in rows:
                    lines.append(", ".join(str(v) for v in row))
                return "\n".join(lines)
        finally:
            conn.close()
    except Exception as e:
        # Surface the real Postgres error back to the model - it can often
        # self-correct a bad column name or syntax error on the next turn,
        # which is exactly the kind of thing a multi-step agent should do
        # rather than just failing outright.
        return f"SQL ERROR: {e}"


TOOLS = [
    {
        "name": "run_sql",
        "description": (
            "Run a single read-only SELECT query against the Mantrac Ghana "
            "warehouse (Postgres) and return the results as CSV-formatted "
            "text. Only SELECT statements are permitted."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "The SELECT statement to run."}
            },
            "required": ["sql"],
        },
    }
]


def build_system_prompt(schema_context: str) -> str:
    return f"""You are a data analyst assistant for Mantrac Ghana, the authorized \
Caterpillar dealer in Ghana. You answer questions about the company's fleet, \
finances, predictive maintenance, and inventory by querying the warehouse \
below via the run_sql tool.

Rules:
- Only run SELECT statements. Never attempt to modify data.
- The tables below are the ONLY ones you have access to - all monetary \
amounts are in GHS unless stated otherwise (see column descriptions).
- If a question is ambiguous (e.g. "recently" without a date range), pick a \
reasonable interpretation and say so in your answer rather than asking a \
clarifying question first.
- After getting query results, answer in plain, direct language - not a \
data dump. Cite the actual numbers you found.
- You may run more than one query if the first one doesn't fully answer the \
question (e.g. looking up an ID before filtering by it).

WAREHOUSE SCHEMA (introspected live from Postgres - always current):
{schema_context}
"""


def ask(question: str) -> str:
    conn_params = get_conn_params()
    schema_context = get_schema_context(conn_params)
    system_prompt = build_system_prompt(schema_context)

    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env
    messages = [{"role": "user", "content": question}]

    for turn in range(MAX_TURNS):
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=system_prompt,
            tools=TOOLS,
            messages=messages,
        )

        if response.stop_reason != "tool_use":
            # model produced a final text answer
            return "".join(block.text for block in response.content if block.type == "text")

        messages.append({"role": "assistant", "content": response.content})
        tool_results = []
        for block in response.content:
            if block.type == "tool_use" and block.name == "run_sql":
                sql = block.input["sql"]
                result = execute_sql(sql, conn_params)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })
        messages.append({"role": "user", "content": tool_results})

    return "I wasn't able to fully answer that within the allotted number of query steps - try narrowing the question."


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("question", nargs="+", help="Question to ask, in plain English")
    args = parser.parse_args()
    question = " ".join(args.question)

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: set ANTHROPIC_API_KEY before running this.", file=sys.stderr)
        sys.exit(1)

    answer = ask(question)
    print(answer)


if __name__ == "__main__":
    main()
