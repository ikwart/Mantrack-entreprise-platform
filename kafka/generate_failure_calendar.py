"""
generate_failure_calendar.py

Produces the ground-truth failure calendar that drives telemetry simulation:
  - Planned failures per machine (component, target date, lead window)
  - False starts (partial degradation ramps that resolve without failure)

This file is a SIMULATION ARTIFACT, not part of the production warehouse
schema - a real telemetry platform wouldn't have this table (if it did, you
wouldn't need a predictive model!). We keep it so that once the Spark scoring
job runs against the simulated telemetry, we can objectively measure model
precision/recall and lead-time-delivered against a known ground truth,
instead of hand-waving the "our model works" claim.

Output: kafka/data/failure_calendar.csv
"""

import csv
import os
import random
from datetime import date, timedelta

import pandas as pd

from telemetry_engine import COMPONENT_PARAMS, component_weighted_choice

random.seed(11)

SEED_DIR = "/home/claude/seed/csv"
OUT_DIR = "/home/claude/repo/mantrac-ghana-data-platform/kafka/data"
os.makedirs(OUT_DIR, exist_ok=True)

TODAY = date(2026, 7, 8)
TELEMETRY_START = TODAY - timedelta(days=730)  # 24 months of telemetry history
FAILURES_PER_YEAR_RANGE = (1.5, 3.5)
FALSE_START_RATE = 0.35  # false starts generated as a fraction of real failure count

equipment = pd.read_csv(f"{SEED_DIR}/dim_equipment.csv", parse_dates=["install_date"])
eligible = equipment[equipment["status"].isin(["Active", "Idle"])].copy()

rows = []
calendar_id = 1

for _, eq in eligible.iterrows():
    telemetry_life_start = max(eq["install_date"].date(), TELEMETRY_START)
    window_days = (TODAY - telemetry_life_start).days
    if window_days < 30:
        continue  # not enough history to plan any failures

    years_covered = window_days / 365.0
    n_failures = max(0, round(random.uniform(*FAILURES_PER_YEAR_RANGE) * years_covered))
    n_false_starts = round(n_failures * FALSE_START_RATE)

    # --- real planned failures -------------------------------------------
    for _ in range(n_failures):
        component_id = component_weighted_choice()
        params = COMPONENT_PARAMS[component_id]
        lead_window_days = random.randint(*params["lead_window"])

        # failure date must land within the telemetry window (leave room for the lead window)
        earliest = telemetry_life_start + timedelta(days=lead_window_days + 1)
        if earliest >= TODAY:
            continue
        failure_date = earliest + timedelta(days=random.randint(0, (TODAY - earliest).days))
        window_start = failure_date - timedelta(days=lead_window_days)

        rows.append([
            calendar_id, eq["equipment_id"], component_id, "Failure",
            window_start.isoformat(), failure_date.isoformat(), lead_window_days,
            params["midpoint"], params["k"], "", ""
        ])
        calendar_id += 1

    # --- false starts -------------------------------------------------------
    for _ in range(n_false_starts):
        component_id = component_weighted_choice()
        params = COMPONENT_PARAMS[component_id]
        lead_window_days = random.randint(*params["lead_window"])
        false_start_cutoff = round(random.uniform(0.4, 0.75), 2)

        earliest = telemetry_life_start
        latest = TODAY - timedelta(days=lead_window_days)
        if latest <= earliest:
            continue
        window_start = earliest + timedelta(days=random.randint(0, (latest - earliest).days))

        rows.append([
            calendar_id, eq["equipment_id"], component_id, "FalseStart",
            window_start.isoformat(), "", lead_window_days,
            params["midpoint"], params["k"], false_start_cutoff, random.randint(4, 8)
        ])
        calendar_id += 1

with open(f"{OUT_DIR}/failure_calendar.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["calendar_id", "equipment_id", "component_id", "event_type",
                      "window_start", "failure_date", "lead_window_days",
                      "midpoint", "k", "false_start_cutoff", "false_start_decay_days"])
    writer.writerows(rows)

n_failures_total = sum(1 for r in rows if r[3] == "Failure")
n_false_starts_total = sum(1 for r in rows if r[3] == "FalseStart")
print(f"Equipment eligible for telemetry: {len(eligible)}")
print(f"Planned failures:   {n_failures_total}")
print(f"False starts:       {n_false_starts_total}")
print(f"Total calendar rows: {len(rows)}")
print(f"Written to {OUT_DIR}/failure_calendar.csv")
