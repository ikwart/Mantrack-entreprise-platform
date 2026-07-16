"""
generate_historical_telemetry.py

Bulk-generates the historical telemetry backfill (last 24 months, up to TODAY)
using the failure calendar + degradation curve engine. This becomes:
  - fact_telemetry_daily.csv    (one row per equipment per day - loads via COPY,
                                  same as the other fact tables)
  - telemetry_fault_events.csv  (event-level detail - not part of the warehouse
                                  schema itself, but useful for replaying through
                                  Kafka to demonstrate the streaming path, and for
                                  debugging/validating the simulation)

This is a BATCH backfill, not something you'd push through Kafka message-by-message
for 24 months of history - that's what telemetry_kafka_producer.py is for, covering
telemetry going forward from TODAY. Real telemetry platforms load historical data
in bulk and stream only new events; this mirrors that.
"""

import csv
import os
import random
from collections import defaultdict
from datetime import date, timedelta

import pandas as pd

from telemetry_engine import (
    COMPONENT_PARAMS, P_BASE_BACKGROUND, P_MAX_DEFAULT,
    logistic_ramp, draw_severity, draw_background_severity,
    sensor_reading, pick_fault_code_for_component, component_progress,
    component_weighted_choice,
)

random.seed(23)

SEED_DIR = "/home/claude/seed/csv"
KAFKA_DATA_DIR = "/home/claude/repo/mantrac-ghana-data-platform/kafka/data"
OUT_DIR = "/home/claude/repo/mantrac-ghana-data-platform/sql/transactions"
os.makedirs(KAFKA_DATA_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

TODAY = date(2026, 7, 8)
TELEMETRY_START = TODAY - timedelta(days=730)

# ---------------------------------------------------------------------------
# Load dimension data
# ---------------------------------------------------------------------------
equipment = pd.read_csv(f"{SEED_DIR}/dim_equipment.csv", parse_dates=["install_date"])
eligible = equipment[equipment["status"].isin(["Active", "Idle"])].copy()

fault_codes = pd.read_csv(f"{SEED_DIR}/dim_fault_code.csv")
bridge = pd.read_csv(f"{SEED_DIR}/bridge_faultcode_component.csv")

faultcode_component_map = defaultdict(list)
for _, row in bridge.iterrows():
    faultcode_component_map[row["component_id"]].append(
        (row["fault_code"], row["correlation_weight"], row["is_direct_indicator"])
    )

calendar = pd.read_csv(
    f"{KAFKA_DATA_DIR}/failure_calendar.csv",
    parse_dates=["window_start", "failure_date"]
)

windows_by_equipment = defaultdict(list)
for _, row in calendar.iterrows():
    is_false_start = row["event_type"] == "FalseStart"
    window_start = row["window_start"].date()
    window_end = window_start + timedelta(days=int(row["lead_window_days"]))
    windows_by_equipment[row["equipment_id"]].append({
        "component_id": row["component_id"],
        "window_start": window_start,
        "window_end": window_end,
        "lead_window_days": int(row["lead_window_days"]),
        "midpoint": row["midpoint"],
        "k": row["k"],
        "is_false_start": is_false_start,
        "false_start_cutoff": row["false_start_cutoff"] if is_false_start else None,
        "decay_days": row["false_start_decay_days"] if is_false_start else None,
    })

# ---------------------------------------------------------------------------
# Simulate
# ---------------------------------------------------------------------------
daily_rows = []
event_rows = []
event_id = 1

for _, eq in eligible.iterrows():
    eq_id = eq["equipment_id"]
    life_start = max(eq["install_date"].date(), TELEMETRY_START)
    windows = windows_by_equipment.get(eq_id, [])

    # starting engine-hours offset: rough proxy for hours accumulated before telemetry rollout
    pre_telemetry_days = max(0, (life_start - eq["install_date"].date()).days)
    engine_hours = pre_telemetry_days * random.uniform(4, 7)

    day = life_start
    while day <= TODAY:
        active = [w for w in windows if w["window_start"] <= day <= w["window_end"]]

        fault_counts = {"Info": 0, "Warning": 0, "Critical": 0}
        max_progress_today = 0.0

        for w in active:
            day_offset = (day - w["window_start"]).days
            progress = component_progress(
                day_offset, w["lead_window_days"], w["is_false_start"],
                w["false_start_cutoff"], w["decay_days"] or 6
            )
            max_progress_today = max(max_progress_today, progress)
            p_fault = logistic_ramp(progress, P_BASE_BACKGROUND, P_MAX_DEFAULT, w["midpoint"], w["k"])

            if random.random() < p_fault:
                severity = draw_severity(progress)
                fc = pick_fault_code_for_component(w["component_id"], severity, faultcode_component_map)
                if fc:
                    fault_counts[severity] += 1
                    event_rows.append([event_id, eq_id, day.isoformat(), fc, severity, w["component_id"]])
                    event_id += 1

        # background noise, independent of any active window
        if random.random() < P_BASE_BACKGROUND:
            comp = component_weighted_choice()
            severity = draw_background_severity()
            fc = pick_fault_code_for_component(comp, severity, faultcode_component_map)
            if fc:
                fault_counts[severity] += 1
                event_rows.append([event_id, eq_id, day.isoformat(), fc, severity, comp])
                event_id += 1

        # utilization / engine hours
        is_weekend = day.weekday() >= 5
        if eq["status"] == "Active":
            utilization = random.uniform(2, 5) if is_weekend else random.uniform(6, 10)
        else:  # Idle
            utilization = random.uniform(0, 2)
        engine_hours += utilization

        reading = sensor_reading(max_progress_today)
        max_reading = round(reading + random.uniform(0, 5 + 15 * max_progress_today), 2)

        daily_rows.append([
            eq_id, day.isoformat(), round(engine_hours, 1), round(utilization, 1),
            fault_counts["Info"], fault_counts["Warning"], fault_counts["Critical"],
            reading, max_reading
        ])

        day += timedelta(days=1)

# ---------------------------------------------------------------------------
# WRITE
# ---------------------------------------------------------------------------
with open(f"{OUT_DIR}/fact_telemetry_daily.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["equipment_id", "date_id", "engine_hours", "utilization_hours",
                      "fault_count_info", "fault_count_warning", "fault_count_critical",
                      "avg_sensor_reading", "max_sensor_reading"])
    writer.writerows(daily_rows)

with open(f"{KAFKA_DATA_DIR}/telemetry_fault_events.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["event_id", "equipment_id", "event_date", "fault_code", "severity", "component_id"])
    writer.writerows(event_rows)

print(f"fact_telemetry_daily rows: {len(daily_rows):,}")
print(f"telemetry_fault_events rows: {len(event_rows):,}")
print(f"Equipment simulated: {eligible.shape[0]}")
