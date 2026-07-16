"""
telemetry_kafka_producer.py

Publishes ONE simulated day of fleet telemetry per run, going forward from
TODAY, to Kafka topics:
  - equipment.fault_events    (one message per fault code emitted)
  - equipment.telemetry_daily (one aggregate message per equipment per day)

This is the live/streaming counterpart to generate_historical_telemetry.py.
In production this script is what you'd run on a schedule (e.g. an Airflow
task, or a simple cron/loop) to simulate new telemetry arriving daily -
mirroring how real equipment would phone home to a platform like VisionLink.

Usage:
  # Against a real Kafka broker (e.g. the docker-compose stack):
  python telemetry_kafka_producer.py --mode kafka --bootstrap-servers localhost:9092

  # Local dry run - no Kafka required, writes messages to a CSV instead so you
  # can inspect/validate output before wiring up a real broker:
  python telemetry_kafka_producer.py --mode local

  # Loop continuously, simulating one new day every N seconds (demo mode):
  python telemetry_kafka_producer.py --mode kafka --loop --sleep-seconds 5
"""

import argparse
import csv
import json
import os
import random
import time
from collections import defaultdict
from datetime import date, timedelta

import pandas as pd

from telemetry_engine import (
    P_BASE_BACKGROUND, P_MAX_DEFAULT,
    logistic_ramp, draw_severity, draw_background_severity,
    sensor_reading, pick_fault_code_for_component, component_progress,
    component_weighted_choice,
)

SEED_DIR = "/home/claude/seed/csv"
KAFKA_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
LOCAL_OUT_DIR = os.path.join(KAFKA_DATA_DIR, "live_dry_run")

FAULT_TOPIC = "equipment.fault_events"
DAILY_TOPIC = "equipment.telemetry_daily"

TODAY_ANCHOR = date(2026, 7, 8)  # simulation's "real" current date


def load_reference_data():
    equipment = pd.read_csv(f"{SEED_DIR}/dim_equipment.csv", parse_dates=["install_date"])
    eligible = equipment[equipment["status"].isin(["Active", "Idle"])].copy()

    bridge = pd.read_csv(f"{SEED_DIR}/bridge_faultcode_component.csv")
    faultcode_component_map = defaultdict(list)
    for _, row in bridge.iterrows():
        faultcode_component_map[row["component_id"]].append(
            (row["fault_code"], row["correlation_weight"], row["is_direct_indicator"])
        )

    calendar = pd.read_csv(f"{KAFKA_DATA_DIR}/failure_calendar.csv",
                            parse_dates=["window_start", "failure_date"])
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

    return eligible, faultcode_component_map, windows_by_equipment


def simulate_one_day(sim_date, eligible, faultcode_component_map, windows_by_equipment):
    """Yields (fault_events, daily_aggregates) for a single simulated day across the fleet."""
    fault_events = []
    daily_aggregates = []

    for _, eq in eligible.iterrows():
        eq_id = eq["equipment_id"]
        if eq["install_date"].date() > sim_date:
            continue  # not yet installed

        windows = windows_by_equipment.get(eq_id, [])
        active = [w for w in windows if w["window_start"] <= sim_date <= w["window_end"]]

        fault_counts = {"Info": 0, "Warning": 0, "Critical": 0}
        max_progress_today = 0.0

        for w in active:
            day_offset = (sim_date - w["window_start"]).days
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
                    # NOTE: component_id is deliberately NOT included in the published
                    # message. Real telemetry reports a fault code, not a pre-labeled
                    # component - that attribution is exactly what bridge_faultcode_component
                    # + the Spark scoring job infer downstream. Including it here would
                    # leak simulation ground truth into what should be an honest signal.
                    fault_events.append({
                        "equipment_id": int(eq_id), "event_date": sim_date.isoformat(),
                        "fault_code": fc, "severity": severity,
                    })

        if random.random() < P_BASE_BACKGROUND:
            comp = component_weighted_choice()
            severity = draw_background_severity()
            fc = pick_fault_code_for_component(comp, severity, faultcode_component_map)
            if fc:
                fault_counts[severity] += 1
                fault_events.append({
                    "equipment_id": int(eq_id), "event_date": sim_date.isoformat(),
                    "fault_code": fc, "severity": severity,
                })

        is_weekend = sim_date.weekday() >= 5
        if eq["status"] == "Active":
            utilization = random.uniform(2, 5) if is_weekend else random.uniform(6, 10)
        else:
            utilization = random.uniform(0, 2)

        reading = sensor_reading(max_progress_today)
        max_reading = round(reading + random.uniform(0, 5 + 15 * max_progress_today), 2)

        daily_aggregates.append({
            "equipment_id": int(eq_id), "date_id": sim_date.isoformat(),
            "utilization_hours": round(utilization, 1),
            "fault_count_info": fault_counts["Info"],
            "fault_count_warning": fault_counts["Warning"],
            "fault_count_critical": fault_counts["Critical"],
            "avg_sensor_reading": reading, "max_sensor_reading": max_reading,
        })

    return fault_events, daily_aggregates


def publish_kafka(fault_events, daily_aggregates, bootstrap_servers):
    from kafka import KafkaProducer  # imported lazily so --mode local needs no kafka-python install

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )
    for event in fault_events:
        producer.send(FAULT_TOPIC, key=event["equipment_id"], value=event)
    for agg in daily_aggregates:
        producer.send(DAILY_TOPIC, key=agg["equipment_id"], value=agg)
    producer.flush()
    print(f"Published {len(fault_events)} fault events + {len(daily_aggregates)} daily aggregates "
          f"to Kafka ({bootstrap_servers})")


def publish_local(fault_events, daily_aggregates, sim_date):
    os.makedirs(LOCAL_OUT_DIR, exist_ok=True)
    fault_path = os.path.join(LOCAL_OUT_DIR, f"fault_events_{sim_date.isoformat()}.csv")
    daily_path = os.path.join(LOCAL_OUT_DIR, f"daily_agg_{sim_date.isoformat()}.csv")

    if fault_events:
        with open(fault_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(fault_events[0].keys()))
            writer.writeheader()
            writer.writerows(fault_events)

    with open(daily_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(daily_aggregates[0].keys()))
        writer.writeheader()
        writer.writerows(daily_aggregates)

    print(f"[local dry run] {sim_date.isoformat()}: {len(fault_events)} fault events, "
          f"{len(daily_aggregates)} daily aggregates -> {LOCAL_OUT_DIR}/")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["kafka", "local"], default="local")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--start-date", default=None,
                         help="ISO date to simulate forward from (default: day after TODAY_ANCHOR)")
    parser.add_argument("--days", type=int, default=1, help="How many simulated days to publish")
    parser.add_argument("--loop", action="store_true", help="Loop continuously (kafka mode demo)")
    parser.add_argument("--sleep-seconds", type=int, default=5)
    args = parser.parse_args()

    eligible, faultcode_component_map, windows_by_equipment = load_reference_data()

    sim_date = (date.fromisoformat(args.start_date) if args.start_date
                else TODAY_ANCHOR + timedelta(days=1))

    days_to_run = args.days
    while True:
        for _ in range(days_to_run):
            fault_events, daily_aggregates = simulate_one_day(
                sim_date, eligible, faultcode_component_map, windows_by_equipment)

            if args.mode == "kafka":
                publish_kafka(fault_events, daily_aggregates, args.bootstrap_servers)
            else:
                publish_local(fault_events, daily_aggregates, sim_date)

            sim_date += timedelta(days=1)

        if not args.loop:
            break
        time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    main()
