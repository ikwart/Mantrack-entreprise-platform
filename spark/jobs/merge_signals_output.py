"""
merge_signals_output.py

Spark writes its output as a directory of part-files (standard distributed
write behavior). This flattens that into a single ml_maintenance_signals.csv
matching the warehouse schema exactly, adding the columns Spark's job doesn't
populate itself (signal_id, contributing_fault_codes, dismiss_reason, created_at).

Run this after predictive_maintenance_scoring.py, before loading into Postgres.
"""

import argparse
import glob
import os
from datetime import datetime, timezone

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default="/opt/sql/transactions/ml_maintenance_signals_raw")
    parser.add_argument("--out-file", default="/opt/sql/transactions/ml_maintenance_signals.csv")
    args = parser.parse_args()

    part_files = glob.glob(os.path.join(args.raw_dir, "part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"No Spark part-files found under {args.raw_dir}")

    df = pd.concat([pd.read_csv(f) for f in part_files], ignore_index=True)

    df.insert(0, "signal_id", range(1, len(df) + 1))
    df["contributing_fault_codes"] = ""   # populated in a future iteration from the trailing-window fault log
    df["dismiss_reason"] = ""
    df["created_at"] = datetime.now(timezone.utc).isoformat()

    df = df[[
        "signal_id", "equipment_id", "scoring_date", "predicted_component_id",
        "failure_probability", "model_version", "recommended_action_window_days",
        "contributing_fault_codes", "signal_status", "dismiss_reason", "created_at",
    ]]

    df.to_csv(args.out_file, index=False)
    print(f"Wrote {len(df):,} signals -> {args.out_file}")
    print(df["signal_status"].value_counts().to_string())


if __name__ == "__main__":
    main()
