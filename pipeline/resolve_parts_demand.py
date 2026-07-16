"""
resolve_parts_demand.py

For every signal that survived triage as 'Action Recommended', resolves the
predicted COMPONENT into actual PARTS the warehouse needs to stock, scoped to
that specific equipment's model (a "hydraulic pump" failure resolves to a
different physical part number on a Cat 785D than on a Cat 320 GC).

Resolution path:
  predicted_component_id -> bridge_component_part -> part_category (+ is_primary)
      -> bridge_part_model_compatibility (scoped to the equipment's model_id)
      -> dim_part (actual part_number, unit_cost)

Writes:
  - parts_demand_forecast.csv  (one row per resolved part)
  - parts_mapping_gaps.csv     (logged whenever a component+model combination
                                 has no compatible part - a genuine catalog
                                 coverage metric, not silently dropped)

Usage:
  python resolve_parts_demand.py --sql-dir /opt/sql
"""

import argparse

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-dir", default="/opt/sql")
    args = parser.parse_args()

    signals = pd.read_csv(f"{args.sql_dir}/transactions/ml_maintenance_signals.csv", parse_dates=["scoring_date"])
    equipment = pd.read_csv(f"{args.sql_dir}/seed_data/dim_equipment.csv")
    bridge_component_part = pd.read_csv(f"{args.sql_dir}/seed_data/bridge_component_part.csv")
    bridge_part_model = pd.read_csv(f"{args.sql_dir}/seed_data/bridge_part_model_compatibility.csv")
    parts = pd.read_csv(f"{args.sql_dir}/seed_data/dim_part.csv")

    eq_model = equipment.set_index("equipment_id")["model_id"].to_dict()

    action_signals = signals[signals["signal_status"] == "Action Recommended"].copy()
    action_signals["model_id"] = action_signals["equipment_id"].map(eq_model)

    forecast_rows = []
    gap_rows = []
    forecast_id = 1
    gap_id = 1

    for _, sig in action_signals.iterrows():
        candidates = bridge_component_part[bridge_component_part["component_id"] == sig["predicted_component_id"]]

        if candidates.empty:
            gap_rows.append([gap_id, sig["signal_id"], sig["predicted_component_id"], sig["model_id"]])
            gap_id += 1
            continue

        resolved_any = False
        for _, cand in candidates.iterrows():
            eligible_parts = parts[parts["part_category"] == cand["part_category"]]
            compatible_part_ids = set(
                bridge_part_model[bridge_part_model["model_id"] == sig["model_id"]]["part_id"]
            )
            eligible_parts = eligible_parts[eligible_parts["part_id"].isin(compatible_part_ids)]

            if eligible_parts.empty:
                gap_rows.append([gap_id, sig["signal_id"], sig["predicted_component_id"], sig["model_id"]])
                gap_id += 1
                continue

            part = eligible_parts.iloc[0]  # seed data compatibility is currently 1:many undifferentiated;
            resolved_any = True            # picks the first eligible match (see note in generate_seed_data.py)

            needed_by = sig["scoring_date"] + pd.Timedelta(days=int(sig["recommended_action_window_days"]))
            forecast_rows.append([
                forecast_id, sig["signal_id"], sig["equipment_id"], part["part_id"],
                cand["typical_qty"], cand["is_primary"], needed_by.date().isoformat(), "Predictive Signal"
            ])
            forecast_id += 1

        if not resolved_any and candidates.empty is False:
            pass  # gaps already logged per-candidate above

    forecast_df = pd.DataFrame(forecast_rows, columns=[
        "forecast_id", "signal_id", "equipment_id", "part_id", "expected_qty",
        "is_primary", "needed_by_date", "demand_source"
    ])
    gap_df = pd.DataFrame(gap_rows, columns=["gap_id", "signal_id", "component_id", "model_id"])
    gap_df["resolved"] = False
    gap_df = gap_df.drop_duplicates(subset=["component_id", "model_id"])  # dedupe recurring gaps to distinct combos
    gap_df["gap_id"] = range(1, len(gap_df) + 1)

    forecast_df.to_csv(f"{args.sql_dir}/transactions/parts_demand_forecast.csv", index=False)
    gap_df.to_csv(f"{args.sql_dir}/transactions/parts_mapping_gaps.csv", index=False)

    print(f"Action Recommended signals processed: {len(action_signals)}")
    print(f"Parts demand forecast rows written:    {len(forecast_df)}")
    print(f"Catalog coverage gaps logged:           {len(gap_df)}")


if __name__ == "__main__":
    main()
