"""
apply_triage_rules.py

Automated pre-filter that runs BETWEEN the Spark scoring job and human review.
Not every signal deserves an advisor's attention - this narrows the queue
before a person ever looks at it.

Rules applied, in order:
  1. Auto-suppress: if the equipment already has an OPEN work order
     (status='Scheduled', i.e. dispatched but not yet completed) landing
     within this signal's recommended action window, suppress it. A
     technician is already headed to this machine soon - creating a second,
     separate predictive dispatch would be a duplicate truck roll. This is a
     deliberate simplification: the schema doesn't track which component an
     existing work order covers (fact_maintenance_events has no component_id),
     so the rule suppresses at the equipment level, not the component level.
     A real system would want that finer-grained link; noted as a follow-up.
  2. SLA-based escalation: Platinum-tier customers (tightest contracted
     response times) get escalated from Watch -> Action Recommended earlier
     than other tiers, reflecting that Mantrac commits to faster intervention
     for its top-tier contracts.

Everything else passes through with the probability-based status the Spark
job already assigned (New / Watch / Action Recommended).

Usage:
  python apply_triage_rules.py --sql-dir /opt/sql
"""

import argparse

import pandas as pd

PLATINUM_ESCALATION_WINDOW_DAYS = 10  # Platinum customers: escalate Watch->Action if failure window is this tight


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-dir", default="/opt/sql")
    args = parser.parse_args()

    signals = pd.read_csv(f"{args.sql_dir}/transactions/ml_maintenance_signals.csv", parse_dates=["scoring_date"])
    signals["dismiss_reason"] = signals["dismiss_reason"].astype("object")
    signals["signal_status"] = signals["signal_status"].astype("object")
    equipment = pd.read_csv(f"{args.sql_dir}/seed_data/dim_equipment.csv")
    customers = pd.read_csv(f"{args.sql_dir}/seed_data/dim_customer.csv")
    work_orders = pd.read_csv(f"{args.sql_dir}/transactions/fact_maintenance_events.csv",
                               parse_dates=["scheduled_date"])

    eq_customer = equipment.set_index("equipment_id")["customer_id"].to_dict()
    cust_tier = customers.set_index("customer_id")["contract_tier"].to_dict()

    signals["customer_id"] = signals["equipment_id"].map(eq_customer)
    signals["contract_tier"] = signals["customer_id"].map(cust_tier)

    # --- Rule 1: auto-suppress duplicates -----------------------------------
    open_orders = work_orders[work_orders["status"] == "Scheduled"]
    open_by_equipment = open_orders.groupby("equipment_id")["scheduled_date"].apply(list).to_dict()

    def has_open_order_within_window(row):
        dates = open_by_equipment.get(row["equipment_id"], [])
        window_end = row["scoring_date"] + pd.Timedelta(days=row["recommended_action_window_days"])
        return any(row["scoring_date"] <= d <= window_end for d in dates)

    suppress_mask = signals.apply(has_open_order_within_window, axis=1) & (signals["signal_status"] != "New")
    signals.loc[suppress_mask, "dismiss_reason"] = "Duplicate - open work order already scheduled for this machine"
    signals.loc[suppress_mask, "signal_status"] = "Dismissed"

    # --- Rule 2: Platinum-tier escalation ------------------------------------
    escalate_mask = (
        (signals["signal_status"] == "Watch")
        & (signals["contract_tier"] == "Platinum")
        & (signals["recommended_action_window_days"] <= PLATINUM_ESCALATION_WINDOW_DAYS)
    )
    signals.loc[escalate_mask, "signal_status"] = "Action Recommended"

    signals = signals.drop(columns=["customer_id", "contract_tier"])
    signals.to_csv(f"{args.sql_dir}/transactions/ml_maintenance_signals.csv", index=False)

    print(f"Auto-suppressed (duplicate dispatch): {suppress_mask.sum()}")
    print(f"Escalated (Platinum SLA):              {escalate_mask.sum()}")
    print()
    print(signals["signal_status"].value_counts().to_string())


if __name__ == "__main__":
    main()
