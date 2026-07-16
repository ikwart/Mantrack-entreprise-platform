"""
convert_signals_to_workorders.py

Simulates the human-in-the-loop review step. A real deployment would have a
service advisor look at each 'Action Recommended' signal in a queue and
decide; here we simulate that decision with a simple, documented rule rather
than pretending a human reviewed thousands of rows:

  - 85% of Action Recommended signals are CONVERTED into a new predictive
    work order (maintenance_type='Predictive', source_signal_id set, linking
    straight back to fact_maintenance_events - this is what lets the
    Predictive vs Reactive ratio metric work later).
  - 15% are DISMISSED, split across realistic advisor reasons (already
    scheduled through another channel, customer declined, judged a false
    reading) - this is what a real advisor queue looks like: not everything
    flagged gets acted on.

The model NEVER writes directly to fact_maintenance_events - only this
human-in-the-loop step does, and only for signals that already passed triage.
This preserves the separation designed earlier: signals are probabilistic,
work orders are committed capacity.

Usage:
  python convert_signals_to_workorders.py --sql-dir /opt/sql
"""

import argparse
import random

import pandas as pd

random.seed(31)

DISMISS_RATE = 0.15
DISMISS_REASONS = [
    "Advisor judgment - likely false reading",
    "Customer declined proactive service at this time",
    "Machine already scheduled for service through another channel",
]

SLA_BY_TIER = {"Platinum": 6, "Gold": 12, "Silver": 24, "Standard": 48}
LABOR_RATE_GHS = 180.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-dir", default="/opt/sql")
    args = parser.parse_args()

    signals = pd.read_csv(f"{args.sql_dir}/transactions/ml_maintenance_signals.csv", parse_dates=["scoring_date"])
    signals["dismiss_reason"] = signals["dismiss_reason"].astype("object")
    signals["signal_status"] = signals["signal_status"].astype("object")

    equipment = pd.read_csv(f"{args.sql_dir}/seed_data/dim_equipment.csv")
    customers = pd.read_csv(f"{args.sql_dir}/seed_data/dim_customer.csv")
    technicians = pd.read_csv(f"{args.sql_dir}/seed_data/dim_technician.csv")
    work_orders = pd.read_csv(f"{args.sql_dir}/transactions/fact_maintenance_events.csv",
                               parse_dates=["scheduled_date", "actual_start", "actual_completion"])

    eq_customer = equipment.set_index("equipment_id")["customer_id"].to_dict()
    cust_tier = customers.set_index("customer_id")["contract_tier"].to_dict()

    CUSTOMER_BRANCH = {   # duplicated from sql/transactions/generate_transactions.py -
        1: "Tarkwa", 2: "Kumasi", 3: "Kumasi", 4: "Tarkwa", 5: "Kumasi",  # in a larger codebase this
        6: "Tarkwa", 7: "Tarkwa", 8: "Accra", 9: "Tarkwa", 10: "Accra",   # belongs in one shared config
        11: "Accra", 12: "Accra", 13: "Accra", 14: "Accra", 15: "Accra",
        16: "Takoradi", 17: "Kumasi", 18: "Accra",
    }
    techs_by_branch = {}
    for _, row in technicians.iterrows():
        techs_by_branch.setdefault(row["branch"], []).append(row["technician_id"])

    action_signals = signals[signals["signal_status"] == "Action Recommended"].copy()
    next_wo_id = work_orders["work_order_id"].max() + 1 if len(work_orders) else 1

    new_work_orders = []
    for idx, sig in action_signals.iterrows():
        eq_id = sig["equipment_id"]
        cust_id = eq_customer.get(eq_id)
        tier = cust_tier.get(cust_id, "Standard")

        if random.random() < DISMISS_RATE:
            signals.at[idx, "signal_status"] = "Dismissed"
            signals.at[idx, "dismiss_reason"] = random.choice(DISMISS_REASONS)
            continue

        signals.at[idx, "signal_status"] = "Converted"

        branch = CUSTOMER_BRANCH.get(cust_id, "Accra")
        tech_pool = techs_by_branch.get(branch, technicians["technician_id"].tolist())

        # book within the recommended window, biased toward sooner for tighter-SLA tiers
        max_offset = max(1, int(sig["recommended_action_window_days"]) - 2)
        offset_days = random.randint(1, max_offset) if tier != "Platinum" else random.randint(1, max(1, max_offset // 2))
        scheduled_date = sig["scoring_date"] + pd.Timedelta(days=offset_days)

        priority = "Critical" if sig["failure_probability"] >= 0.85 else "High"
        sla_target = SLA_BY_TIER.get(tier, 24)

        new_work_orders.append({
            "work_order_id": next_wo_id,
            "equipment_id": eq_id,
            "customer_id": cust_id,
            "technician_id": random.choice(tech_pool),
            "maintenance_type": "Predictive",
            "priority": priority,
            "source_signal_id": sig["signal_id"],
            "scheduled_date": scheduled_date.date().isoformat(),
            "actual_start": "",
            "actual_completion": "",
            "downtime_hours": "",
            "sla_target_hours": sla_target,
            "labor_hours": "",
            "total_cost": "",
            "warranty_flag": False,
            "status": "Scheduled",
            "root_cause": "",
            "follow_up_required": False,
        })
        next_wo_id += 1

    if new_work_orders:
        new_wo_df = pd.DataFrame(new_work_orders)
        work_orders_out = pd.concat([work_orders, new_wo_df], ignore_index=True)
    else:
        work_orders_out = work_orders

    work_orders_out.to_csv(f"{args.sql_dir}/transactions/fact_maintenance_events.csv", index=False)
    signals.to_csv(f"{args.sql_dir}/transactions/ml_maintenance_signals.csv", index=False)

    print(f"Action Recommended signals reviewed: {len(action_signals)}")
    print(f"Converted to predictive work orders:  {len(new_work_orders)}")
    print(f"Dismissed by advisor:                 {len(action_signals) - len(new_work_orders)}")


if __name__ == "__main__":
    main()
