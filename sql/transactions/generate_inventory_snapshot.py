"""
generate_inventory_snapshot.py

Produces a single current-day fact_inventory snapshot (one row per part per
branch), as of TODAY. Rather than fabricating stock levels from nothing,
qty_on_hand/reorder_point/days_of_supply are DERIVED from real historical
consumption in fact_maintenance_parts_used - the same pattern used everywhere
else in this platform (numbers that fall out of the simulation, not numbers
picked to look right).

Logic per part:
  1. avg_daily_consumption = total historical qty used / days of observed history
  2. reorder_point = avg_daily_consumption * lead_time_days * safety_factor
  3. qty_on_hand = avg_daily_consumption * (random 10-45 days of current stock cover)
  4. days_of_supply = qty_on_hand / avg_daily_consumption
  5. qty_on_hand is split across the 4 branches, weighted by each branch's
     share of historical maintenance activity (branches serving more work
     orders carry more stock)

unit_cost is carried in GHS (FX-converted from dim_part's USD reference cost),
consistent with every other transactional fact in this platform - see the
currency convention note in generate_transactions.py.
"""

import csv
import os
import random

import pandas as pd

random.seed(41)

SEED_DIR = "/home/claude/seed/csv"
TX_DIR = "/home/claude/repo/mantrac-ghana-data-platform/sql/transactions"
OUT_DIR = TX_DIR

FX_USD_TO_GHS = 11.40
BRANCHES = ["Accra", "Kumasi", "Takoradi", "Tarkwa"]

CUSTOMER_BRANCH = {  # same mapping used in generate_transactions.py / convert_signals_to_workorders.py
    1: "Tarkwa", 2: "Kumasi", 3: "Kumasi", 4: "Tarkwa", 5: "Kumasi",
    6: "Tarkwa", 7: "Tarkwa", 8: "Accra", 9: "Tarkwa", 10: "Accra",
    11: "Accra", 12: "Accra", 13: "Accra", 14: "Accra", 15: "Accra",
    16: "Takoradi", 17: "Kumasi", 18: "Accra",
}

parts = pd.read_csv(f"{SEED_DIR}/dim_part.csv")
parts_used = pd.read_csv(f"{TX_DIR}/fact_maintenance_parts_used.csv")
work_orders = pd.read_csv(f"{TX_DIR}/fact_maintenance_events.csv", parse_dates=["scheduled_date"])

# attach branch to each work order via its customer
work_orders["branch"] = work_orders["customer_id"].map(CUSTOMER_BRANCH)
parts_used = parts_used.merge(work_orders[["work_order_id", "branch", "scheduled_date"]], on="work_order_id")

history_days = max(1, (work_orders["scheduled_date"].max() - work_orders["scheduled_date"].min()).days)

consumption_by_part = parts_used.groupby("part_id")["qty_used"].sum().to_dict()
consumption_by_part_branch = parts_used.groupby(["part_id", "branch"])["qty_used"].sum().to_dict()

rows = []
for _, part in parts.iterrows():
    part_id = part["part_id"]
    unit_cost_ghs = round(float(part["unit_cost"]) * FX_USD_TO_GHS, 2)
    lead_time = int(part["lead_time_days"])

    total_used = consumption_by_part.get(part_id, 0)
    avg_daily_consumption = max(total_used / history_days, 0.01)  # floor avoids div-by-zero for unused parts

    branch_weights = {
        b: consumption_by_part_branch.get((part_id, b), 0) + 1  # +1 smoothing so every branch carries some stock
        for b in BRANCHES
    }
    weight_sum = sum(branch_weights.values())

    stock_cover_days = random.uniform(10, 45)
    total_qty_on_hand = max(1, round(avg_daily_consumption * stock_cover_days))
    reorder_point = max(1, round(avg_daily_consumption * lead_time * random.uniform(1.3, 1.8)))

    for branch in BRANCHES:
        branch_share = branch_weights[branch] / weight_sum
        qty = max(0, round(total_qty_on_hand * branch_share))
        branch_reorder = max(1, round(reorder_point * branch_share))
        days_of_supply = round(qty / avg_daily_consumption, 1) if avg_daily_consumption > 0 else None

        rows.append([
            part_id, branch, "2026-07-08", qty, unit_cost_ghs, branch_reorder, days_of_supply
        ])

with open(f"{OUT_DIR}/fact_inventory.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["part_id", "branch", "snapshot_date", "qty_on_hand", "unit_cost",
                      "reorder_point", "days_of_supply"])
    writer.writerows(rows)

print(f"fact_inventory rows: {len(rows)} ({len(parts)} parts x {len(BRANCHES)} branches)")
total_value = sum(r[3] * r[4] for r in rows)
print(f"Total inventory value across all branches: GHS {total_value:,.2f}")
below_reorder = sum(1 for r in rows if r[3] < r[5])
print(f"Part-branch combinations currently below reorder point: {below_reorder}")
