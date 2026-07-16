"""
Transactional data generator for the Mantrac Ghana Enterprise Data Platform.

Consumes the dimension seed data (sql/seed_data/*.csv) and produces:
  - fact_equipment_sales.csv
  - fact_rental_contracts.csv
  - fact_maintenance_events.csv
  - fact_maintenance_parts_used.csv
  - fact_tax_filings.csv

Currency convention for the whole platform:
  - dim_equipment_model.list_price_usd and dim_part.unit_cost are USD REFERENCE
    prices (how Cat/Mantrac list equipment and parts internationally).
  - Every transactional fact table (sales, rentals, maintenance, inventory, tax)
    is denominated in GHS (matching real Ghanaian invoicing/GRA filing practice).
    FX_USD_TO_GHS below is applied wherever a USD reference price feeds a GHS fact.

Notes on tax logic (Ghana, effective 1 Jan 2026 under the VAT Act, 2025 / Act 1151):
  - VAT 15%, NHIL 2.5%, GETFund 2.5% are all charged on the SAME base (recoupled;
    the COVID-19 levy was abolished). Combined effective rate ~20%.
  - Withholding tax (creditable, withheld by the customer when paying Mantrac):
    goods 3%, services 7.5%, works 5% (GRA rates, cumulative payments > GHS 2,000).
  - All transactional amounts are generated in GHS. dim_equipment_model.list_price_usd
    is a USD reference price only; a fixed illustrative FX rate is applied to convert
    it into a GHS transaction amount. In reality FX rates float daily - a fixed rate
    is a documented simplification for this simulation.

This script does NOT generate 'Predictive' maintenance events - those are created
later by the Spark predictive-maintenance pipeline once telemetry exists, and are
linked back via source_signal_id. This generator only produces the baseline
Preventive / Corrective / Inspection history.
"""

import csv
import os
import random
from datetime import date, datetime, timedelta

import pandas as pd

random.seed(7)

SEED_DIR = "/home/claude/seed/csv"
OUT_DIR = "/home/claude/seed/transactions"
os.makedirs(OUT_DIR, exist_ok=True)

TODAY = date(2026, 7, 8)
FX_USD_TO_GHS = 11.40  # illustrative fixed rate, see module docstring

VAT_RATE = 0.15
NHIL_RATE = 0.025
GETFUND_RATE = 0.025
WHT_GOODS_RATE = 0.03
WHT_SERVICES_RATE = 0.075
WHT_WORKS_RATE = 0.05

# ---------------------------------------------------------------------------
# Load dimension seed data
# ---------------------------------------------------------------------------
customers = pd.read_csv(f"{SEED_DIR}/dim_customer.csv")
equipment = pd.read_csv(f"{SEED_DIR}/dim_equipment.csv", parse_dates=["install_date"])
models = pd.read_csv(f"{SEED_DIR}/dim_equipment_model.csv")
parts = pd.read_csv(f"{SEED_DIR}/dim_part.csv")
technicians = pd.read_csv(f"{SEED_DIR}/dim_technician.csv")

customer_lookup = customers.set_index("customer_id").to_dict("index")
model_lookup = models.set_index("model_id").to_dict("index")

# Branch assignment per customer (nearest Mantrac branch to their site)
CUSTOMER_BRANCH = {
    1: "Tarkwa", 2: "Kumasi", 3: "Kumasi", 4: "Tarkwa", 5: "Kumasi",
    6: "Tarkwa", 7: "Tarkwa", 8: "Accra", 9: "Tarkwa", 10: "Accra",
    11: "Accra", 12: "Accra", 13: "Accra", 14: "Accra", 15: "Accra",
    16: "Takoradi", 17: "Kumasi", 18: "Accra",
}
technicians_by_branch = {}
for _, row in technicians.iterrows():
    technicians_by_branch.setdefault(row["branch"], []).append(row["technician_id"])

SALESPEOPLE = ["Kwabena Osei", "Ama Darko", "Yaw Frimpong", "Efua Sarpong",
               "Kojo Adjei", "Abena Boateng", "Nii Amoah", "Adjoa Mensah"]

# ---------------------------------------------------------------------------
# 1. fact_equipment_sales — one row per OWNED machine
# ---------------------------------------------------------------------------
sales_rows = []
sale_id = 1
for _, eq in equipment[equipment["ownership_type"] == "Owned"].iterrows():
    model = model_lookup[eq["model_id"]]
    list_price_ghs = model["list_price_usd"] * FX_USD_TO_GHS
    price_variation = random.uniform(0.93, 1.06)
    sale_price = round(list_price_ghs * price_variation, 2)
    margin_pct = random.uniform(0.15, 0.25)
    cost_basis = round(sale_price * (1 - margin_pct), 2)
    sale_date = eq["install_date"].date() - timedelta(days=random.randint(0, 14))

    sales_rows.append([
        sale_id, eq["equipment_id"], eq["customer_id"], sale_date.isoformat(),
        sale_price, cost_basis, random.choice(SALESPEOPLE)
    ])
    sale_id += 1

# ---------------------------------------------------------------------------
# 2. fact_rental_contracts — one (occasionally two) rows per RENTED machine
# ---------------------------------------------------------------------------
rental_rows = []
rental_id = 1
for _, eq in equipment[equipment["ownership_type"] == "Rented"].iterrows():
    model = model_lookup[eq["model_id"]]
    list_price_ghs = model["list_price_usd"] * FX_USD_TO_GHS
    monthly_rate_pct = random.uniform(0.02, 0.04)  # 2-4% of list price / month
    monthly_rate = round(list_price_ghs * monthly_rate_pct, 2)

    contract_start = eq["install_date"].date()
    n_contracts = 1 if random.random() > 0.2 else 2
    cursor = contract_start

    for c in range(n_contracts):
        rate_type = random.choices(["Monthly", "Daily"], weights=[8, 2])[0]
        rate = monthly_rate if rate_type == "Monthly" else round(monthly_rate / 26, 2)

        ongoing = (c == n_contracts - 1) and random.random() < 0.35
        if ongoing:
            contract_end = None
            billed_through = TODAY
        else:
            duration_months = random.randint(3, 20)
            contract_end = cursor + timedelta(days=30 * duration_months)
            if contract_end > TODAY:
                contract_end = TODAY
            billed_through = contract_end

        elapsed_months = max(1, (billed_through - cursor).days // 30)
        if rate_type == "Monthly":
            total_billed = round(rate * elapsed_months, 2)
        else:
            total_billed = round(rate * elapsed_months * 26, 2)

        usage_hours_per_month = random.uniform(140, 210)
        actual_usage_hours = round(usage_hours_per_month * elapsed_months, 1)

        rental_rows.append([
            rental_id, eq["equipment_id"], eq["customer_id"],
            cursor.isoformat(), contract_end.isoformat() if contract_end else "",
            rate_type, rate, total_billed, actual_usage_hours
        ])
        rental_id += 1
        cursor = contract_end + timedelta(days=random.randint(1, 20)) if contract_end else TODAY
        if cursor >= TODAY:
            break

# ---------------------------------------------------------------------------
# 3. fact_maintenance_events + fact_maintenance_parts_used
# ---------------------------------------------------------------------------
CATEGORY_SERVICE_INTERVAL_DAYS = {
    1: 100, 2: 90, 3: 80, 4: 120, 5: 100, 6: 110,
    7: 130, 8: 130, 9: 120, 10: 100, 11: 140, 12: 120,
}
LABOR_RATE_GHS = 180.0  # per hour

def pick_parts_for_event(mtype):
    n = random.randint(1, 3) if mtype != "Corrective" else random.randint(2, 4)
    return parts.sample(n=min(n, len(parts)), random_state=random.randint(0, 999999))

maint_rows = []
parts_used_rows = []
work_order_id = 1

for _, eq in equipment.iterrows():
    model = model_lookup[eq["model_id"]]
    category_id = model["category_id"]
    interval_days = CATEGORY_SERVICE_INTERVAL_DAYS.get(category_id, 100)
    branch = CUSTOMER_BRANCH.get(eq["customer_id"], "Accra")
    tech_pool = technicians_by_branch.get(branch, technicians["technician_id"].tolist())

    start = eq["install_date"].date()
    if eq["status"] == "Decommissioned":
        # give it a shorter operating life ending before today
        life_days = random.randint(365, (TODAY - start).days) if (TODAY - start).days > 365 else (TODAY - start).days
        end = start + timedelta(days=life_days)
    else:
        end = TODAY

    cursor = start + timedelta(days=random.randint(10, interval_days))
    customer_tier = customer_lookup[eq["customer_id"]]["contract_tier"]
    sla_by_tier = {"Platinum": 6, "Gold": 12, "Silver": 24, "Standard": 48}

    while cursor < end:
        # Preventive event
        is_corrective = random.random() < 0.20
        mtype = "Corrective" if is_corrective else random.choices(
            ["Preventive", "Inspection"], weights=[85, 15])[0]

        priority = "Routine"
        if mtype == "Corrective":
            priority = random.choices(["Critical", "High", "Routine"], weights=[25, 45, 30])[0]
        elif mtype == "Inspection":
            priority = "Routine"

        scheduled_date = cursor
        start_offset_hours = random.uniform(0, 6)
        actual_start = datetime.combine(scheduled_date, datetime.min.time()) + timedelta(hours=8 + start_offset_hours)

        if mtype == "Preventive":
            downtime_hours = round(random.uniform(4, 12), 1)
        elif mtype == "Inspection":
            downtime_hours = round(random.uniform(1, 4), 1)
        else:  # Corrective
            base = {"Critical": (24, 72), "High": (12, 36), "Routine": (6, 20)}[priority]
            downtime_hours = round(random.uniform(*base), 1)

        actual_completion = actual_start + timedelta(hours=downtime_hours)
        labor_hours = round(downtime_hours * random.uniform(0.4, 0.75), 1)
        sla_target = sla_by_tier.get(customer_tier, 24)

        event_parts = pick_parts_for_event(mtype)
        parts_cost = 0.0
        for _, p in event_parts.iterrows():
            qty = random.randint(1, 2)
            parts_used_rows.append([work_order_id, p["part_id"], qty])
            # dim_part.unit_cost is a USD reference cost (consistent with
            # dim_equipment_model.list_price_usd) - convert to GHS here since
            # all transactional facts are denominated in GHS.
            parts_cost += float(p["unit_cost"]) * qty * FX_USD_TO_GHS
        labor_cost = labor_hours * LABOR_RATE_GHS
        total_cost = round(parts_cost + labor_cost, 2)

        warranty_flag = (scheduled_date - start).days <= 730 and random.random() < 0.6
        status = "Completed" if scheduled_date < TODAY else "Scheduled"
        follow_up = mtype == "Corrective" and random.random() < 0.15
        root_cause = None
        if mtype == "Corrective":
            root_cause = random.choice([
                "Wear beyond tolerance", "Contamination in fluid system",
                "Overheating due to blocked cooling path", "Fatigue failure",
                "Operator-reported abnormal noise confirmed on inspection"
            ])

        technician_id = random.choice(tech_pool)

        maint_rows.append([
            work_order_id, eq["equipment_id"], eq["customer_id"], technician_id,
            mtype, priority, "",  # source_signal_id blank - populated later by predictive pipeline
            scheduled_date.isoformat(), actual_start.isoformat(), actual_completion.isoformat(),
            downtime_hours, sla_target, labor_hours, total_cost,
            warranty_flag, status, root_cause or "", follow_up
        ])

        work_order_id += 1
        cursor = cursor + timedelta(days=interval_days + random.randint(-10, 15))

# ---------------------------------------------------------------------------
# 4. fact_tax_filings — quarterly, per customer
# ---------------------------------------------------------------------------
sales_df = pd.DataFrame(sales_rows, columns=[
    "sale_id", "equipment_id", "customer_id", "sale_date", "sale_price", "cost_basis", "salesperson"])
sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"])

rental_df = pd.DataFrame(rental_rows, columns=[
    "rental_id", "equipment_id", "customer_id", "start_date", "end_date",
    "rate_type", "rate", "total_billed", "actual_usage_hours"])
rental_df["start_date"] = pd.to_datetime(rental_df["start_date"])

maint_df = pd.DataFrame(maint_rows, columns=[
    "work_order_id", "equipment_id", "customer_id", "technician_id", "maintenance_type",
    "priority", "source_signal_id", "scheduled_date", "actual_start", "actual_completion",
    "downtime_hours", "sla_target_hours", "labor_hours", "total_cost", "warranty_flag",
    "status", "root_cause", "follow_up_required"])
maint_df["scheduled_date"] = pd.to_datetime(maint_df["scheduled_date"])

def quarter_key(d):
    q = (d.month - 1) // 3 + 1
    return d.year, q

def quarter_bounds(year, q):
    start_month = (q - 1) * 3 + 1
    start = date(year, start_month, 1)
    end_month = start_month + 2
    if end_month > 12:
        end = date(year, 12, 31)
    else:
        next_month = end_month + 1
        if next_month > 12:
            end = date(year, 12, 31)
        else:
            end = date(year, next_month, 1) - timedelta(days=1)
    return start, end

tax_rows = []
filing_id = 1

for cust_id in customers["customer_id"]:
    c_sales = sales_df[sales_df["customer_id"] == cust_id]
    c_rentals = rental_df[rental_df["customer_id"] == cust_id]
    c_maint = maint_df[maint_df["customer_id"] == cust_id]

    quarters = set()
    for d in c_sales["sale_date"]:
        quarters.add(quarter_key(d.date()))
    for d in c_rentals["start_date"]:
        quarters.add(quarter_key(d.date()))
    for d in c_maint["scheduled_date"]:
        quarters.add(quarter_key(d.date()))

    for (year, q) in sorted(quarters):
        q_start, q_end = quarter_bounds(year, q)

        goods_revenue = c_sales[(c_sales["sale_date"].dt.date >= q_start) &
                                 (c_sales["sale_date"].dt.date <= q_end)]["sale_price"].sum()
        services_revenue = c_rentals[(c_rentals["start_date"].dt.date >= q_start) &
                                      (c_rentals["start_date"].dt.date <= q_end)]["total_billed"].sum()
        works_revenue = c_maint[(c_maint["scheduled_date"].dt.date >= q_start) &
                                 (c_maint["scheduled_date"].dt.date <= q_end)]["total_cost"].sum()

        taxable_base = goods_revenue + services_revenue + works_revenue
        if taxable_base <= 0:
            continue

        vat_amount = round(taxable_base * VAT_RATE, 2)
        nhil_amount = round(taxable_base * NHIL_RATE, 2)
        getfund_amount = round(taxable_base * GETFUND_RATE, 2)
        withholding = round(
            goods_revenue * WHT_GOODS_RATE +
            services_revenue * WHT_SERVICES_RATE +
            works_revenue * WHT_WORKS_RATE, 2
        )

        if q_end < TODAY:
            status = "Filed" if random.random() > 0.06 else "Overdue"
        else:
            status = "Pending"

        tax_rows.append([
            filing_id, cust_id, q_start.isoformat(), q_end.isoformat(),
            vat_amount, nhil_amount, getfund_amount, withholding, status
        ])
        filing_id += 1

# ---------------------------------------------------------------------------
# WRITE CSVs
# ---------------------------------------------------------------------------
def write_csv(filename, header, rows):
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"Wrote {len(rows):>7} rows -> {filename}")

write_csv("fact_equipment_sales.csv",
          ["sale_id", "equipment_id", "customer_id", "sale_date", "sale_price", "cost_basis", "salesperson"],
          sales_rows)

write_csv("fact_rental_contracts.csv",
          ["rental_id", "equipment_id", "customer_id", "start_date", "end_date",
           "rate_type", "rate", "total_billed", "actual_usage_hours"],
          rental_rows)

write_csv("fact_maintenance_events.csv",
          ["work_order_id", "equipment_id", "customer_id", "technician_id", "maintenance_type",
           "priority", "source_signal_id", "scheduled_date", "actual_start", "actual_completion",
           "downtime_hours", "sla_target_hours", "labor_hours", "total_cost", "warranty_flag",
           "status", "root_cause", "follow_up_required"],
          maint_rows)

write_csv("fact_maintenance_parts_used.csv",
          ["work_order_id", "part_id", "qty_used"],
          parts_used_rows)

write_csv("fact_tax_filings.csv",
          ["filing_id", "customer_id", "period_start", "period_end",
           "vat_amount", "nhil_amount", "getfund_amount", "withholding_tax_amount", "filing_status"],
          tax_rows)

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
print("\n--- Summary ---")
print(f"Equipment sales:        {len(sales_rows)}")
print(f"Rental contracts:       {len(rental_rows)}")
print(f"Maintenance work orders:{len(maint_rows)}")
print(f"  Preventive: {sum(1 for r in maint_rows if r[4]=='Preventive')}")
print(f"  Corrective: {sum(1 for r in maint_rows if r[4]=='Corrective')}")
print(f"  Inspection: {sum(1 for r in maint_rows if r[4]=='Inspection')}")
print(f"Parts-used line items:  {len(parts_used_rows)}")
print(f"Tax filings (quarterly):{len(tax_rows)}")
total_vat = sum(r[4] for r in tax_rows)
total_wht = sum(r[7] for r in tax_rows)
print(f"Total VAT+NHIL+GETFund across all filings: GHS {total_vat:,.2f}")
print(f"Total Withholding Tax across all filings:  GHS {total_wht:,.2f}")
