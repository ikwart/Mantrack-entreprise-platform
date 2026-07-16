"""
Seed data generator for the Mantrac Ghana Enterprise Data Platform.
Produces CSV files for all dimension tables, the fleet (dim_equipment),
and lookup/bridge tables, sized to reflect realistic large-fleet customers
(mining contractors/owners run 15-60+ machines; construction/quarry/logistics
customers run smaller fleets).
"""

import csv
import random
from datetime import date, timedelta

random.seed(42)

OUT = "/home/claude/seed/csv"
import os
os.makedirs(OUT, exist_ok=True)

# ---------------------------------------------------------------------------
# 1. dim_industry
# ---------------------------------------------------------------------------
industries = [
    (1, "Mining"),
    (2, "Construction"),
    (3, "Oil & Gas"),
    (4, "Energy/Power"),
    (5, "Quarrying"),
    (6, "Logistics"),
]

# ---------------------------------------------------------------------------
# 2. dim_customer  (customer_id, code, name, type, industry_id, region, tier, target_fleet_size)
# ---------------------------------------------------------------------------
customers = [
    # Mining - Owner/Operators
    (1,  "GFG-TKW",  "Gold Fields Ghana (Tarkwa)",              "Owner-Operator",         1, "Western",     "Platinum", 34),
    (2,  "AGA-OBS",  "AngloGold Ashanti (Obuasi)",              "Owner-Operator",         1, "Ashanti",     "Platinum", 30),
    (3,  "NEM-AHF",  "Newmont Ghana (Ahafo/Akyem)",             "Owner-Operator",         1, "Ahafo",       "Platinum", 32),
    (4,  "GSR-WBG",  "Golden Star Resources (Wassa/Bogoso)",    "Owner-Operator",         1, "Western",     "Gold",     20),
    (5,  "ASK-GLD",  "Asanko Gold Mine",                        "Owner-Operator",         1, "Ashanti",     "Gold",     18),
    (6,  "GMC-NSU",  "Ghana Manganese Company (Nsuta)",         "Owner-Operator",         1, "Western",     "Gold",     15),
    (7,  "GBX-AWS",  "Ghana Bauxite Company",                   "Owner-Operator",         1, "Western",     "Silver",   10),
    # Mining - Contract Miners
    (8,  "RSI-ACC",  "Rocksure International",                  "Contract Miner",         1, "Greater Accra","Platinum", 46),
    (9,  "ENP-ACC",  "Engineers & Planners (E&P)",               "Contract Miner",         1, "Greater Accra","Platinum", 56),
    # Construction & Infrastructure
    (10, "CCG-ACC",  "Contracta Construction Ghana",             "Construction Contractor",2, "Greater Accra","Gold",     12),
    (11, "MIC-ACC",  "Micheletti & Co. Ghana",                   "Construction Contractor",2, "Greater Accra","Silver",   10),
    (12, "CNS-ACC",  "Consar Limited",                           "Construction Contractor",2, "Greater Accra","Silver",    8),
    (13, "VBC-ACC",  "Vanhein Building & Civil Engineering",     "Construction Contractor",2, "Greater Accra","Standard",  9),
    # Quarrying & Aggregates
    (14, "JMQ-EST",  "Justmoh Construction & Quarry",            "Quarry Operator",        5, "Eastern",     "Silver",    8),
    (15, "RLQ-EST",  "Rocklove Company Limited",                 "Quarry Operator",        5, "Eastern",     "Standard",  6),
    # Oil & Gas / Energy / Logistics
    (16, "GPH-TKD",  "Ghana Ports and Harbours Authority (Takoradi)", "Logistics/Energy",  6, "Western",     "Gold",     11),
    (17, "BPA-BNO",  "Bui Power Authority",                      "Logistics/Energy",       4, "Bono",        "Silver",    6),
    (18, "ZOM-NAT",  "Zoomlion Ghana",                           "Logistics/Energy",       6, "Greater Accra","Gold",    16),
]

# ---------------------------------------------------------------------------
# 3. dim_site  (one primary site per customer, named realistically)
# ---------------------------------------------------------------------------
site_names = {
    1: "Tarkwa Mine Site", 2: "Obuasi Mine Site", 3: "Ahafo Mine Site",
    4: "Wassa/Bogoso Mine Site", 5: "Asanko Mine Site", 6: "Nsuta Mine Site",
    7: "Awaso Bauxite Site", 8: "Rocksure Multi-Site Operations",
    9: "E&P Tarkwa/Damang Contract Site", 10: "Contracta Accra Project Yard",
    11: "Micheletti Accra Depot", 12: "Consar Accra Site",
    13: "Vanhein Accra Site", 14: "Justmoh Quarry - Eastern Region",
    15: "Rocklove Quarry - Eastern Region", 16: "Takoradi Port Terminal",
    17: "Bui Dam Site", 18: "Zoomlion National Depot (Accra)",
}

# Real-world coordinates (WGS84) for each site - Tarkwa and Obuasi verified via
# web search against multiple sources; the rest are well-known Ghanaian towns/
# regions estimated to town-level accuracy, sufficient for a portfolio map
# visualization, not survey-grade GIS precision.
site_coords = {
    1: (5.3038, -1.9896), 2: (6.2012, -1.6913), 3: (7.0167, -2.3667), 4: (5.7833, -1.9667),
    5: (6.5333, -1.9667), 6: (5.2833, -1.9500), 7: (6.2333, -2.3667), 8: (5.6037, -0.1870),
    9: (5.4667, -1.9833), 10: (5.5600, -0.2050), 11: (5.5900, -0.1900), 12: (5.6100, -0.2000),
    13: (5.5800, -0.1700), 14: (6.0833, -0.2500), 15: (6.1000, -0.3000), 16: (4.8845, -1.7554),
    17: (8.2667, -2.2500), 18: (5.6200, -0.2100),
}

# ---------------------------------------------------------------------------
# 4. dim_equipment_category
# ---------------------------------------------------------------------------
categories = [
    (1,  "Excavator",                "Both"),
    (2,  "Hydraulic Mining Shovel",  "Mining"),
    (3,  "Off-Highway Truck",        "Mining"),
    (4,  "Dragline",                 "Mining"),
    (5,  "Dozer",                    "Both"),
    (6,  "Motor Grader",             "Both"),
    (7,  "Compactor",                "Construction"),
    (8,  "Asphalt Paver",            "Construction"),
    (9,  "Backhoe Loader",           "Construction"),
    (10, "Wheel Loader",             "Both"),
    (11, "Skid Steer Loader",        "Construction"),
    (12, "Road Reclaimer",           "Construction"),
]
cat_by_id = {c[0]: c for c in categories}

# ---------------------------------------------------------------------------
# 5. dim_equipment_model  (model_id, category_id, model_name, engine_series, list_price_usd)
# ---------------------------------------------------------------------------
models = [
    # Excavators
    (1, 1, "Cat 320 GC",  "Cat C4.4",  185000),
    (2, 1, "Cat 323",     "Cat C4.4",  210000),
    (3, 1, "Cat 336",     "Cat C9.3B", 310000),
    (4, 1, "Cat 349",     "Cat C9.3B", 420000),
    (5, 1, "Cat 374F",    "Cat C15",   610000),
    # Hydraulic Mining Shovels
    (6, 2, "Cat 6015B",   "Cat C27",  2200000),
    (7, 2, "Cat 6020B",   "Cat C32",  3400000),
    (8, 2, "Cat 6030",    "Cat 3512", 5200000),
    # Off-Highway Trucks
    (9,  3, "Cat 725",    "Cat C9.3", 480000),
    (10, 3, "Cat 730",    "Cat C9.3", 540000),
    (11, 3, "Cat 745",    "Cat C15",  690000),
    (12, 3, "Cat 777",    "Cat C27",  2600000),
    (13, 3, "Cat 785D",   "Cat 3512", 3800000),
    (14, 3, "Cat 793F",   "Cat 3516", 5600000),
    # Draglines
    (15, 4, "Cat 8750",   "Electric Drive", 45000000),
    # Dozers
    (16, 5, "Cat D6",     "Cat C9.3",  520000),
    (17, 5, "Cat D8",     "Cat C15",   890000),
    (18, 5, "Cat D9",     "Cat C18",  1250000),
    (19, 5, "Cat D11",    "Cat 3512", 2450000),
    # Motor Graders
    (20, 6, "Cat 120",    "Cat C7.1",  310000),
    (21, 6, "Cat 140",    "Cat C9.3",  390000),
    (22, 6, "Cat 160",    "Cat C13",   470000),
    # Compactors
    (23, 7, "Cat CS56",   "Cat C4.4",  190000),
    (24, 7, "Cat CS74B",  "Cat C7.1",  240000),
    (25, 7, "Cat PS-360B","Cat C4.4",  210000),
    # Asphalt Pavers
    (26, 8, "Cat AP555F", "Cat C4.4",  310000),
    (27, 8, "Cat AP1055F","Cat C7.1",  460000),
    # Backhoe Loaders
    (28, 9, "Cat 420",    "Cat C4.4",  120000),
    (29, 9, "Cat 432",    "Cat C4.4",  145000),
    (30, 9, "Cat 444",    "Cat C4.4",  165000),
    # Wheel Loaders
    (31, 10, "Cat 950 GC","Cat C7.1",  310000),
    (32, 10, "Cat 966",   "Cat C9.3",  410000),
    (33, 10, "Cat 980",   "Cat C13",   560000),
    (34, 10, "Cat 992",   "Cat C32",  1650000),
    # Skid Steer Loaders
    (35, 11, "Cat 236D",  "Cat C3.3B",  68000),
    (36, 11, "Cat 262D",  "Cat C3.8",   82000),
    (37, 11, "Cat 272D",  "Cat C3.8",   95000),
    # Road Reclaimers
    (38, 12, "Cat RM300", "Cat C13",   520000),
    (39, 12, "Cat RM500", "Cat C18",   780000),
]

# Segment -> which categories are plausible, with weights (heavier weight = more common)
segment_category_weights = {
    "Owner-Operator": {1: 3, 2: 2, 3: 4, 4: 1, 5: 3, 6: 2, 10: 3},
    "Contract Miner": {1: 3, 2: 2, 3: 5, 5: 3, 6: 2, 10: 3},
    "Construction Contractor": {1: 3, 5: 1, 6: 2, 7: 2, 8: 1, 9: 3, 10: 2, 11: 2, 12: 1},
    "Quarry Operator": {1: 3, 3: 2, 5: 2, 10: 3, 7: 1},
    "Logistics/Energy": {10: 3, 9: 2, 1: 1, 7: 1, 11: 2},
}
# Only Gold Fields, AngloGold, Newmont get a chance at a dragline (rare, huge machines)
dragline_eligible = {1, 2, 3}

# ---------------------------------------------------------------------------
# Build dim_equipment
# ---------------------------------------------------------------------------
equipment_rows = []
equipment_id = 1
install_start = date(2016, 1, 1)
install_end = date(2026, 6, 1)

for cust_id, code, name, ctype, ind_id, region, tier, fleet_size in customers:
    weights = segment_category_weights[ctype]
    cat_ids = list(weights.keys())
    cat_weights = list(weights.values())

    for _ in range(fleet_size):
        cat_id = random.choices(cat_ids, weights=cat_weights, k=1)[0]
        if cat_id == 4 and cust_id not in dragline_eligible:
            cat_id = 3  # fall back to off-highway truck
        candidate_models = [m for m in models if m[1] == cat_id]
        model = random.choice(candidate_models)
        model_id = model[0]

        days_span = (install_end - install_start).days
        install_date = install_start + timedelta(days=random.randint(0, days_span))

        ownership = "Rented" if (ctype == "Contract Miner" and random.random() < 0.15) else "Owned"
        status_roll = random.random()
        status = "Active" if status_roll < 0.90 else ("Idle" if status_roll < 0.97 else "Decommissioned")

        serial = f"MG{cust_id:02d}{equipment_id:05d}"

        equipment_rows.append([
            equipment_id, model_id, serial, cust_id, cust_id,  # site_id == customer_id (1 site per customer)
            install_date.isoformat(), ownership, status
        ])
        equipment_id += 1

# ---------------------------------------------------------------------------
# 6. dim_technician
# ---------------------------------------------------------------------------
technician_first = ["Kwame", "Kofi", "Yaw", "Kwabena", "Kwesi", "Ama", "Akosua", "Efua",
                     "Yaa", "Abena", "Nana", "Kojo", "Adjoa", "Esi", "Fiifi", "Nii"]
technician_last = ["Mensah", "Owusu", "Boateng", "Asante", "Appiah", "Osei", "Adjei",
                    "Sarpong", "Frimpong", "Agyeman", "Darko", "Amoah", "Yeboah", "Gyasi"]
branches = ["Accra", "Kumasi", "Takoradi", "Tarkwa"]
technicians = []
for i in range(1, 25):
    fn = random.choice(technician_first)
    ln = random.choice(technician_last)
    branch = branches[(i - 1) % 4]
    level = random.choices(["Certified", "Senior Certified", "Master Technician"],
                            weights=[5, 3, 2])[0]
    technicians.append([i, f"{fn} {ln}", branch, level])

# ---------------------------------------------------------------------------
# 7. dim_component
# ---------------------------------------------------------------------------
components = [
    (1, "Hydraulic Pump",            "Hydraulics"),
    (2, "Hydraulic Cylinder Seal",   "Hydraulics"),
    (3, "Engine Coolant System",     "Engine"),
    (4, "Fuel Injector",             "Engine"),
    (5, "Air Filter System",        "Engine"),
    (6, "Turbocharger",              "Engine"),
    (7, "Undercarriage/Track",       "Undercarriage"),
    (8, "Final Drive",               "Powertrain"),
    (9, "Transmission",              "Powertrain"),
    (10, "Torque Converter",         "Powertrain"),
    (11, "Alternator/Charging System","Electrical"),
    (12, "ECM/Sensor Module",        "Electrical"),
]

# ---------------------------------------------------------------------------
# 8. dim_fault_code
# ---------------------------------------------------------------------------
fault_codes = [
    ("E-101", "Hydraulic pressure below threshold",        "Warning",  "Hydraulics"),
    ("E-102", "Hydraulic pump cavitation detected",         "Critical", "Hydraulics"),
    ("E-103", "Hydraulic cylinder seal leak indicated",      "Warning",  "Hydraulics"),
    ("E-201", "Coolant temperature exceeds limit",           "Critical", "Engine"),
    ("E-202", "Coolant level low",                            "Warning",  "Engine"),
    ("E-203", "Fuel injector pressure deviation",             "Warning",  "Engine"),
    ("E-204", "Fuel rail pressure low",                       "Critical", "Engine"),
    ("E-205", "Air filter restriction high",                  "Info",     "Engine"),
    ("E-206", "Turbocharger boost pressure low",              "Warning",  "Engine"),
    ("E-207", "Turbocharger overspeed",                        "Critical", "Engine"),
    ("E-301", "Track tension deviation",                       "Warning",  "Undercarriage"),
    ("E-302", "Undercarriage wear limit approaching",           "Warning",  "Undercarriage"),
    ("E-303", "Track roller bearing temperature high",          "Critical", "Undercarriage"),
    ("E-401", "Final drive oil temperature high",               "Warning",  "Powertrain"),
    ("E-402", "Final drive metal particulate detected",          "Critical", "Powertrain"),
    ("E-403", "Transmission slip detected",                      "Warning",  "Powertrain"),
    ("E-404", "Transmission fluid pressure low",                  "Critical", "Powertrain"),
    ("E-405", "Torque converter temperature high",                "Warning",  "Powertrain"),
    ("E-501", "Alternator output below spec",                      "Warning",  "Electrical"),
    ("E-502", "ECM communication fault",                             "Critical", "Electrical"),
    ("E-503", "Sensor signal out of range",                          "Info",     "Electrical"),
    ("E-001", "Routine diagnostic check passed with minor note",      "Info",     "Engine"),
    ("E-002", "Minor vibration anomaly logged",                        "Info",     "Undercarriage"),
]

# ---------------------------------------------------------------------------
# 9. bridge_faultcode_component
# ---------------------------------------------------------------------------
fc_component_map = [
    ("E-101", 1, 0.75, True), ("E-102", 1, 0.90, True), ("E-103", 2, 0.85, True),
    ("E-201", 3, 0.90, True), ("E-202", 3, 0.70, True),
    ("E-203", 4, 0.80, True), ("E-204", 4, 0.90, True),
    ("E-205", 5, 0.65, True),
    ("E-206", 6, 0.75, True), ("E-207", 6, 0.90, True),
    ("E-301", 7, 0.70, True), ("E-302", 7, 0.85, True), ("E-303", 7, 0.90, True),
    ("E-401", 8, 0.70, True), ("E-402", 8, 0.90, True),
    ("E-403", 9, 0.75, True), ("E-404", 9, 0.85, True),
    ("E-405", 10, 0.70, True),
    ("E-501", 11, 0.60, True), ("E-502", 12, 0.80, True), ("E-503", 12, 0.50, False),
    ("E-001", 3, 0.15, False), ("E-002", 7, 0.15, False),
]

# ---------------------------------------------------------------------------
# 10. dim_part (with unit_cost = INVENTORY COST, not selling price)
# ---------------------------------------------------------------------------
parts = [
    (1,  "P-HYD-001", "Hydraulic Pump Assembly",        "Hydraulic Component",   8200.00, 21),
    (2,  "P-HYD-002", "Hydraulic Pump Seal Kit",         "Hydraulic Component",    340.00, 7),
    (3,  "P-HYD-003", "Hydraulic Cylinder Seal Kit",     "Hydraulic Component",    210.00, 7),
    (4,  "P-ENG-001", "Coolant Radiator Assembly",       "Engine/Powertrain Component", 3100.00, 14),
    (5,  "P-ENG-002", "Water Pump",                       "Engine/Powertrain Component",  620.00, 10),
    (6,  "P-ENG-003", "Fuel Injector Unit",                "Engine/Powertrain Component", 1450.00, 14),
    (7,  "P-ENG-004", "Fuel Filter",                        "Engine/Powertrain Component",   85.00, 5),
    (8,  "P-ENG-005", "Air Filter Element",                  "Engine/Powertrain Component",  120.00, 5),
    (9,  "P-ENG-006", "Turbocharger Assembly",                "Engine/Powertrain Component", 5400.00, 21),
    (10, "P-UND-001", "Track Chain Assembly",                 "Undercarriage Wear Part",     6200.00, 28),
    (11, "P-UND-002", "Track Roller",                          "Undercarriage Wear Part",      480.00, 14),
    (12, "P-UND-003", "Sprocket Segment",                       "Undercarriage Wear Part",      560.00, 14),
    (13, "P-PWR-001", "Final Drive Assembly",                    "Engine/Powertrain Component", 7800.00, 28),
    (14, "P-PWR-002", "Final Drive Oil Seal Kit",                  "Engine/Powertrain Component",  190.00, 7),
    (15, "P-PWR-003", "Transmission Assembly",                     "Engine/Powertrain Component", 12500.00, 35),
    (16, "P-PWR-004", "Transmission Filter",                        "Engine/Powertrain Component",   95.00, 5),
    (17, "P-PWR-005", "Torque Converter",                             "Engine/Powertrain Component", 4300.00, 21),
    (18, "P-ELE-001", "Alternator Unit",                                "Electrical Component",       850.00, 10),
    (19, "P-ELE-002", "ECM Control Module",                              "Electrical Component",      2100.00, 21),
    (20, "P-ELE-003", "Sensor Kit (multi-position)",                      "Electrical Component",       310.00, 7),
    (21, "P-CON-001", "Engine Oil (per drum, 205L)",                       "Consumable",                640.00, 3),
    (22, "P-CON-002", "Hydraulic Oil (per drum, 205L)",                     "Consumable",                580.00, 3),
    (23, "P-CON-003", "Grease Cartridge Case",                               "Consumable",                 95.00, 3),
]

# ---------------------------------------------------------------------------
# 11. bridge_component_part
# ---------------------------------------------------------------------------
component_part_map = [
    (1, "Hydraulic Component", 1, True), (1, "Consumable", 1, False),
    (2, "Hydraulic Component", 1, True),
    (3, "Engine/Powertrain Component", 1, True), (3, "Consumable", 1, False),
    (4, "Engine/Powertrain Component", 1, True), (4, "Consumable", 1, False),
    (5, "Engine/Powertrain Component", 1, True),
    (6, "Engine/Powertrain Component", 1, True),
    (7, "Undercarriage Wear Part", 2, True),
    (8, "Engine/Powertrain Component", 1, True), (8, "Consumable", 1, False),
    (9, "Engine/Powertrain Component", 1, True), (9, "Consumable", 1, False),
    (10, "Engine/Powertrain Component", 1, True),
    (11, "Electrical Component", 1, True),
    (12, "Electrical Component", 1, True),
]

# ---------------------------------------------------------------------------
# 12. bridge_part_model_compatibility
# Simple rule: engine/powertrain/hydraulic/undercarriage/electrical/consumable
# parts are compatible with all models within categories that plausibly use them.
# For seed purposes: mark every part compatible with every model (simplifies
# generator; can be tightened later with model-specific part numbers).
# ---------------------------------------------------------------------------
part_model_compat = [(p[0], m[0]) for p in parts for m in models]

# ---------------------------------------------------------------------------
# WRITE CSVs
# ---------------------------------------------------------------------------
def write_csv(filename, header, rows):
    path = os.path.join(OUT, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"Wrote {len(rows):>6} rows -> {filename}")

write_csv("dim_industry.csv", ["industry_id", "industry_name"], industries)

write_csv("dim_customer.csv",
          ["customer_id", "customer_code", "customer_name", "customer_type",
           "industry_id", "region", "contract_tier"],
          [(c[0], c[1], c[2], c[3], c[4], c[5], c[6]) for c in customers])

write_csv("dim_site.csv",
          ["site_id", "site_name", "customer_id", "region", "latitude", "longitude"],
          [(cid, site_names[cid], cid, [c[5] for c in customers if c[0] == cid][0],
            site_coords[cid][0], site_coords[cid][1]) for cid in site_names])

write_csv("dim_equipment_category.csv",
          ["category_id", "category_name", "primary_application"], categories)

write_csv("dim_equipment_model.csv",
          ["model_id", "category_id", "model_name", "engine_series", "list_price_usd"], models)

write_csv("dim_equipment.csv",
          ["equipment_id", "model_id", "serial_number", "customer_id", "site_id",
           "install_date", "ownership_type", "status"], equipment_rows)

write_csv("dim_technician.csv",
          ["technician_id", "technician_name", "branch", "certification_level"], technicians)

write_csv("dim_component.csv",
          ["component_id", "component_name", "system_category"], components)

write_csv("dim_fault_code.csv",
          ["fault_code", "description", "severity", "system_category"], fault_codes)

write_csv("bridge_faultcode_component.csv",
          ["fault_code", "component_id", "correlation_weight", "is_direct_indicator"],
          fc_component_map)

write_csv("dim_part.csv",
          ["part_id", "part_number", "part_name", "part_category", "unit_cost", "lead_time_days"],
          parts)

write_csv("bridge_component_part.csv",
          ["component_id", "part_category", "typical_qty", "is_primary"],
          component_part_map)

write_csv("bridge_part_model_compatibility.csv",
          ["part_id", "model_id"], part_model_compat)

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
print("\n--- Fleet size by customer ---")
from collections import Counter
fleet_counts = Counter(row[3] for row in equipment_rows)
cust_lookup = {c[0]: c[2] for c in customers}
for cid, cnt in sorted(fleet_counts.items(), key=lambda x: -x[1]):
    print(f"{cust_lookup[cid]:45s} {cnt:3d} machines")

print(f"\nTOTAL EQUIPMENT INSTANCES: {len(equipment_rows)}")
print(f"TOTAL EQUIPMENT MODELS: {len(models)}")
print(f"TOTAL CUSTOMERS: {len(customers)}")
