"""
telemetry_engine.py

Core simulation logic for equipment telemetry / predictive-maintenance ground
truth. This module has NO Kafka or Postgres dependency - it's pure simulation
math so it can be unit-tested and reused by both:
  - generate_historical_telemetry.py  (bulk backfill, written directly to CSV)
  - telemetry_kafka_producer.py       (live/incremental, published to Kafka)

Implements the degradation-curve design from the architecture doc:
  P(fault_on_day) = P_base + (P_max - P_base) / (1 + exp(-k * (progress - midpoint)))
  - flat/quiet early in a component's lead window, sharp knee near the end
  - severity mix shifts toward Critical as progress -> 1
  - background noise (no active degradation) uses a flat low rate
  - false starts: partial ramp that resets before reaching the planned "failure"
"""

import math
import random

# ---------------------------------------------------------------------------
# Global background noise parameters (healthy machines / no active window)
# ---------------------------------------------------------------------------
P_BASE_BACKGROUND = 0.07     # daily probability of a background (noise) fault
P_MAX_DEFAULT = 0.85         # near-certain emission right before failure

# ---------------------------------------------------------------------------
# Per-component tuning: (lead_window_range_days, midpoint, k)
# Shorter/sharper windows = fails fast once it starts going.
# Longer/later-knee windows = slow mechanical wear (undercarriage, final drive).
# ---------------------------------------------------------------------------
COMPONENT_PARAMS = {
    1:  {"name": "Hydraulic Pump",             "lead_window": (10, 18), "midpoint": 0.55, "k": 10, "weight": 10},
    2:  {"name": "Hydraulic Cylinder Seal",    "lead_window": (8, 15),  "midpoint": 0.50, "k": 10, "weight": 8},
    3:  {"name": "Engine Coolant System",      "lead_window": (14, 25), "midpoint": 0.60, "k": 9,  "weight": 9},
    4:  {"name": "Fuel Injector",               "lead_window": (7, 14),  "midpoint": 0.50, "k": 11, "weight": 7},
    5:  {"name": "Air Filter System",           "lead_window": (5, 10),  "midpoint": 0.45, "k": 12, "weight": 6},
    6:  {"name": "Turbocharger",                "lead_window": (10, 20), "midpoint": 0.60, "k": 10, "weight": 5},
    7:  {"name": "Undercarriage/Track",         "lead_window": (25, 40), "midpoint": 0.75, "k": 12, "weight": 10},
    8:  {"name": "Final Drive",                  "lead_window": (20, 35), "midpoint": 0.70, "k": 8,  "weight": 8},
    9:  {"name": "Transmission",                 "lead_window": (18, 30), "midpoint": 0.65, "k": 9,  "weight": 6},
    10: {"name": "Torque Converter",             "lead_window": (15, 25), "midpoint": 0.60, "k": 9,  "weight": 4},
    11: {"name": "Alternator/Charging System",   "lead_window": (8, 15),  "midpoint": 0.55, "k": 10, "weight": 5},
    12: {"name": "ECM/Sensor Module",            "lead_window": (5, 12),  "midpoint": 0.50, "k": 11, "weight": 4},
}


def logistic_ramp(progress: float, p_base: float, p_max: float, midpoint: float, k: float) -> float:
    """Daily fault-emission probability given how far into the lead window we are (0-1)."""
    progress = max(0.0, min(1.0, progress))
    return p_base + (p_max - p_base) / (1 + math.exp(-k * (progress - midpoint)))


def severity_weights(progress: float):
    """Returns (p_critical, p_warning, p_info) for a fault firing at this progress."""
    p_crit = min(0.05 + 0.55 * (progress ** 2), 0.90)
    p_warn = min(0.35 + 0.10 * progress, 1 - p_crit - 0.01)
    p_info = max(0.0, 1 - p_crit - p_warn)
    return p_crit, p_warn, p_info


def draw_severity(progress: float) -> str:
    p_crit, p_warn, p_info = severity_weights(progress)
    return random.choices(["Critical", "Warning", "Info"], weights=[p_crit, p_warn, p_info])[0]


def draw_background_severity() -> str:
    """Flat, low-trend severity mix for healthy-machine background noise."""
    return random.choices(["Info", "Warning", "Critical"], weights=[80, 18, 2])[0]


def sensor_reading(progress: float, baseline: float = 100.0) -> float:
    """
    Continuous sensor value (e.g. normalized pressure/temperature index).
    Drifts away from baseline and gets noisier as progress -> 1.
    """
    drift = 40 * progress
    noise_std = 3 + 12 * progress
    return round(baseline + drift + random.gauss(0, noise_std), 2)


def pick_fault_code_for_component(component_id: int, severity: str, faultcode_component_map: dict) -> str:
    """
    faultcode_component_map: {component_id: [(fault_code, correlation_weight, is_direct), ...]}
    Picks a fault code correlated with this component, weighted by correlation_weight,
    filtered to plausible severities where possible (falls back to any match).
    """
    candidates = faultcode_component_map.get(component_id, [])
    if not candidates:
        return None
    weights = [c[1] for c in candidates]
    return random.choices([c[0] for c in candidates], weights=weights)[0]


def component_progress(day_offset: int, lead_window_days: int, is_false_start: bool = False,
                        false_start_cutoff: float = None, decay_days: int = 6) -> float:
    """
    day_offset: days since this degradation window started (0 = window start)
    Returns a progress value in [0, 1] (or declining back toward 0 for false starts
    once they pass their cutoff).
    """
    raw_progress = day_offset / lead_window_days

    if not is_false_start:
        return min(1.0, raw_progress)

    # False start: ramps toward false_start_cutoff, then decays back down
    if raw_progress <= false_start_cutoff:
        return raw_progress
    days_past_cutoff = day_offset - (false_start_cutoff * lead_window_days)
    decay_fraction = max(0.0, 1 - (days_past_cutoff / decay_days))
    return false_start_cutoff * decay_fraction


def component_weighted_choice() -> int:
    ids = list(COMPONENT_PARAMS.keys())
    weights = [COMPONENT_PARAMS[c]["weight"] for c in ids]
    return random.choices(ids, weights=weights)[0]
