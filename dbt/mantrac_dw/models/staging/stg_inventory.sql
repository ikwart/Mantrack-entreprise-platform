-- Grain: 1 row per part per branch per snapshot date. Carried AT COST (unit_cost),
-- never selling price - see docs/architecture.md warehouse dashboard spec.
select
    part_id,
    branch,
    snapshot_date,
    qty_on_hand,
    unit_cost as unit_cost_ghs,
    qty_on_hand * unit_cost as inventory_value_ghs,
    reorder_point,
    days_of_supply,
    (qty_on_hand < reorder_point) as is_below_reorder_point
from {{ source('raw', 'fact_inventory') }}
