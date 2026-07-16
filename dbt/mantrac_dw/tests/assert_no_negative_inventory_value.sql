-- Singular test: inventory value should never be negative - a basic sanity
-- guard against a bad unit_cost or qty_on_hand slipping through upstream.
select part_id, branch, inventory_value_ghs
from {{ ref('mart_warehouse_inventory') }}
where inventory_value_ghs < 0
