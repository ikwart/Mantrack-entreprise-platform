-- Singular test: fails if (part_id, branch, snapshot_date) isn't unique in
-- the inventory mart - that combination should be exactly one row.
select part_id, branch, snapshot_date, count(*)
from {{ ref('mart_warehouse_inventory') }}
group by 1, 2, 3
having count(*) > 1
