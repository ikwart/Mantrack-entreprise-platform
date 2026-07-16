-- Historical parts consumption joined to the branch/date it was consumed at,
-- used to derive inventory turnover and fill-rate style metrics.
select
    pu.part_id,
    wo.customer_id,
    wo.scheduled_date,
    pu.qty_used
from {{ ref('stg_maintenance_parts_used') }} pu
inner join {{ ref('stg_maintenance_events') }} wo on pu.work_order_id = wo.work_order_id
