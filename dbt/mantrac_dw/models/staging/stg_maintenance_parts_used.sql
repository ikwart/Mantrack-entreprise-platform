select work_order_id, part_id, qty_used
from {{ source('raw', 'fact_maintenance_parts_used') }}
