-- Grain: 1 row per physical machine (equipment_id).
select
    equipment_id,
    model_id,
    serial_number,
    customer_id,
    site_id,
    install_date,
    ownership_type,
    status
from {{ source('raw', 'dim_equipment') }}
