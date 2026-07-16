-- Grain: 1 row per equipment sale. sale_price/cost_basis are GHS (transactional currency).
select
    sale_id,
    equipment_id,
    customer_id,
    sale_date,
    sale_price,
    cost_basis,
    sale_price - cost_basis as margin,
    salesperson
from {{ source('raw', 'fact_equipment_sales') }}
