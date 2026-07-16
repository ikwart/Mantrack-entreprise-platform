-- One row per customer, light rename only. Grain: 1 row per customer_id.
select
    customer_id,
    customer_code,
    customer_name,
    customer_type,
    industry_id,
    region,
    contract_tier
from {{ source('raw', 'dim_customer') }}
