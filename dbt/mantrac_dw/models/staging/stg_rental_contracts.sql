-- Grain: 1 row per rental contract period. total_billed is GHS.
select
    rental_id,
    equipment_id,
    customer_id,
    start_date,
    end_date,
    (end_date is null) as is_ongoing,
    rate_type,
    rate,
    total_billed,
    actual_usage_hours
from {{ source('raw', 'fact_rental_contracts') }}
