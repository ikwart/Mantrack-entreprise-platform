-- unit_cost here is the USD REFERENCE cost (see currency convention note in
-- sql/transactions/generate_transactions.py). fact_inventory carries the
-- GHS-converted transactional cost - don't mix the two up in a mart.
select
    part_id,
    part_number,
    part_name,
    part_category,
    unit_cost as unit_cost_usd,
    lead_time_days
from {{ source('raw', 'dim_part') }}
