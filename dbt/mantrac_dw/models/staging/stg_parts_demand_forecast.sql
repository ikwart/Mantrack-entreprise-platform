select
    forecast_id,
    signal_id,
    equipment_id,
    part_id,
    expected_qty,
    is_primary,
    needed_by_date,
    demand_source
from {{ source('raw', 'parts_demand_forecast') }}
