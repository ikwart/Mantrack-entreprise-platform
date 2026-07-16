select
    model_id,
    category_id,
    model_name,
    engine_series,
    list_price_usd
from {{ source('raw', 'dim_equipment_model') }}
