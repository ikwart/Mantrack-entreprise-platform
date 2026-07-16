select industry_id, industry_name
from {{ source('raw', 'dim_industry') }}
