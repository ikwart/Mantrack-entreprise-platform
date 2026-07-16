select site_id, site_name, customer_id, region
from {{ source('raw', 'dim_site') }}
