select component_id, component_name, system_category
from {{ source('raw', 'dim_component') }}
