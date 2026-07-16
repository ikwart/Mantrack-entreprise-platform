select category_id, category_name, primary_application
from {{ source('raw', 'dim_equipment_category') }}
