select technician_id, technician_name, branch, certification_level
from {{ source('raw', 'dim_technician') }}
