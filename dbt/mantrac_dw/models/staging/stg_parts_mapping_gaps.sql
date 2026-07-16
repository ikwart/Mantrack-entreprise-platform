select gap_id, signal_id, component_id, model_id, resolved
from {{ source('raw', 'parts_mapping_gaps') }}
