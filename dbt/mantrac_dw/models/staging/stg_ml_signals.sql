select
    signal_id,
    equipment_id,
    scoring_date,
    predicted_component_id,
    failure_probability,
    model_version,
    recommended_action_window_days,
    signal_status,
    dismiss_reason,
    created_at
from {{ source('raw', 'ml_maintenance_signals') }}
