-- Grain: 1 row per work order. total_cost is GHS.
select
    work_order_id,
    equipment_id,
    customer_id,
    technician_id,
    maintenance_type,
    priority,
    source_signal_id,
    (source_signal_id is not null) as is_predictive_sourced,
    scheduled_date,
    actual_start,
    actual_completion,
    downtime_hours,
    sla_target_hours,
    labor_hours,
    total_cost,
    warranty_flag,
    status,
    root_cause,
    follow_up_required
from {{ source('raw', 'fact_maintenance_events') }}
