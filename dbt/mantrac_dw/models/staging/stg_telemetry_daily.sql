-- Grain: 1 row per equipment per day.
select
    equipment_id,
    date_id,
    engine_hours,
    utilization_hours,
    fault_count_info,
    fault_count_warning,
    fault_count_critical,
    fault_count_info + fault_count_warning + fault_count_critical as fault_count_total,
    avg_sensor_reading,
    max_sensor_reading
from {{ source('raw', 'fact_telemetry_daily') }}
