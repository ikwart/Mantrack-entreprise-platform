-- One row per equipment. Powers the Fleet Operations dashboard: utilization,
-- uptime/MTBF/MTTR, owned-vs-rented, and geographic rollups.

with telemetry_summary as (
    select
        equipment_id,
        avg(utilization_hours) as avg_daily_utilization_hours,
        max(engine_hours) as latest_engine_hours,
        sum(fault_count_critical) as total_critical_faults,
        count(*) as telemetry_days_observed
    from {{ ref('stg_telemetry_daily') }}
    group by 1
),

maintenance_summary as (
    select
        equipment_id,
        count(*) filter (where maintenance_type = 'Corrective') as corrective_count,
        avg(downtime_hours) filter (where maintenance_type = 'Corrective') as avg_mttr_hours,
        sum(downtime_hours) as total_downtime_hours,
        max(scheduled_date) as last_service_date
    from {{ ref('stg_maintenance_events') }}
    where status = 'Completed'
    group by 1
),

-- MTBF: average days between consecutive corrective events per machine
corrective_gaps as (
    select
        equipment_id,
        scheduled_date,
        scheduled_date - lag(scheduled_date) over (partition by equipment_id order by scheduled_date) as days_since_prior_corrective
    from {{ ref('stg_maintenance_events') }}
    where maintenance_type = 'Corrective' and status = 'Completed'
),

mtbf as (
    select equipment_id, avg(days_since_prior_corrective) as mtbf_days
    from corrective_gaps
    where days_since_prior_corrective is not null
    group by 1
),

rental_summary as (
    select
        equipment_id,
        sum(total_billed) as total_rental_billed,
        sum(actual_usage_hours) as total_actual_usage_hours
    from {{ ref('stg_rental_contracts') }}
    group by 1
)

select
    e.equipment_id,
    e.serial_number,
    e.ownership_type,
    e.status,
    e.install_date,
    em.model_name,
    ec.category_name,
    ec.primary_application,
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.region,
    ts.avg_daily_utilization_hours,
    ts.latest_engine_hours,
    ts.total_critical_faults,
    ms.corrective_count,
    ms.avg_mttr_hours,
    ms.total_downtime_hours,
    ms.last_service_date,
    mt.mtbf_days,
    rs.total_rental_billed,
    rs.total_actual_usage_hours
from {{ ref('stg_equipment') }} e
left join {{ ref('stg_equipment_models') }} em on e.model_id = em.model_id
left join {{ ref('stg_equipment_categories') }} ec on em.category_id = ec.category_id
left join {{ ref('stg_customers') }} c on e.customer_id = c.customer_id
left join telemetry_summary ts on e.equipment_id = ts.equipment_id
left join maintenance_summary ms on e.equipment_id = ms.equipment_id
left join mtbf mt on e.equipment_id = mt.equipment_id
left join rental_summary rs on e.equipment_id = rs.equipment_id
