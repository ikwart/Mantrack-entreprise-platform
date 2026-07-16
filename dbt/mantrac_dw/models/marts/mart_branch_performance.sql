-- One row per Mantrac BRANCH per month (Accra/Kumasi/Takoradi/Tarkwa) - not
-- to be confused with the Industry slicer (who we serve). Branch answers
-- who at Mantrac did the work, derived from the technician assigned to each
-- work order (dim_technician.branch), which is a more accurate signal of
-- branch workload than customer proximity would be.
--
-- SLA compliance interpretation: sla_target_hours is a response/completion
-- time commitment (see fact_maintenance_events column comment - tighter for
-- higher contract tiers). A job is "SLA met" here if its actual duration
-- (actual_completion - actual_start) fell within that target. Only
-- Completed work orders with real timestamps are measurable - work orders
-- still Scheduled (e.g. newly-converted predictive ones that haven't
-- happened yet in the simulation timeline) are correctly excluded rather
-- than counted as a default pass or fail.

with completed as (
    select m.*, t.branch
    from {{ ref('stg_maintenance_events') }} m
    left join {{ ref('stg_technicians') }} t on m.technician_id = t.technician_id
    where m.status = 'Completed'
),

with_duration as (
    select
        *,
        case when actual_start is not null and actual_completion is not null
            then extract(epoch from (actual_completion - actual_start)) / 3600.0
            else null
        end as actual_duration_hours
    from completed
)

select
    branch,
    date_trunc('month', scheduled_date)::date as period_month,
    count(*) as work_order_count,
    count(*) filter (where maintenance_type = 'Preventive') as preventive_count,
    count(*) filter (where maintenance_type = 'Corrective') as corrective_count,
    count(*) filter (where maintenance_type = 'Predictive') as predictive_count,
    count(*) filter (where maintenance_type = 'Inspection') as inspection_count,
    round(avg(downtime_hours)::numeric, 2) as avg_downtime_hours,
    count(*) filter (where actual_duration_hours is not null) as sla_measurable_count,
    count(*) filter (where actual_duration_hours is not null and actual_duration_hours <= sla_target_hours) as sla_met_count,
    round(
        100.0 * count(*) filter (where actual_duration_hours is not null and actual_duration_hours <= sla_target_hours)
        / nullif(count(*) filter (where actual_duration_hours is not null), 0)
    , 1) as sla_compliance_pct,
    count(distinct technician_id) as active_technicians
from with_duration
group by branch, period_month
order by period_month, branch
