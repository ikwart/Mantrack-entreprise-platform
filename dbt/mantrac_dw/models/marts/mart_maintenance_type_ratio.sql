-- Supporting mart for the "Predictive vs Reactive" ratio KPI - one row per
-- month per maintenance_type, so the dashboard can trend the ratio over time
-- rather than showing a single static number.
select
    date_trunc('month', scheduled_date)::date as period_month,
    maintenance_type,
    is_predictive_sourced,
    count(*) as work_order_count
from {{ ref('stg_maintenance_events') }}
where status = 'Completed'
group by 1, 2, 3
