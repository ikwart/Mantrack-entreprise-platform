-- One row per part per branch (current snapshot). Powers the Warehouse &
-- Inventory dashboard: value AT COST (never selling price - see dim_part
-- staging note), stockout risk, turnover, dead/slow-moving stock, and
-- predictive-vs-baseline demand.
--
-- KNOWN LIMITATION shared by turnover AND dead-stock detection below:
-- int_parts_consumption has no branch column (fact_maintenance_parts_used
-- only links to a work order's customer, not a Mantrac branch), so
-- consumption stats are computed per PART globally, not per part+branch -
-- the same annualized_consumption/last_consumption_date is applied to all 4
-- branch rows of a given part. Pre-existing limitation, not introduced by
-- the dead-stock addition - flagged here rather than silently compounded.

with consumption_stats as (
    -- annualized consumption rate per part, used for turnover.
    -- NOTE: date - date in Postgres returns an integer (days directly),
    -- not an interval - no extract() needed/valid here.
    select
        part_id,
        sum(qty_used) as total_qty_used,
        count(distinct scheduled_date) as days_with_activity,
        max(scheduled_date) as last_consumption_date,
        sum(qty_used) / nullif(
            (select (max(scheduled_date) - min(scheduled_date)) from {{ ref('int_parts_consumption') }}), 0
        ) * 365 as annualized_consumption
    from {{ ref('int_parts_consumption') }}
    group by 1
),

demand_forecast_counts as (
    select
        part_id,
        count(*) filter (where demand_source = 'Predictive Signal') as predictive_demand_signals,
        sum(expected_qty) filter (where demand_source = 'Predictive Signal') as predictive_demand_qty
    from {{ ref('stg_parts_demand_forecast') }}
    group by 1
)

select
    inv.part_id,
    p.part_number,
    p.part_name,
    p.part_category,
    inv.branch,
    inv.snapshot_date,
    inv.qty_on_hand,
    inv.unit_cost_ghs,
    inv.inventory_value_ghs,
    inv.reorder_point,
    inv.days_of_supply,
    inv.is_below_reorder_point,
    coalesce(cs.annualized_consumption, 0) as annualized_consumption,
    case when inv.qty_on_hand > 0
        then round(coalesce(cs.annualized_consumption, 0) / inv.qty_on_hand, 2)
        else null
    end as inventory_turnover_ratio,
    cs.last_consumption_date,
    (inv.snapshot_date - cs.last_consumption_date) as days_since_last_consumption,
    case when inv.qty_on_hand > 0
        and (cs.last_consumption_date is null or (inv.snapshot_date - cs.last_consumption_date) > 180)
        then true else false
    end as is_dead_stock,
    case when inv.qty_on_hand > 0
        and (cs.last_consumption_date is null or (inv.snapshot_date - cs.last_consumption_date) > 180)
        then inv.inventory_value_ghs else 0
    end as dead_stock_value,
    coalesce(df.predictive_demand_signals, 0) as predictive_demand_signals,
    coalesce(df.predictive_demand_qty, 0) as predictive_demand_qty
from {{ ref('stg_inventory') }} inv
left join {{ ref('stg_parts') }} p on inv.part_id = p.part_id
left join consumption_stats cs on inv.part_id = cs.part_id
left join demand_forecast_counts df on inv.part_id = df.part_id
