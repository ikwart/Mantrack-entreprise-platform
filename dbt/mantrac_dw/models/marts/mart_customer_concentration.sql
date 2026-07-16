-- One row per customer, all-time revenue with concentration metrics - the
-- "top 3 customers = X% of revenue" framing a CFO actually asks for, not
-- just a sorted table (which mart_finance's customer-month grain already
-- supports but doesn't frame this way). Deliberately all-time rather than
-- trailing-N-months: with a fleet this small (18 customers), a shorter
-- window would swing wildly month to month and mislead about genuine
-- concentration risk.

with customer_totals as (
    select
        customer_id,
        sum(total_revenue) as total_revenue
    from {{ ref('mart_finance') }}
    group by customer_id
),

ranked as (
    select
        *,
        rank() over (order by total_revenue desc) as revenue_rank,
        sum(total_revenue) over () as company_total_revenue
    from customer_totals
)

select
    r.customer_id,
    c.customer_name,
    c.customer_type,
    c.contract_tier,
    i.industry_name,
    r.revenue_rank,
    r.total_revenue,
    round(100.0 * r.total_revenue / nullif(r.company_total_revenue, 0), 1) as pct_of_total_revenue,
    round(
        100.0 * sum(r.total_revenue) over (order by r.revenue_rank rows between unbounded preceding and current row)
        / nullif(r.company_total_revenue, 0)
    , 1) as cumulative_pct_of_total_revenue
from ranked r
left join {{ ref('stg_customers') }} c on r.customer_id = c.customer_id
left join {{ ref('stg_industries') }} i on c.industry_id = i.industry_id
order by r.revenue_rank
