-- GRA filings (VAT/NHIL/GETFund/withholding) are genuinely quarterly events
-- in Ghana - that's a real regulatory cadence, not an arbitrary modeling
-- choice, so fact_tax_filings stays quarterly at the source. But a business
-- user deciding things month-to-month shouldn't be forced into quarterly
-- revenue just because of that filing cadence. This model allocates each
-- quarter's filed tax amounts down to its 3 constituent months, weighted by
-- each month's ACTUAL share of that quarter's revenue - not a flat 1/3
-- split, since revenue is rarely spread evenly across a quarter (e.g. a
-- customer might buy an excavator in one specific month).
--
-- The filing itself (status, quarter boundaries) is still fully visible per
-- row, so nothing about the real quarterly filing event is hidden - only
-- the tax AMOUNT is apportioned for monthly analysis.

with quarterly_filings as (
    select
        customer_id,
        period_start as quarter_start,
        period_end as quarter_end,
        vat_amount,
        nhil_amount,
        getfund_amount,
        withholding_tax_amount,
        filing_status
    from {{ ref('stg_tax_filings') }}
),

monthly_totals as (
    select customer_id, period_month, sum(revenue) as month_revenue
    from {{ ref('int_revenue_by_stream_month') }}
    group by 1, 2
),

-- expand each quarterly filing into its 3 constituent months
quarter_months as (
    select
        qf.*,
        gs.month_start::date as period_month
    from quarterly_filings qf
    cross join generate_series(qf.quarter_start, qf.quarter_start + interval '2 months', interval '1 month') as gs(month_start)
),

joined as (
    select
        qm.*,
        coalesce(mt.month_revenue, 0) as month_revenue
    from quarter_months qm
    left join monthly_totals mt
        on mt.customer_id = qm.customer_id
        and mt.period_month = qm.period_month
),

quarter_totals as (
    select customer_id, quarter_start, sum(month_revenue) as quarter_revenue_sum
    from joined
    group by 1, 2
)

select
    j.customer_id,
    j.period_month,
    j.quarter_start,
    j.quarter_end,
    j.filing_status,
    -- if the whole quarter had zero revenue (edge case), fall back to an
    -- even split rather than dividing by zero
    case when qt.quarter_revenue_sum > 0 then j.month_revenue / qt.quarter_revenue_sum else 1.0 / 3 end as revenue_share_of_quarter,
    round(j.vat_amount * (case when qt.quarter_revenue_sum > 0 then j.month_revenue / qt.quarter_revenue_sum else 1.0 / 3 end), 2) as vat_amount_allocated,
    round(j.nhil_amount * (case when qt.quarter_revenue_sum > 0 then j.month_revenue / qt.quarter_revenue_sum else 1.0 / 3 end), 2) as nhil_amount_allocated,
    round(j.getfund_amount * (case when qt.quarter_revenue_sum > 0 then j.month_revenue / qt.quarter_revenue_sum else 1.0 / 3 end), 2) as getfund_amount_allocated,
    round(j.withholding_tax_amount * (case when qt.quarter_revenue_sum > 0 then j.month_revenue / qt.quarter_revenue_sum else 1.0 / 3 end), 2) as withholding_tax_allocated
from joined j
join quarter_totals qt
    on j.customer_id = qt.customer_id
    and j.quarter_start = qt.quarter_start
