-- One row per customer per MONTH. Powers the Finance dashboard. Revenue is
-- native monthly data; VAT/NHIL/GETFund/withholding are allocated down from
-- the real quarterly GRA filing (see int_tax_allocated_to_month.sql) rather
-- than forcing revenue itself into quarterly buckets. period_quarter and
-- period_year are included so a BI slicer can roll this up to
-- quarterly/half-yearly/annual views without needing separate models -
-- that's what a date-hierarchy slicer is for.
--
-- MARGIN SCOPE NOTE: equipment_sales_margin is real (fact_equipment_sales
-- carries a genuine cost_basis). Rental and Service revenue do NOT have a
-- true cost basis in this data model - Service revenue is currently
-- modeled as equal to work-order cost (zero designed-in markup), so a
-- blended "total margin" across all three streams would imply cost data
-- that doesn't actually exist for two of them. Only equipment sales margin
-- is surfaced here, deliberately, rather than fabricating the rest.

with revenue_pivoted as (
    select
        customer_id,
        period_month,
        sum(case when revenue_stream = 'Equipment Sales' then revenue else 0 end) as equipment_sales_revenue,
        sum(case when revenue_stream = 'Rental' then revenue else 0 end) as rental_revenue,
        sum(case when revenue_stream = 'Service' then revenue else 0 end) as service_revenue,
        sum(revenue) as total_revenue
    from {{ ref('int_revenue_by_stream_month') }}
    group by 1, 2
),

equipment_sales_margin as (
    select
        customer_id,
        date_trunc('month', sale_date)::date as period_month,
        sum(cost_basis) as equipment_sales_cost,
        sum(margin) as equipment_sales_margin
    from {{ ref('stg_equipment_sales') }}
    group by 1, 2
),

tax as (
    select
        customer_id,
        period_month,
        quarter_start,
        quarter_end,
        filing_status,
        vat_amount_allocated,
        nhil_amount_allocated,
        getfund_amount_allocated,
        (vat_amount_allocated + nhil_amount_allocated + getfund_amount_allocated) as total_output_tax_allocated,
        withholding_tax_allocated
    from {{ ref('int_tax_allocated_to_month') }}
),

combined as (
    select
        coalesce(r.customer_id, t.customer_id) as customer_id,
        coalesce(r.period_month, t.period_month) as period_month,
        coalesce(r.equipment_sales_revenue, 0) as equipment_sales_revenue,
        coalesce(r.rental_revenue, 0) as rental_revenue,
        coalesce(r.service_revenue, 0) as service_revenue,
        coalesce(r.total_revenue, 0) as total_revenue,
        t.quarter_start,
        t.quarter_end,
        t.filing_status,
        t.vat_amount_allocated,
        t.nhil_amount_allocated,
        t.getfund_amount_allocated,
        t.total_output_tax_allocated,
        t.withholding_tax_allocated
    from revenue_pivoted r
    full outer join tax t
        on r.customer_id = t.customer_id
        and r.period_month = t.period_month
)

select
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.contract_tier,
    i.industry_id,
    i.industry_name,
    c.region,
    combined.period_month,
    extract(quarter from combined.period_month)::int as period_quarter,
    extract(year from combined.period_month)::int as period_year,
    combined.equipment_sales_revenue,
    combined.rental_revenue,
    combined.service_revenue,
    combined.total_revenue,
    coalesce(esm.equipment_sales_cost, 0) as equipment_sales_cost,
    coalesce(esm.equipment_sales_margin, 0) as equipment_sales_margin,
    case when combined.equipment_sales_revenue > 0
        then round(100.0 * coalesce(esm.equipment_sales_margin, 0) / combined.equipment_sales_revenue, 1)
        else null
    end as equipment_sales_margin_pct,
    combined.quarter_start as filing_quarter_start,
    combined.quarter_end as filing_quarter_end,
    combined.filing_status,
    combined.vat_amount_allocated,
    combined.nhil_amount_allocated,
    combined.getfund_amount_allocated,
    combined.total_output_tax_allocated,
    combined.withholding_tax_allocated
from combined
left join {{ ref('stg_customers') }} c on combined.customer_id = c.customer_id
left join {{ ref('stg_industries') }} i on c.industry_id = i.industry_id
left join equipment_sales_margin esm
    on combined.customer_id = esm.customer_id
    and combined.period_month = esm.period_month
