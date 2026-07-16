-- Unions all three revenue streams onto a common (customer_id, period_month,
-- revenue_stream, revenue) grain. Monthly is the finest useful grain here -
-- quarter/half-year/year views are just the BI tool's date hierarchy rolling
-- this up, not a separate model. See int_tax_allocated_to_month.sql for why
-- tax stays a quarterly-sourced allocation rather than native monthly data.

with sales as (
    select
        customer_id,
        date_trunc('month', sale_date)::date as period_month,
        'Equipment Sales' as revenue_stream,
        sum(sale_price) as revenue
    from {{ ref('stg_equipment_sales') }}
    group by 1, 2
),

rental as (
    select
        customer_id,
        date_trunc('month', start_date)::date as period_month,
        'Rental' as revenue_stream,
        sum(total_billed) as revenue
    from {{ ref('stg_rental_contracts') }}
    group by 1, 2
),

service as (
    select
        customer_id,
        date_trunc('month', scheduled_date)::date as period_month,
        'Service' as revenue_stream,
        sum(total_cost) as revenue
    from {{ ref('stg_maintenance_events') }}
    where status = 'Completed'
    group by 1, 2
)

select * from sales
union all
select * from rental
union all
select * from service
