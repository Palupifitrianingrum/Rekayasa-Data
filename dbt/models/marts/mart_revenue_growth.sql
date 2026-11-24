with monthly_sales as (
    select
        ym,
        sum(revenue) as monthly_revenue
    from {{ ref('mart_monthly_revenue') }}
    group by ym
),

revenue_with_previous as (
    select
        ym,
        monthly_revenue,
        lag(monthly_revenue) over (
            order by ym
        ) as previous_month_revenue
    from monthly_sales
)

select
    ym,
    monthly_revenue,
    previous_month_revenue,
    monthly_revenue - previous_month_revenue as revenue_growth,
    round(
        100.0 * (monthly_revenue - previous_month_revenue)
        / nullif(previous_month_revenue, 0),
        2
    ) as growth_percentage
from revenue_with_previous
order by ym
