with customer_spending as (
    select
        dc.customer_id,
        dc.customer_name,
        dc.region,
        count(distinct fs.order_id) as total_orders,
        sum(fs.total_amount) as total_spending
    from {{ ref('fact_sales') }} as fs
    join {{ ref('dim_customer') }} as dc
        on fs.customer_id = dc.customer_id
    group by
        dc.customer_id,
        dc.customer_name,
        dc.region
)

select
    *,
    row_number() over (
        order by total_spending desc
    ) as customer_rank
from customer_spending
