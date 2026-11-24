select
    dc.region,
    date_trunc('month', fs.date_key)::date as sales_month,
    sum(fs.total_amount) as total_revenue
from {{ ref('fact_sales') }} as fs
join {{ ref('dim_customer') }} as dc
    on fs.customer_id = dc.customer_id
group by
    dc.region,
    date_trunc('month', fs.date_key)::date