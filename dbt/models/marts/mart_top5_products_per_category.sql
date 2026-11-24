with product_sales as (
    select
        dp.category,
        dp.product_id,
        dp.product_name,
        sum(fs.quantity) as total_quantity,
        sum(fs.total_amount) as total_revenue
    from {{ ref('fact_sales') }} as fs
    join {{ ref('dim_product') }} as dp
        on fs.product_id = dp.product_id
    group by
        dp.category,
        dp.product_id,
        dp.product_name
),

ranked_products as (
    select
        *,
        row_number() over (
            partition by category
            order by total_revenue desc
        ) as product_rank
    from product_sales
)

select *
from ranked_products
where product_rank <= 5
