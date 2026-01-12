with orders as (
  select * from {{ ref('stg_orders') }}
),

line_items as (
  select * from {{ ref('stg_lineitem') }}
),

order_items as (
  select
    order_key,
    sum(extended_price * (1 - discount)) as net_item_revenue,
    sum(extended_price) as gross_item_revenue,
    sum(quantity) as total_quantity,
    avg(discount) as avg_discount,
    count(*) as line_count

  from line_items
  group by 1
),

customer_geo as (
  select
    c.customer_key,
    c.market_segment,
    n.nation_name,
    r.region_name
  from {{ ref('stg_customer') }} c
  left join {{ ref('stg_nation') }} n
    on c.nation_key = n.nation_key
  left join {{ ref('stg_region') }} r
    on n.region_key = r.region_key
),

enriched_orders as (
  select
    o.order_key,
    o.customer_key,
    o.order_date,
    o.order_status,

    -- order-level finance
    o.total_price,
    oi.net_item_revenue,
    oi.gross_item_revenue,
    (o.total_price - coalesce(oi.net_item_revenue, 0)) as price_vs_items_delta,

    -- quantity
    oi.total_quantity,
    oi.line_count,
    oi.avg_discount,

    -- customer attributes
    cg.market_segment,
    cg.nation_name,
    cg.region_name,

    -- helpful analytic flags
    case
      when o.order_status in ('F', 'O') then true else false
    end as is_completed_order,

    case
      when coalesce(oi.net_item_revenue, 0) >= 10000 then 'high_value'
      when coalesce(oi.net_item_revenue, 0) >= 3000 then 'mid_value'
      else 'low_value'
    end as order_value_band

  from orders o
  left join order_items oi
    on o.order_key = oi.order_key
  left join customer_geo cg
    on o.customer_key = cg.customer_key
)

select *
from enriched_orders
where order_key is not null
