with customers as (
  select * from {{ ref('stg_customer') }}
),

nations as (
  select * from {{ ref('stg_nation') }}
),

regions as (
  select * from {{ ref('stg_region') }}
),

orders as (
  select
    customer_key,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    count(*) as lifetime_orders,
    sum(coalesce(total_price, 0)) as lifetime_order_value
  from {{ ref('stg_orders') }}
  where customer_key is not null
  group by 1
),

line_revenue as (
  -- Customer-level revenue from lineitems (more realistic than order total)
  select
    o.customer_key,
    sum(li.extended_price * (1 - li.discount)) as lifetime_net_item_revenue,
    sum(li.quantity) as lifetime_quantity,
    avg(li.discount) as avg_discount_lifetime
  from {{ ref('stg_orders') }} o
  join {{ ref('stg_lineitem') }} li
    on o.order_key = li.order_key
  where o.customer_key is not null
  group by 1
),

enriched as (
  select
    c.customer_key,
    c.customer_name,
    c.market_segment,

    c.nation_key,
    n.nation_name,
    n.region_key,
    r.region_name,

    -- Customer lifecycle signals
    o.first_order_date,
    o.last_order_date,
    o.lifetime_orders,
    o.lifetime_order_value,

    lr.lifetime_net_item_revenue,
    lr.lifetime_quantity,
    lr.avg_discount_lifetime,

    -- Simple segmentation (great for downstream)
    case
      when coalesce(lr.lifetime_net_item_revenue, 0) >= 50000 then 'vip'
      when coalesce(lr.lifetime_net_item_revenue, 0) >= 15000 then 'growth'
      when coalesce(o.lifetime_orders, 0) = 0 then 'no_orders'
      else 'standard'
    end as customer_value_segment
  from customers c
  left join nations n
    on c.nation_key = n.nation_key
  left join regions r
    on n.region_key = r.region_key
  left join orders o
    on c.customer_key = o.customer_key
  left join line_revenue lr
    on c.customer_key = lr.customer_key
)

select * from enriched
where customer_key is not null
