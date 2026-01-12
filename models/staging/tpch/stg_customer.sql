with source as (
  select * from {{ source('tpch', 'CUSTOMER') }}
)

select
  C_CUSTKEY      as customer_key,
  C_NAME         as customer_name,
  C_ADDRESS      as customer_address,
  C_NATIONKEY    as nation_key,
  C_PHONE        as phone,
  C_ACCTBAL      as account_balance,
  C_MKTSEGMENT   as market_segment,
  C_COMMENT      as comment
from source
