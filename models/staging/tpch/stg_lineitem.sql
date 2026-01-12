with source as (
  select * from {{ source('tpch', 'LINEITEM') }}
)

select
  L_ORDERKEY           as order_key,
  L_PARTKEY            as part_key,
  L_SUPPKEY           as supplier_key,
  L_LINENUMBER        as line_number,
  L_QUANTITY          as quantity,
  L_EXTENDEDPRICE    as extended_price,
  L_DISCOUNT         as discount,
  L_TAX              as tax,
  L_RETURNFLAG       as return_flag,
  L_LINESTATUS      as line_status,
  L_SHIPDATE        as ship_date,
  L_COMMITDATE      as commit_date,
  L_RECEIPTDATE     as receipt_date,
  L_SHIPINSTRUCT    as ship_instruction,
  L_SHIPMODE        as ship_mode,
  L_COMMENT         as comment
from source