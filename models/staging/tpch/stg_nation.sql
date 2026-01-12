with source as (
  select * from {{ source('tpch', 'NATION') }}
)

select
  N_NATIONKEY    as nation_key,
  N_NAME         as nation_name,
  N_REGIONKEY    as region_key,
  N_COMMENT      as comment
from source