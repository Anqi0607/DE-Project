{{ config(materialized="table") }}

with valid_values as (
  -- convert var to a json and pass into UNNEST
  -- UNNEST will split the json and make each element a row
  select value as sky_level_coverage
  from unnest({{ var("valid_sky_level_coverage") | tojson }}) as value
)

select *
from valid_values
order by sky_level_coverage
