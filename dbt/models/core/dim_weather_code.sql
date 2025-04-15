{{ config(materialized='table') }}

select distinct present_weather_code as weather_code
from {{ ref("fct_silver_table") }}
order by weather_code