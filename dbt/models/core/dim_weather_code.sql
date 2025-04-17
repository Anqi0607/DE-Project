{{ config(materialized="table") }}

select distinct present_weather_code as weather_code
from {{ ref("fct_silver_table") }}
where present_weather_code is not null 
order by weather_code
