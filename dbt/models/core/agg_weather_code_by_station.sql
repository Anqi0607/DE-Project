{{ config(materialized="view") }}

with silver as (select * from {{ ref("fct_silver_table") }})

select station, present_weather_code, count(*) as cnt
from silver
where present_weather_code is not null
group by 1, 2
order by 1, 3 desc
