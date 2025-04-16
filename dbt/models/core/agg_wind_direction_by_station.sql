{{ config(materialized="view") }}

with silver as (select * from {{ ref("fct_silver_table") }})

select station, wind_direction, count(*) as cnt
from silver
where wind_direction is not null
group by 1, 2
order by 1, 3 desc