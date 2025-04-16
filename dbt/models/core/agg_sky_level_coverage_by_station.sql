{{ config(materialized="view") }}

with silver as (select * from {{ ref("fct_silver_table") }})

select station, sky_level_1_coverage, count(*) as cnt
from silver
where sky_level_1_coverage is not null
group by 1, 2
order by 1, 3 desc
