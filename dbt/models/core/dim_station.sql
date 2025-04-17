{{ config(materialized="table") }}

select distinct station, lat, lon
from {{ ref("fct_silver_table") }}
order by station
