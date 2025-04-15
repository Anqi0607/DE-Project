{{ config(materialized="table") }}

select distinct station
from {{ ref("fct_silver_table") }}
order by station
