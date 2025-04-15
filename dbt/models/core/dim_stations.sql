{{ config(materialized="table") }}

select distinct station
order by station
from {{ ref("fct_silver_table") }}
