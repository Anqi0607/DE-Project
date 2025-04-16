{{ config(materialized="view") }}

with silver as (select * from {{ ref("fct_silver_table") }})

select
    station,
    lat,
    lon,
    round(avg(wind_speed), 2) as avg_wind_speed,
    round(avg(dew_point_temperature - 32 / 1.8), 2) as avg_dew_point,
    round(avg((air_temperature - 32) / 1.8), 2) as avg_air_temperature,
    round(max((air_temperature - 32) / 1.8), 2) as max_air_temperature,
    round(min((air_temperature - 32) / 1.8), 2) as min_air_temperature,
    round(avg(visibility), 2) as avg_visibility,
    round(avg(wind_direction)) as avg_wind_direction,
    round(max(wind_gust)) as max_gust,
    round(avg(wind_gust), 2) as avg_gust

from silver
group by station, lat, lon
