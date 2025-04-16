{{ config(materialized="table") }}

with source as (select * from {{ source("silver", "fct_silver_table") }})
select
    station,
    avg(air_temperature) as avg_air_temperature,
    avg(dew_point_temperature) as avg_dew_point_temperature,
    avg(relative_humidity) as avg_relative_humidity,
    avg(wind_direction) as avg_wind_direction,
    avg(wind_speed) as avg_wind_speed,
    avg(precipitation) as avg_precipitation,
    avg(pressure_altimeter) as avg_pressure_altimeter,
    avg(sea_level_pressure) as avg_sea_level_pressure,
    avg(visibility) as avg_visibility,
    avg(wind_gust) as avg_wind_gust,
    avg(visibility) as avg_,
    avg(sky_level_1_altitude) as avg_sky_level_1_altitude,
    avg(sky_level_2_altitude) as avg_sky_level_2_altitude,
    avg(sky_level_3_altitude) as avg_sky_level_3_altitude,
    avg(sky_level_4_altitude) as avg_sky_level_4_altitude,
    avg(ice_accretion_1hr) as avg_ice_accretion_1hr,
    avg(ice_accretion_3hr) as avg_ice_accretion_3hr,
    avg(ice_accretion_6hr) as avg_ice_accretion_6hr,
    avg(peak_wind_gust) as avg_peak_wind_gust,
    avg(peak_wind_drct) as avg_peak_wind_drct,
    avg(peak_wind_time) as avg_peak_wind_time,
    avg(apparent_temperature) as avg_apparent_temperature,
    avg(snow_depth) as avg_snow_depth
from source
group by station
