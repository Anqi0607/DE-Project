{{ config(
    materialized='incremental',
    unique_key='observation_id',
    partition_by={
        "field": "partition_date",
        "data_type": "date"
    },
    cluster_by=["station"]
) }}

with
    bronze as (select * from {{ ref("stg_bronze_table") }}),
    stats as (select * from {{ ref("agg_silver_stats") }})

select
    b.observation_id,
    b.station,
    b.observation_time,
    b.partition_date,
    b.lon,
    b.lat,
    -- when data size is big, use coalesce along with join will provide better performance
    -- when data size is small, we can use the replace_null_with_avg macro (it will run a query for every numeric column)
    coalesce(b.air_temperature, s.avg_air_temperature) as air_temperature,
    coalesce(
        b.dew_point_temperature, s.avg_dew_point_temperature
    ) as dew_point_temperature,
    coalesce(b.relative_humidity, s.avg_relative_humidity) as relative_humidity,
    {{ replace_wind_direction("wind_direction") }} as wind_direction,
    coalesce(b.wind_speed, s.avg_wind_speed) as wind_speed,
    coalesce(b.precipitation, s.avg_precipitation) as precipitation,
    coalesce(b.pressure_altimeter, s.avg_pressure_altimeter) as pressure_altimeter,
    coalesce(b.sea_level_pressure, s.avg_sea_level_pressure) as sea_level_pressure,
    coalesce(b.visibility, s.avg_visibility) as visibility,
    coalesce(b.wind_gust, s.avg_wind_gust) as wind_gust,
    {{ clean_sky_level_coverage("sky_level_1_coverage") }} as sky_level_1_coverage,
    {{ clean_sky_level_coverage("sky_level_2_coverage") }} as sky_level_2_coverage,
    {{ clean_sky_level_coverage("sky_level_3_coverage") }} as sky_level_3_coverage,
    {{ clean_sky_level_coverage("sky_level_4_coverage") }} as sky_level_4_coverage,
    coalesce(
        b.sky_level_1_altitude, s.avg_sky_level_1_altitude
    ) as sky_level_1_altitude,
    coalesce(
        b.sky_level_2_altitude, s.avg_sky_level_2_altitude
    ) as sky_level_2_altitude,
    coalesce(
        b.sky_level_3_altitude, s.avg_sky_level_3_altitude
    ) as sky_level_3_altitude,
    coalesce(
        b.sky_level_4_altitude, s.avg_sky_level_4_altitude
    ) as sky_level_4_altitude,
    present_weather_code,
    coalesce(b.ice_accretion_1hr, s.avg_ice_accretion_1hr) as ice_accretion_1hr,
    coalesce(b.ice_accretion_3hr, s.avg_ice_accretion_3hr) as ice_accretion_3hr,
    coalesce(b.ice_accretion_6hr, s.avg_ice_accretion_6hr) as ice_accretion_6hr,
    coalesce(b.peak_wind_gust, s.avg_peak_wind_gust) as peak_wind_gust,
    {{ replace_wind_direction("peak_wind_drct") }} as peak_wind_drct,
    coalesce(b.peak_wind_time, s.avg_peak_wind_time) as peak_wind_time,
    coalesce(
        b.apparent_temperature, s.avg_apparent_temperature
    ) as apparent_temperature,
    coalesce(b.snow_depth, s.avg_snow_depth) as snow_depth

from bronze b
left join stats s on b.station = s.station
where
    b.station is not null and b.observation_time is not null

    -- use {{ this }} to refer to the target table of the model
    -- compare the max timestamp of the existing target table and source table
    -- only load the new data
    {% if is_incremental() %}
        and b.observation_time > (
            -- use coalesce for first time load bronze_table
            select coalesce(max(observation_time), '1900-01-01') from {{ this }}
        )

    {% endif %}

-- dbt build --select stg_bronze_table --vars '{"is_test_run": false}' 
{% if var("is_test_run", default=true) %} limit 100 {% endif %}