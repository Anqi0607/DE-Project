{{ config(materialized="incremental", unique_key="observation_id") }}

with bronze as (select * from {{ ref("stg_bronze_table") }})

select
    observation_id,
    station,
    observation_time,
    lon,
    lat,
    air_temperature,
    dew_point_temperature,
    relative_humidity,
    {{ replace_wind_direction("wind_direction") }} as wind_direction,
    wind_speed,
    precipitation,
    pressure_altimeter,
    sea_level_pressure,
    visibility,
    wind_gust,
    {{ clean_sky_level_coverage("sky_level_1_coverage") }} as sky_level_1_coverage,
    {{ clean_sky_level_coverage("sky_level_2_coverage") }} as sky_level_2_coverage,
    {{ clean_sky_level_coverage("sky_level_3_coverage") }} as sky_level_3_coverage,
    {{ clean_sky_level_coverage("sky_level_4_coverage") }} as sky_level_4_coverage,
    sky_level_1_altitude,
    sky_level_2_altitude,
    sky_level_3_altitude,
    sky_level_4_altitude,
    present_weather_code,
    ice_accretion_1hr,
    ice_accretion_3hr,
    ice_accretion_6hr,
    peak_wind_gust,
    {{ replace_wind_direction("peak_wind_drct") }} as peak_wind_drct,
    peak_wind_time,
    apparent_temperature,
    snow_depth
from bronze
where station is not null and observation_time is not null

-- use {{ this }} to refer to the target table of the model
-- compare the max timestamp of the existing target table and source table
-- only load the new data
{% if is_incremental() %}
       and observation_time > (
            -- use coalesce for first time load bronze_table
            select coalesce(max(observation_time), '1900-01-01') from {{ this }}
        )

{% endif %}

-- dbt build --select stg_bronze_table --vars '{"is_test_run": false}' 
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
