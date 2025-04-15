{{ config(materialized="incremental", unique_key="observation_id") }}

with
    source as (select * from {{ source("staging", "bronze_table") }}),

    renamed as (

        select

            -- identifiers 
            {{ dbt_utils.generate_surrogate_key(["valid", "station"]) }}
            as observation_id,
            station,
            valid as observation_time,

            -- geolocation info
            lon,
            lat,

            -- weather info
            tmpf as air_temperature,
            dwpf as dew_point_temperature,
            relh as relative_humidity,
            drct as wind_direction,
            sknt as wind_speed,
            p01i as precipitation,
            alti as pressure_altimeter,
            mslp as sea_level_pressure,
            vsby as visibility,
            gust as wind_gust,
            skyc1 as sky_level_1_coverage,
            skyc2 as sky_level_2_coverage,
            skyc3 as sky_level_3_coverage,
            skyc4 as sky_level_4_coverage,
            skyl1 as sky_level_1_altitude,
            skyl2 as sky_level_2_altitude,
            skyl3 as sky_level_3_altitude,
            skyl4 as sky_level_4_altitude,
            wxcodes as present_weather_code,
            ice_accretion_1hr,
            ice_accretion_3hr,
            ice_accretion_6hr,
            peak_wind_gust,
            peak_wind_drct,
            peak_wind_time,
            feel as apparent_temperature,
            snowdepth as snow_depth

        from source

    )

select *
from renamed

-- use {{ this }} to refer to the target table of the model
-- compare the max timestamp of the existing target table and source table
-- only load the new data
{% if is_incremental() %}

    where
        observation_time > (
            -- use coalesce for first time load bronze_table
            select coalesce(max(observation_time), '1900-01-01') from {{ this }}
        )

{% endif %}

-- dbt build --select stg_bronze_table --vars '{"is_test_run": false}' 
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
