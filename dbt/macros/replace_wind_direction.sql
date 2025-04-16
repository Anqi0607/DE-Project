{#
    This macro replace the wind direction degree 0 to 360 
    and replace null with avg value of each station
#}

{% macro replace_wind_direction(wind_direction) -%}

    case
        when {{ "b." ~ wind_direction }} = 0 then 360
        when {{ "b." ~ wind_direction }} is null then {{ "s.avg_" ~ wind_direction }}
        else {{ "b." ~ wind_direction  }}
    end

{%- endmacro %}