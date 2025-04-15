{#
    This macro replace the wind direction degree 0 to 360 
#}

{% macro replace_wind_direction(wind_direction) -%}

    case
        when {{ wind_direction }} = 0 then 360
        else {{ wind_direction }}
    end

{%- endmacro %}