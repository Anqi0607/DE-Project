{#
    This macro replace the abnormal sky level coverage with null 
#}

{% macro clean_sky_level_coverage(sky_level_coverage) -%}

    case
        when {{ sky_level_coverage }} in ('FEW', 'OVC', 'SCT', 'BKN', 'NSC', 'VV') then {{ sky_level_coverage }}
        else null
    end

{%- endmacro %}