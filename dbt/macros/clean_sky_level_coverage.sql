{#
    This macro replace the abnormal sky level coverage with null 
#}

{% macro clean_sky_level_coverage(column) -%}
    {% set valid_vals = var('valid_sky_level_coverage') %}
    case
        when {{ column }} in (
            {% for val in valid_vals %}
                '{{ val }}'{% if not loop.last %}, {% endif %}
            {% endfor %}
        )
        then {{ column }}
        else null
    end
{%- endmacro %}
