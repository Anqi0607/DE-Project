{#
    This macro replace the null value in numeric columns with avg value of each station
#}

{% macro replace_null_with_avg(column_name) %}
  case
      when {{ column_name }} is null then (
          select {{ "avg_" ~ column_name }}
          from {{ ref('agg_silver_stats') }} as stats
          -- because in stats table, avgs are group by station
          where stats.station = bronze.station
      )
      else {{ column_name }}
  end
{% endmacro %}
