{{ config(materialized='table') }}

WITH
  max_dt AS (
    SELECT
      DATE(MAX(observation_time)) AS max_d
    FROM {{ ref('fct_silver_table') }}
  ),

  spine AS (
    SELECT
      day AS date_day
    FROM max_dt,
    UNNEST(
      GENERATE_DATE_ARRAY(
        CAST('2023-01-01' AS DATE), 
        max_d,
        INTERVAL 1 DAY                 
      )
    ) AS day
  )

SELECT
  date_day,
  EXTRACT(YEAR  FROM date_day)    AS year,
  EXTRACT(MONTH FROM date_day)    AS month,
  EXTRACT(WEEK  FROM date_day)    AS week_of_year,
  'test' as test_ci_jobs,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1,7) THEN TRUE
    ELSE FALSE
  END AS is_weekend
FROM spine
