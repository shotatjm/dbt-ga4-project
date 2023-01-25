{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by={
      "field": "event_date",
      "data_type": "date",
    },
  )
}}

SELECT
  event_date,
  COUNT(1) AS unique_users,
  COUNTIF(frequency_segment = 'light') AS light_users,
  COUNTIF(frequency_segment = 'medium') AS medium_users,
  COUNTIF(frequency_segment = 'heavy') AS heavy_users,
  COUNTIF(frequency_segment = 'royal') AS royal_users,
  SUM(sessoins) AS sessoins,
  SUM(page_views) AS page_views,
  SUM(articles) AS articles,
  SUM(read_to_ends) AS read_to_ends,
FROM
  {{ ref('whs_ga__users') }}
WHERE
  is_visited_on_the_day
{% if is_incremental() %}
  AND event_date >= _dbt_max_partition
{% endif %}
GROUP BY
  1
