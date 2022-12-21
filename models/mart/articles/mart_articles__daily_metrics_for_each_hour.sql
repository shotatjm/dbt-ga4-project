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

WITH
  grouped AS (
    SELECT
      event_date,
      article_id,
      EXTRACT(HOUR FROM created_at AT TIME ZONE 'Asia/Tokyo') AS hour,
      ANY_VALUE(published_at) AS published_at,
      COUNT(1) AS page_views,
      COUNT(DISTINCT session_key) AS sessions,
      COUNT(DISTINCT user_pseudo_id) AS unique_users,
      COUNTIF(read_to_end) AS read_to_ends,
    FROM
      {{ ref('whs_ga__page_views') }}
    {% if is_incremental() %}
    WHERE
      event_date >= _dbt_max_partition
    {% endif %}
    GROUP BY
      1,
      2,
      3
  )
SELECT
  *,
  event_date = DATE(published_at) AS is_published_date,
  {{ is_1st_week('event_date', 'published_at') }} AS is_1st_week,
  {{ is_1st_month('event_date', 'published_at') }} AS is_1st_month,
FROM
  grouped
