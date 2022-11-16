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
  joined AS (
    SELECT
      event_date,
      article_id,
      sessions.source,
      sessions.medium,
      published_at,
      session_key,
      user_pseudo_id,
      read_to_end,
    FROM
      {{ ref('whs_ga__page_views') }} AS page_views
      JOIN {{ ref('whs_ga__sessions') }} AS sessions USING (event_date, user_pseudo_id, session_key)
    {% if is_incremental() %}
    WHERE
      event_date >= _dbt_max_partition
    {% endif %}
  ),
  grouped AS (
    SELECT
      event_date,
      article_id,
      {{ classify_referrer('source, medium') }} AS referrer,
      ANY_VALUE(published_at) AS published_at,
      COUNT(1) AS page_views,
      COUNT(DISTINCT session_key) AS sessions,
      COUNT(DISTINCT user_pseudo_id) AS unique_users,
      COUNTIF(read_to_end) AS read_to_ends,
    FROM
      joined
  )
SELECT
  *,
  event_date = DATE(published_at) AS is_published_date,
  {{ is_1st_week('event_date', 'published_at') }} AS is_1st_week,
  {{ is_1st_month('event_date', 'published_at') }} AS is_1st_month,
FROM
  grouped
