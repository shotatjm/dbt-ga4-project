{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by={
      "field": "event_week",
      "data_type": "date",
    },
  )
}}

WITH
  stg_user_metrics_numbered AS (
    SELECT
      *,
      DATE_TRUNC(event_date, WEEK(MONDAY)) AS event_week,
      ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC(event_date, WEEK(MONDAY)) ORDER BY event_date DESC) AS rn
    FROM
      {{ ref('whs_ga__users') }}
    {% if is_incremental() %}
    WHERE
      event_date >= _dbt_max_partition
    {% endif %}
  ),
  stg_user_metrics AS (
    SELECT
      * EXCEPT (rn, event_date)
    FROM
      stg_user_metrics_numbered
    WHERE
      rn = 1
  ),
  user_metrics AS (
    SELECT
      event_week,
      COUNT(1) AS unique_users,
      COUNTIF(frequency_segment = 'light') AS light_users,
      COUNTIF(frequency_segment = 'medium') AS medium_users,
      COUNTIF(frequency_segment = 'heavy') AS heavy_users,
      COUNTIF(frequency_segment = 'royal') AS royal_users,
      SUM(sessions_last_4_weeks) AS sessions,
      SUM(IF(frequency_segment = 'light', sessions_last_4_weeks, 0)) AS light_users_sessions,
      SUM(IF(frequency_segment = 'medium', sessions_last_4_weeks, 0)) AS medium_users_sessions,
      SUM(IF(frequency_segment = 'heavy', sessions_last_4_weeks, 0)) AS heavy_users_sessions,
      SUM(IF(frequency_segment = 'royal', sessions_last_4_weeks, 0)) AS royal_users_sessions,
      SUM(page_views_last_4_weeks) AS page_views,
      SUM(IF(frequency_segment = 'light', page_views_last_4_weeks, 0)) AS light_users_page_views,
      SUM(IF(frequency_segment = 'medium', page_views_last_4_weeks, 0)) AS medium_users_page_views,
      SUM(IF(frequency_segment = 'heavy', page_views_last_4_weeks, 0)) AS heavy_users_page_views,
      SUM(IF(frequency_segment = 'royal', page_views_last_4_weeks, 0)) AS royal_users_page_views,
      SUM(read_to_ends_last_4_weeks) AS read_to_ends,
      SUM(IF(frequency_segment = 'light', read_to_ends_last_4_weeks, 0)) AS light_users_read_to_ends,
      SUM(IF(frequency_segment = 'medium', read_to_ends_last_4_weeks, 0)) AS medium_users_read_to_ends,
      SUM(IF(frequency_segment = 'heavy', read_to_ends_last_4_weeks, 0)) AS heavy_users_read_to_ends,
      SUM(IF(frequency_segment = 'royal', read_to_ends_last_4_weeks, 0)) AS royal_users_read_to_ends,
      SUM(articles_last_4_weeks) AS articles,
      SUM(IF(frequency_segment = 'light', articles_last_4_weeks, 0)) AS light_users_articles,
      SUM(IF(frequency_segment = 'medium', articles_last_4_weeks, 0)) AS medium_users_articles,
      SUM(IF(frequency_segment = 'heavy', articles_last_4_weeks, 0)) AS heavy_users_articles,
      SUM(IF(frequency_segment = 'royal', articles_last_4_weeks, 0)) AS royal_users_articles,
    FROM
      stg_user_metrics
    GROUP BY
      1
  )
SELECT
  *,
  SAFE_DIVIDE(sessions,unique_users) AS frequency,
  SAFE_DIVIDE(page_views, sessions) AS pages_per_session,
  SAFE_DIVIDE(read_to_ends, articles) AS read_to_end_rate,
FROM
  user_metrics
