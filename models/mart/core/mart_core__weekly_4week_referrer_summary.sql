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
  target_term AS (
    SELECT
      event_occured_week,
      target_week,
    FROM
    {% if is_incremental() %}
      UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(_dbt_max_partition, INTERVAL 4 WEEK), CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 WEEK)) AS event_occured_week,
      UNNEST(GENERATE_DATE_ARRAY(_dbt_max_partition, CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 WEEK)) AS target_week
    {% else %}
      UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST({{ var('ga_start_date') }} AS STRING)), WEEK(MONDAY)), CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 WEEK)) AS event_occured_week,
      UNNEST(GENERATE_DATE_ARRAY(event_occured_week, DATE_ADD(event_occured_week, INTERVAL 4 WEEK), INTERVAL 1 WEEK)) AS target_week
    {% endif %}
  ),
  weekly_referrer_metrics AS (
    SELECT
      DATE_TRUNC(event_date, WEEK(MONDAY)) AS event_occured_week,
      utility.CLASSIFY_REFERRER(source, medium) AS referrer,
      SUM(page_views) AS page_views,
      COUNT(1) AS sessions,
      COUNT(DISTINCT user_pseudo_id) AS unique_users,
      SUM(read_to_ends) AS read_to_ends,
      SUM(articles) AS articles,
    FROM
      {{ ref('whs_ga__sessions') }}
    {% if is_incremental() %}
    WHERE
      event_date >= DATE_SUB(_dbt_max_partition, INTERVAL 4 WEEK)
    {% endif %}
    GROUP BY
      1,
      2
  ),
  joined AS (
    SELECT
      target_week AS event_week,
      referrer,
      SUM(sessions) AS sessions,
      SUM(page_views) AS page_views,
      SUM(articles) AS articles,
      SUM(read_to_ends) AS read_to_ends,
    FROM
      target_term
      JOIN weekly_referrer_metrics USING (event_occured_week)
    WHERE
      target_week <= CURRENT_DATE('Asia/Tokyo')
    GROUP BY
      1,
      2
  )
SELECT
  *,
  SAFE_DIVIDE(page_views, sessions) AS pages_per_session,
  SAFE_DIVIDE(read_to_ends, articles) AS read_to_end_rate,
FROM
  joined
