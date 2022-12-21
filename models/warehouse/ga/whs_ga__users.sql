{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by={
      "field": "event_date",
      "data_type": "date",
    },
    tags=["hourly"],
  )
}}

WITH
  target_term AS (
    SELECT
      event_occured_date,
      target_date
    FROM
    {% if is_incremental() %}
      UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(_dbt_max_partition, INTERVAL 27 day), CURRENT_DATE('Asia/Tokyo'))) AS event_occured_date,
      UNNEST(GENERATE_DATE_ARRAY(_dbt_max_partition, CURRENT_DATE('Asia/Tokyo'))) AS target_date
    {% else %}
      UNNEST(GENERATE_DATE_ARRAY(PARSE_DATE('%Y%m%d', {{ var('ga_start_date') }}), CURRENT_DATE('Asia/Tokyo'))) AS event_occured_date,
      UNNEST(GENERATE_DATE_ARRAY(event_occured_date, DATE_ADD(event_occured_date, INTERVAL 27 day))) AS target_date
    {% endif %}
  ),
  user_performance AS (
    SELECT
      user_pseudo_id,
      event_date AS event_occured_date,
      MAX(user_id) AS user_id,
      COUNT(1) AS sessions,
      SUM(page_views) AS page_views,
      SUM(articles) AS articles,
      SUM(read_to_ends) AS read_to_ends,
    FROM
      {{ ref('whs_ga__sessions') }}
    {% if is_incremental() %}
    WHERE
      event_date >= DATE_SUB(_dbt_max_partition, INTERVAL 27 day)
    {% endif %}
    GROUP BY
      1,
      2
  ),
  joined AS (
    SELECT
      user_pseudo_id,
      target_date AS event_date,
      MAX(user_id) AS user_id,
      COUNT(DISTINCT event_occured_date) AS frequency,
      COUNT(1) AS sessions_last_4_weeks,
      SUM(page_views) AS page_views_last_4_weeks,
      SUM(articles) AS articles_last_4_weeks,
      SUM(read_to_ends) AS read_to_ends_last_4_weeks,
      LOGICAL_OR(event_occured_date = target_date) AS is_visited_on_the_day,
      COUNTIF(event_occured_date = target_date) AS sessions,
      SUM(IF(event_occured_date = target_date, page_views, 0)) AS page_views,
      SUM(IF(event_occured_date = target_date, articles, 0)) AS articles,
      SUM(IF(event_occured_date = target_date, read_to_ends, 0)) AS read_to_ends,
    FROM
      target_term
      JOIN user_performance USING (event_occured_date)
    WHERE
      target_date <= CURRENT_DATE('Asia/Tokyo')
    GROUP BY
      1,
      2
  )
SELECT
  *,
  {{ calc_frequency_segment('frequency') }} AS frequency_segment
FROM
  joined
