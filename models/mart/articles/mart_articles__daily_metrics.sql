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
  track_category_metrics AS (
    SELECT
      event_date,
      article_id,
      COUNTIF(track_category = 'Article Amazon Link') AS amazon_clicks,
      COUNTIF(track_category = 'Article series link end') AS series_end_clicks,
      COUNTIF(track_category = 'Article Related Link Next') AS related_next_clicks,
      COUNTIF(track_category = 'Article Denshiban Link') AS denshiban_clicks,
      COUNTIF(track_category = 'Article Photolink Top') AS photo_top_clicks,
      COUNTIF(track_category = 'Article Comiclink Top') AS comic_top_clicks,
      COUNTIF(track_category = 'Article Social Share bottom') AS share_bottom_clicks,
      COUNTIF(track_category = 'Article Leaks Link') AS leaks_clicks,
      COUNTIF(track_category = 'Article Othercut Bottom') AS othercut_clicks,
    FROM
      {{ ref('whs_ga__clicks') }}
    WHERE
    {% if is_incremental() %}
      event_date >= _dbt_max_partition
      AND
    {% endif %}
      article_id IS NOT NULL
    GROUP BY
      1,
      2
  ),
  page_view_metrics AS (
    SELECT
      event_date,
      article_id,
      ANY_VALUE(published_at) AS published_at,
      ANY_VALUE(article_type) AS article_type,
      LOGICAL_OR(page_type = 'ad') AS is_ad,
      COUNT(1) AS page_views,
      COUNT(DISTINCT session_key) AS sessions,
      COUNT(DISTINCT user_pseudo_id) AS unique_users,
      COUNTIF(read_to_end) AS read_to_ends
    FROM
      {{ ref('whs_ga__page_views') }}
    WHERE
    {% if is_incremental() %}
      event_date >= _dbt_max_partition
      AND
    {% endif %}
      article_id IS NOT NULL
    GROUP BY
      1,
      2
  ),
  joined AS (
    SELECT
      *
    FROM
      page_view_metrics
      LEFT JOIN track_category_metrics USING (event_date, article_id)
  )
SELECT
  *,
  event_date = DATE(published_at) AS is_published_date,
  {{ is_1st_week('event_date', 'published_at') }} AS is_1st_week,
  {{ is_1st_month('event_date', 'published_at') }} AS is_1st_month,
FROM
  joined
