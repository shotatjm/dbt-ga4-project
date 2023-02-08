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
  stg_following_log AS (
    SELECT
      event_date,
      article_id,
      LEAD(article_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS following_article_id,
      published_at,
    FROM
      {{ ref('whs_ga__page_views') }}
    {% if is_incremental() %}
    WHERE
      event_date >= _dbt_max_partition
    {% endif %}
  ),
  following_log AS (
    SELECT
      event_date,
      article_id,
      following_article_id AS related_article_id,
      ANY_VALUE(published_at) AS published_at,
      COUNT(1) AS clicks,
    FROM
      stg_following_log
    WHERE
      following_article_id IS NOT NULL
      AND following_article_id != article_id
    GROUP BY
      1,
      2,
      3
  ),
  click_log AS (
    SELECT
      event_date,
      article_id,
      REGEXP_EXTRACT(link_url, r"https://bunshun\.jp/articles/[-_a-z]+/([0-9]+).*") AS related_article_id,
      COUNTIF(track_category = 'Article Related Link') AS clicks_related_link,
      COUNTIF(track_category = 'Article Related Link Next') AS clicks_related_link_next,
      COUNTIF(track_category = 'Article series link') AS clicks_series_link,
      COUNTIF(track_category = 'Article series link end') AS clicks_series_link_end,
      COUNTIF(track_category = 'Article Inner Link') AS clicks_inner_link
    FROM
      {{ ref('whs_ga__clicks') }}
    WHERE
    {% if is_incremental() %}
      event_date >= _dbt_max_partition
      AND
    {% endif %}
      track_category IN ('Article Related Link', 'Article Related Link Next', 'Article series link', 'Article series link end', 'Article Inner Link')
      AND NOT outbound
    GROUP BY
      1,
      2,
      3
  ),
  joined AS (
    SELECT
      following_log.*,
      IFNULL(clicks_related_link, 0) AS clicks_related_link,
      IFNULL(clicks_related_link_next, 0) AS clicks_related_link_next,
      IFNULL(clicks_series_link, 0) AS clicks_series_link,
      IFNULL(clicks_series_link_end, 0) AS clicks_series_link_end,
      IFNULL(clicks_inner_link, 0) AS clicks_inner_link,
    FROM
      following_log
      LEFT JOIN
      click_log USING (event_date, article_id, related_article_id)
  )
SELECT
  *,
  clicks - (clicks_related_link + clicks_related_link_next + clicks_series_link + clicks_series_link_end + clicks_inner_link) AS clicks_others,
  event_date = DATE(published_at) AS is_published_date,
  {{ is_1st_week('event_date', 'published_at') }} AS is_1st_week,
  {{ is_1st_month('event_date', 'published_at') }} AS is_1st_month,
FROM
  joined
