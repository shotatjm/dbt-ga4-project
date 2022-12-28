WITH
  article_metrics AS (
    SELECT
      article_id,
      -- page view metrics
      ANY_VALUE(article_type) AS article_type,
      ANY_VALUE(is_ad) AS is_ad,
      SUM(page_views) AS page_views,
      SUM(sessions) AS sessions,
      SUM(unique_users) AS unique_users,
      SUM(read_to_ends) AS read_to_ends,
      SUM(IF(is_1st_week, page_views, 0)) AS page_views_1st_week,
      SUM(IF(is_1st_week, sessions, 0)) AS sessions_1st_week,
      SUM(IF(is_1st_week, unique_users, 0)) AS unique_users_1st_week,
      SUM(IF(is_1st_week, read_to_ends, 0)) AS read_to_ends_1st_week,
      SUM(IF(is_1st_month, page_views, 0)) AS page_views_1st_month,
      SUM(IF(is_1st_month, sessions, 0)) AS sessions_1st_month,
      SUM(IF(is_1st_month, unique_users, 0)) AS unique_users_1st_month,
      SUM(IF(is_1st_month, read_to_ends, 0)) AS read_to_ends_1st_month,
      -- track category metrics
      SUM(photo_top_clicks) AS photo_top_clicks,
      SUM(comic_top_clicks) AS comic_top_clicks,
      SUM(amazon_clicks) AS amazon_clicks,
      SUM(denshiban_clicks) AS denshiban_clicks,
      SUM(leaks_clicks) AS leaks_clicks,
      SUM(series_end_clicks) AS series_end_clicks,
      SUM(related_next_clicks) AS related_next_clicks,
      SUM(othercut_clicks) AS othercut_clicks,
      SUM(share_bottom_clicks) AS share_bottom_clicks,
    FROM
      {{ ref('mart_articles__daily_metrics') }}
    GROUP BY
      1
  )
SELECT
  *
FROM
  {{ ref('whs_cms__articles') }} AS articles
  JOIN article_metrics USING (article_id);
