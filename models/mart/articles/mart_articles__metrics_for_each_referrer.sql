SELECT
  article_id,
  referrer,
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
FROM
  {{ ref('mart_articles__daily_metrics_for_each_referrer') }}
GROUP BY
  1,
  2;
