SELECT
  daily_metrics_for_each_related.article_id,
  related_article_id,
  ANY_VALUE(article_log.title) AS related_article_title,
  ANY_VALUE(article_log.thumbnail) AS related_article_thumbnail,
  ANY_VALUE(article_log.link) AS related_article_link,
  ANY_VALUE(article_log.published_at) AS related_article_published_at,
  ANY_VALUE(article_log.creator) AS related_article_creator,
  SUM(clicks) AS clicks,
  SUM(clicks_related_link) AS clicks_related_link,
  SUM(clicks_related_link_next) AS clicks_related_link_next,
  SUM(clicks_related_link_popin) AS clicks_related_link_popin,
  SUM(clicks_related_link_logly) AS clicks_related_link_logly,
  SUM(clicks_series_link) AS clicks_series_link,
  SUM(clicks_series_link_end) AS clicks_series_link_end,
  SUM(clicks_inner_link) AS clicks_inner_link,
  SUM(clicks_others) AS clicks_others,
  SUM(IF(is_1st_week, clicks, 0)) AS clicks_1st_week,
  SUM(IF(is_1st_month, clicks, 0)) AS clicks_1st_month,
FROM
  {{ ref('mart_articles__daily_metrics_for_each_related') }} AS daily_metrics_for_each_related
  LEFT JOIN {{ ref('whs_cms__article_log') }} AS article_log ON daily_metrics_for_each_related.related_article_id = article_log.article_id
GROUP BY
  1,
  2;
