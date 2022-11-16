WITH
  session_agg AS (
    SELECT
      session_key,
      COUNT(1) AS page_views,
      COUNT(DISTINCT article_id) AS articles,
      COUNT(DISTINCT IF(read_to_end, article_id, NULL)) AS read_to_ends,
    FROM
      {{ ref('whs_ga__page_views') }}
    GROUP BY
      1
  ),
  session_window AS (
    SELECT DISTINCT
      session_key,
      user_pseudo_id,
      -- ENTRANCE
      FIRST_VALUE(event_date) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS event_date,
      FIRST_VALUE(event_timestamp) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS entrance_timestamp,
      FIRST_VALUE(article_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS entrance_article_id,
      FIRST_VALUE(page_view_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS entrance_page_view_id,
      FIRST_VALUE(page_url) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS entrance_page_url,
      FIRST_VALUE(page_url_canonical) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS entrance_page_url_canonical,
      FIRST_VALUE(source) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS source,
      FIRST_VALUE(medium) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS medium,
      FIRST_VALUE(campaign) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS campaign,
      FIRST_VALUE(content) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS content,
      FIRST_VALUE(term) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS term,
      FIRST_VALUE(session_number) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS session_number,
      FIRST_VALUE(prefecture) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS prefecture,
      -- EXIT
      FIRST_VALUE(user_id) OVER (PARTITION BY session_key ORDER BY event_timestamp DESC) AS user_id,
      FIRST_VALUE(event_timestamp) OVER (PARTITION BY session_key ORDER BY event_timestamp DESC) AS exit_timestamp,
      FIRST_VALUE(article_id) OVER (PARTITION BY session_key ORDER BY event_timestamp DESC) AS exit_article_id,
      FIRST_VALUE(page_view_id) OVER (PARTITION BY session_key ORDER BY event_timestamp DESC) AS exit_page_view_id,
      FIRST_VALUE(page_url) OVER (PARTITION BY session_key ORDER BY event_timestamp DESC) AS exit_page_url,
      FIRST_VALUE(page_url_canonical) OVER (PARTITION BY session_key ORDER BY event_timestamp DESC) AS exit_page_url_canonical
    FROM
      {{ ref('whs_ga__page_views') }}
  )
SELECT
  *,
  TIMESTAMP_DIFF(exit_timestamp, entrance_timestamp, SECOND) AS session_duration,
FROM
  session_agg
  JOIN session_window USING (session_key);
