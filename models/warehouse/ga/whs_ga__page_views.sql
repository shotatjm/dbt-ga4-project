WITH
  event_extracted AS (
    SELECT
      *,
    FROM
      {{ ref('whs_ga__events') }}
    WHERE
      event_name = 'page_view'
  ),
  add_useful_columns AS (
    SELECT
      *,
      -- entrance, exit, bounce
      LAG(user_pseudo_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) IS NULL AS is_entrance,
      LEAD(user_pseudo_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) IS NULL AS is_exit,
      LAG(user_pseudo_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) IS NULL AND LEAD(user_pseudo_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) IS NULL AS is_bounce,
      -- following page_view
      LEAD(page_url) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS following_page_url,
      LEAD(page_url_canonical) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS following_page_url_canonical,
      LEAD(article_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS following_article_id,
      LEAD(page_number) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS following_page_number,
      LEAD(page_type) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS following_page_type,
      -- previous page_view
      LAG(page_url) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS previous_page_url,
      LAG(page_url_canonical) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS previous_page_url_canonical,
      LAG(article_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS previous_article_id,
      LAG(page_number) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS previous_page_number,
      LAG(page_type) OVER (PARTITION BY session_key ORDER BY event_timestamp) AS previous_page_type,
      -- read_to_end
      EXISTS(
        SELECT
          page_view_id
        FROM
          {{ ref('whs_ga__read_to_ends') }} AS read_to_end
        WHERE
          read_to_end.event_date = event_extracted.event_date
          AND read_to_end.page_view_id = event_extracted.page_view_id
      ) AS read_to_end,
    FROM
      event_extracted
  )
SELECT
  *
FROM
  add_useful_columns
