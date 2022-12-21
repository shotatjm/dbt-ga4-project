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
  session_agg AS (
    SELECT
      session_key,
      MIN(event_date) AS event_date,
      COUNT(1) AS page_views,
      COUNT(DISTINCT article_id) AS articles,
      COUNT(DISTINCT IF(read_to_end, article_id, NULL)) AS read_to_ends,
    FROM
      {{ ref('whs_ga__page_views') }}
    {% if is_incremental() %}
    WHERE
      event_date >= _dbt_max_partition
    {% endif %}
    GROUP BY
      1
  ),
  session_window AS (
    SELECT DISTINCT
      session_key,
      user_pseudo_id,
      -- ENTRANCE
      FIRST_VALUE(event_date) OVER (PARTITION BY session_key ORDER BY created_at) AS event_date,
      FIRST_VALUE(created_at) OVER (PARTITION BY session_key ORDER BY created_at) AS entrance_timestamp,
      FIRST_VALUE(article_id) OVER (PARTITION BY session_key ORDER BY created_at) AS entrance_article_id,
      FIRST_VALUE(page_view_id) OVER (PARTITION BY session_key ORDER BY created_at) AS entrance_page_view_id,
      FIRST_VALUE(page_url) OVER (PARTITION BY session_key ORDER BY created_at) AS entrance_page_url,
      FIRST_VALUE(page_url_canonical) OVER (PARTITION BY session_key ORDER BY created_at) AS entrance_page_url_canonical,
      FIRST_VALUE(source) OVER (PARTITION BY session_key ORDER BY created_at) AS source,
      FIRST_VALUE(medium) OVER (PARTITION BY session_key ORDER BY created_at) AS medium,
      FIRST_VALUE(campaign) OVER (PARTITION BY session_key ORDER BY created_at) AS campaign,
      FIRST_VALUE(content) OVER (PARTITION BY session_key ORDER BY created_at) AS content,
      FIRST_VALUE(term) OVER (PARTITION BY session_key ORDER BY created_at) AS term,
      FIRST_VALUE(ga_session_number) OVER (PARTITION BY session_key ORDER BY created_at) AS ga_session_number,
      -- EXIT
      FIRST_VALUE(user_id) OVER (PARTITION BY session_key ORDER BY created_at DESC) AS user_id,
      FIRST_VALUE(created_at) OVER (PARTITION BY session_key ORDER BY created_at DESC) AS exit_timestamp,
      FIRST_VALUE(article_id) OVER (PARTITION BY session_key ORDER BY created_at DESC) AS exit_article_id,
      FIRST_VALUE(page_view_id) OVER (PARTITION BY session_key ORDER BY created_at DESC) AS exit_page_view_id,
      FIRST_VALUE(page_url) OVER (PARTITION BY session_key ORDER BY created_at DESC) AS exit_page_url,
      FIRST_VALUE(page_url_canonical) OVER (PARTITION BY session_key ORDER BY created_at DESC) AS exit_page_url_canonical
    FROM
      {{ ref('whs_ga__page_views') }}
    {% if is_incremental() %}
    WHERE
      event_date >= _dbt_max_partition
    {% endif %}
  )
SELECT
  *,
  TIMESTAMP_DIFF(exit_timestamp, entrance_timestamp, SECOND) AS session_duration,
FROM
  session_agg
  JOIN session_window USING (session_key, event_date)
