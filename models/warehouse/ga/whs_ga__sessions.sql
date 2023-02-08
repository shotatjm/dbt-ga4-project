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
  session_entrance AS (
    SELECT DISTINCT
      session_key,
      created_at AS entrance_timestamp,
      article_id AS entrance_article_id,
      page_url AS entrance_page_url,
      source,
      medium,
      campaign,
      content,
      term,
      ga_session_number,
    FROM
      {{ ref('whs_ga__page_views') }}
    WHERE
      LAG(user_pseudo_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) IS NULL
    {% if is_incremental() %}
      AND event_date >= _dbt_max_partition
    {% endif %}
  ),
  session_exit AS (
    SELECT DISTINCT
      session_key,
      created_at AS exit_timestamp,
      article_id AS exit_article_id,
      page_url AS exit_page_url,
      user_id,
    FROM
      {{ ref('whs_ga__page_views') }}
    WHERE
      LEAD(user_pseudo_id) OVER (PARTITION BY session_key ORDER BY event_timestamp) IS NULL
    {% if is_incremental() %}
      AND event_date >= _dbt_max_partition
    {% endif %}
  )
SELECT
  *,
  TIMESTAMP_DIFF(exit_timestamp, entrance_timestamp, SECOND) AS session_duration,
FROM
  session_agg
  JOIN session_entrance USING (session_key)
  JOIN session_exit USING (session_key)
