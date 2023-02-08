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
  event_extracted AS (
    SELECT
      *,
    FROM
      {{ ref('whs_ga__events') }}
    WHERE
      event_name = 'page_view'
    {% if is_incremental() %}
      AND event_date >= _dbt_max_partition
    {% endif %}
  ),
  add_useful_columns AS (
    SELECT
      *,
      EXISTS(
        SELECT
          page_view_id
        FROM
          {{ ref('whs_ga__read_to_ends') }} AS read_to_end
        WHERE
          read_to_end.event_date = event_extracted.event_date
        {% if is_incremental() %}
          AND event_date >= _dbt_max_partition
        {% endif %}
          AND read_to_end.page_view_id = event_extracted.page_view_id
      ) AS read_to_end,
    FROM
      event_extracted
  )
SELECT
  *
FROM
  add_useful_columns
