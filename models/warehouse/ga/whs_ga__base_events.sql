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

SELECT
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  event_timestamp,
  event_name,
  event_params,
  event_previous_timestamp,
  -- event_value_in_usd,
  event_bundle_sequence_id,
  event_server_timestamp_offset,
  user_id,
  user_pseudo_id,
  privacy_info,
  user_properties,
  user_first_touch_timestamp,
  -- user_ltv,
  device,
  geo,
  app_info,
  traffic_source,
  stream_id,
  platform,
  -- ecommerce,
  -- items,
FROM
  {{ source('ga', 'events') }}
WHERE
  _TABLE_SUFFIX NOT LIKE '%intraday%'
  AND CAST(_TABLE_SUFFIX AS INT64) >= {{ var('ga_start_date') }}
{% if is_incremental() %}
  AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= _dbt_max_partition
{% endif %}
