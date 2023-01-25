-- This model will be unioned with `base_events` which means that their columns must match
-- source('ga', 'events_intraday') may not exist depending on runtime timing

{%- set table_exists = load_relation(source('ga', 'events_intraday_today')) is not none -%}

{% if table_exists %}

SELECT
  PARSE_DATE('%Y%m%d',event_date) AS event_date,
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
  {{ source('ga', 'events_intraday') }}

{% else %}

WITH blank_table AS (
  SELECT
    SAFE_CAST(NULL AS DATE) AS event_date,
    SAFE_CAST(NULL AS INT64) AS event_timestamp,
    SAFE_CAST(NULL AS STRING) AS event_name,
    SAFE_CAST(NULL AS ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>) AS event_params,
    SAFE_CAST(NULL AS INT64) AS event_previous_timestamp,
    SAFE_CAST(NULL AS INT64) AS event_bundle_sequence_id,
    SAFE_CAST(NULL AS INT64) AS event_server_timestamp_offset,
    SAFE_CAST(NULL AS STRING) AS user_id,
    SAFE_CAST(NULL AS STRING) AS user_pseudo_id,
    SAFE_CAST(NULL AS STRUCT<analytics_storage STRING, ads_storage STRING, uses_transient_token STRING>) AS privacy_info,
    SAFE_CAST(NULL AS ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64, set_timestamp_micros INT64>>>) AS user_properties,
    SAFE_CAST(NULL AS INT64) AS user_first_touch_timestamp,
    SAFE_CAST(NULL AS STRUCT<category STRING, mobile_brand_name STRING, mobile_model_name STRING, mobile_marketing_name STRING, mobile_os_hardware_model STRING, operating_system STRING, operating_system_version STRING, vendor_id STRING, advertising_id STRING, language STRING, is_limited_ad_tracking STRING, time_zone_offset_seconds INT64, browser STRING, browser_version STRING, web_info STRUCT<browser STRING, browser_version STRING, hostname STRING>>) AS device,
    SAFE_CAST(NULL AS STRUCT<continent STRING, country STRING, region STRING, city STRING, sub_continent STRING, metro STRING>) AS geo,
    SAFE_CAST(NULL AS STRUCT<id STRING, version STRING, install_store STRING, firebase_app_id STRING, install_source STRING>) AS app_info,
    SAFE_CAST(NULL AS STRUCT<name STRING, medium STRING, source STRING>) AS traffic_source,
    SAFE_CAST(NULL AS STRING) AS stream_id,
    SAFE_CAST(NULL AS STRING) AS platform,
)
SELECT
  *
FROM
  blank_table
WHERE
  event_date IS NOT NULL

{% endif %}
