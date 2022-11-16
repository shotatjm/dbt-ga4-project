WITH
  unioned AS (
    SELECT * FROM {{ ref('whs_ga__base_events') }}
    UNION ALL
    SELECT * FROM {{ ref('whs_ga__base_events_intraday') }}
  ),
  unnested AS (
    SELECT
      *,
      {{ ga_unnest_key('event_params', 'ga_session_id', 'int_value') }},
      {{ ga_unnest_key('event_params', 'ga_session_number',  'int_value') }},
      {{ ga_unnest_key('event_params', 'page_location', rename_column = 'page_url') }},
      {{ ga_unnest_key('event_params', 'page_title') }},
      {{ ga_unnest_key('event_params', 'page_referrer') }},
      {{ ga_unnest_key('event_params', 'source') }},
      {{ ga_unnest_key('event_params', 'medium') }},
      {{ ga_unnest_key('event_params', 'campaign') }},
      {{ ga_unnest_key('event_params', 'content') }},
      {{ ga_unnest_key('event_params', 'term') }},
      {{ ga_unnest_key('event_params', 'page_view_id') }}, # custom event parameter
      {{ ga_unnest_key('event_params', 'page_type') }}, # custom event parameter
      {{ ga_unnest_key('event_params', 'article_id') }}, # custom event parameter
      {{ ga_unnest_key('event_params', 'article_type') }}, # custom event parameter
      {{ ga_unnest_key('event_params', 'published_at') }}, # custom event parameter
    FROM
      unioned
  ),
  casted AS (
    SELECT
      * EXCEPT( event_name, published_at ),
      LOWER(REPLACE(TRIM(event_name), " ", "_")) AS event_name,
      SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', published_at) AS published_at, # custom event parameter
    FROM
      unnested
  ),
  add_useful_columns AS (
    SELECT
      *,
      {{ normalize_url('page_url') }} AS page_url_canonical,
      TO_BASE64(MD5(CONCAT(stream_id, user_pseudo_id, CAST(ga_session_id AS STRING)))) AS session_key,
      {{ convert_region('geo.region') }} AS prefecture,
      TIMESTAMP_MICROS(event_timestamp) AS created_at,
      {{ calc_page_number('page_url', 'page_type', 'device.category') }} AS page_number, # custom event parameter
    FROM
      casted
  )
SELECT
  *
FROM
  add_useful_columns
