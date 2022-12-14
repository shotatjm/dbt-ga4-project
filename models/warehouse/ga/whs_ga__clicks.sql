WITH
  event_extracted AS (
    SELECT
      *,
    FROM
      {{ ref('whs_ga__events') }}
    WHERE
      event_name = 'click'
  ),
  unnested AS (
    SELECT
      *,
      {{ ga_unnest_key('event_params', 'track_category') }}, # custom event parameter
      {{ ga_unnest_key('event_params', 'link_url') }},
      {{ ga_unnest_key('event_params', 'outbound') }},
    FROM
      event_extracted
  ),
  casted AS (
    SELECT
      * EXCEPT ( outbound ),
      {{ normalize_url('link_url') }} AS link_url_canonical,
      COALESCE(CAST(outbound AS BOOLEAN), FALSE) AS outbound,
    FROM
      unnested
  )
SELECT
  *
FROM
  casted
