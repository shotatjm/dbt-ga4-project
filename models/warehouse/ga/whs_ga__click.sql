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
      {{ ga_unnest_key('event_params', 'track_category') }},
      {{ ga_unnest_key('event_params', 'share_type') }},
      {{ ga_unnest_key('event_params', 'link_url') }},
      {{ ga_unnest_key('event_params', 'outbound') }},
    FROM
      event_extracted
  ),
  casted AS (
    * EXCEPT ( outbound ),
    {{ normalize_url('link_url') }} AS link_url_canonical,
    COALESCE(CAST(outbound AS BOOLEAN), FALSE) AS outbound,
  )
SELECT
  *
FROM
  casted
