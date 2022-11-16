WITH
  event_extracted AS (
    SELECT
      *,
      {{ ga_unnest_key('event_params', 'method') }},
    FROM
      {{ ref('whs_ga__events') }}
    WHERE
      event_name = 'login'
  )
SELECT
  *
FROM
  event_extracted
