WITH
  event_extracted AS (
    SELECT
      *,
    FROM
      {{ ref('whs_ga__events') }}
    WHERE
      event_name = 'read_to_end'
  )
SELECT
  *
FROM
  event_extracted
