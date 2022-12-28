WITH
  numbered AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY created_timestamp DESC) AS rn
    FROM
      {{ source('cms', 'article_log') }}
    )
SELECT
  * EXCEPT (rn, created_timestamp)
FROM
  numbered
WHERE
  rn = 1
