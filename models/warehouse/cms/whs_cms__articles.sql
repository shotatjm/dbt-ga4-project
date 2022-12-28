WITH
  numbered AS (
    SELECT
      *,
      ROW_NUMBER() OVER (partition by article_id order by created_timestamp desc) as rn
    FROM
      {{ source('cms', 'article_log') }}
    )
SELECT
  * EXCEPT (rn, created_timestamp)
FROM
  numbered
WHERE
  rn = 1;
