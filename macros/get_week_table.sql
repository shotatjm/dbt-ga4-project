{%- macro get_week_table(start_week, end_week=none) -%}
( SELECT event_week FROM
  UNNEST(
  {%- if end_week is none -%}
    GENERATE_DATE_ARRAY(DATE('{{ start_week }}'), DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), WEEK(MONDAY)), INTERVAL 1 WEEK)
  {%- else -%}
    GENERATE_DATE_ARRAY(DATE('{{ start_week }}'), DATE('{{ end_week }}'), INTERVAL 1 WEEK)
  {%- endif -%}
) AS event_week )
{%- endmacro -%}
