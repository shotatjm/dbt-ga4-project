{%- macro get_month_table(start_month, end_month=none) -%}
( SELECT event_month FROM UNNEST(
  {%- if end_month is none -%}
    GENERATE_DATE_ARRAY(DATE('{{ start_month }}'), DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), MONTH), INTERVAL 1 MONTH)
  {%- else -%}
    GENERATE_DATE_ARRAY(DATE('{{ start_month }}'), DATE('{{ end_month }}'), INTERVAL 1 MONTH)
  {%- endif -%}
) AS event_month )
{%- endmacro -%}
