{%- macro get_date_table(start_date, end_date=none) -%}
( SELECT event_date FROM UNNEST(
  {%- if end_date is none -%}
    GENERATE_DATE_ARRAY(DATE('{{ start_date }}'), CURRENT_DATE('Asia/Tokyo'))
  {%- else -%}
    GENERATE_DATE_ARRAY(DATE('{{ start_date }}'), DATE('{{ end_date }}'))
  {%- endif -%}
) AS event_date )
{%- endmacro -%}
