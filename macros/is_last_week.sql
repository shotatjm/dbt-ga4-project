{%- macro is_last_week(date_column) -%}

  DATE_TRUNC({{ date_column }}, WEEK(MONDAY)) = DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), WEEK(MONDAY)), INTERVAL 1 WEEK)

{%- endmacro -%}
