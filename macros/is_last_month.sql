{%- macro is_last_month(date_column) -%}

  DATE_TRUNC({{ date_column }}, MONTH) = DATE_SUB(DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), MONTH), INTERVAL 1 MONTH)

{%- endmacro -%}
