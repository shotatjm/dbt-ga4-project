{%- macro is_this_month(date_column) -%}

  DATE_TRUNC({{ date_column }}, MONTH) = DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), MONTH)

{%- endmacro -%}
