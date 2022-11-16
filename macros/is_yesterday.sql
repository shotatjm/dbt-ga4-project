{%- macro is_yesterday(date_column) -%}

  {{ date_column }} = DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)

{%- endmacro -%}
