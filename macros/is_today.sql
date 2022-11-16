{%- macro is_today(date_column) -%}

  {{ date_column }} = CURRENT_DATE('Asia/Tokyo')

{%- endmacro -%}
