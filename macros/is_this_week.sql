{%- macro is_this_week(date_column) -%}

  DATE_TRUNC({{ date_column }}, WEEK(MONDAY)) = DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), WEEK(MONDAY))

{%- endmacro -%}
