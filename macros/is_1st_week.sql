{%- macro is_1st_week(event_date, published_at) -%}

  {{ event_date }} BETWEEN DATE({{ published_at }}) AND DATE_ADD(DATE({{ published_at }}), INTERVAL 6 DAY)

{%- endmacro -%}
