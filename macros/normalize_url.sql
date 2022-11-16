{%- macro normalize_url(page_url) -%}

  RTRIM(REGEXP_EXTRACT({{ page_url }}, r"^[^\?&%#]+"), '/')

{%- endmacro -%}
