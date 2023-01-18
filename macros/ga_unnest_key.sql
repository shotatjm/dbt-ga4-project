{%- macro ga_unnest_key(column_to_unnest, key_to_extract, value_type = "string_value", rename_column = "default", if_null = "default") -%}

{% if  if_null == "default" -%}
    (SELECT value.{{value_type}} FROM unnest({{column_to_unnest}}) WHERE key = '{{key_to_extract}}')
{% elif  if_null == "check_int_value" -%}
    IFNULL(
        (SELECT value.{{value_type}} FROM unnest({{column_to_unnest}}) WHERE key = '{{key_to_extract}}'),
        CAST((SELECT value.int_value FROM unnest({{column_to_unnest}}) WHERE key = '{{key_to_extract}}') AS STRING)
    )
{%- endif -%}
    AS
{% if  rename_column == "default" -%}
    {{ key_to_extract }}
{%- else -%}
    {{rename_column}}
{%- endif -%}

{%- endmacro -%}
