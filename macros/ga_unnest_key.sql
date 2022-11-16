{%- macro ga_unnest_key(column_to_unnest, key_to_extract, value_type = "string_value", rename_column = "default") -%}

(SELECT value.{{value_type}} FROM unnest({{column_to_unnest}}) WHERE key = '{{key_to_extract}}') AS
    {% if  rename_column == "default" -%}
    {{ key_to_extract }}
    {%- else -%}
    {{rename_column}}
    {%- endif -%}

{%- endmacro -%}
