{%- macro calc_frequency_segment(frequency) -%}

  CASE
    WHEN {{ frequency }} = 1 THEN 'light'
    WHEN {{ frequency }} BETWEEN 2 AND 4 THEN 'medium'
    WHEN {{ frequency }} BETWEEN 5 AND 14 THEN 'heavy'
    WHEN {{ frequency }} > 14 THEN 'royal'
  END

{%- endmacro -%}
