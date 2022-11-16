{%- macro calc_page_number(page_url, page_type, device_category) -%}

  CASE
    WHEN REGEXP_CONTAINS({{ page_url }}, r"\?.*(page|pn)=\d+") THEN CAST(REGEXP_EXTRACT({{ page_url }}, r"\?.*(?:page|pn)=(\d+)") AS INT64)
    -- ページ番号がないとき、サマリーページが表示される条件ならば0、そうでないならば1
    WHEN {{ page_type }} = 'article' AND {{ device_category }} != 'desktop' THEN 0
    -- ページ番号がなく、かつ写真ページかギャラリーページのときは1
    WHEN {{ page_type }} IN ('photo', 'gallery') THEN 1
    ELSE 1
  END

{%- endmacro -%}
