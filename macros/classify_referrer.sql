{%- macro classify_referrer(source, medium) -%}

  CASE
    WHEN {{ medium }} = 'organic' THEN '検索エンジン'
    WHEN {{ source }} IN ('(direct)', '') OR {{ source }} IS NULL THEN 'Discover・不明'
    WHEN {{ source }} IN ('headlines.yahoo.co.jp', 'news.yahoo.co.jp') THEN 'Yahoo!ニュース'
    WHEN {{ source }} = 'smartnews.com' THEN 'SmartNews'
    WHEN {{ source }} = 'news.line.me' THEN 'LINE NEWS'
    WHEN {{ source }} = 'gunosy.com' THEN 'Gunosy'
    WHEN {{ source }} = 'news.google.com' THEN 'Google ニュース'
    WHEN {{ source }} IN ('news.livedoor.com', 'news.goo.ne.jp', 'topics.smt.docomo.ne.jp', 'docomo.ne.jp', 'news.nifty.com', 'article.auone.jp', 'msn.com', 'news.infoseek.co.jp') THEN 'ポータルサイト'
    WHEN {{ source }} IN ('twitter.com', 't.co') THEN 'Twitter'
    WHEN {{ source }} IN ('facebook.com', 'm.facebook.com', 'newspicks.com', 'b.hatena.ne.jp', 'blog.livedoor.jp') THEN 'SNS'
    WHEN {{ source }} = 'push' THEN 'PUSH通知'
    WHEN REGEXP_CONTAINS({{ source }}, r"ampproject") THEN 'AMP'
    ELSE 'その他'
  END

{%- endmacro -%}
