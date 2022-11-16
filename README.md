# dbt project for GA4
GA4からBigQueryにエクスポートしたデータを変換・整理するdbtプロジェクトのサンプルです。
Webメディアのデータ基盤を想定しています。

# Data Platform Overview
## イベントの収集
- 行動データ: GA4/GTMで収集
  - page_view
  - read_to_end
  - click
  - share
  - subscribe
  - sign_up
  - login
- 記事データ: CMSから抽出するAPIとスクリプトを用いて収集
- 会員データ: 会員DMから抽出するAPIとスクリプトを用いて収集

## データの保管
BigQueryでELTを実現するためのデータセットアーキテクチャ
- source
  - 外部データソースから持ってきたデータを加工せずに置く
  - 物理テーブル
  - source_{ServiceName}
    - 複数媒体ある場合: source_{MediaName}_{ServiceName}
- warehouse
  - sourceを前処理したデータを置く
  - 論理テーブル
    - データ量が多い場合は物理テーブル
  - 個人情報は暗号化する
  - warehouse_{ServiceName}
    - 複数媒体ある場合: warehouse_{MediaName}_{ServiceName}
- secure
  - 個人情報を含むデータを置く
  - 論理テーブル
  - secure_{ServiceName}
    - 複数媒体ある場合: secure_{MediaName}_{ServiceName}
- mart
  - ダッシュボードで使用するために集計したデータを置く
  - 物理テーブル
  - mart_{Purpose}
    - 複数媒体ある場合: mart_{MediaName}_{Purpose}
- analytics
  - 個人で分析するためのデータを置く
  - 物理テーブル/論理テーブル
  - analytics_{PersonName}

## データの変換・集計
sourceのデータ変換はdbtで管理する

## データ可視化


## ワークフロー管理


# dbt Models
- ディレクトリ階層はデータセット名を表す
  - デフォルト設定だと一つのデータセットしか作られないため、 `get_custom_schema`マクロを上書き
- SQLファイル名（モデル名）はプロジェクトで一意にする
  - デフォルト設定だとモデル名がテーブル名になってしまい冗長なため、 `get_custom_alias`マクロを上書き
