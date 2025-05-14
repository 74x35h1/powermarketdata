# 電力市場データ収集プロジェクト - コード構造

## プロジェクト概要

このプロジェクトは、日本の電力市場データを収集・分析するための包括的なツールセットを提供します。主に以下の機能があります：

1. 電力会社（TSO）からの需要・供給データのダウンロード
2. JEPXからの市場価格データの取得
3. OCCTOからの発電実績・需給関連データの取得
4. JMAからの気象データの取得
5. データベースへの保存と分析
6. `.env`ファイルによる設定管理

## ディレクトリ構成

```
powermarketdata/
├── .env.example                 # 環境変数設定ファイルのサンプル (.envとしてコピーして使用)
├── cli/
│   └── menu.py                  # メインCLIメニュー
├── config/
│   └── tso_urls.json            # TSO関連URL設定 (現在は未使用の可能性あり、要確認)
├── data_sources/
│   ├── jepx/
│   │   ├── jepx_da_price.py     # JEPXスポット価格ダウンローダー・DBインポーター
│   │   └── jepx_bid.py          # JEPX入札情報ダウンローダー・DBインポーター
│   ├── jma/
│   │   ├── jma_historical.py    # JMA気象観測履歴データダウンローダー
│   │   └── db_importer.py       # JMA気象データDBインポーター
│   ├── occto/
│   │   ├── 30min_gendata_downloader.py # OCCTO 30分発電実績ダウンローダー (サブプロセスとして実行)
│   │   └── db_importer.py            # OCCTO 30分発電実績DBインポーター
│   └── tso/
│       ├── __init__.py
│       ├── db_importer.py         # TSOデータDBインポーター
│       ├── parser.py              # TSOデータパーサー・標準化
│       ├── tso_url_templates.py   # TSOごとのURLテンプレート (現在は未使用の可能性あり、要確認)
│       └── unified_downloader.py  # TSOデータ統合ダウンローダー
├── db/
│   ├── duckdb_connection.py     # DuckDB接続管理 (.envのDB_PATH参照)
│   └── schema_definition.sql    # DBスキーマ定義
├── docs/                          # (オプション) 詳細ドキュメント用
├── examples/
│   └── import_tso_data_to_db.py # (旧サンプル、現行のmain.pyやmenu.py参照推奨)
├── logs/                          # (オプション) ログファイル保存用
├── tests/                         # (オプション) テストコード用
├── main.py                        # メインスクリプト（ポータル、CLI引数処理）
├── README.md                      # プロジェクト概要、使い方など
├── project_structure.md           # このファイル
├── requirements.txt               # Python依存パッケージ
└── .gitignore                     # Git無視リスト
```

## `data_sources` 配下の主要モジュール役割

-   **`jepx/jepx_da_price.py`**: JEPXスポット価格（エリアプライス、システムプライス）をウェブサイトから取得し、整形してDBに保存。
-   **`jepx/jepx_bid.py`**: JEPX入札カーブ情報（買入札量、売入札量など）をウェブサイトから取得し、整形してDBに保存。
-   **`jma/jma_historical.py`**: 気象庁の過去の気象観測データ（気温、日照時間、風速、天気など）をウェブサイトから取得し、整形。DB保存は `db_importer.py` に移譲する想定（現状は同ファイル内でDB保存まで実施）。
-   **`jma/db_importer.py`**: `jma_historical.py` で取得・整形された気象データを `jma_weather` テーブルにインポート。
-   **`occto/30min_gendata_downloader.py`**: OCCTOの30分毎発電実績データをウェブサイトから取得。`main.py`や`cli/menu.py`からサブプロセスとして呼び出され、取得したデータをDBに保存（内部で `db_importer.py` を使用）。
-   **`occto/db_importer.py`**: `30min_gendata_downloader.py` で取得された発電実績データを `occto_30min_generation` テーブルにインポート。
-   **`tso/unified_downloader.py`**: 各TSOのウェブサイトから需給実績（エリア需要、発電実績内訳など）や週間供給力などをダウンロード。
-   **`tso/parser.py`**: `unified_downloader.py` でダウンロードされたTSOごとの異なるCSV/ZIPフォーマットを解析し、標準化されたDataFrameに変換。
-   **`tso/db_importer.py`**: `parser.py` で標準化されたTSOデータを `tso_data` および `tso_area_X_data` テーブルにインポート。

## 拡張性

-   新しいデータソースの追加: `data_sources` ディレクトリ以下に新しいサブディレクトリを作成し、データ取得用スクリプトとDBインポート用スクリプトを配置。`main.py` や `cli/menu.py` に呼び出し処理を追加。
-   新しいTSOやTSOのデータフォーマット変更への対応: 主に `data_sources/tso/parser.py` と、必要に応じて `data_sources/tso/unified_downloader.py` を修正。

## 主要な処理フロー（一般化）

1.  **設定**: 開発者は `.env` ファイルを準備し `DB_PATH` 等を設定。
2.  **実行**: ユーザーは `main.py` (CLI引数付き) または `cli/menu.py` (対話型) を実行。
3.  **データ取得**: 各データソースのダウンローダースクリプトがそれぞれのウェブサイト等からデータをダウンロード。
4.  **データ整形/パース**: ダウンロードされたデータは必要に応じて整形・パースされ、扱いやすい形式（通常はPandas DataFrame）に変換。
5.  **データベース保存**: 整形済みデータが `db/duckdb_connection.py` を介してDuckDBデータベースの対応するテーブルに保存される。

## 主要モジュールと機能

### メインアプリケーション (`main.py`, `cli/menu.py`)

プロジェクトのコマンドラインインターフェースおよび対話型メニューのエントリーポイントです：

-   CLI引数解析 (`main.py`)
-   対話型メニューの提供 (`cli/menu.py`)
-   各データソースのダウンローダー/インポーターへの処理の委譲
-   エラー処理とロギング

### データベース接続 (`db/duckdb_connection.py`)

DuckDBへの接続と操作を提供する中心的なモジュールです：

-   データベース接続の初期化と管理。`.env` ファイルから `DB_PATH` を読み込み、指定がなければフォールバックパスを使用。
-   SQLクエリ実行
-   Pandas DataFrameのテーブルへの登録・挿入（`ON CONFLICT DO NOTHING` など重複排除対応）
-   トランザクション管理（コミット、ロールバック）
-   エラーハンドリング

### データベーススキーマ (`db/schema_definition.sql`)

データベーステーブルのSQL CREATE TABLE文を記述したファイルです。 `DuckDBConnection` や各DBインポーターが参照し、テーブルが存在しない場合に作成します。

-   `jepx_da_price` (JEPXスポット価格)
-   `occto_30min_generation` (OCCTO 30分発電実績)
-   `jma_weather` (JMA気象データ)
-   `tso_data` (TSO統合テーブル)
-   `tso_area_X_data` (TSOエリア別テーブル)
-   その他、必要に応じて補助テーブル

### JMA気象データ (`data_sources/jma/`)

-   `jma_historical.py`: 気象庁のウェブサイトから過去の気象観測データをダウンロードし、DataFrameに整形。DB保存処理も含む。
-   `db_importer.py`: `jma_historical.py` から呼び出され、整形済みDataFrameを `jma_weather` テーブルにインポート。

### OCCTO 30分発電実績 (`data_sources/occto/`)

-   `30min_gendata_downloader.py`: OCCTOのウェブサイトから30分毎の発電実績JSONデータをダウンロードし、DataFrameに整形。DB保存処理も含む（`db_importer.py` を使用）。
-   `db_importer.py`: `30min_gendata_downloader.py` から呼び出され、整形済みDataFrameを `occto_30min_generation` テーブルにインポート。

(その他のモジュール説明は `README.md` と重複するため、ここでは省略。必要に応じて追記可能)

## データベーススキーマ (主要テーブル詳細)

### `jepx_da_price` (JEPX前日スポット価格)

-   `date` TEXT - 日付 (YYYYMMDD)
-   `slot` INTEGER - 時間帯 (1-48)
-   `apX_areaname` DOUBLE - 各エリアのスポット価格 (例: `ap1_hokkaido`, `ap2_tohoku`)
-   `spot_avg_price` DOUBLE - システムプライス (全国平均)
-   PRIMARY KEY: (`date`, `slot`)

### `occto_30min_generation` (OCCTO 30分毎発電実績)

-   `master_key` TEXT PRIMARY KEY - YYYYMMDD_プラントコード_ユニット番号
-   `date` TEXT - 日付 (YYYYMMDD)
-   `plant_code` TEXT - プラントコード
-   `unit_num` TEXT - ユニット番号 (ない場合は '0' などで補完)
-   `plant_name` TEXT - プラント名
-   `output_mw` DOUBLE - 平均出力 (MW) ※元データは時間断面ごとの値
-   `area_code` TEXT - 広域機関エリアコード (例: 'TKA01')
-   `system_code` TEXT - 一般送配電事業者エリアコード (例: '03')
-   `generation_type_code` TEXT - 発電方式コード (例: '0101')
-   `generation_type_name` TEXT - 発電方式名 (例: '一般水力')
-   `slot1` DOUBLE ... `slot48` DOUBLE - 30分毎の発電実績(MW)

### `jma_weather` (JMA気象データ - 1時間ごと)

-   `primary_key` TEXT PRIMARY KEY - 地点ID_年月日時分 (例: 47662_202301010000)
-   `station_id` TEXT - 気象台地点ID (例: 47662)
-   `date` TEXT - 日付 (YYYY-MM-DD)
-   `time` TEXT - 時刻 (HH:MM)
-   `temperature` DOUBLE - 気温(℃)
-   `sunshine_duration` DOUBLE - 日照時間(h)
-   `global_solar_radiation` DOUBLE - 全天日射量(MJ/m2)
-   `wind_speed` DOUBLE - 風速(m/s)
-   `wind_direction_sin` DOUBLE - 風向のSin成分 (北:0, 東:1, 南:0, 西:-1)
-   `wind_direction_cos` DOUBLE - 風向のCos成分 (北:1, 東:0, 南:-1, 西:0)
-   `weather_description` TEXT - 天気概況 (JMAのテキスト記述)
-   `snowfall_depth` DOUBLE - 積雪深(cm)

(TSO関連テーブルスキーマは `README.md` を参照)

## 最近の更新と改善点

1.  **JMA気象データ収集機能の追加**: `data_sources/jma/` に履歴データ取得とDB保存を実装。
2.  **OCCTO 30分発電実績データ収集・処理機能の改善**: ダウンローダーとDBインポーターの修正、スキーマ変更（master_key導入、カラム名変更、データ型修正）。
3.  **データベースパスの `.env` 化**: `DB_PATH` 環境変数を `.env` ファイルから読み込むように `db/duckdb_connection.py` を修正。
4.  TSOデータ処理のモジュール化とリファクタリング。
5.  全体的なエラーハンドリングとロギングの改善。

## 拡張ポイント

このプロジェクトは以下の方向に拡張できます：

1.  新しいデータソースの追加（例：他のOCCTOデータ、EEXなどの海外市場データ）
2.  データ可視化機能の実装（Streamlit, Dashなど）
3.  予測モデルの統合（時系列予測など）
4.  Web APIインターフェースの開発 (FastAPIなど)
5.  テストカバレッジの向上 