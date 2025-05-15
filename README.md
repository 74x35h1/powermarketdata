# 電力市場データ収集ツール

このリポジトリは、日本の電力市場データ（TSO各社の需要・供給、JEPX市場価格、OCCTO発電実績、JMA気象データなど）を効率的に収集・標準化・保存・分析するためのツール群を提供します。

## 主な特徴

- **TSO（送電系統運用者）データの統合ダウンロード・パース・DB保存**
- **JEPX市場価格データの取得**
- **OCCTO（電力広域的運営推進機関）データの取得**
    - 30分発電実績データ
- **JMA（気象庁）の気象データの取得**
    - 複数地点対応（設定ファイル経由）
- **柔軟な設定・拡張性（新TSOや新フォーマット追加が容易）**
- **CLI・対話型・スクリプトからの多様な利用方法**
- **`.env` ファイルによるデータベースパスなどの設定管理**

## ディレクトリ構成（主要部分）

```
powermarketdata/
├── .env.example             # 環境変数設定ファイルのサンプル
├── cli/
│   └── menu.py              # メインCLIメニュー
├── data_sources/
│   ├── jepx/
│   │   ├── jepx_da_price.py # JEPXスポット価格
│   │   └── jepx_bid.py      # JEPX入札情報
│   ├── jma/
│   │   ├── jma_config.py     # JMA観測地点リスト設定
│   │   ├── jma_historical.py # JMA気象データ履歴取得
│   │   └── db_importer.py    # JMAデータDBインポート
│   ├── occto/
│   │   ├── 30min_gendata_downloader.py # OCCTO 30分発電実績取得
│   │   └── db_importer.py            # OCCTOデータDBインポート
│   └── tso/
│       ├── __init__.py
│       ├── db_importer.py         # TSOデータのDBインポート
│       ├── parser.py              # TSOデータのパース・標準化
│       ├── tso_url_templates.py   # TSOごとのURLテンプレート管理
│       └── unified_downloader.py  # 統合ダウンローダー（全TSO対応・メイン入口）
├── db/
│   ├── duckdb_connection.py     # DuckDB接続管理
│   └── schema_definition.sql    # DBスキーマ定義
├── main.py                    # メインスクリプト（ポータル）
├── README.md
└── project_structure.md
```

## データ処理の流れ

1.  **設定**: `.env`ファイルにデータベースパス (`DB_PATH`) などを設定します。
2.  **URLテンプレート管理 (TSO)**: `data_sources/tso/tso_url_templates.py` で各TSOのデータ取得URLパターンを一元管理。
3.  **データダウンロード**:
    - `main.py` または `cli/menu.py` から各データソースのダウンローダーを実行。
    - TSO: `data_sources/tso/unified_downloader.py`
    - JEPX: `data_sources/jepx/` 内の各スクリプト
    - OCCTO: `data_sources/occto/30min_gendata_downloader.py`
    - JMA: `data_sources/jma/jma_historical.py`
4.  **データパース・標準化 (TSO)**: `data_sources/tso/parser.py` で各TSOのCSV/ZIPを標準化DataFrameに変換。
5.  **DBインポート**: 各 `data_sources/**/db_importer.py` が標準化/取得済みデータをDuckDBに保存。

## 使い方（CLI例）

```bash
# .env ファイルを作成し、DB_PATHなどを設定
# cp .env.example .env
# (必要に応じて .env ファイルを編集)

# JEPXスポット価格データのダウンロードとDB保存
python main.py jepx-price

# TSOデータのダウンロードとDB保存 (対話的に期間やTSOを選択する場合)
python main.py menu 
# (メニューから "8. Supply and Demand Data from TSO" などを選択)

# OCCTO 30分発電実績データのダウンロードとDB保存 (対話的に期間を選択する場合)
python main.py menu
# (メニューから "4. Power Plant Operation Data from OCCTO" などを選択)

# JMA気象データのダウンロードとDB保存 (対話的に期間を選択する場合)
# デフォルトでは data_sources/jma/jma_config.py に記載の全地点のデータを取得
python data_sources/jma/jma_historical.py

# JMA気象データのダウンロードとDB保存 (特定の地点を指定する場合)
python data_sources/jma/jma_historical.py --stations 47662,47772 --start 2023-01-01 --end 2023-01-31

# (参考) TSOデータの直接指定ダウンロード (メインスクリプト経由)
python main.py tso-data --tso-ids tepco kansai --start-date 2024-03-01 --end-date 2024-03-31
```

## 拡張性・カスタマイズ

- 新しいTSOや新フォーマット追加は、`tso_url_templates.py` と `parser.py` の拡張のみで対応可能。
- 設定ファイル（`config/tso_urls.json`など）でURLやエリア情報を柔軟に管理。
- 新しいデータソースは `data_sources` 以下にモジュールとして追加。

## データベース

ダウンロードしたデータはDuckDBデータベースに保存されます。
データベースファイルのパスは、プロジェクトルートの `.env` ファイル内の `DB_PATH` で指定します。
指定がない場合のフォールバックパスは `db/duckdb_connection.py` 内にありますが、`.env` での明示的な指定を推奨します。

### データベースのスキーマ (主要テーブル)

1.  **`jepx_da_price`** - JEPX前日スポット価格
    -   `date` TEXT - 日付 (YYYYMMDD)
    -   `slot` INTEGER - 時間帯 (1-48)
    -   エリア別価格カラム (`ap1_hokkaido`, `ap2_tohoku` など) DOUBLE
    -   `spot_avg_price` DOUBLE - スポット平均価格

2.  **`occto_30min_generation`** - OCCTO 30分毎発電実績
    -   `master_key` TEXT PRIMARY KEY - YYYYMMDD_プラントコード_ユニット番号
    -   `date` TEXT - 日付 (YYYYMMDD)
    -   `plant_code` TEXT - プラントコード
    -   `unit_num` TEXT - ユニット番号
    -   `plant_name` TEXT - プラント名
    -   `output_mw` DOUBLE - 出力 (MW)
    -   `area_code` TEXT - 広域機関エリアコード
    -   `system_code` TEXT - 一般電気事業者エリアコード
    -   `generation_type_code` TEXT - 発電方式コード
    -   `generation_type_name` TEXT - 発電方式名
    -   `slot1` DOUBLE ... `slot48` DOUBLE - 30分毎の発電実績(MW)

3.  **`jma_weather`** - JMA気象データ (1時間ごと)
    -   `primary_key` TEXT PRIMARY KEY - 地点ID_年月日時分 (例: 47662_202301010000)
    -   `station_id` TEXT - 気象台地点ID (例: 47662)
    -   `date` TEXT - 日付 (YYYY-MM-DD)
    -   `time` TEXT - 時刻 (HH:MM)
    -   `temperature` DOUBLE - 気温(℃)
    -   `sunshine_duration` DOUBLE - 日照時間(h)
    -   `global_solar_radiation` DOUBLE - 全天日射量(MJ/m2)
    -   `wind_speed` DOUBLE - 風速(m/s)
    -   `wind_direction_sin` DOUBLE - 風向のSin成分
    -   `wind_direction_cos` DOUBLE - 風向のCos成分
    -   `weather_description` TEXT - 天気概況
    -   `snowfall_depth` DOUBLE - 積雪深(cm)

4.  `tso_data` - TSO統合テーブル (全TSOデータを横持ち)
    -   `master_key` VARCHAR PRIMARY KEY - date_slot_areacode形式
    -   `date` VARCHAR - 日付 (YYYYMMDD形式)
    -   `slot` INTEGER - 時間枠 (1-48または1-96)
    -   エリアコード別カラム (例: `2_area_demand`, `2_nuclear` など)

5.  `tso_area_X_data` - エリア別テーブル (X=1〜9のエリアコード)
    -   `master_key` VARCHAR PRIMARY KEY
    -   `date` TEXT - 日付
    -   `slot` INTEGER - 時間枠
    -   `area_demand` DOUBLE - エリア需要
    -   電源別カラム (`nuclear`, `LNG`, `coal` など)

## 最近の主な更新

- **JMA気象データ収集機能の追加**: `data_sources/jma/` にて履歴データ取得とDB保存を実装。
- **JMA気象データ取得の複数地点対応と設定ファイル (`jma_config.py`) の導入**
- **OCCTO 30分発電実績データ収集・処理機能の改善**: ダウンローダーとDBインポーターの修正、スキーマ変更（master_key導入、カラム名変更、データ型修正）。
- **データベースパスの `.env` 化**: `DB_PATH` 環境変数を `.env` ファイルから読み込むように `db/duckdb_connection.py` を修正。
- TSOデータ処理のモジュール化（パース・DL・DB保存の分離）
- `parser.py`・`tso_url_templates.py` の追加
- データベーススキーマの標準化・拡張
- 東北電力など新フォーマット対応
- 多数のバグ修正とリファクタリング。

## 今後の開発予定

- 新TSO・新フォーマット対応 (継続)
- 他のOCCTOデータ（連系線潮流、需給バランスなど）の取得機能追加
- 他のJMAデータ（予報データなど）の取得機能追加
- データ可視化・Web UI
- 予測モデル・高度な分析

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。 