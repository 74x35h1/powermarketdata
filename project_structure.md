# 電力市場データ収集プロジェクト - コード構造

## プロジェクト概要

このプロジェクトは、日本の電力市場データを収集・分析するための包括的なツールセットを提供します。主に以下の機能があります：

1. 電力会社（TSO）からの需要・供給データのダウンロード
2. JEPXからの市場価格データの取得
3. データベースへの保存と分析

## ディレクトリ構造

```
powermarketdata/
├── cli/                      # CLIモジュール
│   ├── __init__.py
│   └── menu.py               # インタラクティブメニュー
├── config/                   # 設定ファイル
│   └── tso_urls.json         # TSO URLs設定
├── data/                     # ダウンロードされたデータ格納
├── data_sources/             # データソースパッケージ
│   ├── __init__.py           # パッケージ初期化
│   ├── jepx/                 # JEPX（日本卸電力取引所）データ
│   │   ├── __init__.py
│   │   ├── jepx_bid.py       # 入札データダウンローダー
│   │   └── jepx_da_price.py  # 前日スポット価格ダウンローダー
│   └── tso/                  # TSO（送電系統運用者）データ
│       ├── __init__.py
│       ├── db_importer.py    # データベースインポーター
│       ├── tso_urls.py       # URL・エリア情報
│       └── unified_downloader.py # 統合ダウンローダー
├── db/                       # データベース関連モジュール
│   ├── __init__.py
│   ├── duckdb_connection.py  # DuckDB接続管理
│   └── schema_definition.sql # スキーマ定義
├── docs/                     # ドキュメント
├── examples/                 # サンプルコード
│   ├── import_tso_data_to_db.py    # DBインポート例
│   └── interactive_tso_downloader.py # 対話型CLIの例
├── exports/                  # エクスポートデータ格納
├── exporter/                 # データエクスポート機能
├── ingestion/                # データ取り込み機能
├── logs/                     # ログファイル
├── tests/                    # テストコード
├── transformation/           # データ変換機能
├── main.py                   # メインアプリケーションエントリーポイント
├── project_structure.md      # このファイル
├── README.md                 # プロジェクト説明
├── requirements.txt          # 依存パッケージリスト
└── run_tso_cli.sh            # 対話型CLI実行スクリプト
```

## 主要モジュールと機能

### メインアプリケーション (`main.py`)

プロジェクトのコマンドラインインターフェースのエントリーポイントです：

- CLIの引数解析
- TSO、JEPX、その他のデータソースへの統一アクセス
- 対話型メニューの起動
- エラー処理とロギング

### データベース接続 (`db/duckdb_connection.py`)

DuckDBへの接続と操作を提供する中心的なモジュールです：

- データベース接続の初期化と管理
- クエリ実行
- データフレームの保存
- エラーハンドリング

### データベーススキーマ (`db/schema_definition.sql`)

データベーステーブルの定義ファイルです：

- TSO統合テーブル (`tso_data`)
- エリア別テーブル (`tso_area_X_data`)
- JEPX価格テーブル (`jepx_da_price`)
- その他の補助テーブル

### TSO統合ダウンローダー (`data_sources/tso/unified_downloader.py`)

すべての電力会社からのデータを統一的にダウンロードする機能を提供します：

- 単一または複数のTSOからのデータダウンロード
- 需要データと供給データの両方に対応
- ZIPファイルの自動処理
- 異なるCSVフォーマットへの対応
- 日付範囲指定ダウンロード
- 正規表現によるURL生成と日付抽出

### TSO データベースインポーター (`data_sources/tso/db_importer.py`)

ダウンロードしたTSOデータをデータベースにインポートする機能を提供します：

- データベーステーブルの自動作成と管理
- データフレームの前処理とDB保存
- データの整形とmaster_keyの生成
- エリアごとのテーブル管理
- トランザクション管理とエラーハンドリング

### JEPX データダウンローダー

JEPX（日本卸電力取引所）からのデータ取得機能：

- 前日スポット価格データのダウンロード (`data_sources/jepx/jepx_da_price.py`)
- 価格データのDB保存と変換

## 主な使用例

### メインアプリケーションの実行

```bash
# 東北電力の需要データをダウンロード (2024年4月1日〜10日)
python main.py tso-data --tso-ids tohoku --start-date 2024-04-01 --end-date 2024-04-10

# JEPXの価格データをダウンロード
python main.py jepx-price

# インタラクティブメニューを表示
python main.py menu
```

### 対話型インターフェース

```bash
# シェルスクリプトから実行
./run_tso_cli.sh

# または直接実行
python data_sources/tso/unified_downloader.py
```

このインターフェースでは、ユーザーが対話的にTSOとデータ期間を選択し、需要データを表示できます。

### データベースへのインポート

```bash
# コマンドラインから実行
python data_sources/tso/db_importer.py --start-date 2024-01-01 --end-date 2024-01-31

# 特定のTSOのみ
python data_sources/tso/db_importer.py --tso-id tepco --tso-id kansai
```

### プログラムからの使用

```python
from data_sources.tso.unified_downloader import UnifiedTSODownloader
from db.duckdb_connection import DuckDBConnection
from datetime import date

# DB接続を作成
db = DuckDBConnection("/Volumes/MacMiniSSD/powermarketdata/power_market_data")

# ダウンローダーを初期化
downloader = UnifiedTSODownloader(
    tso_ids=['tepco', 'kansai'],
    db_connection=db,
    url_type='demand'
)

# 特定期間のデータをダウンロード
start_date = date(2024, 1, 1)
end_date = date(2024, 1, 31)
results = downloader.download_files(start_date, end_date)
```

## データベーススキーマ

最新のテーブル構造：

### TSO統合テーブル (`tso_data`)

このテーブルは全てのTSOデータを横持ち（各エリアのデータを同一行に格納）する形式で保存します：

- `master_key VARCHAR PRIMARY KEY` - ユニーク識別子 (date_slot_areacode形式)
- `date VARCHAR` - 日付 (YYYYMMDD形式)
- `slot INTEGER` - 時間枠 (1-48または1-96)
- エリアコード別カラム:
  - `1_area_demand`, `1_nuclear`, `1_LNG`... (北海道電力)
  - `2_area_demand`, `2_nuclear`, `2_LNG`... (東北電力)
  - ...
  - `9_area_demand`, `9_nuclear`, `9_LNG`... (九州電力)

### エリア別テーブル (`tso_area_X_data`)

各TSOエリアのデータを個別に格納するテーブル（X=1〜9のエリアコード）：

- `master_key VARCHAR PRIMARY KEY` - ユニーク識別子
- `date TEXT` - 日付
- `slot INTEGER` - 時間枠
- `area_demand DOUBLE` - エリア需要
- `nuclear DOUBLE` - 原子力発電
- `LNG DOUBLE` - LNG火力発電
- `coal DOUBLE` - 石炭火力発電
- その他の電源種別カラム

### JEPX価格テーブル (`jepx_da_price`)

- `date TEXT` - 日付
- `slot INTEGER` - 時間帯
- `ap1_hokkaido` ~ `ap9_kyushu` - エリア別価格
- `spot_avg_price` - スポット平均価格
- その他の市場関連データ

## 最近の更新と改善点

1. 東北電力(tohoku)のURLテンプレート更新
   - 最新のデータフォーマットに対応するよう修正

2. データベーススキーマの改善
   - `date`カラムを`DATE`型から`VARCHAR`型に変更して日付形式の一貫性を保持
   - `slot`カラムを`VARCHAR`から`INTEGER`に変更してデータ分析を容易化

3. トランザクション管理の最適化
   - エラー処理の改善
   - パフォーマンスの向上

## 拡張ポイント

このプロジェクトは以下の方向に拡張できます：

1. 新しいデータソースの追加
2. データ可視化機能の実装
3. 予測モデルの統合
4. Webインターフェースの開発
5. 複数電力会社データの高度な分析機能の追加 