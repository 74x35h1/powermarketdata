# 電力市場データ収集プロジェクト - コード構造

## プロジェクト概要

このプロジェクトは、日本の電力市場データを収集・分析するための包括的なツールセットを提供します。主に以下の機能があります：

1. 電力会社（TSO）からの需要・供給データのダウンロード
2. JEPXからの市場価格データの取得
3. データベースへの保存と分析

## ディレクトリ構造

```
powermarketdata/
├── config/                    # 設定ファイル
│   └── tso_urls.json         # TSO URLs設定
├── data_sources/              # データソースパッケージ
│   ├── __init__.py           # パッケージ初期化
│   ├── db_connection.py      # データベース接続
│   ├── jepx/                 # JEPX（日本卸電力取引所）データ
│   │   ├── __init__.py
│   │   ├── jepx_bid.py       # 入札データダウンローダー
│   │   └── jepx_da_price.py  # 前日スポット価格ダウンローダー
│   └── tso/                  # TSO（送電系統運用者）データ
│       ├── __init__.py
│       ├── db_importer.py    # データベースインポーター
│       ├── tso_urls.py       # URL・エリア情報
│       └── unified_downloader.py # 統合ダウンローダー
├── examples/                  # サンプルコード
│   ├── import_tso_data_to_db.py    # DBインポート例
│   └── interactive_tso_downloader.py # 対話型CLIの例
└── run_tso_cli.sh            # 対話型CLI実行スクリプト
```

## 主要モジュールと機能

### データベース接続 (`data_sources/db_connection.py`)

DuckDBへの接続と操作を提供する中心的なモジュールです。主な機能：

- データベース接続の初期化と管理
- クエリ実行
- データフレームの保存
- エラーハンドリング

DuckDBがインストールされていない場合はモックオブジェクトとして機能し、エラーを発生させずに処理を続行できます。

### TSO URLs・エリア情報 (`data_sources/tso/tso_urls.py`)

電力会社のURL、エリアコード、名前などのメタデータを管理します：

- URLの取得 (`get_tso_url`)
- エリアコードの取得 (`get_area_code`) 
- TSO名の取得 (`get_tso_name`)
- エリアコードからTSOの検索 (`get_tso_by_area_code`)

### 統合TSO ダウンローダー (`data_sources/tso/unified_downloader.py`)

すべての電力会社からのデータを統一的にダウンロードする機能を提供します：

- 単一または複数のTSOからのデータダウンロード
- 需要データと供給データの両方に対応
- ZIPファイルの自動処理
- 異なるCSVフォーマットへの対応
- 日付範囲指定ダウンロード

### TSO データベースインポーター (`data_sources/tso/db_importer.py`)

ダウンロードしたTSOデータをデータベースにインポートする機能を提供します：

- データベーステーブルの自動作成と管理
- データフレームの前処理とDB保存
- コマンドライン引数のサポート
- エラーハンドリングとロギング

### JEPX データダウンローダー

JEPX（日本卸電力取引所）からのデータ取得機能：

- 入札データのダウンロード (`jepx_bid.py`)
- 前日スポット価格データのダウンロード (`jepx_da_price.py`)

## 主な使用例

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
python data_sources/tso/db_importer.py --tso-id tepco --tso-id kepco
```

### プログラムからの使用

```python
from data_sources import UnifiedTSODownloader, DuckDBConnection
from datetime import date

# ダウンローダーを初期化
downloader = UnifiedTSODownloader(
    tso_ids=['tepco', 'kepco'],
    url_type='demand'
)

# 特定期間のデータをダウンロード
start_date = date(2024, 1, 1)
end_date = date(2024, 1, 31)
results = downloader.download_files(start_date, end_date)

# DB接続を作成してデータを保存
db = DuckDBConnection()
for target_date, tso_id, df in results:
    db.save_dataframe(df, 'tso_demand')
```

## データベーススキーマ

主なテーブル：

1. **tso_demand** - 電力需要データ
   - 日付、時間、需要実績、TSO情報など

2. **tso_supply** - 電力供給データ
   - 日付、時間、電源種別ごとの供給量など

3. **tso_areas** - エリア情報マスタ
   - TSO ID、名前、エリアコード、地域

4. **jepx_da_price** - JEPX前日スポット価格
   - 日付、時間帯、システムプライス、エリアプライスなど

## 拡張ポイント

このプロジェクトは以下の方向に拡張できます：

1. 新しいデータソースの追加
2. データ可視化機能の実装
3. 予測モデルの統合
4. Webインターフェースの開発 