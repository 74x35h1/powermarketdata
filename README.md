# 電力市場データ収集ツール

このリポジトリは、日本の電力市場データを収集・分析するためのツールを提供します。

## 主な機能

- 日本の電力会社（TSO）からの需要・供給データのダウンロード
- JEPXからの市場価格データのダウンロード
- 気象データの収集
- データの標準化と保存

## TSOデータダウンロード

このプロジェクトでは、日本の主要な電力会社（TSO）からデータをダウンロードするための統合されたインターフェースを提供しています。

### エリア番号とTSO対応

日本の電力会社は以下のエリア番号で管理されています：

1. 北海道電力（hokkaido）- エリア番号01
2. 東北電力（tohoku）- エリア番号02
3. 東京電力（tepco）- エリア番号03
4. 中部電力（chubu）- エリア番号04
5. 北陸電力（hokuriku）- エリア番号05
6. 関西電力（kansai）- エリア番号06 (以前はkepcoと表記)
7. 中国電力（chugoku）- エリア番号07
8. 四国電力（shikoku）- エリア番号08
9. 九州電力（kyushu）- エリア番号09

### 統合ダウンローダーの特徴

統合ダウンローダー(`UnifiedTSODownloader`)は以下の利点を持ちます：

1. コードの重複を削減
2. 新しいTSOを追加する際の作業量を削減
3. 一度に複数のTSOからデータをダウンロードする機能
4. 設定ベースのアプローチによる柔軟性の向上
5. 各電力会社の異なるCSV形式に対応

### 最近の更新内容

最近、以下の問題の修正と機能強化を行いました：

1. 東北電力(tohoku)のURLテンプレートを更新し、最新のデータフォーマットに対応
2. データベーススキーマを改善：
   - `date`カラムを`DATE`型から`VARCHAR`型に変更して日付形式の一貫性を保持
   - `slot`カラムを`VARCHAR`から`INTEGER`に変更してデータ分析を容易化
3. トランザクション管理を最適化し、エラー処理を改善

### 使用方法

#### メインアプリケーションからの実行

統合CLIでTSOデータをダウンロードするには：

```bash
# 最近10日間の東北電力データをダウンロード
python main.py tso-data --tso-ids tohoku --start-date 2024-04-01 --end-date 2024-04-10

# 複数のTSOからデータをダウンロード
python main.py tso-data --tso-ids tepco kansai --start-date 2024-03-01 --end-date 2024-03-31
```

#### 対話形式CLI（データ表示のみ）

対話形式のコマンドラインインターフェース（CLI）を使用すると、エリアコードと取得したい月を選択するだけで簡単にデータをダウンロードして表示できます。

```bash
# 対話形式CLIを起動 (オプション1 - シェルスクリプト使用)
./run_tso_cli.sh

# または (オプション2 - Pythonから直接実行)
python examples/interactive_tso_downloader.py

# または (オプション3 - 統合ダウンローダーから直接実行)
python data_sources/tso/unified_downloader.py

# 実行権限付与後は直接実行することも可能
chmod +x data_sources/tso/unified_downloader.py
./data_sources/tso/unified_downloader.py
```

このCLIでは以下の操作が可能です：
1. 対象のエリア（北海道電力、東京電力など）を選択
2. 取得したい年月を指定
3. データをダウンロードして表示

DBへの保存機能は含まれていません。表示のみです。

#### データベースへのインポート

TSOデータをダウンロードしてデータベースに保存するには、専用のインポートスクリプトを使用します：

```bash
# コマンドラインからDB保存を実行
python data_sources/tso/db_importer.py --start-date 2024-01-01 --end-date 2024-01-31

# 特定のTSOに限定してインポート
python data_sources/tso/db_importer.py --tso-id tepco --tso-id kansai --start-date 2024-01-01

# 供給データをインポート
python data_sources/tso/db_importer.py --url-type supply
```

また、サンプルスクリプトを使ってデータのインポートと確認ができます：

```bash
# サンプルスクリプトの実行
python examples/import_tso_data_to_db.py
```

#### コードでの使用

```python
from datetime import date
from data_sources.tso.unified_downloader import UnifiedTSODownloader
from db.duckdb_connection import DuckDBConnection

# データベース接続を作成
db = DuckDBConnection("/Volumes/MacMiniSSD/powermarketdata/power_market_data")

# 特定のTSOのダウンローダーを作成
tepco_downloader = UnifiedTSODownloader(
    tso_id="tepco",
    db_connection=db,
    url_type="demand"
)

# 特定の日付のデータをダウンロード
start_date = date(2023, 1, 1)
end_date = date(2023, 1, 31)
results = tepco_downloader.download_files(start_date, end_date)

# 複数のTSOのダウンローダーを作成
multi_downloader = UnifiedTSODownloader(
    tso_ids=["tepco", "kansai", "tohoku"],
    db_connection=db,
    url_type="demand"
)

# 複数のTSOからデータをダウンロード
results = multi_downloader.download_files(start_date, end_date)
```

## 設定ファイル

TSOのURLは `config/tso_urls.json` ファイルで管理されています。新しいTSOを追加したり、既存のURLを更新したりする場合は、このファイルを編集してください。

### URLのプレースホルダー

設定ファイルのURLには、以下のプレースホルダーを使用できます：

- `{YYYY}` - 4桁の西暦（例：2024）
- `{MM}` - 2桁の月（例：01, 12）

これらのプレースホルダーは、ダウンロード時に自動的に置換されます。

### ZIPファイルのサポート

URLが `.zip` で終わる場合、ダウンローダーは自動的にZIPファイルをダウンロードして展開し、
内部のCSVファイルを処理します。日付パターンに一致するCSVファイルが優先的に選択されます。

## データベース

ダウンロードしたデータはDuckDBデータベースに保存されます。デフォルトのデータベースファイルパスは `/Volumes/MacMiniSSD/powermarketdata/power_market_data` です。

### データベースのスキーマ

最新のスキーマ構造：

1. `tso_data` - 統合テーブル (全TSOデータを横持ち)
   - `master_key` VARCHAR - プライマリキー (date_slot_areacode形式)
   - `date` VARCHAR - 日付 (YYYYMMDD形式)
   - `slot` INTEGER - 時間枠 (1-48または1-96)
   - エリアコード別カラム (例: `2_area_demand`, `2_nuclear` など)

2. `tso_area_X_data` - エリア別テーブル (X=1〜9のエリアコード)
   - `master_key` VARCHAR - プライマリキー
   - `date` TEXT - 日付
   - `slot` INTEGER - 時間枠
   - `area_demand` DOUBLE - エリア需要
   - 電源別カラム (`nuclear`, `LNG`, `coal` など)

3. `jepx_da_price` - JEPX前日スポット価格
   - `date` TEXT - 日付
   - `slot` INTEGER - 時間帯
   - エリア別価格カラム (`ap1_hokkaido`, `ap2_tohoku` など)

データベースに対するクエリ例：

```sql
-- 東京電力エリアの最大需要を日ごとに取得
SELECT date, MAX("3_area_demand") as max_demand
FROM tso_data
GROUP BY date
ORDER BY date DESC
LIMIT 10;

-- 各電力会社の最新データの比較
SELECT 
  SUBSTR(date, 1, 4) || '-' || SUBSTR(date, 5, 2) || '-' || SUBSTR(date, 7, 2) as formatted_date,
  AVG("1_area_demand") as hokkaido,
  AVG("2_area_demand") as tohoku,
  AVG("3_area_demand") as tokyo,
  AVG("9_area_demand") as kyushu
FROM tso_data
WHERE date = (SELECT MAX(date) FROM tso_data)
GROUP BY date;
```

## 今後の開発予定

- より高度なデータ分析機能
- 可視化ツールの追加
- 予測モデルの実装
- 複数電力会社データの結合・分析の強化

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。 