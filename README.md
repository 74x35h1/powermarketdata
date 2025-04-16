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
6. 関西電力（kepco）- エリア番号06
7. 中国電力（chugoku）- エリア番号07
8. 四国電力（shikoku）- エリア番号08
9. 九州電力（kyushu）- エリア番号09

### 以前の構造との違い

以前の構造では、各TSOごとに個別のダウンローダークラスが実装されていましたが、現在の構造では以下の利点を持つ統合されたダウンローダーに変更されています：

1. コードの重複を削減
2. 新しいTSOを追加する際の作業量を削減
3. 一度に複数のTSOからデータをダウンロードする機能
4. 設定ベースのアプローチによる柔軟性の向上

### 使用方法

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
python data_sources/tso/db_importer.py --tso-id tepco --tso-id kepco --start-date 2024-01-01

# 供給データをインポート
python data_sources/tso/db_importer.py --url-type supply
```

また、サンプルスクリプトを使ってデータのインポートと確認ができます：

```bash
# サンプルスクリプトの実行
python examples/import_tso_data_to_db.py
```

このスクリプトは次のことを行います：
1. 今月の需要データをすべてのTSOからダウンロードしてDBに保存
2. 東京電力と関西電力の供給データをダウンロードしてDBに保存
3. 保存されたデータをクエリして結果を表示

#### コマンドラインからの使用

```bash
# すべてのTSOから最近7日間の需要データをダウンロード
python examples/download_tso_data.py

# 特定のTSO（例：TEPCO）から特定の期間のデータをダウンロード
python examples/download_tso_data.py --tso-id tepco --start-date 2023-01-01 --end-date 2023-01-31

# 供給データをダウンロード
python examples/download_tso_data.py --url-type supply
```

#### コードでの使用

```python
from datetime import date
from data_sources.tso import UnifiedTSODownloader
from data_sources.db_connection import DuckDBConnection

# データベース接続を作成
db = DuckDBConnection("power_market.duckdb")

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
    tso_ids=["tepco", "kepco", "tohoku"],
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

ダウンロードしたデータはDuckDBデータベースに保存されます。デフォルトのデータベースファイルは `powermarket.duckdb` です。

以下の主要テーブルが作成されます：

1. `tso_demand` - 電力需要データ
   - 日付、時間、需要実績、予測値など

2. `tso_supply` - 電力供給データ
   - 日付、時間、供給力、発電種別（原子力、火力、水力、太陽光、風力など）

3. `tso_areas` - エリア情報
   - TSOのID、名前、エリアコード、地域

データベースに対するクエリ例：

```sql
-- 東京電力の最大需要を日ごとに取得
SELECT date, MAX(demand_actual) as max_demand
FROM tso_demand
WHERE tso_id = 'tepco'
GROUP BY date
ORDER BY date DESC;

-- 各電力会社の最新データの比較
SELECT tso_id, date, AVG(demand_actual) as avg_demand
FROM tso_demand
WHERE date = (SELECT MAX(date) FROM tso_demand)
GROUP BY tso_id, date
ORDER BY avg_demand DESC;
```

## 今後の開発予定

- より高度なデータ分析機能
- 可視化ツールの追加
- 予測モデルの実装

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。 