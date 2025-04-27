# 電力市場データ収集ツール

このリポジトリは、日本の電力市場データ（TSO各社の需要・供給、JEPX市場価格など）を効率的に収集・標準化・保存・分析するためのツール群を提供します。

## 主な特徴

- **TSO（送電系統運用者）データの統合ダウンロード・パース・DB保存**
- **JEPX市場価格データの取得**
- **柔軟な設定・拡張性（新TSOや新フォーマット追加が容易）**
- **CLI・対話型・スクリプトからの多様な利用方法**

## ディレクトリ構成（TSO関連）

```
data_sources/
  └── tso/
      ├── __init__.py
      ├── db_importer.py         # TSOデータのDBインポート
      ├── downloader.py          # TSOデータのダウンロード（補助的/個別用途）
      ├── parser.py              # TSOデータのパース・標準化
      ├── tso_url_templates.py   # TSOごとのURLテンプレート管理
      └── unified_downloader.py  # 統合ダウンローダー（全TSO対応・メイン入口）
```

## TSOデータ処理の流れ

1. **URLテンプレート管理**  
   `tso_url_templates.py` で各TSOのデータ取得URLパターンを一元管理。新TSO追加やURL変更もここで完結。

2. **データダウンロード**  
   - `unified_downloader.py`：複数TSO・複数日付範囲を一括ダウンロード。ZIP展開や日付自動置換も対応。
   - `downloader.py`：個別用途や補助的なダウンロード処理。

3. **データパース・標準化**  
   `parser.py` で各TSOのCSV/ZIPを標準化DataFrameに変換。  
   - 列名マッピング・日付/時間変換・数値整形・マスターキー生成などを一元管理。
   - 新フォーマットや特殊TSOにも柔軟に対応。

4. **DBインポート**  
   `db_importer.py` で標準化データをDuckDBに保存。  
   - エリア別テーブル・統合テーブル両対応。
   - トランザクション管理・重複排除・エラー処理も実装。

## 使い方（CLI例）

```bash
# 東北電力の需要データを10日分ダウンロード
python main.py tso-data --tso-ids tohoku --start-date 2024-04-01 --end-date 2024-04-10

# 複数TSOのデータを一括ダウンロード
python main.py tso-data --tso-ids tepco kansai --start-date 2024-03-01 --end-date 2024-03-31

# DBインポート
python data_sources/tso/db_importer.py --tso-id tepco --start-date 2024-01-01 --end-date 2024-01-31
```

## 拡張性・カスタマイズ

- 新しいTSOや新フォーマット追加は、`tso_url_templates.py` と `parser.py` の拡張のみで対応可能。
- 設定ファイル（`config/tso_urls.json`）でURLやエリア情報を柔軟に管理。

## データベース

ダウンロードしたデータはDuckDBデータベースに保存されます。デフォルトのデータベースファイルパスは `/Volumes/MacMiniSSD/powermarketdata/power_market_data` です。

### データベースのスキーマ

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

## 最近の主な更新

- TSOデータ処理の完全モジュール化（パース・DL・DB保存の分離）
- `parser.py`・`tso_url_templates.py` の追加
- データベーススキーマの標準化・拡張
- 東北電力など新フォーマット対応

## 今後の開発予定

- 新TSO・新フォーマット対応
- データ可視化・Web UI
- 予測モデル・高度な分析

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。 