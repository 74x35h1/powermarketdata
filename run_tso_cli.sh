#!/bin/bash

# プロジェクトのルートディレクトリに移動
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# Pythonパスを設定してスクリプトを実行
PYTHONPATH="$SCRIPT_DIR" python examples/interactive_tso_downloader.py

# 終了ステータスを保持
exit $? 