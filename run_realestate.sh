#!/bin/bash
# ============================================================
# 不動産物件クローラー — ワンクリック実行スクリプト
# Automator / ショートカットから呼び出し用
# SUUMO & LIFULL HOME'S から賃貸・新築・中古マンション情報を取得
#
# 使い方:
#   ./run_realestate.sh          # クロール実行
#   ./run_realestate.sh --serve  # クロール後にHTTPサーバーも起動
#
# cron設定例（毎日 8:00）:
#   0 8 * * * /path/to/run_realestate.sh
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRAWLER="$SCRIPT_DIR/realestate_crawler.py"
LOG_FILE="$SCRIPT_DIR/realestate_data/crawler.log"

# ── 環境変数の読み込み ──
# .zshrc / .bashrc からAPIキーなどを取得（Automatorから起動時に必要）
if [ -z "$PATH" ] || ! which python3 >/dev/null 2>&1; then
  [ -f "$HOME/.zshrc" ] && source "$HOME/.zshrc" 2>/dev/null
  [ -f "$HOME/.bashrc" ] && source "$HOME/.bashrc" 2>/dev/null
  [ -f "$HOME/.zprofile" ] && source "$HOME/.zprofile" 2>/dev/null
fi
# .env ファイルがあれば読み込み
[ -f "$SCRIPT_DIR/.env" ] && source "$SCRIPT_DIR/.env" 2>/dev/null

# ── Python パス ──
PYTHON=$(which python3 2>/dev/null || which python 2>/dev/null || echo "/usr/bin/python3")

# ── macOS通知 ──
notify() {
  osascript -e "display notification \"$1\" with title \"🏠 不動産クロール\" subtitle \"$2\"" 2>/dev/null
}

# ── データディレクトリ確認 ──
mkdir -p "$SCRIPT_DIR/realestate_data"

# ── 実行 ──
notify "物件情報の収集を開始します..." "実行中"
echo "$(date '+%Y-%m-%d %H:%M:%S') ── 実行開始" >> "$LOG_FILE"

cd "$SCRIPT_DIR"
"$PYTHON" "$CRAWLER" "$@" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  # 物件数をカウント
  COUNT=$("$PYTHON" -c "
import json, os
latest = os.path.join('$SCRIPT_DIR', 'realestate_data', 'latest.json')
if os.path.exists(latest):
    d = json.load(open(latest))
    total = sum(len(v) if isinstance(v, list) else 0 for v in d.values())
    print(total)
else:
    print('?')
" 2>/dev/null)
  notify "${COUNT:-?}件の物件情報を収集しました ✅" "完了"
  echo "$(date '+%Y-%m-%d %H:%M:%S') ── 完了 (${COUNT:-?}件)" >> "$LOG_FILE"

  # --serve オプションでHTTPサーバーを起動
  if [ "$1" = "--serve" ]; then
    echo "🌐 HTTPサーバーを起動中 (http://localhost:8000/realestate_dashboard.html)..."
    cd "$SCRIPT_DIR"
    "$PYTHON" -m http.server 8000
  fi
else
  notify "収集に失敗しました ❌ ログを確認してください" "エラー"
  echo "$(date '+%Y-%m-%d %H:%M:%S') ── エラー (exit: $EXIT_CODE)" >> "$LOG_FILE"
fi

exit $EXIT_CODE
