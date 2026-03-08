#!/usr/bin/env python3
"""
Daily News Crawler using Claude API
毎日のニュースを自動収集・要約してJSONに保存するスクリプト
"""

import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
import urllib.request
import urllib.error

# ============================================================
# 設定 / Configuration
# ============================================================
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OUTPUT_DIR = Path(__file__).parent / "news_data"
MODEL = "claude-haiku-4-5-20251001"

# curl https://api.anthropic.com/v1/models \
#     -H 'anthropic-version: 2023-06-01' \
#     -H "X-Api-Key: $ANTHROPIC_API_KEY"
# "id": "claude-sonnet-4-6",
# "id": "claude-opus-4-6",
# "id": "claude-opus-4-5-20251101",
# "id": "claude-haiku-4-5-20251001",
# "id": "claude-sonnet-4-5-20250929",
# "id": "claude-opus-4-1-20250805",
# "id": "claude-opus-4-20250514",
# "id": "claude-sonnet-4-20250514",

TOPICS = [
    "AI・人工知能の最新動向",
    "フィンテック・金融テクノロジーのニュース",
    "テクノロジーニュース",
    "ビジネス・経済の重要ニュース",
    "政治・国際情勢の注目トピック",
]

SYSTEM_PROMPT = """ニュースアナリスト。指定トピックの今日の重要ニュースを検索し、JSONのみ返答（前置き不要）:
{"topic":"名前","articles":[{"title":"タイトル","summary":"2文要約","importance":"high/medium/low","source":"媒体名","url":"URL"}],"overview":"全体トレンド1文"}
記事は5〜10件。JSONのみ。"""


# ============================================================
# API呼び出し
# ============================================================
def fetch_news_for_topic(topic: str, retry: int = 5) -> dict:
    """Claude APIを呼び出してトピックのニュースを取得（リトライ付き）"""
    payload = {
        "model": MODEL,
        "max_tokens": 4096,
        "tools": [{"type": "web_search_20250305", "name": "web_search"}],
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": f"今日（{datetime.now().strftime('%Y年%m月%d日')}）の「{topic}」重要ニュースをJSON形式で。"
            }
        ]
    }

    for attempt in range(retry):
        try:
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages",
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": API_KEY,
                    "anthropic-version": "2023-06-01",
                },
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            break  # 成功したらループを抜ける

        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            err = json.loads(error_body) if error_body.startswith("{") else {}
            err_type = err.get("error", {}).get("type", "")

            if err_type == "rate_limit_error" and attempt < retry - 1:
                # retry-afterヘッダがあればそれを使う、なければ指数バックオフ
                retry_after = e.headers.get("retry-after")
                if retry_after:
                    wait = int(float(retry_after)) + 5
                else:
                    wait = 60 * (2 ** attempt)  # 60s, 120s, 240s, 480s
                print(f"\n   ⚠️  レート制限。{wait}秒待機してリトライ ({attempt+1}/{retry})...", end=" ", flush=True)
                time.sleep(wait)
                continue
            if err_type == "overloaded_error" and attempt < retry - 1:
                wait = 30 * (attempt + 1)
                print(f"\n   ⚠️  API過負荷。{wait}秒待機してリトライ ({attempt+1}/{retry})...", end=" ", flush=True)
                time.sleep(wait)
                continue
            raise RuntimeError(f"API Error {e.code}: {error_body}")

    # レスポンスからテキスト部分を抽出
    text = ""
    for block in data.get("content", []):
        if block.get("type") == "text":
            text += block.get("text", "")

    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # トレーリングカンマを除去して再試行
    cleaned = re.sub(r',\s*([}\]])', r'\1', text)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # 切り詰められたJSONの修復を試行: 閉じ括弧を補完
    repaired = cleaned.rstrip().rstrip(',')
    open_braces = repaired.count('{') - repaired.count('}')
    open_brackets = repaired.count('[') - repaired.count(']')
    # 末尾が文字列の途中で切れている場合、引用符を閉じる
    if repaired.count('"') % 2 == 1:
        repaired += '"'
    repaired += ']' * open_brackets + '}' * open_braces
    try:
        return json.loads(repaired)
    except json.JSONDecodeError:
        return {
            "topic": topic,
            "articles": [],
            "overview": f"パースエラー: {text[:200]}"
        }


# ============================================================
# メイン処理
# ============================================================
def main():
    if not API_KEY:
        print("エラー: ANTHROPIC_API_KEY 環境変数が設定されていません。")
        print("  export ANTHROPIC_API_KEY='your-key-here'")
        sys.exit(1)

    OUTPUT_DIR.mkdir(exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    output_file = OUTPUT_DIR / f"{today}.json"

    # 既に今日のファイルが存在する場合はスキップ（--forceで強制実行）
    if output_file.exists() and "--force" not in sys.argv:
        print(f"今日のニュース ({today}) は既に収集済みです。--force で再実行できます。")
        sys.exit(0)

    print(f"📰 ニュース収集開始: {today}")
    print(f"   収集トピック: {len(TOPICS)}件\n")

    results = {
        "date": today,
        "generated_at": datetime.now().isoformat(),
        "topics": []
    }

    for i, topic in enumerate(TOPICS, 1):
        print(f"[{i}/{len(TOPICS)}] 収集中: {topic} ...", end=" ", flush=True)
        try:
            data = fetch_news_for_topic(topic)
            results["topics"].append(data)
            article_count = len(data.get("articles", []))
            print(f"✅ {article_count}件")
        except Exception as e:
            print(f"❌ エラー: {e}")
            results["topics"].append({
                "topic": topic,
                "articles": [],
                "overview": f"収集エラー: {str(e)}"
            })

        # レート制限対策: トピック間に待機（最後のトピック以外）
        # 30,000 input tokens/分の制限に対応するため60秒待機
        if i < len(TOPICS):
            print(f"   ⏳ レート制限回避のため60秒待機中...")
            time.sleep(60)

    # 保存
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # ダッシュボード用の最新ファイルも更新
    latest_file = OUTPUT_DIR / "latest.json"
    with open(latest_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # 履歴インデックスを更新
    index_file = OUTPUT_DIR / "index.json"
    index = []
    if index_file.exists():
        with open(index_file) as f:
            index = json.load(f)
    if today not in index:
        index.insert(0, today)
    index = index[:30]  # 最新30日分を保持
    with open(index_file, "w") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    total_articles = sum(len(t.get("articles", [])) for t in results["topics"])
    print(f"\n✨ 完了！ 合計 {total_articles} 件の記事を収集しました。")
    print(f"   保存先: {output_file}")
    print(f"   ダッシュボード: ブラウザで dashboard.html を開いてください")


if __name__ == "__main__":
    main()
