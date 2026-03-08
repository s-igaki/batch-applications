#!/usr/bin/env python3
"""
Engineer/Tech Crawler using Claude API
エンジニア向け技術情報・イベント・ツールを自動収集してLINE通知するスクリプト
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
LINE_CLIENT_ID = os.environ.get("LINE_CLIENT_ID", "")
LINE_CLIENT_SECRET = os.environ.get("LINE_CLIENT_SECRET", "")

OUTPUT_DIR = Path(__file__).parent / "engineer_data"
MODEL = "claude-haiku-4-5-20251001"

# 収集トピック
TOPICS = [
    {
        "category": "技術トレンド",
        "query": "プログラミング言語・フレームワーク・ライブラリの最新トレンド、GitHub人気プロジェクト、注目の技術スタック",
        "keywords": ["GitHub", "フレームワーク", "ライブラリ", "プログラミング言語", "バージョン更新"]
    },
    {
        "category": "開発ツール・サービス",
        "query": "新しい開発者向けツール・SaaS・API・プラットフォーム・IDE・エディタのリリース・アップデート情報",
        "keywords": ["新サービス", "ベータ版", "リリース", "開発ツール", "IDE"]
    },
    {
        "category": "技術カンファレンス・イベント",
        "query": "日本国内の技術カンファレンス、勉強会、ハッカソン、IT系セミナー・ワークショップの開催情報",
        "keywords": ["カンファレンス", "勉強会", "ハッカソン", "セミナー", "ワークショップ"]
    },
    {
        "category": "AI・機械学習",
        "query": "AI・機械学習・LLM・深層学習の最新技術、新しいモデル・ライブラリ・論文・プロダクト",
        "keywords": ["AI", "機械学習", "LLM", "深層学習", "ChatGPT", "Claude"]
    },
    {
        "category": "クラウド・インフラ",
        "query": "AWS・Azure・GCPの新サービス、Kubernetes・Docker・インフラ技術のアップデート情報",
        "keywords": ["AWS", "Azure", "GCP", "Kubernetes", "Docker", "クラウド"]
    },
    {
        "category": "セキュリティ",
        "query": "サイバーセキュリティの脆弱性情報、セキュリティアップデート、重要なパッチ情報、セキュリティベストプラクティス",
        "keywords": ["脆弱性", "CVE", "セキュリティパッチ", "アップデート", "ゼロデイ"]
    },
]

SYSTEM_PROMPT = """エンジニア・テックアナリスト。指定トピックの最新情報を検索し、JSONのみ返答（前置き不要）:
{"category":"カテゴリ名","items":[{"title":"タイトル","description":"詳細説明2-3文","date":"日付（YYYY-MM-DD形式、不明な場合は空文字）","url":"URL","tags":["タグ1","タグ2"],"importance":"high/medium/low"}],"summary":"全体トレンド1-2文"}
アイテムは5〜15件。技術的に重要な情報を優先。JSONのみ。"""


# ============================================================
# LINE通知
# ============================================================
def get_line_access_token():
    """LINE Notify アクセストークンを取得"""
    if not LINE_CLIENT_ID or not LINE_CLIENT_SECRET:
        return None

    try:
        payload = {
            "grant_type": "client_credentials",
            "client_id": LINE_CLIENT_ID,
            "client_secret": LINE_CLIENT_SECRET
        }
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(
            "https://notify-bot.line.me/oauth/token",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("access_token")
    except Exception as e:
        print(f"⚠️  LINE トークン取得エラー: {e}")
        return None


def send_line_notification(message: str, access_token: str = None):
    """LINE Notifyでメッセージを送信"""
    if not access_token:
        access_token = get_line_access_token()
    if not access_token:
        print("⚠️  LINE通知をスキップ（トークン未取得）")
        return False

    try:
        payload = {"message": message}
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(
            "https://notify-api.line.me/api/notify",
            data=data,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/x-www-form-urlencoded"
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("status") == 200
    except Exception as e:
        print(f"⚠️  LINE送信エラー: {e}")
        return False


# ============================================================
# Claude API呼び出し
# ============================================================
def fetch_content_for_topic(topic: dict, retry: int = 5) -> dict:
    """Claude APIを呼び出してトピックの情報を取得（リトライ付き）"""
    category = topic["category"]
    query = topic["query"]

    payload = {
        "model": MODEL,
        "max_tokens": 4096,
        "tools": [{"type": "web_search_20250305", "name": "web_search"}],
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": f"今日（{datetime.now().strftime('%Y年%m月%d日')}）時点の「{query}」の最新情報をJSON形式で。カテゴリ名は「{category}」。"
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
            break

        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            err = json.loads(error_body) if error_body.startswith("{") else {}
            err_type = err.get("error", {}).get("type", "")

            if err_type == "rate_limit_error" and attempt < retry - 1:
                retry_after = e.headers.get("retry-after")
                if retry_after:
                    wait = int(float(retry_after)) + 5
                else:
                    wait = 60 * (2 ** attempt)
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

    # 切り詰められたJSONの修復を試行
    repaired = cleaned.rstrip().rstrip(',')
    open_braces = repaired.count('{') - repaired.count('}')
    open_brackets = repaired.count('[') - repaired.count(']')
    if repaired.count('"') % 2 == 1:
        repaired += '"'
    repaired += ']' * open_brackets + '}' * open_braces
    try:
        return json.loads(repaired)
    except json.JSONDecodeError:
        return {
            "category": category,
            "items": [],
            "summary": f"パースエラー: {text[:200]}"
        }


# ============================================================
# 通知メッセージ生成
# ============================================================
def generate_notification_message(results: dict) -> str:
    """収集結果からLINE通知用のメッセージを生成"""
    date = results.get("date", "")
    categories = results.get("categories", [])

    message_parts = [f"💻 エンジニア情報 ({date})"]

    for cat in categories:
        category = cat.get("category", "不明")
        items = cat.get("items", [])
        summary = cat.get("summary", "")

        if not items:
            continue

        # カテゴリごとのヘッダー
        if "技術トレンド" in category or "トレンド" in category:
            emoji = "📊"
        elif "ツール" in category or "サービス" in category:
            emoji = "🛠️"
        elif "イベント" in category or "カンファレンス" in category:
            emoji = "📅"
        elif "AI" in category or "機械学習" in category:
            emoji = "🤖"
        elif "クラウド" in category or "インフラ" in category:
            emoji = "☁️"
        elif "セキュリティ" in category:
            emoji = "🔒"
        else:
            emoji = "💻"

        message_parts.append(f"\n{emoji} {category} ({len(items)}件)")

        # 重要度highのアイテムを最大3件表示
        high_items = [item for item in items if item.get("importance") == "high"][:3]
        for item in high_items:
            title = item.get("title", "不明")
            tags = item.get("tags", [])
            tag_str = " ".join([f"#{tag}" for tag in tags[:3]]) if tags else ""
            message_parts.append(f"  • {title}")
            if tag_str:
                message_parts.append(f"    {tag_str}")

        # サマリーを追加
        if summary:
            message_parts.append(f"  💡 {summary[:80]}")

    message_parts.append(f"\n詳細: engineer_data/{date}.json")

    return "\n".join(message_parts)


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
        print(f"今日のエンジニア情報 ({today}) は既に収集済みです。--force で再実行できます。")
        sys.exit(0)

    print(f"💻 エンジニア情報収集開始: {today}")
    print(f"   収集カテゴリ: {len(TOPICS)}件\n")

    results = {
        "date": today,
        "generated_at": datetime.now().isoformat(),
        "categories": []
    }

    for i, topic in enumerate(TOPICS, 1):
        category = topic["category"]
        print(f"[{i}/{len(TOPICS)}] 収集中: {category} ...", end=" ", flush=True)
        try:
            data = fetch_content_for_topic(topic)
            results["categories"].append(data)
            item_count = len(data.get("items", []))
            print(f"✅ {item_count}件")
        except Exception as e:
            print(f"❌ エラー: {e}")
            results["categories"].append({
                "category": category,
                "items": [],
                "summary": f"収集エラー: {str(e)}"
            })

        # レート制限対策: カテゴリ間に待機（最後以外）
        if i < len(TOPICS):
            print(f"   ⏳ レート制限回避のため60秒待機中...")
            time.sleep(60)

    # 保存
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # 最新ファイルも更新
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

    total_items = sum(len(c.get("items", [])) for c in results["categories"])
    print(f"\n✨ 完了！ 合計 {total_items} 件の情報を収集しました。")
    print(f"   保存先: {output_file}")

    # LINE通知
    if LINE_CLIENT_ID and LINE_CLIENT_SECRET:
        print(f"\n📱 LINE通知を送信中...")
        notification_msg = generate_notification_message(results)
        if send_line_notification(notification_msg):
            print("   ✅ LINE通知を送信しました")
        else:
            print("   ⚠️  LINE通知の送信に失敗しました")
    else:
        print("\n⚠️  LINE通知はスキップされました（環境変数未設定）")


if __name__ == "__main__":
    main()
