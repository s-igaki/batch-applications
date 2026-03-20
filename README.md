# Batch Applications

各種情報を自動収集してLINE通知するPythonスクリプト集

## 📋 収集スクリプト一覧

### 1. 不動産情報クローラー (`realestate_crawler.py`)
- **対象**: SUUMO & LIFULL HOME'S の賃貸・新築・中古マンション
- **エリア**: 吉祥寺、西荻窪、荻窪、高円寺、代々木、千駄ヶ谷、信濃町、四ツ谷、市谷、飯田橋
- **条件**: 専有面積40㎡以上、築20年以内、賃料24万円以下（賃貸のみ）
- **実行**: 毎日 06:00 JST
- **保存先**: `realestate_data/`

## 🚀 セットアップ

### 1. 環境変数の設定

```bash
# Claude API Key（必須）
export ANTHROPIC_API_KEY='your-api-key-here'

# LINE通知（任意）
export LINE_CLIENT_ID='your-line-client-id'
export LINE_CLIENT_SECRET='your-line-client-secret'
```

### 2. 依存関係のインストール

```bash
pip install -r requirements.txt
```

### 3. GitHub Secretsの設定

GitHub リポジトリの Settings > Secrets and variables > Actions で以下を設定:

- `ANTHROPIC_API_KEY`: Claude API キー
- `LINE_CLIENT_ID`: LINE Notify クライアントID（任意）
- `LINE_CLIENT_SECRET`: LINE Notify クライアントシークレット（任意）

## 💻 使い方

### ローカルで手動実行

```bash
# 不動産情報を収集
python realestate_crawler.py

# 強制再実行（既に今日のデータがある場合）
python realestate_crawler.py --force
```

### GitHub Actionsで自動実行

- **不動産**: 毎日 06:00 JST 自動実行

手動実行も可能:
1. GitHub リポジトリの Actions タブを開く
2. 該当ワークフローを選択
3. "Run workflow" をクリック

## 📁 データ構造

各スクリプトは以下の形式でJSONを保存します:

```json
{
  "date": "2026-03-05",
  "generated_at": "2026-03-05T09:00:00+09:00",
  "categories": [
    {
      "category": "カテゴリ名",
      "items": [
        {
          "title": "タイトル",
          "description": "詳細説明",
          "date": "2026-03-05",
          "url": "https://example.com",
          "importance": "high"
        }
      ],
      "summary": "全体トレンド"
    }
  ]
}
```

また、以下のファイルも自動生成されます:
- `latest.json`: 最新のデータ
- `index.json`: 過去30日分のインデックス

## 📱 LINE通知

LINE通知を有効にすると、収集完了時に以下の情報が送信されます:
- 収集日
- カテゴリごとのサマリー
- 重要度 "high" のアイテム（最大3件/カテゴリ）

## 🔧 カスタマイズ

### 収集トピックの変更

各スクリプトの `TOPICS` 配列を編集:

```python
TOPICS = [
    {
        "category": "カテゴリ名",
        "query": "収集したい情報の説明",
        "keywords": ["キーワード1", "キーワード2"]
    },
]
```

### 実行時刻の変更

`.github/workflows/*.yaml` の `cron` を編集:

```yaml
schedule:
  - cron: "0 0 * * *"  # UTC時刻で指定
```

## 📝 ライセンス

MIT License
