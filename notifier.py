"""LINE Messaging API 通知"""

import time
import requests
from datetime import datetime

from crawlers.base import log


LINE_TOKEN_URL = 'https://api.line.me/v2/oauth/accessToken'
LINE_BROADCAST_URL = 'https://api.line.me/v2/bot/message/broadcast'


def get_line_access_token(profile):
    """LINE Messaging API のアクセストークンを取得"""
    if not profile.line_client_id or not profile.line_client_secret:
        log('LINE_CLIENT_ID / LINE_CLIENT_SECRET が未設定のためLINE通知をスキップ')
        return None

    try:
        resp = requests.post(LINE_TOKEN_URL, data={
            'grant_type': 'client_credentials',
            'client_id': profile.line_client_id,
            'client_secret': profile.line_client_secret,
        }, headers={'Content-Type': 'application/x-www-form-urlencoded'}, timeout=30)
        if resp.status_code == 200:
            token = resp.json().get('access_token')
            log(f"LINE アクセストークン取得成功")
            return token
        else:
            log(f"LINE トークン取得失敗: HTTP {resp.status_code} {resp.text}")
            return None
    except Exception as e:
        log(f"LINE トークン取得エラー: {e}")
        return None


def send_line_broadcast(token, messages):
    """LINE Messaging API でブロードキャストメッセージを送信
    messages: list of message dicts (max 5 per request)
    """
    try:
        resp = requests.post(LINE_BROADCAST_URL, json={
            'messages': messages,
        }, headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}',
        }, timeout=30)
        if resp.status_code == 200:
            log(f"LINE 送信成功")
            return True
        else:
            log(f"LINE 送信失敗: HTTP {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        log(f"LINE 送信エラー: {e}")
        return False


# カテゴリ定義
LINE_CATEGORIES = {
    'new':    {'label': '新築マンション', 'emoji': '🏠', 'price_unit': 'mansaku'},
    'rental': {'label': '賃貸',           'emoji': '🏢', 'price_unit': 'man'},
    'used':   {'label': '中古マンション', 'emoji': '🏘️', 'price_unit': 'mansaku'},
}


def _format_price(price, category):
    """カテゴリに応じた価格フォーマット"""
    if not price:
        return '価格未定'
    if category in ('new', 'used'):
        if price >= 10000:
            return f"{price / 10000:.1f}億円".replace('.0億', '億')
        return f"{price:.0f}万円"
    else:  # rental
        return f"{price}万円"


def _build_category_message(category, listings, all_props):
    """1カテゴリ分のメッセージテキストを生成"""
    cat_info = LINE_CATEGORIES[category]
    emoji = cat_info['emoji']
    label = cat_info['label']

    if not listings:
        return f'{emoji} {label} {emoji}\n新着情報はありません。\n'

    url_to_prop = {p.get('detail_url', ''): p for p in all_props if p.get('detail_url')}

    lines = []
    lines.append(f'{emoji} {label} 新着物件 {emoji}')
    lines.append(f'({len(listings)}件)')
    lines.append('')

    for listing in listings:
        name = listing.get('name', '不明')
        station = listing.get('station', '不明')
        detail_url = listing.get('detail_url', '')

        prop = url_to_prop.get(detail_url, {})
        price = prop.get('price') or listing.get('price')
        area_text = prop.get('area_text', '')
        area = prop.get('area')
        address = prop.get('address', '')

        price_str = _format_price(price, category)
        area_str = area_text or (f"{area}m²" if area else '')

        lines.append(f'▼ {name}')
        lines.append(f'  📍 {station}駅')
        lines.append(f'  💰 {price_str}')
        if area_str:
            lines.append(f'  📐 {area_str}')
        if address:
            lines.append(f'  🏢 {address}')
        if detail_url:
            lines.append(f'  🔗 {detail_url}')
        lines.append('')

    return '\n'.join(lines)


def _format_single_property(listing, all_props, category):
    """1物件分のテキストを生成（分割送信用）"""
    url_to_prop = {p.get('detail_url', ''): p for p in all_props if p.get('detail_url')}
    name = listing.get('name', '不明')
    station = listing.get('station', '不明')
    detail_url = listing.get('detail_url', '')
    prop = url_to_prop.get(detail_url, {})
    price = prop.get('price') or listing.get('price')
    area_text = prop.get('area_text', '')
    area = prop.get('area')
    address = prop.get('address', '')

    price_str = _format_price(price, category)
    area_str = area_text or (f"{area}m²" if area else '')

    text = f'\n▼ {name}\n  📍 {station}駅 / 💰 {price_str}'
    if area_str:
        text += f' / 📐 {area_str}'
    if address:
        text += f'\n  🏢 {address}'
    if detail_url:
        text += f'\n  🔗 {detail_url}'
    text += '\n'
    return text


def _split_and_send(token, category, listings, all_props):
    """1カテゴリ分のメッセージを生成し、必要に応じて分割送信"""
    cat_info = LINE_CATEGORIES[category]
    MAX_LEN = 4500

    message_text = _build_category_message(category, listings, all_props)

    if len(message_text) <= MAX_LEN:
        return [{'type': 'text', 'text': message_text}]

    # 長い場合は分割
    emoji = cat_info['emoji']
    label = cat_info['label']
    chunks = []
    header = f'{emoji} {label} 新着物件 ({len(listings)}件) {emoji}\n'
    current_chunk = header
    for listing in listings:
        prop_text = _format_single_property(listing, all_props, category)
        if len(current_chunk) + len(prop_text) > MAX_LEN:
            chunks.append(current_chunk)
            current_chunk = f'{emoji} {label} 新着物件（続き）{emoji}\n'
        current_chunk += prop_text
    if current_chunk.strip():
        chunks.append(current_chunk)

    return [{'type': 'text', 'text': c} for c in chunks]


def notify_line_new_listings(profile, changes, all_rental, all_new, all_used):
    """賃貸・新築・中古の新着物件をLINEで通知"""
    new_listings = changes.get('new_listings', [])

    by_type = {
        'rental': [c for c in new_listings if c.get('type') == 'rental'],
        'new':    [c for c in new_listings if c.get('type') == 'new'],
        'used':   [c for c in new_listings if c.get('type') == 'used'],
    }
    all_props_map = {
        'rental': all_rental,
        'new':    all_new,
        'used':   all_used,
    }

    total = sum(len(v) for v in by_type.values())
    log(f"新着物件 → 賃貸{len(by_type['rental'])}件 / 新築{len(by_type['new'])}件 / 中古{len(by_type['used'])}件")
    log("LINE通知送信中...")

    token = get_line_access_token(profile)
    if not token:
        log("LINE アクセストークン取得失敗 - 通知をスキップ")
        return

    # 日付ヘッダーを先頭に追加
    today_str = datetime.now().strftime('%Y年%-m月%-d日')
    date_header = {
        'type': 'text',
        'text': f'📅 {today_str}のデータ 📅'
    }

    # カテゴリごとにメッセージを作成
    all_messages = [date_header]
    for cat in ['new', 'rental', 'used']:
        msgs = _split_and_send(token, cat, by_type[cat], all_props_map[cat])
        all_messages.extend(msgs)

    # サマリーURLを末尾に追加
    summary_footer = {
        'type': 'text',
        'text': '📊 サマリーはこちら\nhttps://s-igaki.github.io/batch-applications/index.html',
    }
    all_messages.append(summary_footer)

    # 5メッセージずつ送信（LINE API制限）
    for i in range(0, len(all_messages), 5):
        batch = all_messages[i:i+5]
        send_line_broadcast(token, batch)
        if i + 5 < len(all_messages):
            time.sleep(1)

    log("LINE 新着物件通知完了")
