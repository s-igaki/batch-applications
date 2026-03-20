"""共通ユーティリティ - パース関数、HTTPセッション管理"""

import re
import hashlib
import requests
from datetime import datetime
from bs4 import BeautifulSoup


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def create_session(headers):
    s = requests.Session()
    s.headers.update(headers)
    return s


def fetch_soup(session, url, params=None):
    """HTMLを取得してBeautifulSoupオブジェクトを返す"""
    try:
        resp = session.get(url, params=params, timeout=30, allow_redirects=True)
        resp.encoding = 'utf-8'
        if resp.status_code != 200:
            log(f"  HTTP {resp.status_code}: {url}")
            return None
        return BeautifulSoup(resp.text, 'html.parser')
    except Exception as e:
        log(f"  Fetch error: {e}")
        return None


def parse_number(text):
    """テキストから数値を抽出"""
    if not text:
        return None
    text = text.replace(',', '').replace('，', '')
    m = re.search(r'([\d.]+)', text)
    return float(m.group(1)) if m else None


def parse_buy_price(text):
    """売買価格テキストから万円単位の数値を抽出
    例: '1億5,000万円' → 15000, '7,900万円' → 7900, '2億円' → 20000
    レンジ表記の場合は最小値を返す: '4,900万円～5,500万円' → 4900
    """
    if not text:
        return None
    # レンジ表記の場合は最初の価格を使用
    text = re.split(r'[～~〜]', text)[0]
    text = text.replace(',', '').replace('，', '').replace(' ', '')

    # 「X億Y万円」形式
    m = re.search(r'(\d+)億(\d+)万', text)
    if m:
        return int(m.group(1)) * 10000 + int(m.group(2))

    # 「X億円」形式（万の部分がない）
    m = re.search(r'(\d+)億', text)
    if m:
        return int(m.group(1)) * 10000

    # 「X万円」形式
    m = re.search(r'([\d.]+)万', text)
    if m:
        return float(m.group(1))

    return None


def parse_age_years(text):
    """築年数テキストから年数を計算"""
    if not text:
        return None
    if '新築' in text:
        return 0
    # 「築3年」形式
    m = re.search(r'築(\d+)年', text)
    if m:
        return int(m.group(1))
    # 「2020年3月」形式 → 現在からの年数
    m = re.search(r'(\d{4})年(\d{1,2})?月?', text)
    if m:
        year = int(m.group(1))
        now = datetime.now()
        return now.year - year
    return None


def extract_walk_minutes(text):
    """駅徒歩分数を抽出"""
    if not text:
        return None
    m = re.search(r'徒歩(\d+)分', text)
    if not m:
        m = re.search(r'歩(\d+)分', text)
    return int(m.group(1)) if m else None


def make_unique_id(url_or_text):
    """URLまたはテキストからユニークIDを生成"""
    return hashlib.md5(url_or_text.encode('utf-8')).hexdigest()[:12]
