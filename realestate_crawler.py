#!/usr/bin/env python3
"""
不動産物件クローラー - SUUMO & LIFULL HOME'S
賃貸・新築・中古マンションの物件情報を取得し、JSONスナップショットとして保存する。

対象駅: 吉祥寺, 西荻窪, 荻窪, 高円寺, 代々木, 千駄ヶ谷, 信濃町, 四ツ谷, 市谷, 飯田橋,
       渋谷, 代々木上原, 代々木公園, 明治神宮前, 表参道
条件:
  共通: 専有面積40㎡以上, 築20年以内
  賃貸: 24万円以下
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import re
import sys
import time
import hashlib
import traceback
import statistics
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlencode

# ============================================================
# 設定
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, 'realestate_data')

# 対象駅とSUUMO駅コード
STATIONS = {
    '吉祥寺':    '11640',
    '西荻窪':    '28500',
    '荻窪':      '06640',
    '高円寺':    '13930',
    '代々木':    '41280',
    '千駄ヶ谷':  '21520',
    '信濃町':    '17470',
    '四ツ谷':    '41160',
    '市谷':      '02980',
    '飯田橋':    '01820',
    '渋谷':      '17640',
    '代々木上原': '41290',
    '代々木公園': '41300',
    '明治神宮前': '39010',
    '表参道':    '07240',
}

# SUUMO表記ゆれ対応
STATION_ALIASES = {
    '千駄ケ谷': '千駄ヶ谷',
    '市ケ谷':   '市谷',
    '市ヶ谷':   '市谷',
    '明治神宮前〈原宿〉': '明治神宮前',
    '明治神宮前（原宿）': '明治神宮前',
}

# 検索条件
MIN_AREA = 40       # 専有面積 40㎡以上
MAX_AGE = 20        # 築20年以内
MAX_RENT = 24.0     # 賃料 24万円以下（賃貸のみ）

# HTTP設定
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}
REQUEST_DELAY = 2  # リクエスト間隔（秒）
MAX_PAGES = 5      # 1駅あたりの最大ページ数

# HOMES エリアスラッグ（対象駅が含まれる市区町村）
HOMES_AREAS = {
    'musashino-city':  ['吉祥寺'],
    'suginami-city':   ['西荻窪', '荻窪', '高円寺'],
    'shibuya-city':    ['代々木', '千駄ヶ谷', '渋谷', '代々木上原', '代々木公園', '明治神宮前', '表参道'],
    'shinjuku-city':   ['信濃町', '四ツ谷', '市谷'],
    'chiyoda-city':    ['飯田橋', '市谷'],
    'minato-city':     ['表参道'],
}


# ============================================================
# ユーティリティ
# ============================================================
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def create_session():
    s = requests.Session()
    s.headers.update(HEADERS)
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


def station_matches(text):
    """テキストに対象駅名が含まれるか確認し、駅名を返す"""
    if not text:
        return None
    for station in STATIONS:
        if station in text:
            return station
    for alias, canonical in STATION_ALIASES.items():
        if alias in text:
            return canonical
    return None


def make_unique_id(url_or_text):
    """URLまたはテキストからユニークIDを生成"""
    return hashlib.md5(url_or_text.encode('utf-8')).hexdigest()[:12]


# ============================================================
# SUUMO クローラー
# ============================================================
class SuumoCrawler:
    BASE = 'https://suumo.jp'

    def __init__(self, session):
        self.session = session

    # --- 賃貸 ---
    def crawl_rental(self):
        """SUUMO賃貸物件をクロール"""
        log("SUUMO 賃貸クロール開始...")
        all_properties = {}

        for station_name, station_code in STATIONS.items():
            log(f"  駅: {station_name} (ek_{station_code})")
            url = f"{self.BASE}/chintai/tokyo/ek_{station_code}/"

            for page in range(1, MAX_PAGES + 1):
                params = {'page': page} if page > 1 else None
                soup = fetch_soup(self.session, url, params)
                if not soup:
                    break

                items = soup.select('.cassetteitem')
                if not items:
                    break

                new_count = 0
                for item in items:
                    props = self._parse_rental_cassetteitem(item, station_name)
                    for p in props:
                        # 対象駅に紐づかない物件はスキップ
                        if not p.get('station'):
                            continue
                        # 条件フィルタリング
                        if p.get('area') and p['area'] < MIN_AREA:
                            continue
                        if p.get('age_years') is not None and p['age_years'] > MAX_AGE:
                            continue
                        if p.get('price') and p['price'] > MAX_RENT:
                            continue
                        uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
                        if uid not in all_properties:
                            all_properties[uid] = p
                            new_count += 1

                log(f"    ページ{page}: {len(items)}棟, 新規{new_count}件")

                # 次のページがあるかチェック
                next_link = soup.select_one('.pagination-parts a[rel="next"], .paginate_set-nav a:last-child')
                if not next_link:
                    break

                time.sleep(REQUEST_DELAY)

            time.sleep(REQUEST_DELAY)

        result = list(all_properties.values())
        log(f"  SUUMO賃貸: 合計{len(result)}件")
        return result

    def _parse_rental_cassetteitem(self, item, search_station):
        """SUUMO賃貸のcassetteitemをパース（1棟=複数部屋）"""
        properties = []

        # 建物情報
        building_name = ''
        title_el = item.select_one('.cassetteitem_content-title')
        if title_el:
            building_name = title_el.get_text(strip=True)

        # 建物画像
        building_img = ''
        img_el = item.select_one('.cassetteitem_object img.js-noContextMenu')
        if img_el:
            building_img = img_el.get('rel', '') or img_el.get('src', '')
        if not building_img or 'data:image' in building_img:
            img_el2 = item.select_one('.cassetteitem_object img')
            if img_el2:
                building_img = img_el2.get('rel', '') or img_el2.get('src', '')

        # 住所
        address = ''
        addr_el = item.select_one('.cassetteitem_detail-col1')
        if addr_el:
            address = addr_el.get_text(strip=True)

        # 駅・アクセス
        access_texts = []
        access_els = item.select('.cassetteitem_detail-col2 .cassetteitem_detail-text')
        for el in access_els:
            access_texts.append(el.get_text(strip=True))

        # 築年数
        age_text = ''
        age_el = item.select_one('.cassetteitem_detail-col3')
        if age_el:
            divs = age_el.select('div')
            if divs:
                age_text = divs[0].get_text(strip=True)
        age_years = parse_age_years(age_text)

        # 駅情報抽出（対象駅がアクセス情報に含まれる場合のみ採用）
        walk_min_default = None
        walk_station_default = None
        for acc in access_texts:
            matched = station_matches(acc)
            if matched:
                walk_min_default = extract_walk_minutes(acc)
                walk_station_default = matched
                break
        if walk_min_default is None and access_texts:
            walk_min_default = extract_walk_minutes(access_texts[0])

        # 部屋ごとの情報
        rows = item.select('table.cassetteitem_other tbody tr.js-cassette_link')
        for row in rows:
            tds = row.select('td')
            if len(tds) < 7:
                continue

            # 詳細URL
            detail_url = ''
            link = row.select_one('a[href*="/chintai/"]')
            if not link:
                link = row.select_one('a[href]')
            if link:
                href = link.get('href', '')
                if href and href != '#' and 'javascript' not in href:
                    detail_url = urljoin(self.BASE, href)

            # 部屋画像（サムネイルのdata-imgs）
            room_img = building_img
            thumb = row.select_one('[data-imgs]')
            if thumb:
                imgs = thumb.get('data-imgs', '').split(',')
                if imgs and imgs[0]:
                    room_img = imgs[0]

            # 賃料（万円）
            rent_text = ''
            rent_val = None
            # cassetteitem_other--emphasis span内に賃料あり
            rent_spans = row.select('span.cassetteitem_other--emphasis')
            if rent_spans:
                rent_text = rent_spans[0].get_text(strip=True)
            if not rent_text and len(tds) >= 4:
                rent_text = tds[3].get_text(strip=True)

            if rent_text:
                m = re.search(r'([\d.]+)\s*万円', rent_text)
                if m:
                    rent_val = float(m.group(1))

            # 間取り・専有面積
            area_text = ''
            area_val = None
            if len(tds) >= 6:
                area_cell = tds[5]
                txt = area_cell.get_text(strip=True)
                m = re.search(r'([\d.]+)\s*m', txt)
                if m:
                    area_val = float(m.group(1))
                    area_text = f"{area_val}m²"

            prop = {
                'source': 'SUUMO',
                'type': '賃貸',
                'name': building_name,
                'detail_url': detail_url,
                'price': rent_val,
                'price_text': rent_text,
                'area': area_val,
                'area_text': area_text,
                'walk_minutes': walk_min_default,
                'station': walk_station_default,
                'age_years': age_years,
                'age_text': age_text,
                'image_url': room_img,
                'address': address,
                'access': ' / '.join(access_texts),
            }
            properties.append(prop)

        return properties

    # --- 新築マンション ---
    def crawl_new(self):
        """SUUMO新築マンションをクロール"""
        log("SUUMO 新築マンションクロール開始...")
        return self._crawl_buy_type('shinchiku', '新築')

    # --- 中古マンション ---
    def crawl_used(self):
        """SUUMO中古マンションをクロール"""
        log("SUUMO 中古マンションクロール開始...")
        return self._crawl_buy_type('chuko', '中古')

    def _crawl_buy_type(self, path_segment, type_label):
        """SUUMO売買物件（新築/中古）をクロール"""
        all_properties = {}

        for station_name, station_code in STATIONS.items():
            log(f"  駅: {station_name}")
            url = f"{self.BASE}/ms/{path_segment}/tokyo/ek_{station_code}/"

            for page in range(1, MAX_PAGES + 1):
                params = {'page': page} if page > 1 else None
                soup = fetch_soup(self.session, url, params)
                if not soup:
                    break

                units = soup.select('.property_unit')
                if not units:
                    break

                new_count = 0
                for unit in units:
                    p = self._parse_property_unit(unit, station_name, type_label)
                    if not p:
                        continue
                    # 対象駅に紐づかない物件はスキップ
                    if not p.get('station'):
                        continue
                    # 条件フィルタリング
                    if p.get('area') and p['area'] < MIN_AREA:
                        continue
                    if p.get('age_years') is not None and p['age_years'] > MAX_AGE:
                        continue
                    uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
                    if uid not in all_properties:
                        all_properties[uid] = p
                        new_count += 1

                log(f"    ページ{page}: {len(units)}件, 新規{new_count}件")

                next_link = soup.select_one('.pagination-parts a[rel="next"], .paginate_set-nav a:last-child')
                if not next_link:
                    break

                time.sleep(REQUEST_DELAY)

            time.sleep(REQUEST_DELAY)

        result = list(all_properties.values())
        log(f"  SUUMO {type_label}: 合計{len(result)}件")
        return result

    def _parse_property_unit(self, unit, search_station, type_label):
        """SUUMO売買のproperty_unitをパース（中古dt/dd形式 & 新築cassette形式 両対応）"""
        # 新築マンションはcassette形式のHTMLを使う
        is_cassette = bool(unit.select_one('.cassette_header-title, .cassette_basic'))
        if is_cassette:
            return self._parse_cassette_unit(unit, search_station, type_label)
        return self._parse_dottable_unit(unit, search_station, type_label)

    def _parse_cassette_unit(self, unit, search_station, type_label):
        """SUUMO新築マンションのcassette形式をパース"""
        try:
            # 物件名
            name = ''
            title_el = unit.select_one('.cassette_header-title, a.cassette_header-title')
            if title_el:
                name = title_el.get_text(strip=True)

            # 詳細URL
            detail_url = ''
            title_link = unit.select_one('a.cassette_header-title[href], .cassette_header-title a[href]')
            if title_link:
                detail_url = urljoin(self.BASE, title_link.get('href', ''))
            if not detail_url:
                link = unit.select_one('a[href*="/nc_"]')
                if link:
                    detail_url = urljoin(self.BASE, link.get('href', ''))

            # 画像（cassette_thumbcarousel内のメイン画像）
            image_url = ''
            img = unit.select_one('.cassette_thumbcarousel-itembox img.js-noContextMenu')
            if not img:
                img = unit.select_one('.cassette_thumbcarousel img.js-noContextMenu')
            if not img:
                img = unit.select_one('.cassette-object img.js-noContextMenu')
            if img:
                image_url = img.get('rel', '') or img.get('src', '')
                if image_url and 'data:image' in image_url:
                    image_url = img.get('rel', '')

            # cassette_basic から所在地・交通を取得
            address = ''
            access = ''
            walk_min = None
            walk_station = None
            for title_p in unit.select('.cassette_basic-title'):
                label = title_p.get_text(strip=True)
                value_p = title_p.find_next_sibling('p', class_='cassette_basic-value')
                if not value_p:
                    # 親divの次の要素を探す
                    parent = title_p.parent
                    if parent:
                        value_p = parent.select_one('.cassette_basic-value')
                if not value_p:
                    continue
                value = value_p.get_text(strip=True)
                if '所在地' in label:
                    address = value
                elif '交通' in label:
                    access = value
                    matched = station_matches(access)
                    if matched:
                        walk_station = matched
                    walk_min = extract_walk_minutes(access)

            # 販売価格（cassette_price-accent）
            price_text = ''
            price_val = None
            price_el = unit.select_one('.cassette_price-accent')
            if price_el:
                price_text = price_el.get_text(strip=True)
                price_val = parse_buy_price(price_text)

            # 面積・間取り情報（cassette_price-description）
            area_text = ''
            area_val = None
            desc_el = unit.select_one('.cassette_price-description')
            if desc_el:
                desc_text = desc_el.get_text(strip=True)
                # "1LDK+S～3LDK / 53.5m²～67.35m²" のようなテキストから面積を抽出
                area_m = re.search(r'([\d.]+)\s*m[²2]\s*[～~〜]\s*([\d.]+)\s*m[²2]', desc_text)
                if area_m:
                    # 最小面積を使用
                    area_val = float(area_m.group(1))
                    area_text = f"{area_m.group(1)}m²～{area_m.group(2)}m²"
                else:
                    area_m2 = re.search(r'([\d.]+)\s*m[²2]', desc_text)
                    if area_m2:
                        area_val = float(area_m2.group(1))
                        area_text = f"{area_m2.group(1)}m²"

            # 新築マンションは築年数0
            age_text = '新築'
            age_years = 0

            return {
                'source': 'SUUMO',
                'type': type_label,
                'name': name,
                'detail_url': detail_url,
                'price': price_val,
                'price_text': price_text,
                'area': area_val,
                'area_text': area_text,
                'walk_minutes': walk_min,
                'station': walk_station,
                'age_years': age_years,
                'age_text': age_text,
                'image_url': image_url,
                'address': address,
                'access': access,
            }
        except Exception as e:
            log(f"  新築パースエラー: {e}")
            return None

    def _parse_dottable_unit(self, unit, search_station, type_label):
        """SUUMO中古マンションのdt/dd（dottable）形式をパース"""
        try:
            # 物件名
            name = ''
            name_dt = unit.find('dt', string=re.compile('物件名'))
            if name_dt:
                dd = name_dt.find_next_sibling('dd')
                if dd:
                    name = dd.get_text(strip=True)

            # 詳細URL
            detail_url = ''
            title_link = unit.select_one('.property_unit-title a[href]')
            if title_link:
                detail_url = urljoin(self.BASE, title_link.get('href', ''))
            if not detail_url:
                link = unit.select_one('a[href*="/nc_"]')
                if link:
                    detail_url = urljoin(self.BASE, link.get('href', ''))

            # 画像
            image_url = ''
            img = unit.select_one('.property_unit-object img.js-noContextMenu, .property_unit-object img')
            if img:
                image_url = img.get('rel', '') or img.get('src', '')
                if image_url and 'data:image' in image_url:
                    image_url = img.get('rel', '')

            # 販売価格
            price_text = ''
            price_val = None
            price_dt = unit.find('dt', string=re.compile('販売価格|価格'))
            if price_dt:
                dd = price_dt.find_next_sibling('dd')
                if dd:
                    v = dd.select_one('.dottable-value')
                    price_text = (v or dd).get_text(strip=True)
                    price_val = parse_buy_price(price_text)

            # 所在地
            address = ''
            addr_dt = unit.find('dt', string=re.compile('所在地'))
            if addr_dt:
                dd = addr_dt.find_next_sibling('dd')
                if dd:
                    address = dd.get_text(strip=True)

            # 沿線・駅
            access = ''
            walk_min = None
            walk_station = None
            ensen_dt = unit.find('dt', string=re.compile('沿線'))
            if ensen_dt:
                dd = ensen_dt.find_next_sibling('dd')
                if dd:
                    access = dd.get_text(strip=True)
                    matched = station_matches(access)
                    if matched:
                        walk_station = matched
                    walk_min = extract_walk_minutes(access)

            # 専有面積
            area_text = ''
            area_val = None
            area_dt = unit.find('dt', string=re.compile('専有面積'))
            if area_dt:
                dd = area_dt.find_next_sibling('dd')
                if dd:
                    area_text = dd.get_text(strip=True)
                    area_val = parse_number(area_text)

            # 築年月
            age_text = ''
            age_years = None
            age_dt = unit.find('dt', string=re.compile('築年'))
            if age_dt:
                dd = age_dt.find_next_sibling('dd')
                if dd:
                    age_text = dd.get_text(strip=True)
                    age_years = parse_age_years(age_text)

            return {
                'source': 'SUUMO',
                'type': type_label,
                'name': name,
                'detail_url': detail_url,
                'price': price_val,
                'price_text': price_text,
                'area': area_val,
                'area_text': area_text,
                'walk_minutes': walk_min,
                'station': walk_station,
                'age_years': age_years,
                'age_text': age_text,
                'image_url': image_url,
                'address': address,
                'access': access,
            }
        except Exception as e:
            log(f"  パースエラー: {e}")
            return None


# ============================================================
# LIFULL HOME'S クローラー
# ============================================================
class HomesCrawler:
    BASE = 'https://www.homes.co.jp'

    def __init__(self, session):
        self.session = session

    def crawl_rental(self):
        """HOMES賃貸物件をクロール（エリア別）"""
        log("HOMES 賃貸クロール開始...")
        all_properties = {}

        for area_slug, area_stations in HOMES_AREAS.items():
            log(f"  エリア: {area_slug} ({', '.join(area_stations)})")
            for page in range(1, MAX_PAGES + 1):
                url = f"{self.BASE}/chintai/tokyo/{area_slug}/list/"
                params = {'page': page} if page > 1 else None

                soup = fetch_soup(self.session, url, params)
                if not soup:
                    break

                buildings = soup.select('.prg-building')
                if not buildings:
                    break

                new_count = 0
                for bldg in buildings:
                    props = self._parse_rental_building(bldg)
                    for p in props:
                        if p.get('station') is None:
                            continue
                        if p.get('area') and p['area'] < MIN_AREA:
                            continue
                        if p.get('age_years') is not None and p['age_years'] > MAX_AGE:
                            continue
                        if p.get('price') and p['price'] > MAX_RENT:
                            continue
                        uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
                        if uid not in all_properties:
                            all_properties[uid] = p
                            new_count += 1

                log(f"    ページ{page}: {len(buildings)}棟, 対象駅マッチ{new_count}件")

                next_link = soup.select_one('a[rel="next"], .prg-paging a:last-child')
                if not next_link:
                    break

                time.sleep(REQUEST_DELAY)

            time.sleep(REQUEST_DELAY)

        result = list(all_properties.values())
        log(f"  HOMES賃貸: 合計{len(result)}件")
        return result

    def _parse_rental_building(self, bldg):
        """HOMES賃貸のprg-buildingをパース"""
        properties = []
        try:
            # 建物名
            building_name = ''
            name_el = bldg.select_one('.bukkenName')
            if name_el:
                building_name = name_el.get_text(strip=True)

            # 建物画像
            building_img = ''
            img_el = bldg.select_one('.bukkenPhoto img')
            if img_el:
                building_img = img_el.get('data-original', '') or img_el.get('src', '')
                if building_img and 'loading' in building_img:
                    building_img = ''
            if not building_img:
                noscript = bldg.select_one('.bukkenPhoto noscript')
                if noscript:
                    ns_soup = BeautifulSoup(str(noscript), 'html.parser')
                    ns_img = ns_soup.select_one('img')
                    if ns_img:
                        building_img = ns_img.get('src', '')

            # スペック表から情報取得
            spec_table = bldg.select_one('.bukkenSpec table')
            address = ''
            access_texts = []
            age_text = ''
            age_years = None

            if spec_table:
                for tr in spec_table.select('tr'):
                    th = tr.select_one('th')
                    td = tr.select_one('td')
                    if not th or not td:
                        continue
                    label = th.get_text(strip=True)
                    value = td.get_text(strip=True)

                    if '所在地' in label:
                        address = value
                    elif '交通' in label:
                        station_spans = td.select('.prg-stationText')
                        for span in station_spans:
                            access_texts.append(span.get_text(strip=True))
                        if not station_spans:
                            access_texts.append(value)
                    elif '築年' in label:
                        age_text = value
                        age_years = parse_age_years(age_text)

            # この建物が対象駅に関連するかチェック
            matched_station = None
            walk_min = None
            for acc in access_texts:
                matched = station_matches(acc)
                if matched:
                    matched_station = matched
                    walk_min = extract_walk_minutes(acc)
                    break

            if not matched_station:
                return []  # 対象駅に近くない建物はスキップ

            # 建物リンクURL
            building_url = ''
            link = bldg.select_one('.prg-bukkenNameAnchor')
            if link:
                building_url = link.get('href', '')

            # 部屋ユニット情報
            unit_rows = bldg.select('.unitSummary tbody tr')
            if not unit_rows:
                # 部屋情報がない場合は建物情報のみで1件作成
                properties.append({
                    'source': 'HOMES',
                    'type': '賃貸',
                    'name': building_name,
                    'detail_url': building_url,
                    'price': None,
                    'price_text': '',
                    'area': None,
                    'area_text': '',
                    'walk_minutes': walk_min,
                    'station': matched_station,
                    'age_years': age_years,
                    'age_text': age_text,
                    'image_url': building_img,
                    'address': address,
                    'access': ' / '.join(access_texts),
                })
                return properties

            for row in unit_rows:
                tds = row.select('td')
                if len(tds) < 4:
                    continue

                # 詳細URL
                detail_url = building_url
                link = row.select_one('a[href*="/chintai/room/"]')
                if not link:
                    link = row.select_one('a[href]')
                if link:
                    href = link.get('href', '')
                    if href and href != '#' and 'javascript' not in href:
                        detail_url = href if href.startswith('http') else urljoin(self.BASE, href)

                # 賃料
                rent_text = ''
                rent_val = None
                price_td = row.select_one('td.price')
                if not price_td and len(tds) >= 3:
                    price_td = tds[2]
                if price_td:
                    rent_text = price_td.get_text(strip=True)
                    m = re.search(r'([\d.]+)\s*万円', rent_text)
                    if m:
                        rent_val = float(m.group(1))

                # 間取り/専有面積
                area_text = ''
                area_val = None
                layout_td = row.select_one('td.layout')
                if not layout_td and len(tds) >= 4:
                    layout_td = tds[3]
                if layout_td:
                    txt = layout_td.get_text(strip=True)
                    m = re.search(r'([\d.]+)\s*m', txt)
                    if m:
                        area_val = float(m.group(1))
                        area_text = f"{area_val}m²"

                # 画像
                room_img = building_img
                floor_img = row.select_one('img')
                if floor_img:
                    src = floor_img.get('data-original', '') or floor_img.get('src', '')
                    if src and 'loading' not in src:
                        room_img = src

                properties.append({
                    'source': 'HOMES',
                    'type': '賃貸',
                    'name': building_name,
                    'detail_url': detail_url,
                    'price': rent_val,
                    'price_text': rent_text,
                    'area': area_val,
                    'area_text': area_text,
                    'walk_minutes': walk_min,
                    'station': matched_station,
                    'age_years': age_years,
                    'age_text': age_text,
                    'image_url': room_img,
                    'address': address,
                    'access': ' / '.join(access_texts),
                })

        except Exception as e:
            log(f"  HOMESパースエラー: {e}")

        return properties

    def crawl_new(self):
        """HOMES新築マンションをクロール"""
        log("HOMES 新築マンションクロール開始...")
        return self._crawl_buy_type('mansion/shinchiku', '新築')

    def crawl_used(self):
        """HOMES中古マンションをクロール"""
        log("HOMES 中古マンションクロール開始...")
        return self._crawl_buy_type('mansion/chuko', '中古')

    def _crawl_buy_type(self, path, type_label):
        """HOMES売買物件をクロール（エリア別）"""
        all_properties = {}

        for area_slug, area_stations in HOMES_AREAS.items():
            log(f"  エリア: {area_slug}")
            for page in range(1, MAX_PAGES + 1):
                url = f"{self.BASE}/{path}/tokyo/{area_slug}/list/"
                params = {'page': page} if page > 1 else None

                soup = fetch_soup(self.session, url, params)
                if not soup:
                    break

                buildings = soup.select('.prg-building')
                if not buildings:
                    buildings = soup.select('[class*=building]')
                if not buildings:
                    break

                new_count = 0
                for bldg in buildings:
                    p = self._parse_buy_building(bldg, type_label)
                    if not p or not p.get('station'):
                        continue
                    if p.get('area') and p['area'] < MIN_AREA:
                        continue
                    if p.get('age_years') is not None and p['age_years'] > MAX_AGE:
                        continue
                    uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
                    if uid not in all_properties:
                        all_properties[uid] = p
                        new_count += 1

                log(f"    ページ{page}: {len(buildings)}件, 対象駅マッチ{new_count}件")

                next_link = soup.select_one('a[rel="next"]')
                if not next_link:
                    break

                time.sleep(REQUEST_DELAY)

            time.sleep(REQUEST_DELAY)

        result = list(all_properties.values())
        log(f"  HOMES {type_label}: 合計{len(result)}件")
        return result

    def _parse_buy_building(self, bldg, type_label):
        """HOMES売買物件をパース"""
        try:
            name = ''
            name_el = bldg.select_one('.bukkenName, h2 a, h2')
            if name_el:
                name = name_el.get_text(strip=True)

            detail_url = ''
            link = bldg.select_one('h2 a[href], .bukkenName a[href], .prg-bukkenNameAnchor')
            if link:
                href = link.get('href', '')
                detail_url = href if href.startswith('http') else urljoin(self.BASE, href)

            image_url = ''
            img = bldg.select_one('.bukkenPhoto img')
            if img:
                image_url = img.get('data-original', '') or img.get('src', '')
                if image_url and 'loading' in image_url:
                    image_url = ''

            # 交通情報から駅チェック
            access_texts = []
            for span in bldg.select('.prg-stationText'):
                access_texts.append(span.get_text(strip=True))

            if not access_texts:
                for tr in bldg.select('table tr'):
                    th = tr.select_one('th')
                    td = tr.select_one('td')
                    if th and td and '交通' in th.get_text():
                        access_texts.append(td.get_text(strip=True))

            matched_station = None
            walk_min = None
            for acc in access_texts:
                matched = station_matches(acc)
                if matched:
                    matched_station = matched
                    walk_min = extract_walk_minutes(acc)
                    break

            price_text = ''
            price_val = None
            area_text = ''
            area_val = None
            age_text = ''
            age_years = None
            address = ''

            for tr in bldg.select('table tr'):
                th = tr.select_one('th')
                td = tr.select_one('td')
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(strip=True)

                if '価格' in label:
                    price_text = value
                    price_val = parse_buy_price(value)
                elif '面積' in label:
                    area_text = value
                    area_val = parse_number(value)
                elif '築年' in label:
                    age_text = value
                    age_years = parse_age_years(value)
                elif '所在地' in label:
                    address = value

            return {
                'source': 'HOMES',
                'type': type_label,
                'name': name,
                'detail_url': detail_url,
                'price': price_val,
                'price_text': price_text,
                'area': area_val,
                'area_text': area_text,
                'walk_minutes': walk_min,
                'station': matched_station,
                'age_years': age_years,
                'age_text': age_text,
                'image_url': image_url,
                'address': address,
                'access': ' / '.join(access_texts),
            }
        except Exception as e:
            log(f"  HOMESパースエラー: {e}")
            return None


# ============================================================
# cowcamo クローラー
# ============================================================
class CowcamoCrawler:
    BASE = 'https://cowcamo.jp'
    MAX_PAGES = 10  # cowcamoは全物件を新着順で返すため、多めにページを巡回

    def __init__(self, session):
        self.session = session

    def crawl_used(self):
        """cowcamo中古マンションをクロール（/update ページを巡回）"""
        log("cowcamo 中古マンションクロール開始...")
        all_properties = {}

        for page in range(1, self.MAX_PAGES + 1):
            url = f"{self.BASE}/update"
            params = {'page': page} if page > 1 else None
            soup = fetch_soup(self.session, url, params)
            if not soup:
                break

            cards = soup.select('.p-entry')
            if not cards:
                break

            new_count = 0
            for card in cards:
                p = self._parse_entry(card)
                if not p:
                    continue
                # 対象駅に紐づかない物件はスキップ
                if not p.get('station'):
                    continue
                # 条件フィルタリング（面積のみ。cowcamoの一覧に築年数は非表示）
                if p.get('area') and p['area'] < MIN_AREA:
                    continue
                uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
                if uid not in all_properties:
                    all_properties[uid] = p
                    new_count += 1

            log(f"  ページ{page}: {len(cards)}件, 対象駅マッチ{new_count}件")

            # 次のページがあるか
            next_link = soup.select_one('a[href*="page"][rel="next"]')
            if not next_link:
                # "Next" テキストのリンクもチェック
                for a in soup.select('a[href*="update?page="]'):
                    if 'Next' in a.get_text():
                        next_link = a
                        break
            if not next_link:
                break

            time.sleep(REQUEST_DELAY)

        result = list(all_properties.values())
        log(f"  cowcamo中古: 合計{len(result)}件")
        return result

    def _parse_entry(self, card):
        """cowcamo の .p-entry カードをパース"""
        try:
            # タイトル
            name = ''
            title_el = card.select_one('.p-entry__title')
            if title_el:
                name = title_el.get_text(strip=True)

            # 詳細URL（.p-entry__cover の href）
            detail_url = ''
            cover = card.select_one('.p-entry__cover')
            if cover:
                href = cover.get('href', '')
                if href:
                    detail_url = urljoin(self.BASE, href)

            # 画像
            image_url = ''
            img = card.select_one('.p-entry__thumbnail')
            if img:
                image_url = img.get('src', '')

            # 価格（例: "14,990万円", "5,780万円(改装前価格)"）
            price_text = ''
            price_val = None
            price_el = card.select_one('.p-entry__price')
            if price_el:
                price_text = price_el.get_text(strip=True)
                price_val = parse_buy_price(price_text)

            # 面積・間取り（例: "142.83㎡・3LDK"）
            area_text = ''
            area_val = None
            layout_el = card.select_one('.p-entry__layout')
            if layout_el:
                area_text = layout_el.get_text(strip=True)
                m = re.search(r'([\d.]+)\s*[㎡m²]', area_text)
                if m:
                    area_val = float(m.group(1))

            # 駅・住所（.p-entry__misc 内の span 要素）
            walk_station = None
            walk_min = None
            address = ''
            misc = card.select_one('.p-entry__misc')
            if misc:
                spans = misc.select('span')
                if len(spans) >= 1:
                    station_text = spans[0].get_text(strip=True)
                    # 対象駅マッチ
                    walk_station = station_matches(station_text)
                    walk_min = extract_walk_minutes(station_text)
                if len(spans) >= 2:
                    address = spans[1].get_text(strip=True)

            return {
                'source': 'cowcamo',
                'type': '中古',
                'name': name,
                'detail_url': detail_url,
                'price': price_val,
                'price_text': price_text,
                'area': area_val,
                'area_text': area_text,
                'walk_minutes': walk_min,
                'station': walk_station,
                'age_years': None,  # cowcamo一覧では築年数非表示
                'age_text': '',
                'image_url': image_url,
                'address': address,
                'access': '',
            }
        except Exception as e:
            log(f"  cowcamoパースエラー: {e}")
            return None


# ============================================================
# LINE Messaging API 通知
# ============================================================

LINE_CLIENT_ID = os.getenv('LINE_CLIENT_ID', '')
LINE_CLIENT_SECRET = os.getenv('LINE_CLIENT_SECRET', '')
LINE_TOKEN_URL = 'https://api.line.me/v2/oauth/accessToken'
LINE_BROADCAST_URL = 'https://api.line.me/v2/bot/message/broadcast'


def get_line_access_token():
    """LINE Messaging API のアクセストークンを取得"""
    if not LINE_CLIENT_ID or not LINE_CLIENT_SECRET:
        log('LINE_CLIENT_ID / LINE_CLIENT_SECRET が未設定のためLINE通知をスキップ')
        return None

    try:
        resp = requests.post(LINE_TOKEN_URL, data={
            'grant_type': 'client_credentials',
            'client_id': LINE_CLIENT_ID,
            'client_secret': LINE_CLIENT_SECRET,
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


def notify_line_new_listings(changes, all_rental, all_new, all_used):
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

    token = get_line_access_token()
    if not token:
        log("LINE アクセストークン取得失敗 - 通知をスキップ")
        return

    # 日付ヘッダーを先頭に追加
    from datetime import datetime as _dt
    today_str = _dt.now().strftime('%Y年%-m月%-d日')
    date_header = {
        'type': 'text',
        'text': f'📅 {today_str}のデータ 📅'
    }

    # カテゴリごとにメッセージを作成
    all_messages = [date_header]
    for cat in ['new', 'rental', 'used']:
        msgs = _split_and_send(token, cat, by_type[cat], all_props_map[cat])
        all_messages.extend(msgs)

    # 5メッセージずつ送信（LINE API制限）
    for i in range(0, len(all_messages), 5):
        batch = all_messages[i:i+5]
        send_line_broadcast(token, batch)
        if i + 5 < len(all_messages):
            time.sleep(1)

    log("LINE 新着物件通知完了")


# ============================================================
# 駅別集計 & 物件追跡 & 値下げ検出
# ============================================================

HISTORY_PATH = os.path.join(DATA_DIR, 'history.json')
TIMESERIES_PATH = os.path.join(DATA_DIR, 'time_series.json')
STALE_DAYS = 14  # この日数以上掲載で「滞留物件」


def load_history():
    """物件追跡履歴を読み込み"""
    if os.path.exists(HISTORY_PATH):
        try:
            with open(HISTORY_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, KeyError):
            pass
    return {'properties': {}}


def save_history(history):
    """物件追跡履歴を保存"""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(HISTORY_PATH, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    log(f"物件履歴更新: {len(history['properties'])}件追跡中")


def load_time_series():
    """時系列データを読み込み"""
    if os.path.exists(TIMESERIES_PATH):
        try:
            with open(TIMESERIES_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, KeyError):
            pass
    return {'dates': [], 'stations': {}, 'overall': {}}


def save_time_series(ts):
    """時系列データを保存"""
    with open(TIMESERIES_PATH, 'w', encoding='utf-8') as f:
        json.dump(ts, f, ensure_ascii=False, indent=2)
    log(f"時系列データ更新: {len(ts['dates'])}日分")


def safe_median(values):
    """空リストでも安全なmedian"""
    return round(statistics.median(values), 4) if values else None


def safe_mean(values):
    """空リストでも安全なmean"""
    return round(statistics.mean(values), 4) if values else None


def percentile(values, pct):
    """パーセンタイル計算"""
    if not values:
        return None
    sorted_v = sorted(values)
    idx = int(len(sorted_v) * pct / 100)
    idx = min(idx, len(sorted_v) - 1)
    return round(sorted_v[idx], 4)


def compute_station_stats(properties, prop_type):
    """物件リストから駅別統計を計算"""
    by_station = {}
    for p in properties:
        st = p.get('station')
        if not st:
            continue
        by_station.setdefault(st, []).append(p)

    stats = {}
    for st, props in by_station.items():
        prices = [p['price'] for p in props if p.get('price')]
        areas = [p['area'] for p in props if p.get('area')]
        ages = [p['age_years'] for p in props if p.get('age_years') is not None]
        walks = [p['walk_minutes'] for p in props if p.get('walk_minutes') is not None]

        # m²単価
        price_per_sqm = []
        for p in props:
            if p.get('price') and p.get('area') and p['area'] > 0:
                price_per_sqm.append(round(p['price'] / p['area'], 4))

        stats[st] = {
            'count': len(props),
            'avg_price': safe_mean(prices),
            'median_price': safe_median(prices),
            'min_price': round(min(prices), 2) if prices else None,
            'max_price': round(max(prices), 2) if prices else None,
            'avg_price_per_sqm': safe_mean(price_per_sqm),
            'median_price_per_sqm': safe_median(price_per_sqm),
            'p25_price_per_sqm': percentile(price_per_sqm, 25),
            'p75_price_per_sqm': percentile(price_per_sqm, 75),
            'avg_area': safe_mean(areas),
            'avg_age': safe_mean(ages),
            'avg_walk': safe_mean(walks),
        }

    return stats


def update_history_and_detect_changes(history, all_rental, all_new, all_used, today_str):
    """物件履歴を更新し、値下げ・新規・消失を検出"""
    today_uids = set()
    changes = {
        'price_reduced': [],   # 値下げされた物件
        'new_listings': [],    # 新規掲載
        'delisted': [],        # 掲載終了
        'stale': [],           # 滞留物件
    }

    # ── 現在の全物件を走査 ──
    for prop_type, props in [('rental', all_rental), ('new', all_new), ('used', all_used)]:
        for p in props:
            uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
            today_uids.add(uid)

            if uid in history['properties']:
                # 既知の物件 → 価格変動チェック
                rec = history['properties'][uid]
                rec['last_seen'] = today_str
                old_price = rec['price_history'][-1]['price'] if rec['price_history'] else None

                if p.get('price') and old_price and p['price'] < old_price:
                    change_pct = round((p['price'] - old_price) / old_price * 100, 1)
                    changes['price_reduced'].append({
                        'uid': uid,
                        'name': p.get('name', ''),
                        'station': p.get('station', ''),
                        'type': prop_type,
                        'old_price': old_price,
                        'new_price': p['price'],
                        'change_pct': change_pct,
                        'detail_url': p.get('detail_url', ''),
                        'area': p.get('area'),
                        'first_seen': rec.get('first_seen', ''),
                    })

                # 価格が変動した場合のみ履歴追加
                if p.get('price') and (not old_price or p['price'] != old_price):
                    rec['price_history'].append({
                        'date': today_str,
                        'price': p['price'],
                    })

                # 滞留チェック
                first_dt = rec.get('first_seen', today_str)
                try:
                    days = (datetime.fromisoformat(today_str) - datetime.fromisoformat(first_dt)).days
                except Exception:
                    days = 0
                if days >= STALE_DAYS:
                    changes['stale'].append({
                        'uid': uid,
                        'name': p.get('name', ''),
                        'station': p.get('station', ''),
                        'type': prop_type,
                        'days_listed': days,
                        'price': p.get('price'),
                        'detail_url': p.get('detail_url', ''),
                    })
            else:
                # 新規物件
                history['properties'][uid] = {
                    'name': p.get('name', ''),
                    'type': prop_type,
                    'station': p.get('station', ''),
                    'first_seen': today_str,
                    'last_seen': today_str,
                    'price_history': [{'date': today_str, 'price': p.get('price')}] if p.get('price') else [],
                    'detail_url': p.get('detail_url', ''),
                    'area': p.get('area'),
                    'age_years': p.get('age_years'),
                }
                changes['new_listings'].append({
                    'uid': uid,
                    'name': p.get('name', ''),
                    'station': p.get('station', ''),
                    'type': prop_type,
                    'price': p.get('price'),
                    'detail_url': p.get('detail_url', ''),
                })

    # ── 消失物件の検出 ──
    yesterday = (datetime.fromisoformat(today_str) - timedelta(days=1)).isoformat()[:10]
    for uid, rec in history['properties'].items():
        if uid not in today_uids:
            last_seen = rec.get('last_seen', '')[:10]
            # 前回以降に見えなくなった物件のみ（古すぎるものは除外）
            if last_seen >= yesterday:
                changes['delisted'].append({
                    'uid': uid,
                    'name': rec.get('name', ''),
                    'station': rec.get('station', ''),
                    'type': rec.get('type', ''),
                    'last_price': rec['price_history'][-1]['price'] if rec.get('price_history') else None,
                    'detail_url': rec.get('detail_url', ''),
                    'first_seen': rec.get('first_seen', ''),
                    'last_seen': rec.get('last_seen', ''),
                })

    # 変動サマリを駅別に集計
    station_changes = {}
    for st in STATIONS:
        station_changes[st] = {
            'new_count': len([c for c in changes['new_listings'] if c.get('station') == st]),
            'delisted_count': len([c for c in changes['delisted'] if c.get('station') == st]),
            'price_reduced_count': len([c for c in changes['price_reduced'] if c.get('station') == st]),
            'stale_count': len([c for c in changes['stale'] if c.get('station') == st]),
        }

    changes['station_summary'] = station_changes
    changes['summary'] = {
        'total_new': len(changes['new_listings']),
        'total_delisted': len(changes['delisted']),
        'total_price_reduced': len(changes['price_reduced']),
        'total_stale': len(changes['stale']),
    }

    return changes


def update_time_series(ts, today_str, station_stats, changes):
    """時系列データに本日の集計を追加"""
    date_key = today_str[:10]

    # 同日のデータがあれば上書き
    if date_key in ts['dates']:
        idx = ts['dates'].index(date_key)
    else:
        ts['dates'].append(date_key)
        idx = len(ts['dates']) - 1

    for st_name in STATIONS:
        if st_name not in ts['stations']:
            ts['stations'][st_name] = {}

        for prop_type in ['rental', 'new', 'used']:
            if prop_type not in ts['stations'][st_name]:
                ts['stations'][st_name][prop_type] = {
                    'count': [], 'median_price': [], 'median_price_per_sqm': [],
                    'p25_price_per_sqm': [], 'avg_price': [], 'min_price': [],
                    'avg_area': [], 'avg_age': [], 'avg_walk': [],
                    'new_listings': [], 'delisted': [],
                    'price_reduced': [], 'stale': [],
                }

            ts_cat = ts['stations'][st_name][prop_type]
            stat = station_stats.get(prop_type, {}).get(st_name, {})
            ch = changes.get('station_summary', {}).get(st_name, {})

            # 各系列をidx位置にセット（足りない分はNoneで埋める）
            for key, val in [
                ('count', stat.get('count', 0)),
                ('median_price', stat.get('median_price')),
                ('median_price_per_sqm', stat.get('median_price_per_sqm')),
                ('p25_price_per_sqm', stat.get('p25_price_per_sqm')),
                ('avg_price', stat.get('avg_price')),
                ('min_price', stat.get('min_price')),
                ('avg_area', stat.get('avg_area')),
                ('avg_age', stat.get('avg_age')),
                ('avg_walk', stat.get('avg_walk')),
                ('new_listings', ch.get('new_count', 0)),
                ('delisted', ch.get('delisted_count', 0)),
                ('price_reduced', ch.get('price_reduced_count', 0)),
                ('stale', ch.get('stale_count', 0)),
            ]:
                while len(ts_cat[key]) < idx:
                    ts_cat[key].append(None)
                if len(ts_cat[key]) <= idx:
                    ts_cat[key].append(val)
                else:
                    ts_cat[key][idx] = val

    # 全体集計
    if 'overall' not in ts:
        ts['overall'] = {}
    for prop_type in ['rental', 'new', 'used']:
        if prop_type not in ts['overall']:
            ts['overall'][prop_type] = {
                'count': [], 'median_price': [], 'median_price_per_sqm': [],
            }
        all_stats = station_stats.get(prop_type, {})
        total_count = sum(s.get('count', 0) for s in all_stats.values())

        # 全駅合算の中央値（各駅の中央値の平均ではなく、物件個別の値を使うべきだが簡易版）
        all_prices = []
        all_ppsqm = []
        for s in all_stats.values():
            if s.get('median_price') is not None:
                all_prices.append(s['median_price'])
            if s.get('median_price_per_sqm') is not None:
                all_ppsqm.append(s['median_price_per_sqm'])

        ts_ov = ts['overall'][prop_type]
        for key, val in [
            ('count', total_count),
            ('median_price', safe_median(all_prices)),
            ('median_price_per_sqm', safe_median(all_ppsqm)),
        ]:
            while len(ts_ov[key]) < idx:
                ts_ov[key].append(None)
            if len(ts_ov[key]) <= idx:
                ts_ov[key].append(val)
            else:
                ts_ov[key][idx] = val

    return ts


# ============================================================
# データ保存
# ============================================================
def save_snapshot(data):
    """クロール結果をJSONスナップショットとして保存"""
    os.makedirs(DATA_DIR, exist_ok=True)

    now = datetime.now()
    filename = now.strftime('%Y-%m-%d_%H%M%S') + '.json'
    filepath = os.path.join(DATA_DIR, filename)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"スナップショット保存: {filename}")

    # latest.json を更新
    latest_path = os.path.join(DATA_DIR, 'latest.json')
    with open(latest_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # index.json を更新
    update_index(filename, data)

    return filename


def update_index(new_file, data):
    """index.jsonにスナップショット一覧を記録"""
    index_path = os.path.join(DATA_DIR, 'index.json')

    if os.path.exists(index_path):
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                index = json.load(f)
        except (json.JSONDecodeError, KeyError):
            index = {'snapshots': []}
    else:
        index = {'snapshots': []}

    summary = data.get('summary', {})
    entry = {
        'file': new_file,
        'date': data.get('crawled_at', ''),
        'rental_count': summary.get('rental_count', 0),
        'new_count': summary.get('new_count', 0),
        'used_count': summary.get('used_count', 0),
    }
    index['snapshots'].insert(0, entry)

    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)


# ============================================================
# メイン
# ============================================================
def main():
    log("=" * 50)
    log("不動産物件クローラー 起動")
    log(f"対象駅: {', '.join(STATIONS.keys())}")
    log(f"条件: 面積{MIN_AREA}㎡以上, 築{MAX_AGE}年以内, 賃貸{MAX_RENT}万円以下")
    log("=" * 50)

    session = create_session()

    # SUUMO
    suumo = SuumoCrawler(session)
    suumo_rental = suumo.crawl_rental()
    suumo_new = suumo.crawl_new()
    suumo_used = suumo.crawl_used()

    # HOMES
    homes = HomesCrawler(session)
    homes_rental = homes.crawl_rental()
    homes_new = homes.crawl_new()
    homes_used = homes.crawl_used()

    # cowcamo
    cowcamo = CowcamoCrawler(session)
    cowcamo_used = cowcamo.crawl_used()

    # 結果集計
    all_rental = suumo_rental + homes_rental
    all_new = suumo_new + homes_new
    all_used = suumo_used + homes_used + cowcamo_used

    # ── 駅別集計 ──
    log("")
    log("駅別統計を計算中...")
    station_stats = {
        'rental': compute_station_stats(all_rental, 'rental'),
        'new': compute_station_stats(all_new, 'new'),
        'used': compute_station_stats(all_used, 'used'),
    }

    # ── 物件追跡 & 値下げ検出 ──
    log("物件追跡・値下げ検出中...")
    today_str = datetime.now().isoformat()[:10]
    history = load_history()
    changes = update_history_and_detect_changes(history, all_rental, all_new, all_used, today_str)
    save_history(history)

    # 駅別統計に変動情報をマージ
    for st in STATIONS:
        ch = changes.get('station_summary', {}).get(st, {})
        for prop_type in ['rental', 'new', 'used']:
            if st in station_stats[prop_type]:
                station_stats[prop_type][st].update({
                    'new_listings': ch.get('new_count', 0),
                    'delisted': ch.get('delisted_count', 0),
                    'price_reduced': ch.get('price_reduced_count', 0),
                    'stale_count': ch.get('stale_count', 0),
                })

    # ── 時系列データ更新 ──
    log("時系列データ更新中...")
    ts = load_time_series()
    ts = update_time_series(ts, today_str, station_stats, changes)
    save_time_series(ts)

    data = {
        'crawled_at': datetime.now().isoformat(),
        'conditions': {
            'stations': list(STATIONS.keys()),
            'min_area_sqm': MIN_AREA,
            'max_age_years': MAX_AGE,
            'max_rent_man': MAX_RENT,
        },
        'summary': {
            'rental_count': len(all_rental),
            'new_count': len(all_new),
            'used_count': len(all_used),
            'suumo_rental': len(suumo_rental),
            'suumo_new': len(suumo_new),
            'suumo_used': len(suumo_used),
            'homes_rental': len(homes_rental),
            'homes_new': len(homes_new),
            'homes_used': len(homes_used),
            'cowcamo_used': len(cowcamo_used),
        },
        'station_stats': station_stats,
        'changes': changes,
        'rental': all_rental,
        'new': all_new,
        'used': all_used,
    }

    filename = save_snapshot(data)

    log("")
    log("=" * 50)
    log("クロール完了！")
    log(f"  賃貸: {len(all_rental)}件 (SUUMO: {len(suumo_rental)}, HOMES: {len(homes_rental)})")
    log(f"  新築: {len(all_new)}件 (SUUMO: {len(suumo_new)}, HOMES: {len(homes_new)})")
    log(f"  中古: {len(all_used)}件 (SUUMO: {len(suumo_used)}, HOMES: {len(homes_used)}, cowcamo: {len(cowcamo_used)})")
    log(f"  保存先: realestate_data/{filename}")
    ch_sum = changes.get('summary', {})
    log(f"  変動: 新規{ch_sum.get('total_new',0)} / 消失{ch_sum.get('total_delisted',0)} / 値下げ{ch_sum.get('total_price_reduced',0)} / 滞留{ch_sum.get('total_stale',0)}")
    log("=" * 50)

    # ── LINE通知: 賃貸・新築・中古の新着 ──
    notify_line_new_listings(changes, all_rental, all_new, all_used)


if __name__ == '__main__':
    main()
