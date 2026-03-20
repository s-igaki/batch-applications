"""cowcamo クローラー"""

import re
import time
from urllib.parse import urljoin

from .base import (
    log, fetch_soup, parse_buy_price, extract_walk_minutes, extract_walk_text,
    make_unique_id,
)


class CowcamoCrawler:
    BASE = 'https://cowcamo.jp'
    COWCAMO_MAX_PAGES = 10

    def __init__(self, session, profile):
        self.session = session
        self.profile = profile

    def crawl_used(self):
        """cowcamo中古マンションをクロール（/update ページを巡回）"""
        log("cowcamo 中古マンションクロール開始...")
        all_properties = {}

        for page in range(1, self.COWCAMO_MAX_PAGES + 1):
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
                if not p.get('station'):
                    continue
                if not self.profile.should_include(p, 'used'):
                    continue
                uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
                if uid not in all_properties:
                    all_properties[uid] = p
                    new_count += 1

            log(f"  ページ{page}: {len(cards)}件, 対象駅マッチ{new_count}件")

            next_link = soup.select_one('a[href*="page"][rel="next"]')
            if not next_link:
                for a in soup.select('a[href*="update?page="]'):
                    if 'Next' in a.get_text():
                        next_link = a
                        break
            if not next_link:
                break

            time.sleep(self.profile.request_delay)

        result = list(all_properties.values())
        log(f"  cowcamo中古: 合計{len(result)}件")
        return result

    def _parse_entry(self, card):
        """cowcamo の .p-entry カードをパース"""
        try:
            name = ''
            title_el = card.select_one('.p-entry__title')
            if title_el:
                name = title_el.get_text(strip=True)

            detail_url = ''
            cover = card.select_one('.p-entry__cover')
            if cover:
                href = cover.get('href', '')
                if href:
                    detail_url = urljoin(self.BASE, href)

            image_url = ''
            img = card.select_one('.p-entry__thumbnail')
            if img:
                image_url = img.get('src', '')

            price_text = ''
            price_val = None
            price_el = card.select_one('.p-entry__price')
            if price_el:
                price_text = price_el.get_text(strip=True)
                price_val = parse_buy_price(price_text)

            area_text = ''
            area_val = None
            layout_el = card.select_one('.p-entry__layout')
            if layout_el:
                area_text = layout_el.get_text(strip=True)
                m = re.search(r'([\d.]+)\s*[㎡m²]', area_text)
                if m:
                    area_val = float(m.group(1))

            walk_station = None
            walk_min = None
            address = ''
            misc = card.select_one('.p-entry__misc')
            if misc:
                spans = misc.select('span')
                if len(spans) >= 1:
                    station_text = spans[0].get_text(strip=True)
                    walk_station = self.profile.station_matches(station_text)
                    walk_min = extract_walk_minutes(station_text)
                if len(spans) >= 2:
                    address = spans[1].get_text(strip=True)

            access_raw = ''
            if misc:
                spans = misc.select('span')
                if spans:
                    access_raw = spans[0].get_text(strip=True)

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
                'walk_text': extract_walk_text(access_raw, walk_station),
                'station': walk_station,
                'age_years': None,
                'age_text': '',
                'image_url': image_url,
                'address': address,
                'access': access_raw,
            }
        except Exception as e:
            log(f"  cowcamoパースエラー: {e}")
            return None
