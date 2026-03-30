"""LIFULL HOME'S クローラー"""

import re
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from .base import (
    log, fetch_soup, parse_number, parse_buy_price,
    parse_age_years, extract_walk_minutes, extract_walk_text,
    parse_management_fee, make_unique_id,
)


class HomesCrawler:
    BASE = 'https://www.homes.co.jp'

    def __init__(self, session, profile):
        self.session = session
        self.profile = profile

    def crawl_rental(self):
        """HOMES賃貸物件をクロール（エリア別）"""
        log("HOMES 賃貸クロール開始...")
        all_properties = {}

        for area_slug, area_stations in self.profile.homes_areas.items():
            log(f"  エリア: {area_slug} ({', '.join(area_stations)})")
            # area_slug が "region/area" 形式ならそのまま使用、そうでなければ tokyo/ を付与
            area_path = area_slug if '/' in area_slug else f"tokyo/{area_slug}"
            for page in range(1, self.profile.max_pages + 1):
                url = f"{self.BASE}/chintai/{area_path}/list/"
                params = {'page': page} if page > 1 else None

                soup = fetch_soup(self.session, url, params)
                if not soup:
                    break

                buildings = soup.select('.prg-building')
                if not buildings:
                    break

                new_count = 0
                for bldg in buildings:
                    props = self._parse_rental_building(bldg, area_slug)
                    for p in props:
                        if p.get('station') is None:
                            continue
                        if not self.profile.should_include(p, 'rental'):
                            continue
                        uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
                        if uid not in all_properties:
                            all_properties[uid] = p
                            new_count += 1

                log(f"    ページ{page}: {len(buildings)}棟, 対象駅マッチ{new_count}件")

                next_link = soup.select_one('a[rel="next"], .prg-paging a:last-child')
                if not next_link:
                    break

                time.sleep(self.profile.request_delay)

            time.sleep(self.profile.request_delay)

        result = list(all_properties.values())
        log(f"  HOMES賃貸: 合計{len(result)}件")
        return result

    def _parse_rental_building(self, bldg, area_slug=None):
        """HOMES賃貸のprg-buildingをパース"""
        properties = []
        try:
            building_name = ''
            name_el = bldg.select_one('.bukkenName')
            if name_el:
                building_name = name_el.get_text(strip=True)

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

            matched_station = None
            walk_min = None
            for acc in access_texts:
                matched = self.profile.station_matches(acc)
                if matched:
                    matched_station = matched
                    walk_min = extract_walk_minutes(acc)
                    break

            # 駅マッチしない場合、町名で住所マッチを試みる
            if not matched_station:
                matched_town = self.profile.address_matches_town(address, area_slug)
                if matched_town:
                    matched_station = matched_town
                    if access_texts:
                        walk_min = extract_walk_minutes(access_texts[0])
                else:
                    return []

            building_url = ''
            link = bldg.select_one('.prg-bukkenNameAnchor')
            if link:
                building_url = link.get('href', '')

            unit_rows = bldg.select('.unitSummary tbody tr')
            if not unit_rows:
                properties.append({
                    'source': 'HOMES',
                    'type': '賃貸',
                    'name': building_name,
                    'detail_url': building_url,
                    'price': None,
                    'price_text': '',
                    'management_fee': None,
                    'management_fee_text': '',
                    'area': None,
                    'area_text': '',
                    'walk_minutes': walk_min,
                    'walk_text': extract_walk_text(' / '.join(access_texts), matched_station),
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

                detail_url = building_url
                link = row.select_one('a[href*="/chintai/room/"]')
                if not link:
                    link = row.select_one('a[href]')
                if link:
                    href = link.get('href', '')
                    if href and href != '#' and 'javascript' not in href:
                        detail_url = href if href.startswith('http') else urljoin(self.BASE, href)

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

                # 管理費・共益費
                mgmt_fee_text = ''
                mgmt_fee_val = None
                mgmt_td = row.select_one('td.adminFee')
                if not mgmt_td:
                    # 賃料の次のtdを管理費として試行
                    if price_td:
                        mgmt_td = price_td.find_next_sibling('td')
                if mgmt_td:
                    mgmt_fee_text = mgmt_td.get_text(strip=True)
                    mgmt_fee_val = parse_management_fee(mgmt_fee_text)

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
                    'management_fee': mgmt_fee_val,
                    'management_fee_text': mgmt_fee_text,
                    'area': area_val,
                    'area_text': area_text,
                    'walk_minutes': walk_min,
                    'walk_text': extract_walk_text(' / '.join(access_texts), matched_station),
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

        prop_type = 'new' if type_label == '新築' else 'used_mansion'
        for area_slug, area_stations in self.profile.homes_areas.items():
            area_path = area_slug if '/' in area_slug else f"tokyo/{area_slug}"
            log(f"  エリア: {area_slug}")
            for page in range(1, self.profile.max_pages + 1):
                url = f"{self.BASE}/{path}/{area_path}/list/"
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
                    p = self._parse_buy_building(bldg, type_label, area_slug)
                    if not p or not p.get('station'):
                        continue
                    if not self.profile.should_include(p, prop_type):
                        continue
                    uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
                    if uid not in all_properties:
                        all_properties[uid] = p
                        new_count += 1

                log(f"    ページ{page}: {len(buildings)}件, 対象駅マッチ{new_count}件")

                next_link = soup.select_one('a[rel="next"]')
                if not next_link:
                    break

                time.sleep(self.profile.request_delay)

            time.sleep(self.profile.request_delay)

        result = list(all_properties.values())
        log(f"  HOMES {type_label}: 合計{len(result)}件")
        return result

    def _parse_buy_building(self, bldg, type_label, area_slug=None):
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
                matched = self.profile.station_matches(acc)
                if matched:
                    matched_station = matched
                    walk_min = extract_walk_minutes(acc)
                    break

            # 駅マッチしない場合、町名で住所マッチを試みる
            if not matched_station and area_slug:
                matched_town = self.profile.address_matches_town(address, area_slug)
                if matched_town:
                    matched_station = matched_town
                    if access_texts:
                        walk_min = extract_walk_minutes(access_texts[0])

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
                'walk_text': extract_walk_text(' / '.join(access_texts), matched_station),
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
