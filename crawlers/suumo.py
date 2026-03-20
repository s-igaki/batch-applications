"""SUUMO クローラー"""

import re
import time
from urllib.parse import urljoin

from .base import (
    log, fetch_soup, parse_number, parse_buy_price,
    parse_age_years, extract_walk_minutes, extract_walk_text,
    parse_management_fee, make_unique_id,
)


class SuumoCrawler:
    BASE = 'https://suumo.jp'

    def __init__(self, session, profile):
        self.session = session
        self.profile = profile

    # --- 賃貸 ---
    def crawl_rental(self):
        """SUUMO賃貸物件をクロール"""
        log("SUUMO 賃貸クロール開始...")
        all_properties = {}

        for station_name, station_code in self.profile.stations.items():
            region = self.profile.get_station_region(station_name)
            log(f"  駅: {station_name} (ek_{station_code}, {region})")
            url = f"{self.BASE}/chintai/{region}/ek_{station_code}/"

            for page in range(1, self.profile.max_pages + 1):
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
                        if not p.get('station'):
                            continue
                        if not self.profile.should_include(p, 'rental'):
                            continue
                        uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
                        if uid not in all_properties:
                            all_properties[uid] = p
                            new_count += 1

                log(f"    ページ{page}: {len(items)}棟, 新規{new_count}件")

                next_link = soup.select_one('.pagination-parts a[rel="next"], .paginate_set-nav a:last-child')
                if not next_link:
                    break

                time.sleep(self.profile.request_delay)

            time.sleep(self.profile.request_delay)

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
            matched = self.profile.station_matches(acc)
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
            rent_spans = row.select('span.cassetteitem_other--emphasis')
            if rent_spans:
                rent_text = rent_spans[0].get_text(strip=True)
            if not rent_text and len(tds) >= 4:
                rent_text = tds[3].get_text(strip=True)

            if rent_text:
                m = re.search(r'([\d.]+)\s*万円', rent_text)
                if m:
                    rent_val = float(m.group(1))

            # 管理費・共益費
            mgmt_fee_text = ''
            mgmt_fee_val = None
            if len(tds) >= 5:
                mgmt_fee_text = tds[3].get_text(strip=True)
                # rent_spansで賃料を取得済みの場合、tds[3]は管理費の可能性が高い
                if rent_spans:
                    mgmt_fee_val = parse_management_fee(mgmt_fee_text)
                else:
                    # 賃料がtds[3]から取得された場合、tds[4]が管理費
                    mgmt_fee_text = tds[4].get_text(strip=True) if len(tds) >= 5 else ''
                    mgmt_fee_val = parse_management_fee(mgmt_fee_text)

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
                'management_fee': mgmt_fee_val,
                'management_fee_text': mgmt_fee_text,
                'area': area_val,
                'area_text': area_text,
                'walk_minutes': walk_min_default,
                'walk_text': extract_walk_text(' / '.join(access_texts), walk_station_default),
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
        return self._crawl_buy_type('chuko', '中古', url_prefix='/ms/')

    # --- 中古一戸建て ---
    def crawl_used_ikkodate(self):
        """SUUMO中古一戸建てをクロール"""
        log("SUUMO 中古一戸建てクロール開始...")
        return self._crawl_buy_type('chukoikkodate', '中古一戸建て', url_prefix='/')

    def _crawl_buy_type(self, path_segment, type_label, url_prefix='/ms/'):
        """SUUMO売買物件（新築/中古/一戸建て）をクロール"""
        all_properties = {}

        prop_type = 'new' if type_label == '新築' else 'used'
        for station_name, station_code in self.profile.stations.items():
            region = self.profile.get_station_region(station_name)
            log(f"  駅: {station_name} ({region})")
            url = f"{self.BASE}{url_prefix}{path_segment}/{region}/ek_{station_code}/"

            for page in range(1, self.profile.max_pages + 1):
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
                    if not p.get('station'):
                        continue
                    if not self.profile.should_include(p, prop_type):
                        continue
                    uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
                    if uid not in all_properties:
                        all_properties[uid] = p
                        new_count += 1

                log(f"    ページ{page}: {len(units)}件, 新規{new_count}件")

                next_link = soup.select_one('.pagination-parts a[rel="next"], .paginate_set-nav a:last-child')
                if not next_link:
                    break

                time.sleep(self.profile.request_delay)

            time.sleep(self.profile.request_delay)

        result = list(all_properties.values())
        log(f"  SUUMO {type_label}: 合計{len(result)}件")
        return result

    def _parse_property_unit(self, unit, search_station, type_label):
        """SUUMO売買のproperty_unitをパース（中古dt/dd形式 & 新築cassette形式 両対応）"""
        is_cassette = bool(unit.select_one('.cassette_header-title, .cassette_basic'))
        if is_cassette:
            return self._parse_cassette_unit(unit, search_station, type_label)
        return self._parse_dottable_unit(unit, search_station, type_label)

    def _parse_cassette_unit(self, unit, search_station, type_label):
        """SUUMO新築マンションのcassette形式をパース"""
        try:
            name = ''
            title_el = unit.select_one('.cassette_header-title, a.cassette_header-title')
            if title_el:
                name = title_el.get_text(strip=True)

            detail_url = ''
            title_link = unit.select_one('a.cassette_header-title[href], .cassette_header-title a[href]')
            if title_link:
                detail_url = urljoin(self.BASE, title_link.get('href', ''))
            if not detail_url:
                link = unit.select_one('a[href*="/nc_"]')
                if link:
                    detail_url = urljoin(self.BASE, link.get('href', ''))

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

            address = ''
            access = ''
            walk_min = None
            walk_station = None
            for title_p in unit.select('.cassette_basic-title'):
                label = title_p.get_text(strip=True)
                value_p = title_p.find_next_sibling('p', class_='cassette_basic-value')
                if not value_p:
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
                    matched = self.profile.station_matches(access)
                    if matched:
                        walk_station = matched
                    walk_min = extract_walk_minutes(access)

            price_text = ''
            price_val = None
            price_el = unit.select_one('.cassette_price-accent')
            if price_el:
                price_text = price_el.get_text(strip=True)
                price_val = parse_buy_price(price_text)

            area_text = ''
            area_val = None
            desc_el = unit.select_one('.cassette_price-description')
            if desc_el:
                desc_text = desc_el.get_text(strip=True)
                area_m = re.search(r'([\d.]+)\s*m[²2]\s*[～~〜]\s*([\d.]+)\s*m[²2]', desc_text)
                if area_m:
                    area_val = float(area_m.group(1))
                    area_text = f"{area_m.group(1)}m²～{area_m.group(2)}m²"
                else:
                    area_m2 = re.search(r'([\d.]+)\s*m[²2]', desc_text)
                    if area_m2:
                        area_val = float(area_m2.group(1))
                        area_text = f"{area_m2.group(1)}m²"

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
                'walk_text': extract_walk_text(access, walk_station),
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
            name = ''
            name_dt = unit.find('dt', string=re.compile('物件名'))
            if name_dt:
                dd = name_dt.find_next_sibling('dd')
                if dd:
                    name = dd.get_text(strip=True)

            detail_url = ''
            title_link = unit.select_one('.property_unit-title a[href]')
            if title_link:
                detail_url = urljoin(self.BASE, title_link.get('href', ''))
            if not detail_url:
                link = unit.select_one('a[href*="/nc_"]')
                if not link:
                    link = unit.select_one('a[href*="/chukoikkodate/"]')
                if link:
                    detail_url = urljoin(self.BASE, link.get('href', ''))

            image_url = ''
            img = unit.select_one('.property_unit-object img.js-noContextMenu, .property_unit-object img')
            if img:
                image_url = img.get('rel', '') or img.get('src', '')
                if image_url and 'data:image' in image_url:
                    image_url = img.get('rel', '')

            price_text = ''
            price_val = None
            price_dt = unit.find('dt', string=re.compile('販売価格|価格'))
            if price_dt:
                dd = price_dt.find_next_sibling('dd')
                if dd:
                    v = dd.select_one('.dottable-value')
                    price_text = (v or dd).get_text(strip=True)
                    price_val = parse_buy_price(price_text)

            address = ''
            addr_dt = unit.find('dt', string=re.compile('所在地'))
            if addr_dt:
                dd = addr_dt.find_next_sibling('dd')
                if dd:
                    address = dd.get_text(strip=True)

            access = ''
            walk_min = None
            walk_station = None
            ensen_dt = unit.find('dt', string=re.compile('沿線'))
            if ensen_dt:
                dd = ensen_dt.find_next_sibling('dd')
                if dd:
                    access = dd.get_text(strip=True)
                    matched = self.profile.station_matches(access)
                    if matched:
                        walk_station = matched
                    walk_min = extract_walk_minutes(access)

            area_text = ''
            area_val = None
            area_dt = unit.find('dt', string=re.compile('専有面積|建物面積'))
            if area_dt:
                dd = area_dt.find_next_sibling('dd')
                if dd:
                    area_text = dd.get_text(strip=True)
                    area_val = parse_number(area_text)

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
                'walk_text': extract_walk_text(access, walk_station),
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
