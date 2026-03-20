#!/usr/bin/env python3
"""
不動産物件クローラー - SUUMO & LIFULL HOME'S & cowcamo
賃貸・新築・中古マンションの物件情報を取得し、JSONスナップショットとして保存する。

設定は crawler_config.json に定義されたプロファイルごとに実行される。
各プロファイルは独自の駅・条件・LINE通知先を持つ。
"""

import json
import os
import sys
import traceback
from datetime import datetime

from crawlers.base import log, create_session
from crawlers import SuumoCrawler, HomesCrawler, CowcamoCrawler
from analyzer import (
    compute_station_stats, load_history, save_history,
    update_history_and_detect_changes, compute_changes_summary,
    load_time_series, update_time_series, save_time_series,
    save_snapshot,
)
from notifier import notify_line_new_listings


# ============================================================
# 設定読み込み
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, 'crawler_config.json')

# SUUMO表記ゆれ対応（デフォルト）
DEFAULT_STATION_ALIASES = {
    '千駄ケ谷': '千駄ヶ谷',
    '市ケ谷':   '市谷',
    '市ヶ谷':   '市谷',
    '明治神宮前〈原宿〉': '明治神宮前',
    '明治神宮前（原宿）': '明治神宮前',
}


def load_config():
    """crawler_config.json を読み込む"""
    if not os.path.exists(CONFIG_PATH):
        log(f"設定ファイルが見つかりません: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


class ProfileConfig:
    """1プロファイル分の設定を保持するクラス"""

    def __init__(self, profile_dict, global_dict, station_aliases):
        self.name = profile_dict['name']
        self.station_aliases = station_aliases

        # 駅情報のパース（文字列コード or {code, region} オブジェクト両対応）
        raw_stations = profile_dict.get('stations', {})
        self.stations = {}       # {駅名: コード}
        self.station_regions = {} # {駅名: 地域}
        for name, val in raw_stations.items():
            if isinstance(val, dict):
                self.stations[name] = val['code']
                self.station_regions[name] = val.get('region', 'tokyo')
            else:
                self.stations[name] = val
                self.station_regions[name] = 'tokyo'

        self.homes_areas = profile_dict.get('homes_areas', {})

        # 条件のパース（フラット形式 or 種類別形式 両対応）
        cond = profile_dict.get('conditions', {})
        if any(k in cond for k in ('rental', 'new', 'used')):
            self._init_per_type_conditions(cond)
        else:
            self._init_flat_conditions(cond)

        line = profile_dict.get('line', {})
        self.line_client_id = os.getenv(line.get('client_id_env', ''), '')
        self.line_client_secret = os.getenv(line.get('client_secret_env', ''), '')

        self.request_delay = global_dict.get('request_delay', 2)
        self.max_pages = global_dict.get('max_pages', 5)
        self.headers = global_dict.get('headers', {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        self.stale_days = global_dict.get('stale_days', 14)

        # データ保存先（プロファイルごとに分離）
        self.data_dir = os.path.join(SCRIPT_DIR, 'realestate_data', self.name)

    def _init_flat_conditions(self, cond):
        """従来のフラット形式の条件を初期化"""
        self.min_area = cond.get('min_area', 40)
        self.max_age = cond.get('max_age', 20)
        self.max_rent = cond.get('max_rent', 24.0)
        self.type_conditions = {
            'rental': {
                'enabled': True,
                'min_area': self.min_area,
                'max_age': self.max_age,
                'max_rent': self.max_rent,
            },
            'new': {
                'enabled': True,
                'min_area': self.min_area,
                'max_age': self.max_age,
            },
            'used': {
                'enabled': True,
                'min_area': self.min_area,
                'max_age': self.max_age,
            },
        }

    def _init_per_type_conditions(self, cond):
        """種類別条件を初期化"""
        self.type_conditions = {}
        for type_key in ('rental', 'new', 'used'):
            tc = cond.get(type_key, {})
            self.type_conditions[type_key] = dict(tc)
            self.type_conditions[type_key].setdefault('enabled', True)
        # 後方互換属性（ログ表示等で使用）
        rental = self.type_conditions.get('rental', {})
        self.min_area = rental.get('min_area', 40)
        self.max_age = rental.get('max_age', 20)
        self.max_rent = rental.get('max_rent', 24.0)

    def is_type_enabled(self, prop_type):
        """指定種類が有効か"""
        return self.type_conditions.get(prop_type, {}).get('enabled', True)

    def get_station_region(self, station_name):
        """駅の地域（tokyo/kanagawa等）を返す"""
        return self.station_regions.get(station_name, 'tokyo')

    def should_include(self, prop, prop_type):
        """物件が条件を満たすかチェック"""
        cond = self.type_conditions.get(prop_type, {})

        min_area = cond.get('min_area')
        if min_area and prop.get('area') and prop['area'] < min_area:
            return False

        max_age = cond.get('max_age')
        if max_age is not None and prop.get('age_years') is not None and prop['age_years'] > max_age:
            return False

        # 賃貸は max_rent、売買は max_price で価格フィルタ
        if prop_type == 'rental':
            max_price = cond.get('max_rent')
        else:
            max_price = cond.get('max_price')
        if max_price is not None and prop.get('price') and prop['price'] > max_price:
            return False

        max_walk = cond.get('max_walk')
        if max_walk is not None and prop.get('walk_minutes') and prop['walk_minutes'] > max_walk:
            return False

        return True

    def station_matches(self, text):
        """テキストに対象駅名が含まれるか確認し、駅名を返す"""
        if not text:
            return None
        for station in self.stations:
            if station in text:
                return station
        for alias, canonical in self.station_aliases.items():
            if alias in text and canonical in self.stations:
                return canonical
        return None


# ============================================================
# プロファイル実行
# ============================================================
def run_profile(profile):
    """1プロファイル分のクロール・集計・通知を実行"""
    log("")
    log("=" * 50)
    log(f"プロファイル: {profile.name}")
    log(f"対象駅: {', '.join(profile.stations.keys())}")
    log(f"条件: 面積{profile.min_area}㎡以上, 築{profile.max_age}年以内, 賃貸{profile.max_rent}万円以下")
    log("=" * 50)

    session = create_session(profile.headers)

    # SUUMO
    suumo = SuumoCrawler(session, profile)
    suumo_rental = suumo.crawl_rental() if profile.is_type_enabled('rental') else []
    suumo_new = suumo.crawl_new() if profile.is_type_enabled('new') else []
    suumo_used = suumo.crawl_used() if profile.is_type_enabled('used') else []
    suumo_used_ikkodate = suumo.crawl_used_ikkodate() if profile.is_type_enabled('used') else []

    # HOMES
    homes = HomesCrawler(session, profile)
    homes_rental = homes.crawl_rental() if profile.is_type_enabled('rental') else []
    homes_new = homes.crawl_new() if profile.is_type_enabled('new') else []
    homes_used = homes.crawl_used() if profile.is_type_enabled('used') else []

    # cowcamo
    cowcamo = CowcamoCrawler(session, profile)
    cowcamo_used = cowcamo.crawl_used() if profile.is_type_enabled('used') else []

    # 結果集計
    all_rental = suumo_rental + homes_rental
    all_new = suumo_new + homes_new
    all_used = suumo_used + suumo_used_ikkodate + homes_used + cowcamo_used

    # 駅別集計
    log("")
    log("駅別統計を計算中...")
    station_stats = {
        'rental': compute_station_stats(all_rental, 'rental'),
        'new': compute_station_stats(all_new, 'new'),
        'used': compute_station_stats(all_used, 'used'),
    }

    # 物件追跡 & 値下げ検出
    log("物件追跡・値下げ検出中...")
    today_str = datetime.now().isoformat()[:10]
    history = load_history(profile.data_dir)
    changes = update_history_and_detect_changes(
        history, all_rental, all_new, all_used, today_str, profile.stale_days
    )
    save_history(profile.data_dir, history)

    # 変動サマリ集計
    changes = compute_changes_summary(changes, profile.stations)

    # 駅別統計に変動情報をマージ
    for st in profile.stations:
        ch = changes.get('station_summary', {}).get(st, {})
        for prop_type in ['rental', 'new', 'used']:
            if st in station_stats[prop_type]:
                station_stats[prop_type][st].update({
                    'new_listings': ch.get('new_count', 0),
                    'delisted': ch.get('delisted_count', 0),
                    'price_reduced': ch.get('price_reduced_count', 0),
                    'stale_count': ch.get('stale_count', 0),
                })

    # 時系列データ更新
    log("時系列データ更新中...")
    ts = load_time_series(profile.data_dir)
    ts = update_time_series(ts, today_str, station_stats, changes, profile.stations)
    save_time_series(profile.data_dir, ts)

    data = {
        'crawled_at': datetime.now().isoformat(),
        'profile': profile.name,
        'conditions': {
            'stations': list(profile.stations.keys()),
            'min_area_sqm': profile.min_area,
            'max_age_years': profile.max_age,
            'max_rent_man': profile.max_rent,
        },
        'summary': {
            'rental_count': len(all_rental),
            'new_count': len(all_new),
            'used_count': len(all_used),
            'suumo_rental': len(suumo_rental),
            'suumo_new': len(suumo_new),
            'suumo_used': len(suumo_used),
            'suumo_used_ikkodate': len(suumo_used_ikkodate),
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

    filename = save_snapshot(profile.data_dir, data)

    log("")
    log("=" * 50)
    log(f"プロファイル [{profile.name}] クロール完了！")
    log(f"  賃貸: {len(all_rental)}件 (SUUMO: {len(suumo_rental)}, HOMES: {len(homes_rental)})")
    log(f"  新築: {len(all_new)}件 (SUUMO: {len(suumo_new)}, HOMES: {len(homes_new)})")
    log(f"  中古: {len(all_used)}件 (SUUMO: {len(suumo_used)}, SUUMO一戸建: {len(suumo_used_ikkodate)}, HOMES: {len(homes_used)}, cowcamo: {len(cowcamo_used)})")
    log(f"  保存先: realestate_data/{profile.name}/{filename}")
    ch_sum = changes.get('summary', {})
    log(f"  変動: 新規{ch_sum.get('total_new',0)} / 消失{ch_sum.get('total_delisted',0)} / 値下げ{ch_sum.get('total_price_reduced',0)} / 滞留{ch_sum.get('total_stale',0)}")
    log("=" * 50)

    # LINE通知
    notify_line_new_listings(profile, changes, all_rental, all_new, all_used)


# ============================================================
# メイン
# ============================================================
def main():
    config = load_config()
    global_conf = config.get('global', {})
    station_aliases = config.get('station_aliases', DEFAULT_STATION_ALIASES)
    profiles = config.get('profiles', [])

    if not profiles:
        log("プロファイルが設定されていません。crawler_config.json を確認してください。")
        sys.exit(1)

    # 特定プロファイルのみ実行（コマンドライン引数で指定可能）
    target_profiles = None
    if len(sys.argv) > 1:
        target_profiles = sys.argv[1:]

    log("=" * 50)
    log("不動産物件クローラー 起動")
    log(f"プロファイル数: {len(profiles)}")
    log("=" * 50)

    for prof_dict in profiles:
        prof_name = prof_dict.get('name', '')
        if target_profiles and prof_name not in target_profiles:
            log(f"プロファイル [{prof_name}] をスキップ（対象外）")
            continue

        profile = ProfileConfig(prof_dict, global_conf, station_aliases)
        try:
            run_profile(profile)
        except Exception as e:
            log(f"プロファイル [{prof_name}] でエラー発生: {e}")
            traceback.print_exc()

    log("")
    log("全プロファイルの処理完了")


if __name__ == '__main__':
    main()
