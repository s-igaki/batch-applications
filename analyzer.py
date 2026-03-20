"""統計計算、変更検知、履歴管理、時系列更新"""

import json
import os
import statistics
from datetime import datetime, timedelta

from crawlers.base import log, make_unique_id


def load_history(data_dir):
    """物件追跡履歴を読み込み"""
    path = os.path.join(data_dir, 'history.json')
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, KeyError):
            pass
    return {'properties': {}}


def save_history(data_dir, history):
    """物件追跡履歴を保存"""
    os.makedirs(data_dir, exist_ok=True)
    path = os.path.join(data_dir, 'history.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    log(f"物件履歴更新: {len(history['properties'])}件追跡中")


def load_time_series(data_dir):
    """時系列データを読み込み"""
    path = os.path.join(data_dir, 'time_series.json')
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, KeyError):
            pass
    return {'dates': [], 'stations': {}, 'overall': {}}


def save_time_series(data_dir, ts):
    """時系列データを保存"""
    path = os.path.join(data_dir, 'time_series.json')
    with open(path, 'w', encoding='utf-8') as f:
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


def update_history_and_detect_changes(history, all_rental, all_new, all_used, today_str, stale_days):
    """物件履歴を更新し、値下げ・新規・消失を検出"""
    today_uids = set()
    changes = {
        'price_reduced': [],
        'new_listings': [],
        'delisted': [],
        'stale': [],
    }

    for prop_type, props in [('rental', all_rental), ('new', all_new), ('used', all_used)]:
        for p in props:
            uid = make_unique_id(p.get('detail_url', '') or p.get('name', ''))
            today_uids.add(uid)

            if uid in history['properties']:
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

                if p.get('price') and (not old_price or p['price'] != old_price):
                    rec['price_history'].append({
                        'date': today_str,
                        'price': p['price'],
                    })

                first_dt = rec.get('first_seen', today_str)
                try:
                    days = (datetime.fromisoformat(today_str) - datetime.fromisoformat(first_dt)).days
                except Exception:
                    days = 0
                if days >= stale_days:
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

    # 消失物件の検出
    yesterday = (datetime.fromisoformat(today_str) - timedelta(days=1)).isoformat()[:10]
    for uid, rec in history['properties'].items():
        if uid not in today_uids:
            last_seen = rec.get('last_seen', '')[:10]
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

    return changes


def compute_changes_summary(changes, stations):
    """変動サマリを駅別に集計"""
    station_changes = {}
    for st in stations:
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


def update_time_series(ts, today_str, station_stats, changes, stations):
    """時系列データに本日の集計を追加"""
    date_key = today_str[:10]

    if date_key in ts['dates']:
        idx = ts['dates'].index(date_key)
    else:
        ts['dates'].append(date_key)
        idx = len(ts['dates']) - 1

    for st_name in stations:
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


def save_snapshot(data_dir, data):
    """クロール結果をJSONスナップショットとして保存"""
    os.makedirs(data_dir, exist_ok=True)

    now = datetime.now()
    filename = now.strftime('%Y-%m-%d_%H%M%S') + '.json'
    filepath = os.path.join(data_dir, filename)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"スナップショット保存: {filename}")

    # latest.json を更新
    latest_path = os.path.join(data_dir, 'latest.json')
    with open(latest_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # index.json を更新
    update_index(data_dir, filename, data)

    return filename


def update_index(data_dir, new_file, data):
    """index.jsonにスナップショット一覧を記録"""
    index_path = os.path.join(data_dir, 'index.json')

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
