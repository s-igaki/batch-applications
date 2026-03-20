"""analyzer.py のテスト"""

import json
import os
import tempfile

from analyzer import (
    safe_median,
    safe_mean,
    percentile,
    compute_station_stats,
    load_history,
    save_history,
    update_history_and_detect_changes,
    compute_changes_summary,
    load_time_series,
    save_time_series,
    update_time_series,
    save_snapshot,
    update_index,
)


# ============================================================
# safe_median / safe_mean / percentile
# ============================================================
class TestStatHelpers:
    def test_safe_median_normal(self):
        assert safe_median([1, 2, 3, 4, 5]) == 3

    def test_safe_median_empty(self):
        assert safe_median([]) is None

    def test_safe_mean_normal(self):
        assert safe_mean([10, 20, 30]) == 20.0

    def test_safe_mean_empty(self):
        assert safe_mean([]) is None

    def test_percentile_25(self):
        vals = list(range(1, 101))  # 1..100
        result = percentile(vals, 25)
        assert result == 26  # idx=25 → sorted[25]=26

    def test_percentile_75(self):
        vals = list(range(1, 101))
        result = percentile(vals, 75)
        assert result == 76

    def test_percentile_empty(self):
        assert percentile([], 50) is None


# ============================================================
# compute_station_stats
# ============================================================
class TestComputeStationStats:
    def test_basic(self):
        props = [
            {'station': '吉祥寺', 'price': 5000, 'area': 50, 'age_years': 10, 'walk_minutes': 5},
            {'station': '吉祥寺', 'price': 6000, 'area': 60, 'age_years': 5, 'walk_minutes': 3},
            {'station': '荻窪', 'price': 4000, 'area': 45, 'age_years': 15, 'walk_minutes': 7},
        ]
        stats = compute_station_stats(props, 'used')

        assert '吉祥寺' in stats
        assert '荻窪' in stats
        assert stats['吉祥寺']['count'] == 2
        assert stats['荻窪']['count'] == 1
        assert stats['吉祥寺']['avg_price'] == 5500.0
        assert stats['吉祥寺']['median_price'] == 5500.0

    def test_no_station(self):
        props = [{'price': 5000, 'area': 50}]  # station なし
        stats = compute_station_stats(props, 'rental')
        assert stats == {}

    def test_empty_list(self):
        stats = compute_station_stats([], 'new')
        assert stats == {}

    def test_missing_fields(self):
        props = [{'station': '渋谷', 'price': None, 'area': None}]
        stats = compute_station_stats(props, 'used')
        assert stats['渋谷']['count'] == 1
        assert stats['渋谷']['avg_price'] is None


# ============================================================
# load_history / save_history
# ============================================================
class TestHistory:
    def test_load_nonexistent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            h = load_history(tmpdir)
            assert h == {'properties': {}}

    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            history = {'properties': {'abc123': {'name': 'テスト物件'}}}
            save_history(tmpdir, history)
            loaded = load_history(tmpdir)
            assert loaded['properties']['abc123']['name'] == 'テスト物件'

    def test_load_corrupted(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'history.json')
            with open(path, 'w') as f:
                f.write("not json")
            h = load_history(tmpdir)
            assert h == {'properties': {}}


# ============================================================
# update_history_and_detect_changes
# ============================================================
class TestUpdateHistoryAndDetectChanges:
    def test_new_listing(self):
        history = {'properties': {}}
        rental = [{'name': '物件A', 'station': '吉祥寺', 'price': 10, 'detail_url': 'http://a'}]
        changes = update_history_and_detect_changes(history, rental, [], [], '2026-01-01', 14)

        assert len(changes['new_listings']) == 1
        assert changes['new_listings'][0]['name'] == '物件A'
        assert len(history['properties']) == 1

    def test_price_reduced(self):
        from crawlers.base import make_unique_id
        uid = make_unique_id('http://a')
        history = {'properties': {
            uid: {
                'name': '物件A', 'type': 'rental', 'station': '吉祥寺',
                'first_seen': '2025-12-01', 'last_seen': '2025-12-31',
                'price_history': [{'date': '2025-12-01', 'price': 12}],
                'detail_url': 'http://a',
            }
        }}
        rental = [{'name': '物件A', 'station': '吉祥寺', 'price': 10, 'detail_url': 'http://a'}]
        changes = update_history_and_detect_changes(history, rental, [], [], '2026-01-01', 14)

        assert len(changes['price_reduced']) == 1
        assert changes['price_reduced'][0]['old_price'] == 12
        assert changes['price_reduced'][0]['new_price'] == 10

    def test_delisted(self):
        from crawlers.base import make_unique_id
        uid = make_unique_id('http://a')
        history = {'properties': {
            uid: {
                'name': '物件A', 'type': 'rental', 'station': '吉祥寺',
                'first_seen': '2025-12-01', 'last_seen': '2025-12-31',
                'price_history': [{'date': '2025-12-01', 'price': 10}],
                'detail_url': 'http://a',
            }
        }}
        # 物件Aが今日のクロール結果に含まれない → 消失
        changes = update_history_and_detect_changes(history, [], [], [], '2026-01-01', 14)

        assert len(changes['delisted']) == 1
        assert changes['delisted'][0]['name'] == '物件A'

    def test_stale_detection(self):
        from crawlers.base import make_unique_id
        uid = make_unique_id('http://a')
        history = {'properties': {
            uid: {
                'name': '物件A', 'type': 'used', 'station': '荻窪',
                'first_seen': '2025-12-01', 'last_seen': '2025-12-30',
                'price_history': [{'date': '2025-12-01', 'price': 5000}],
                'detail_url': 'http://a',
            }
        }}
        used = [{'name': '物件A', 'station': '荻窪', 'price': 5000, 'detail_url': 'http://a'}]
        changes = update_history_and_detect_changes(history, [], [], used, '2026-01-15', 14)

        # 2025-12-01 → 2026-01-15 = 45日 ≥ 14日
        assert len(changes['stale']) == 1
        assert changes['stale'][0]['days_listed'] == 45


# ============================================================
# compute_changes_summary
# ============================================================
class TestComputeChangesSummary:
    def test_summary(self):
        changes = {
            'new_listings': [
                {'station': '吉祥寺', 'type': 'rental'},
                {'station': '吉祥寺', 'type': 'new'},
                {'station': '荻窪', 'type': 'used'},
            ],
            'delisted': [],
            'price_reduced': [{'station': '荻窪', 'type': 'used'}],
            'stale': [],
        }
        stations = {'吉祥寺': {}, '荻窪': {}}
        result = compute_changes_summary(changes, stations)

        assert result['summary']['total_new'] == 3
        assert result['summary']['total_price_reduced'] == 1
        # 種別ごとに分かれている
        assert result['station_summary']['吉祥寺']['rental']['new_count'] == 1
        assert result['station_summary']['吉祥寺']['new']['new_count'] == 1
        assert result['station_summary']['吉祥寺']['used']['new_count'] == 0
        assert result['station_summary']['荻窪']['used']['new_count'] == 1
        assert result['station_summary']['荻窪']['used']['price_reduced_count'] == 1


# ============================================================
# load_time_series / save_time_series
# ============================================================
class TestTimeSeries:
    def test_load_nonexistent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ts = load_time_series(tmpdir)
            assert ts == {'dates': [], 'stations': {}, 'overall': {}}

    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ts = {'dates': ['2026-01-01'], 'stations': {}, 'overall': {}}
            save_time_series(tmpdir, ts)
            loaded = load_time_series(tmpdir)
            assert loaded['dates'] == ['2026-01-01']


# ============================================================
# save_snapshot / update_index
# ============================================================
class TestSnapshot:
    def test_save_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data = {
                'crawled_at': '2026-01-01T12:00:00',
                'summary': {'rental_count': 5, 'new_count': 3, 'used_count': 2},
            }
            filename = save_snapshot(tmpdir, data)
            assert filename.endswith('.json')

            # latest.json が作成されている
            latest_path = os.path.join(tmpdir, 'latest.json')
            assert os.path.exists(latest_path)

            # index.json が作成されている
            index_path = os.path.join(tmpdir, 'index.json')
            assert os.path.exists(index_path)
            with open(index_path, 'r') as f:
                index = json.load(f)
            assert len(index['snapshots']) == 1
            assert index['snapshots'][0]['rental_count'] == 5

    def test_update_index_append(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # 最初のエントリ
            data1 = {'crawled_at': '2026-01-01', 'summary': {'rental_count': 5, 'new_count': 0, 'used_count': 0}}
            update_index(tmpdir, 'file1.json', data1)
            # 2番目のエントリ
            data2 = {'crawled_at': '2026-01-02', 'summary': {'rental_count': 8, 'new_count': 0, 'used_count': 0}}
            update_index(tmpdir, 'file2.json', data2)

            with open(os.path.join(tmpdir, 'index.json'), 'r') as f:
                index = json.load(f)
            assert len(index['snapshots']) == 2
            # 最新が先頭
            assert index['snapshots'][0]['file'] == 'file2.json'
