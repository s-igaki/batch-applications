"""realestate_crawler.py のテスト"""

from realestate_crawler import ProfileConfig


# ============================================================
# ProfileConfig
# ============================================================
class TestProfileConfig:
    def _make_profile(self, **overrides):
        profile_dict = {
            'name': 'test',
            'stations': {'吉祥寺': 'sc_123', '荻窪': 'sc_456'},
            'homes_areas': {'武蔵野市': 'musashino'},
            'conditions': {'min_area': 40, 'max_age': 20, 'max_rent': 24.0},
            'line': {},
        }
        profile_dict.update(overrides)
        global_dict = {
            'request_delay': 2,
            'max_pages': 5,
            'headers': {'User-Agent': 'test'},
            'stale_days': 14,
        }
        aliases = {'千駄ケ谷': '千駄ヶ谷'}
        return ProfileConfig(profile_dict, global_dict, aliases)

    def test_basic_attrs(self):
        p = self._make_profile()
        assert p.name == 'test'
        assert p.min_area == 40
        assert p.max_age == 20
        assert p.max_rent == 24.0
        assert p.stale_days == 14

    def test_station_matches_direct(self):
        p = self._make_profile()
        assert p.station_matches('吉祥寺駅 徒歩5分') == '吉祥寺'
        assert p.station_matches('荻窪駅 徒歩3分') == '荻窪'

    def test_station_matches_alias(self):
        p = self._make_profile(
            stations={'千駄ヶ谷': 'sc_789'},
        )
        p.station_aliases = {'千駄ケ谷': '千駄ヶ谷'}
        assert p.station_matches('千駄ケ谷駅 徒歩2分') == '千駄ヶ谷'

    def test_station_matches_none(self):
        p = self._make_profile()
        assert p.station_matches('新宿駅 徒歩1分') is None
        assert p.station_matches(None) is None
        assert p.station_matches('') is None

    def test_default_conditions(self):
        profile_dict = {
            'name': 'minimal',
            'stations': {},
            'line': {},
        }
        p = ProfileConfig(profile_dict, {}, {})
        assert p.min_area == 40
        assert p.max_age == 20
        assert p.max_rent == 24.0
