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

    def test_per_type_conditions(self):
        p = self._make_profile(conditions={
            'rental': {'enabled': True, 'min_area': 50, 'max_age': 15, 'max_rent': 15.0, 'max_walk': 20},
            'new': {'enabled': False},
            'used_mansion': {'enabled': True, 'min_area': 60, 'max_price': 5000, 'max_walk': 20},
            'used_kodate': {'enabled': False},
        })
        assert p.is_type_enabled('rental') is True
        assert p.is_type_enabled('new') is False
        assert p.is_type_enabled('used_mansion') is True
        assert p.is_type_enabled('used_kodate') is False
        assert p.min_area == 50  # 後方互換: rental の値
        assert p.max_rent == 15.0

    def test_used_backward_compat(self):
        """旧形式の used キーが used_mansion / used_kodate に展開される"""
        p = self._make_profile(conditions={
            'rental': {'enabled': True},
            'new': {'enabled': False},
            'used': {'enabled': True, 'min_area': 60, 'max_price': 5000},
        })
        assert p.is_type_enabled('used_mansion') is True
        assert p.is_type_enabled('used_kodate') is True
        assert p.should_include({'area': 70, 'price': 3000}, 'used_mansion')
        assert not p.should_include({'area': 50, 'price': 3000}, 'used_mansion')
        assert p.should_include({'area': 70, 'price': 3000}, 'used_kodate')

    def test_should_include_rental(self):
        p = self._make_profile(conditions={
            'rental': {'enabled': True, 'min_area': 50, 'max_age': 15, 'max_rent': 15.0, 'max_walk': 20},
            'new': {'enabled': False},
            'used_mansion': {'enabled': True, 'min_area': 60, 'max_price': 5000},
            'used_kodate': {'enabled': False},
        })
        # 条件を満たす
        assert p.should_include({'area': 55, 'age_years': 10, 'price': 12.0, 'walk_minutes': 10}, 'rental')
        # 面積不足
        assert not p.should_include({'area': 40, 'age_years': 10, 'price': 12.0}, 'rental')
        # 築年数オーバー
        assert not p.should_include({'area': 55, 'age_years': 20, 'price': 12.0}, 'rental')
        # 家賃オーバー
        assert not p.should_include({'area': 55, 'age_years': 10, 'price': 20.0}, 'rental')
        # 徒歩オーバー
        assert not p.should_include({'area': 55, 'age_years': 10, 'price': 12.0, 'walk_minutes': 25}, 'rental')

    def test_should_include_used_mansion(self):
        p = self._make_profile(conditions={
            'rental': {'enabled': True, 'min_area': 50, 'max_rent': 15.0},
            'new': {'enabled': False},
            'used_mansion': {'enabled': True, 'min_area': 60, 'max_price': 5000, 'max_walk': 20},
            'used_kodate': {'enabled': True, 'min_area': 80},
        })
        # 中古マンション - 条件を満たす（築年数制限なし）
        assert p.should_include({'area': 70, 'age_years': 50, 'price': 3000, 'walk_minutes': 15}, 'used_mansion')
        # 中古マンション - 価格オーバー
        assert not p.should_include({'area': 70, 'age_years': 10, 'price': 6000}, 'used_mansion')
        # 中古マンション - 面積不足
        assert not p.should_include({'area': 50, 'age_years': 10, 'price': 3000}, 'used_mansion')
        # 中古一戸建て - 条件を満たす
        assert p.should_include({'area': 90, 'age_years': 10, 'price': 3000}, 'used_kodate')
        # 中古一戸建て - 面積不足（80㎡基準）
        assert not p.should_include({'area': 70, 'age_years': 10, 'price': 3000}, 'used_kodate')

    def test_station_region(self):
        p = self._make_profile(stations={
            '吉祥寺': {'code': '11640', 'region': 'tokyo'},
            '鎌倉': {'code': '08890', 'region': 'kanagawa'},
        })
        assert p.stations == {'吉祥寺': '11640', '鎌倉': '08890'}
        assert p.get_station_region('吉祥寺') == 'tokyo'
        assert p.get_station_region('鎌倉') == 'kanagawa'
        assert p.station_matches('吉祥寺駅 徒歩5分') == '吉祥寺'
        assert p.station_matches('鎌倉駅 徒歩10分') == '鎌倉'

    def test_station_string_format_backward_compat(self):
        """従来の文字列コード形式が引き続き動作する"""
        p = self._make_profile(stations={'吉祥寺': '11640', '荻窪': '06640'})
        assert p.stations == {'吉祥寺': '11640', '荻窪': '06640'}
        assert p.get_station_region('吉祥寺') == 'tokyo'
        assert p.get_station_region('荻窪') == 'tokyo'
