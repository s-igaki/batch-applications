"""notifier.py のテスト"""

from unittest.mock import MagicMock, patch

from notifier import (
    _format_price,
    _build_category_message,
    _format_single_property,
    _split_and_send,
)


# ============================================================
# _format_price
# ============================================================
class TestFormatPrice:
    def test_rental(self):
        assert _format_price(12.5, 'rental') == "12.5万円"

    def test_used_man(self):
        assert _format_price(7900, 'used') == "7900万円"

    def test_used_oku(self):
        assert _format_price(15000, 'new') == "1.5億円"

    def test_used_oku_exact(self):
        assert _format_price(20000, 'new') == "2億円"

    def test_none_price(self):
        assert _format_price(None, 'rental') == "価格未定"

    def test_zero_price(self):
        assert _format_price(0, 'rental') == "価格未定"


# ============================================================
# _build_category_message
# ============================================================
class TestBuildCategoryMessage:
    def test_empty_listings(self):
        msg = _build_category_message('rental', [], [])
        assert '新着情報はありません' in msg
        assert '🏢' in msg

    def test_with_listings(self):
        listings = [
            {'name': 'テストマンション', 'station': '吉祥寺', 'detail_url': 'http://test', 'price': 10},
        ]
        all_props = [
            {'detail_url': 'http://test', 'price': 10, 'area': 50, 'area_text': '50m²', 'address': '東京都'},
        ]
        msg = _build_category_message('rental', listings, all_props)
        assert 'テストマンション' in msg
        assert '吉祥寺' in msg
        assert '50m²' in msg
        assert '(1件)' in msg


# ============================================================
# _format_single_property
# ============================================================
class TestFormatSingleProperty:
    def test_basic(self):
        listing = {'name': '物件A', 'station': '渋谷', 'detail_url': 'http://a', 'price': 5000}
        all_props = [{'detail_url': 'http://a', 'price': 5000, 'area': 60, 'address': '渋谷区'}]
        text = _format_single_property(listing, all_props, 'used')
        assert '物件A' in text
        assert '渋谷' in text
        assert '5000万円' in text


# ============================================================
# _split_and_send
# ============================================================
class TestSplitAndSend:
    def test_short_message_no_split(self):
        listings = [{'name': '物件A', 'station': '渋谷', 'detail_url': '', 'price': 10}]
        all_props = [{'detail_url': '', 'price': 10}]
        msgs = _split_and_send("token", 'rental', listings, all_props)
        assert len(msgs) == 1
        assert msgs[0]['type'] == 'text'

    def test_empty_listings(self):
        msgs = _split_and_send("token", 'new', [], [])
        assert len(msgs) == 1
        assert '新着情報はありません' in msgs[0]['text']

    def test_long_message_split(self):
        # 大量の物件で分割が発生するケース
        listings = [
            {'name': f'テストマンション{i}号棟 非常に長い物件名をつけて文字数を稼ぐ', 'station': '吉祥寺',
             'detail_url': f'http://example.com/very/long/path/property/{i}', 'price': 5000 + i}
            for i in range(50)
        ]
        all_props = [
            {'detail_url': f'http://example.com/very/long/path/property/{i}',
             'price': 5000 + i, 'area': 60, 'area_text': '60.5m²', 'address': '東京都武蔵野市吉祥寺本町1丁目'}
            for i in range(50)
        ]
        msgs = _split_and_send("token", 'used', listings, all_props)
        assert len(msgs) > 1
        for m in msgs:
            assert len(m['text']) <= 4500
