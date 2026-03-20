"""crawlers/base.py のテスト"""

import hashlib
from unittest.mock import MagicMock, patch

from crawlers.base import (
    parse_number,
    parse_buy_price,
    parse_age_years,
    extract_walk_minutes,
    extract_walk_text,
    parse_management_fee,
    make_unique_id,
    fetch_soup,
)


# ============================================================
# parse_number
# ============================================================
class TestParseNumber:
    def test_integer(self):
        assert parse_number("123") == 123.0

    def test_float(self):
        assert parse_number("12.5万") == 12.5

    def test_with_comma(self):
        assert parse_number("1,234") == 1234.0

    def test_fullwidth_comma(self):
        assert parse_number("1，234") == 1234.0

    def test_none_input(self):
        assert parse_number(None) is None

    def test_empty_string(self):
        assert parse_number("") is None

    def test_no_digits(self):
        assert parse_number("abc") is None

    def test_mixed_text(self):
        assert parse_number("約50平米") == 50.0


# ============================================================
# parse_buy_price
# ============================================================
class TestParseBuyPrice:
    def test_man_en(self):
        assert parse_buy_price("7,900万円") == 7900.0

    def test_oku_man(self):
        assert parse_buy_price("1億5,000万円") == 15000

    def test_oku_only(self):
        assert parse_buy_price("2億円") == 20000

    def test_range(self):
        assert parse_buy_price("4,900万円～5,500万円") == 4900.0

    def test_range_tilde(self):
        assert parse_buy_price("3,000万円~4,000万円") == 3000.0

    def test_none_input(self):
        assert parse_buy_price(None) is None

    def test_empty_string(self):
        assert parse_buy_price("") is None

    def test_no_price(self):
        assert parse_buy_price("未定") is None

    def test_decimal_man(self):
        assert parse_buy_price("3980.5万円") == 3980.5


# ============================================================
# parse_age_years
# ============================================================
class TestParseAgeYears:
    def test_shinchiku(self):
        assert parse_age_years("新築") == 0

    def test_chiku_n_nen(self):
        assert parse_age_years("築3年") == 3

    def test_chiku_long(self):
        assert parse_age_years("築15年") == 15

    def test_year_format(self):
        # 2020年 → 現在年 - 2020
        result = parse_age_years("2020年3月")
        assert isinstance(result, int)
        assert result >= 5  # 2025年以降なら少なくとも5

    def test_none_input(self):
        assert parse_age_years(None) is None

    def test_empty_string(self):
        assert parse_age_years("") is None

    def test_no_match(self):
        assert parse_age_years("不明") is None


# ============================================================
# extract_walk_minutes
# ============================================================
class TestExtractWalkMinutes:
    def test_normal(self):
        assert extract_walk_minutes("徒歩5分") == 5

    def test_double_digit(self):
        assert extract_walk_minutes("徒歩12分") == 12

    def test_short_form(self):
        assert extract_walk_minutes("歩3分") == 3

    def test_none_input(self):
        assert extract_walk_minutes(None) is None

    def test_no_match(self):
        assert extract_walk_minutes("バス10分") is None


# ============================================================
# extract_walk_text
# ============================================================
class TestExtractWalkText:
    def test_with_station_and_walk(self):
        assert extract_walk_text("JR総武線/吉祥寺駅 徒歩12分", "吉祥寺") == "吉祥寺駅 徒歩12分"

    def test_station_only(self):
        assert extract_walk_text("バス10分", "渋谷") == "渋谷駅"

    def test_walk_only(self):
        assert extract_walk_text("徒歩5分", None) == "徒歩5分"

    def test_empty(self):
        assert extract_walk_text("", None) == ""

    def test_none(self):
        assert extract_walk_text(None, None) == ""


# ============================================================
# parse_management_fee
# ============================================================
class TestParseManagementFee:
    def test_yen(self):
        assert parse_management_fee("5,000円") == 0.5

    def test_man_yen(self):
        assert parse_management_fee("1万円") == 1.0

    def test_dash(self):
        assert parse_management_fee("-") is None

    def test_komi(self):
        assert parse_management_fee("込み") is None

    def test_none(self):
        assert parse_management_fee(None) is None

    def test_empty(self):
        assert parse_management_fee("") is None

    def test_small_amount(self):
        assert parse_management_fee("3000円") == 0.3

    def test_large_amount(self):
        assert parse_management_fee("15,000円") == 1.5


# ============================================================
# make_unique_id
# ============================================================
class TestMakeUniqueId:
    def test_deterministic(self):
        assert make_unique_id("test") == make_unique_id("test")

    def test_length(self):
        assert len(make_unique_id("anything")) == 12

    def test_different_inputs(self):
        assert make_unique_id("a") != make_unique_id("b")

    def test_matches_md5(self):
        text = "https://example.com/property/123"
        expected = hashlib.md5(text.encode('utf-8')).hexdigest()[:12]
        assert make_unique_id(text) == expected


# ============================================================
# fetch_soup
# ============================================================
class TestFetchSoup:
    def test_success(self):
        session = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.text = "<html><body><p>Hello</p></body></html>"
        session.get.return_value = resp

        soup = fetch_soup(session, "http://example.com")
        assert soup is not None
        assert soup.find("p").text == "Hello"

    def test_http_error(self):
        session = MagicMock()
        resp = MagicMock()
        resp.status_code = 404
        session.get.return_value = resp

        assert fetch_soup(session, "http://example.com") is None

    def test_exception(self):
        session = MagicMock()
        session.get.side_effect = Exception("connection error")

        assert fetch_soup(session, "http://example.com") is None
