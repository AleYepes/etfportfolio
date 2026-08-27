import json
from datetime import datetime
from pathlib import Path

from etfportfolio.core.db import connect
from etfportfolio.ingestion.series import (
    determine_price_period,
    determine_sentiment_dates,
    extract_series_date_range,
    get_last_series_info,
    store_series,
    validate_overlap,
)


def test_determine_price_period():
    assert determine_price_period(None) == "MAX"

    ref_now = datetime(2026, 8, 26, 12, 0, 0)
    assert determine_price_period(datetime(2026, 8, 10), now=ref_now) == "1M"
    assert determine_price_period(datetime(2026, 6, 1), now=ref_now) == "3M"
    assert determine_price_period(datetime(2026, 4, 1), now=ref_now) == "6M"
    assert determine_price_period(datetime(2025, 12, 1), now=ref_now) == "1Y"
    assert determine_price_period(datetime(2024, 1, 1), now=ref_now) == "3Y"
    assert determine_price_period(datetime(2020, 1, 1), now=ref_now) == "10Y"
    assert determine_price_period(datetime(2010, 1, 1), now=ref_now) == "MAX"


def test_determine_sentiment_dates():
    ref_now = datetime(2026, 8, 26, 12, 0, 0)
    from_d, to_d = determine_sentiment_dates(None, now=ref_now)
    assert from_d == "2000-01-01"
    assert to_d == "2026-08-26"

    from_d2, to_d2 = determine_sentiment_dates(datetime(2026, 8, 1), now=ref_now)
    assert from_d2 == "2026-08-01"
    assert to_d2 == "2026-08-26"


def test_extract_series_date_range():
    price_fixture = json.loads(Path("tests/fixtures/price_incremental.json").read_text(encoding="utf-8"))
    date_range = extract_series_date_range("price", price_fixture)
    assert date_range is not None
    first_dt, last_dt = date_range
    assert first_dt < last_dt

    sentiment_fixture = json.loads(Path("tests/fixtures/sentiment_incremental.json").read_text(encoding="utf-8"))
    sent_range = extract_series_date_range("sentiment", sentiment_fixture)
    assert sent_range is not None
    s_first, s_last = sent_range
    assert s_first < s_last


def test_validate_overlap_matching_and_mismatch():
    p1 = {
        "plot": {
            "series": [
                {
                    "plotData": [
                        {"x": 1000, "close": 50.0, "open": 49.5},
                        {"x": 2000, "close": 51.0, "open": 50.0},
                    ]
                }
            ]
        }
    }
    # Matching overlap
    p2 = {
        "plot": {
            "series": [
                {
                    "plotData": [
                        {"x": 2000, "close": 51.0, "open": 50.0},
                        {"x": 3000, "close": 52.0, "open": 51.0},
                    ]
                }
            ]
        }
    }
    assert validate_overlap("price", p2, p1) is True

    # Conflicting overlap
    p3 = {
        "plot": {
            "series": [
                {
                    "plotData": [
                        {"x": 2000, "close": 99.0, "open": 50.0},
                        {"x": 3000, "close": 52.0, "open": 51.0},
                    ]
                }
            ]
        }
    }
    assert validate_overlap("price", p3, p1) is False


def test_store_and_get_last_series(tmp_path):
    db_file = str(tmp_path / "test_series.duckdb")
    fixture = json.loads(Path("tests/fixtures/price_incremental.json").read_text(encoding="utf-8"))
    first_dt, last_dt = extract_series_date_range("price", fixture)

    with connect(db_file) as conn:
        conn.execute(
            "INSERT INTO bronze.products (product_id, type, symbol, created_at, updated_at) VALUES (2001, 'ETF', 'VOO', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        )

        store_series(
            conn=conn,
            product_id=2001,
            url_prefix="fundamentals/mf_performance_chart/",
            url_slug="2001?chart_period=1M&lang=en",
            first_date=first_dt,
            last_date=last_dt,
            payload=fixture,
        )

        stored_last_date, stored_payload = get_last_series_info(conn, 2001, "fundamentals/mf_performance_chart/")
        assert stored_last_date == last_dt
        assert stored_payload is not None
        assert stored_payload["plot"]["series"][0]["name"] == "price"
