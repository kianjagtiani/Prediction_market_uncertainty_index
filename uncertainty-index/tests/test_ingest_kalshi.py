import json
from pathlib import Path

import httpx
import pandas as pd
import pytest

from uindex.ingest import kalshi

FIXTURES = Path(__file__).parent / "fixtures"


def test_markets_to_df_schema():
    payload = json.loads((FIXTURES / "kalshi_markets.json").read_text())
    df = kalshi.markets_to_df(payload["markets"])
    assert list(df.columns) == [
        "market_id", "venue", "question", "venue_category", "event_ticker",
        "ticker", "series_ticker", "total_volume_usd", "open_date", "close_date",
    ]
    assert (df["venue"] == "kalshi").all()
    assert df["market_id"].str.startswith("ka_").all()
    assert pd.api.types.is_datetime64_any_dtype(df["close_date"])
    # All fixture markets have volume_fp "0.00" (Task 2 checkpoint), so also
    # assert directly on venue_category and on a synthetic non-zero volume_fp
    # market to catch a regression that silently reads a nonexistent "volume"
    # field instead of "volume_fp".
    assert (df["venue_category"] == "").all()
    assert (df["total_volume_usd"] == 0).all()


def test_markets_to_df_total_volume_usd_from_volume_fp():
    markets = [{
        "ticker": "TEST-1",
        "event_ticker": "TEST",
        "series_ticker": "TEST",
        "title": "Test market",
        "volume_fp": "200.00",
        "open_time": "2024-01-01T00:00:00Z",
        "close_time": "2024-02-01T00:00:00Z",
    }]
    df = kalshi.markets_to_df(markets)
    assert df["venue_category"].iloc[0] == ""
    # volume_fp is a contract count; ~$0.50 avg price notional proxy.
    assert df["total_volume_usd"].iloc[0] == 100.0


def _api_market(ticker):
    return {
        "ticker": ticker,
        "event_ticker": ticker.split("-")[0],
        "title": f"Market {ticker}",
        "volume_fp": "10.00",
        "open_time": "2024-01-01T00:00:00Z",
        "close_time": "2024-02-01T00:00:00Z",
    }


def test_fetch_all_markets_streams_pages_to_slim_df(monkeypatch):
    # Returns a converted DataFrame, not raw dicts: Kalshi's full catalog of
    # raw market dicts hit ~9 GB RSS and never finished on the 8 GB machine.
    monkeypatch.setattr(kalshi.time, "sleep", lambda s: None)
    pages = {
        None: {"markets": [_api_market("AAA-1"), _api_market("AAA-2")],
               "cursor": "next1"},
        "next1": {"markets": [_api_market("BBB-1")], "cursor": "next2"},
        "next2": {"markets": [], "cursor": ""},  # empty final page
    }
    calls = []

    def handler(request):
        cursor = request.url.params.get("cursor")
        calls.append(dict(request.url.params))
        return httpx.Response(200, json=pages[cursor or None])

    df = kalshi.fetch_all_markets(_mock_client(handler))
    assert isinstance(df, pd.DataFrame)
    assert list(df["market_id"]) == ["ka_AAA-1", "ka_AAA-2", "ka_BBB-1"]
    assert "cursor" not in calls[0]
    assert calls[1]["cursor"] == "next1"


def test_history_todo_skips_done_and_provably_ineligible():
    from uindex import config
    floor = config.KALSHI_MIN_ROLLING_NOTIONAL_USD
    # total_volume_usd is volume_fp * $0.50; true notional <= volume_fp * $1.00
    # = total_volume_usd * 2, so markets with total_volume_usd * 2 < floor can
    # never pass the rolling-notional universe filter.
    meta = pd.DataFrame({
        "market_id": ["ka_A", "ka_B", "ka_C"],
        "total_volume_usd": [floor / 2, floor / 2 - 0.01, floor],
    })
    todo = kalshi.history_todo(meta, done={"ka_C"})
    assert list(todo["market_id"]) == ["ka_A"]


def test_candles_to_df_prob_and_notional():
    payload = json.loads((FIXTURES / "kalshi_candles.json").read_text())
    df = kalshi.candles_to_df(payload, market_id="ka_TEST")
    assert list(df.columns) == ["market_id", "date", "close_prob", "daily_notional_usd"]
    assert df["close_prob"].between(0, 1).all()  # cents converted
    assert (df["daily_notional_usd"] >= 0).all()
    assert not df["date"].duplicated().any()


def _candle_payload(close_dollars="0.50", volume_fp="10.00"):
    return {
        "candlesticks": [{
            "end_period_ts": 1783656000,
            "price": {"close_dollars": close_dollars},
            "volume_fp": volume_fp,
        }],
    }


def _mock_client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_candles_no_retry_when_first_attempt_succeeds():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, json=_candle_payload())

    client = _mock_client(handler)
    payload = kalshi.fetch_candles(client, "HIGHNY", "HIGHNY-TICKER", 0, 1)

    assert len(payload["candlesticks"]) == 1
    assert len(calls) == 1
    assert "/series/HIGHNY/" in calls[0]


def test_fetch_candles_retries_with_kx_prefix_on_404():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if "/series/HIGHNY/" in str(request.url):
            return httpx.Response(404, json={"error": "not found"})
        return httpx.Response(200, json=_candle_payload())

    client = _mock_client(handler)
    payload = kalshi.fetch_candles(client, "HIGHNY", "HIGHNY-TICKER", 0, 1)

    assert len(payload["candlesticks"]) == 1
    assert len(calls) == 2
    assert "/series/KXHIGHNY/" in calls[1]


def test_fetch_candles_both_attempts_404_returns_empty():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "not found"})

    client = _mock_client(handler)
    payload = kalshi.fetch_candles(client, "HIGHNY", "HIGHNY-TICKER", 0, 1)

    assert payload == {"candlesticks": []}


def test_fetch_candles_both_attempts_empty_returns_empty():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"candlesticks": []})

    client = _mock_client(handler)
    payload = kalshi.fetch_candles(client, "HIGHNY", "HIGHNY-TICKER", 0, 1)

    assert payload == {"candlesticks": []}


def test_fetch_candles_already_kx_prefixed_404_returns_empty_no_retry():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(404, json={"error": "not found"})

    client = _mock_client(handler)
    payload = kalshi.fetch_candles(client, "KXHIGHNY", "HIGHNY-TICKER", 0, 1)

    assert payload == {"candlesticks": []}
    assert len(calls) == 1  # already KX-prefixed: no fallback to try


def test_fetch_candles_unexpected_5xx_still_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        if "/series/HIGHNY/" in str(request.url):
            return httpx.Response(404, json={"error": "not found"})
        return httpx.Response(500, json={"error": "server error"})

    client = _mock_client(handler)
    with pytest.raises(httpx.HTTPStatusError):
        kalshi.fetch_candles(client, "HIGHNY", "HIGHNY-TICKER", 0, 1)
