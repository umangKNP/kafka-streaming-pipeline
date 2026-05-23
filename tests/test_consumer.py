"""
tests/test_consumer.py
Unit tests for VWAPAccumulator and window logic in consumer.py
"""

import pytest
from datetime import datetime, timezone
from consumer import VWAPAccumulator, get_window_start


# ── get_window_start ──────────────────────────────────────────────────────────

def test_window_start_truncates_seconds():
    ts = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=timezone.utc)
    w  = get_window_start(ts)
    assert w.second      == 0
    assert w.microsecond == 0
    assert w.minute      == 30
    assert w.hour        == 10


def test_window_start_exact_minute_unchanged():
    ts = datetime(2024, 1, 15, 10, 30, 0, 0, tzinfo=timezone.utc)
    assert get_window_start(ts) == ts


# ── VWAPAccumulator ───────────────────────────────────────────────────────────

def make_ts(hour=10, minute=30, second=0):
    return datetime(2024, 1, 15, hour, minute, second, tzinfo=timezone.utc)


def test_single_tick_vwap():
    acc = VWAPAccumulator()
    acc.add("AAPL", 185.0, 1000, make_ts(10, 30, 5))
    rows = acc.flush_completed_windows(make_ts(10, 31, 0))
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["vwap"] == pytest.approx(185.0)
    assert rows[0]["total_volume"] == 1000
    assert rows[0]["trade_count"]  == 1


def test_vwap_weighted_correctly():
    acc = VWAPAccumulator()
    # Trade 1: price=100, volume=100  → pv=10000
    # Trade 2: price=200, volume=100  → pv=20000
    # VWAP = 30000 / 200 = 150.0
    acc.add("MSFT", 100.0, 100, make_ts(10, 30, 5))
    acc.add("MSFT", 200.0, 100, make_ts(10, 30, 55))
    rows = acc.flush_completed_windows(make_ts(10, 31, 0))
    assert rows[0]["vwap"] == pytest.approx(150.0)


def test_multiple_symbols_same_window():
    acc = VWAPAccumulator()
    acc.add("AAPL", 185.0, 500, make_ts(10, 30, 1))
    acc.add("GOOG", 175.0, 300, make_ts(10, 30, 2))
    rows = acc.flush_completed_windows(make_ts(10, 31, 0))
    symbols = {r["symbol"] for r in rows}
    assert symbols == {"AAPL", "GOOG"}


def test_current_window_not_flushed():
    acc = VWAPAccumulator()
    ts_now = make_ts(10, 30, 5)
    acc.add("AAPL", 185.0, 1000, ts_now)
    # Flush at same minute — should NOT return anything
    rows = acc.flush_completed_windows(ts_now)
    assert rows == []


def test_multiple_windows_accumulate_separately():
    acc = VWAPAccumulator()
    acc.add("AAPL", 100.0, 1000, make_ts(10, 29, 30))  # window 10:29
    acc.add("AAPL", 200.0, 1000, make_ts(10, 30, 30))  # window 10:30
    # Flush from 10:31 — both should appear
    rows = acc.flush_completed_windows(make_ts(10, 31, 0))
    assert len(rows) == 2
    windows = {r["window_start"].minute for r in rows}
    assert windows == {29, 30}


def test_empty_accumulator_flush():
    acc = VWAPAccumulator()
    rows = acc.flush_completed_windows(make_ts(10, 31, 0))
    assert rows == []


def test_trade_count_increments():
    acc = VWAPAccumulator()
    for i in range(5):
        acc.add("META", 520.0, 100, make_ts(10, 30, i))
    rows = acc.flush_completed_windows(make_ts(10, 31, 0))
    assert rows[0]["trade_count"] == 5
