from __future__ import annotations

import math

from crypto_momentum.indicators import build_signal_row, assign_percentiles
from crypto_momentum.models import Candle
from crypto_momentum.utils import round_down, round_up, completed_hour_window, maker_buy_limit_price, base_size_for_cash
from datetime import datetime, timezone


def synthetic_candles(n: int = 350, hourly_growth: float = 0.001) -> list[Candle]:
    out = []
    price = 100.0
    for i in range(n):
        o = price
        price *= 1.0 + hourly_growth
        c = price
        out.append(Candle(start=i * 3600, low=min(o, c) * 0.9998, high=max(o, c) * 1.0002, open=o, close=c, volume=1000))
    return out


def cfg():
    return {
        "lookbacks_hours": [24, 72, 168, 336],
        "weights": [0.15, 0.25, 0.35, 0.25],
        "ema_fast_hours": 24,
        "ema_slow_hours": 72,
        "ema_slope_lookback_hours": 6,
        "atr_hours": 14,
        "realized_vol_hours": 72,
        "breakout_hours": 3,
    }


def test_rising_series_passes_trend_and_breakout():
    row = build_signal_row("TEST-USD", synthetic_candles(), cfg())
    assert row is not None
    assert row.trend_pass is True
    assert row.breakout is True
    assert row.r24 > 0 and row.r336 > 0
    assert row.ema_fast > row.ema_slow
    assert math.isfinite(row.atr_pct) and row.atr_pct > 0


def test_percentile_ordering():
    a = build_signal_row("A-USD", synthetic_candles(hourly_growth=0.0005), cfg())
    b = build_signal_row("B-USD", synthetic_candles(hourly_growth=0.0010), cfg())
    c = build_signal_row("C-USD", synthetic_candles(hourly_growth=0.0015), cfg())
    ranked = assign_percentiles([a, b, c])
    assert ranked[0].product_id == "C-USD"
    assert ranked[0].percentile == 1.0
    assert ranked[-1].percentile == 0.0


def test_round_down_increment():
    assert round_down(12.349, "0.01") == "12.34"
    assert round_down(0.123456, "0.0001") == "0.1234"


def test_completed_hour_window_stays_below_live_candle_boundary():
    now = datetime(2026, 9, 12, 16, 38, 46, tzinfo=timezone.utc)
    start, end, buckets = completed_hour_window(now, 350)
    assert buckets == 349
    assert end == int(datetime(2026, 9, 12, 16, 0, 0, tzinfo=timezone.utc).timestamp())
    assert end - start == 349 * 3600
    assert end % 3600 == 0


def test_maker_price_stays_post_only_and_improves_bid():
    # 100.00 / 100.10 with 0.01 ticks -> midpoint 100.05, still below ask.
    assert maker_buy_limit_price(100.00, 100.10, "0.01", 0.50) == "100.05"
    # One-tick spread cannot be improved without crossing; remain at bid.
    assert maker_buy_limit_price(100.00, 100.01, "0.01", 0.50) == "100.00"


def test_fee_aware_base_sizing_stays_inside_cash_budget():
    size = base_size_for_cash(29.00, 100.00, 0.006, "0.0001")
    assert float(size) > 0
    assert float(size) * 100.00 * 1.006 <= 29.00 + 1e-9


def test_round_up_increment():
    assert round_up(12.341, "0.01") == "12.35"
