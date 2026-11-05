from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

try:
    import coinbase.rest  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    coinbase = types.ModuleType("coinbase")
    rest = types.ModuleType("coinbase.rest")
    class RESTClient:  # pragma: no cover
        pass
    rest.RESTClient = RESTClient
    coinbase.rest = rest
    sys.modules["coinbase"] = coinbase
    sys.modules["coinbase.rest"] = rest

import yaml

from crypto_momentum.coinbase_client import CoinbaseGateway
from crypto_momentum.db import StateDB
from crypto_momentum.indicators import build_signal_row_15m
from crypto_momentum.models import Candle, ProductMeta, SignalRow
from crypto_momentum.shadow import Shadow15mEngine
from crypto_momentum.strategy import StrategyEngine
from crypto_momentum.utils import completed_bucket_window


def test_v147_config_uses_15m_live_decisions_and_30m_persistence() -> None:
    root = Path(__file__).resolve().parents[1]
    data = yaml.safe_load((root / "config" / "strategy.yaml").read_text(encoding="utf-8"))
    assert data["runtime"]["signal_scan_interval_minutes"] == 15
    assert data["signal"]["persistence_scans"] == 3
    assert data["universe"]["refresh_minutes"] == 30
    assert data["runtime"]["position_exit_check_minutes"] == 5
    shadow = data["shadow_15m"]
    assert shadow["enabled"] is True
    assert shadow["scan_interval_minutes"] == 15
    assert shadow["persistence_scans"] == 3
    assert shadow["lookback_bars"] == [24, 72, 168, 336]


def test_completed_15m_window_is_aligned_and_capped() -> None:
    now = datetime(2026, 9, 15, 12, 37, 45, tzinfo=timezone.utc)
    start, end, n = completed_bucket_window(900, now=now, requested_buckets=999)
    assert n == 349
    assert end == int(datetime(2026, 9, 15, 12, 30, tzinfo=timezone.utc).timestamp())
    assert end - start == 349 * 900


def test_gateway_15m_candles_uses_coinbase_fifteen_minute_granularity() -> None:
    gateway = CoinbaseGateway.__new__(CoinbaseGateway)
    seen = {}

    class Client:
        def get_candles(self, **kwargs):  # pragma: no cover - called through fake gateway.call
            return {}

    gateway.client = Client()

    def fake_call(fn, *args, **kwargs):
        seen.update(kwargs)
        # Return two old candles; filtering can safely keep them regardless of current time.
        return {"candles": [
            {"start": "1", "low": "1", "high": "2", "open": "1", "close": "2", "volume": "3"},
            {"start": "901", "low": "2", "high": "3", "open": "2", "close": "3", "volume": "4"},
        ]}

    gateway.call = fake_call  # type: ignore[method-assign]
    gateway.candles_15m("TEST-USD", bars=100)
    assert seen["granularity"] == "FIFTEEN_MINUTE"
    assert seen["limit"] == 100


def _shadow_cfg() -> dict:
    return {
        "enabled": True,
        "scan_interval_minutes": 15,
        "scan_offset_minute_utc": 3,
        "candle_bars": 349,
        "min_history_bars": 337,
        "lookback_bars": [24, 72, 168, 336],
        "weights": [0.15, 0.25, 0.35, 0.25],
        "ema_fast_bars": 24,
        "ema_slow_bars": 72,
        "ema_slope_lookback_bars": 6,
        "atr_bars": 14,
        "realized_vol_bars": 72,
        "top_percentile": 0.90,
        "persistence_scans": 3,
        "breakout_bars": 3,
        "max_one_bar_sigma": 2.5,
        "max_distance_atr": 2.0,
        "estimated_maker_fee_pct": 0.006,
        "estimated_taker_fee_pct": 0.012,
        "log_path": "logs/shadow_15m.log",
    }


def _settings(tmp_path: Path):
    return SimpleNamespace(
        project_root=tmp_path,
        shadow_15m=_shadow_cfg(),
        risk={
            "atr_stop_multiple": 2.0,
            "min_stop_pct": 0.04,
            "max_stop_pct": 0.07,
            "trail_atr_multiple": 2.5,
            "max_new_entries_per_24h": 2,
            "stopped_token_cooldown_hours": 8,
        },
        execution={
            "max_spread_pct": 0.0035,
            "max_maker_roundtrip_friction_pct": 0.025,
            "min_24h_return_to_maker_friction_multiple": 2.5,
        },
        regime={
            "btc_product": "BTC-USD",
            "breadth_risk_on": 0.50,
            "breadth_risk_on_exit": 0.45,
            "breadth_risk_off_enter": 0.35,
            "breadth_risk_off_exit": 0.40,
            "block_new_entries_in_risk_off": True,
        },
        rotation={
            "hold_rank_floor_percentile": 0.70,
            "replacement_min_percentile": 0.90,
            "replacement_score_gap": 0.15,
        },
    )


def _row(price: float = 100.0) -> SignalRow:
    return SignalRow(
        product_id="TEST-USD", price=price,
        r24=0.10, r72=0.15, r168=0.20, r336=0.25,
        vol72=0.01, ema_fast=95.0, ema_slow=90.0, ema_slope=0.01,
        atr=2.0, atr_pct=0.02, one_hour_return=0.01, one_hour_sigma=1.0,
        distance_atr=1.0, breakout=True, trend_pass=True,
        percentile=1.0, rank=1,
    )


def test_shadow_trade_stats_include_estimated_fees_and_stopouts(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "data" / "state.db")
    engine = Shadow15mEngine(SimpleNamespace(), db, _settings(tmp_path))
    events: list[str] = []
    now = datetime(2026, 9, 15, 12, 3, tzinfo=timezone.utc)
    position = engine._enter_position(_row(100.0), now, events)
    engine._exit_position(position, 96.0, "STOP", now.replace(minute=33), events)
    stats = db.shadow_stats()
    assert stats["trades"] == 1
    assert stats["stopouts"] == 1
    assert abs(stats["avg_gross"] + 0.04) < 1e-12
    assert stats["avg_net"] < stats["avg_gross"]
    assert any("SHADOW ENTRY" in e for e in events)
    assert any("SHADOW EXIT" in e for e in events)


def test_true_15m_indicator_uses_bar_periods() -> None:
    candles: list[Candle] = []
    price = 10.0
    for i in range(349):
        o = price
        # alternating increments create non-zero realized volatility while preserving trend
        price *= 1.002 if i % 2 == 0 else 1.001
        c = price
        candles.append(Candle(i * 900, min(o, c) * 0.999, max(o, c) * 1.001, o, c, 1000))
    row = build_signal_row_15m("TEST-USD", candles, _shadow_cfg())
    assert row is not None
    assert row.r24 > 0 and row.r72 > row.r24
    assert row.ema_fast > row.ema_slow


def test_in_between_live_scan_refreshes_quotes_without_changing_membership(tmp_path: Path) -> None:
    class Gateway:
        def best_bid_asks(self, product_ids=None):
            assert product_ids == ["TEST-USD"]
            return {"TEST-USD": (101.0, 103.0)}

    settings = SimpleNamespace()
    engine = StrategyEngine(Gateway(), StateDB(tmp_path / "state.db"), settings)
    engine._trading_products = {
        "TEST-USD": ProductMeta(
            product_id="TEST-USD", base_currency="TEST", quote_currency="USD", price=100.0,
            volume_24h_base=1.0, quote_volume_24h=2_000_000,
            base_increment="0.001", quote_increment="0.01", base_min_size=0.001,
            quote_min_size=1.0, best_bid=99.0, best_ask=101.0, spread_pct=0.02,
        )
    }
    engine._refresh_trading_quotes()
    meta = engine._trading_products["TEST-USD"]
    assert meta.best_bid == 101.0 and meta.best_ask == 103.0
    assert meta.price == 102.0
    assert list(engine._trading_products) == ["TEST-USD"]
