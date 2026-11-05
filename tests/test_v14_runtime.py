from __future__ import annotations

import sys
import types
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
    websocket = types.ModuleType("coinbase.websocket")
    class WSClient:  # pragma: no cover
        pass
    websocket.WSClient = WSClient
    coinbase.websocket = websocket
    sys.modules["coinbase"] = coinbase
    sys.modules["coinbase.rest"] = rest
    sys.modules["coinbase.websocket"] = websocket


try:
    import coinbase.websocket  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    websocket = types.ModuleType("coinbase.websocket")
    class WSClient:  # pragma: no cover
        pass
    websocket.WSClient = WSClient
    sys.modules["coinbase.websocket"] = websocket

from crypto_momentum.db import StateDB
from crypto_momentum.indicators import build_signal_row
from crypto_momentum.models import Candle, ProductMeta
from crypto_momentum.strategy import StrategyEngine


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


def candles_with_flat_tail(n: int = 349) -> list[Candle]:
    out: list[Candle] = []
    price = 50.0
    for i in range(n):
        o = price
        price *= 1.001
        c = price
        out.append(Candle(i * 3600, min(o, c) * 0.999, max(o, c) * 1.001, o, c, 1000))
    # Make the last few completed highs slightly above the completed close so the
    # completed-bar signal is not a breakout, while a fresh intrahour price can be.
    for c in out[-3:]:
        c.high = out[-1].close * 1.01
    return out


def test_live_price_overlay_can_create_half_hour_breakout_without_mutating_hourly_atr():
    cs = candles_with_flat_tail()
    completed = build_signal_row("TEST-USD", cs, cfg())
    live = build_signal_row("TEST-USD", cs, cfg(), current_price=cs[-1].high * 1.002)
    assert completed is not None and live is not None
    assert completed.breakout is False
    assert live.breakout is True
    # ATR is still computed from completed hourly candles; only its percentage denominator
    # changes with the fresh mark.
    assert abs(completed.atr - live.atr) < 1e-12
    assert live.price > completed.price


class MonitorGateway:
    def __init__(self, candles: list[Candle]):
        self._candles = candles

    def get_product(self, product_id: str):
        return {
            "product_id": product_id,
            "base_currency_id": "TEST",
            "quote_currency_id": "USD",
            "price": "110.00",
            "best_bid_price": "109.50",
            "best_ask_price": "110.50",
            "volume_24h": "1000000",
            "approximate_quote_24h_volume": "2000000",
            "base_increment": "0.0001",
            "quote_increment": "0.01",
            "base_min_size": "0.0001",
            "quote_min_size": "1",
        }

    @staticmethod
    def product_meta(p):
        bid = float(p["best_bid_price"])
        ask = float(p["best_ask_price"])
        return ProductMeta(
            product_id=p["product_id"],
            base_currency=p["base_currency_id"],
            quote_currency=p["quote_currency_id"],
            price=float(p["price"]),
            volume_24h_base=float(p["volume_24h"]),
            quote_volume_24h=float(p["approximate_quote_24h_volume"]),
            base_increment=p["base_increment"],
            quote_increment=p["quote_increment"],
            base_min_size=float(p["base_min_size"]),
            quote_min_size=float(p["quote_min_size"]),
            best_bid=bid,
            best_ask=ask,
            spread_pct=(ask-bid)/((ask+bid)/2),
        )

    def candles(self, product_id: str, hours: int = 349):
        return self._candles[-hours:]


def test_position_monitor_uses_sell_side_best_bid(tmp_path: Path):
    cs = candles_with_flat_tail()
    settings = SimpleNamespace(
        universe={"min_history_hours": 337},
        signal=cfg(),
    )
    db = StateDB(tmp_path / "state.db")
    engine = StrategyEngine(MonitorGateway(cs), db, settings)
    row, product = engine.position_monitor("TEST-USD")
    assert product is not None and row is not None
    assert row.price == 109.50


def test_v147_runtime_defaults_are_15m_signal_and_5m_position_check():
    # Configuration-level regression test: these are the requested production cadences.
    import yaml
    root = Path(__file__).resolve().parents[1]
    data = yaml.safe_load((root / "config" / "strategy.yaml").read_text())
    assert data["runtime"]["signal_scan_interval_minutes"] == 15
    assert data["runtime"]["position_exit_check_minutes"] == 5
    assert data["universe"]["refresh_minutes"] == 30
    assert data["regime_universe"]["refresh_minutes"] == 120

from crypto_momentum.models import PositionState, SignalRow
from crypto_momentum.runner import BotRunner


class TinyDB:
    def __init__(self, state):
        self.state = state
        self.values = {}
    def get_position(self):
        return self.state
    def set_json(self, key, value):
        self.values[key] = value


class FakeExec:
    def __init__(self, state):
        self.state = state
        self.exits = []
        self.trailed = False
    def reconcile(self):
        return self.state
    def risk_halts(self, products):
        return False, ""
    def ensure_protection(self, state, product):
        return state
    def profit_lock_levels(self, state, observed_price=None):
        return {
            "armed": False,
            "mfe_return": 0.0,
            "protected_return": 0.0,
            "trigger_price": 0.0,
            "limit_return": 0.0,
            "limit_price": 0.0,
        }
    def exit(self, state, product, reason):
        self.exits.append(reason)
        return True
    def trail_stop(self, state, row, product):
        self.trailed = True
        return state


class FakeStrategy:
    def __init__(self, row, product):
        self.row = row
        self.product = product
    def position_monitor(self, product_id):
        return self.row, self.product


def _state(stop=100.0):
    return PositionState(
        product_id="TEST-USD", base_currency="TEST", quantity=1.0, entry_price=105.0,
        entry_time="2026-09-13T00:00:00+00:00", initial_stop_price=100.0,
        stop_price=stop, stop_order_id="stop-1", highest_close=110.0,
        initial_risk_pct=0.05,
    )


def _product():
    return ProductMeta(
        product_id="TEST-USD", base_currency="TEST", quote_currency="USD", price=101.0,
        volume_24h_base=1_000_000, quote_volume_24h=10_000_000,
        base_increment="0.0001", quote_increment="0.01", base_min_size=0.0001,
        quote_min_size=1.0, best_bid=100.5, best_ask=101.0, spread_pct=0.005,
    )


def _row(price: float, ema_fast: float = 95.0, r24: float = 0.10):
    return SignalRow(
        product_id="TEST-USD", price=price, r24=r24, r72=0.2, r168=0.3, r336=0.4,
        vol72=0.02, ema_fast=ema_fast, ema_slow=90.0, ema_slope=0.01,
        atr=2.0, atr_pct=0.02, one_hour_return=0.0, one_hour_sigma=0.0,
        distance_atr=0.0, breakout=False, trend_pass=True,
    )


def _runner_for_monitor(row: SignalRow, state: PositionState):
    r = BotRunner.__new__(BotRunner)
    r.db = TinyDB(state)
    r.exec = FakeExec(state)
    r.strategy = FakeStrategy(row, _product())
    r.s = SimpleNamespace(is_live=True, run_mode="live")
    return r


def test_5m_monitor_uses_market_exit_backstop_at_stop_trigger():
    r = _runner_for_monitor(_row(99.5), _state(stop=100.0))
    r.position_check_once()
    assert r.exec.exits == ["FIVE_MIN_STOP_BACKSTOP"]
    assert r.exec.trailed is False


def test_5m_monitor_exits_intrahour_trend_failure():
    r = _runner_for_monitor(_row(94.0, ema_fast=95.0, r24=-0.01), _state(stop=90.0))
    r.position_check_once()
    assert r.exec.exits == ["TREND_FAILURE_5M"]
    assert r.exec.trailed is False


def test_live_overlay_does_not_erase_last_completed_hour_anti_chase_move():
    cs = candles_with_flat_tail()
    # Force the last completed hour to be a large positive move.
    cs[-2].close = cs[-1].close / 1.08
    cs[-2].high = max(cs[-2].high, cs[-2].close)
    row = build_signal_row("TEST-USD", cs, cfg(), current_price=cs[-1].close * 1.0001)
    assert row is not None
    assert row.one_hour_return > 0.07
