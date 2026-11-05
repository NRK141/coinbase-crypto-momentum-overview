from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pytest

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

from crypto_momentum.execution import ExecutionEngine
from crypto_momentum.models import PositionState, ProductMeta, SignalRow
from crypto_momentum.runner import BotRunner


class MemoryDB:
    def __init__(self, state: PositionState | None = None):
        self.state = state

    def set_position(self, state):
        self.state = state

    def get_position(self):
        return self.state


class NoopGateway:
    pass


def settings(*, live: bool = False):
    return SimpleNamespace(
        is_live=live,
        run_mode="live" if live else "dry_run",
        risk={
            "stop_limit_buffer_pct": 0.015,
            "trail_atr_multiple": 2.5,
            "trail_activation_r": 1.0,
            "profit_lock_activation_pct": 0.05,
            "profit_lock_min_trigger_pct": 0.035,
            "profit_lock_min_limit_pct": 0.025,
            "profit_capture_ratio": 0.65,
            "profit_capture_limit_gap_pct": 0.01,
        },
    )


def state(*, high=100.0, stop=95.0, risk=0.04):
    return PositionState(
        product_id="TEST-USD",
        base_currency="TEST",
        quantity=1.0,
        entry_price=100.0,
        entry_time="2026-09-22T00:00:00+00:00",
        initial_stop_price=100.0 * (1.0 - risk),
        stop_price=stop,
        stop_order_id="stop-1",
        highest_close=high,
        initial_risk_pct=risk,
    )


def row(*, price=105.0, atr=2.0):
    return SignalRow(
        product_id="TEST-USD",
        price=price,
        r24=0.10,
        r72=0.20,
        r168=0.30,
        r336=0.40,
        vol72=0.02,
        ema_fast=95.0,
        ema_slow=90.0,
        ema_slope=0.01,
        atr=atr,
        atr_pct=atr / price,
        one_hour_return=0.0,
        one_hour_sigma=0.0,
        distance_atr=0.0,
        breakout=True,
        trend_pass=True,
    )


def product():
    return ProductMeta(
        product_id="TEST-USD",
        base_currency="TEST",
        quote_currency="USD",
        price=105.0,
        volume_24h_base=1_000_000,
        quote_volume_24h=10_000_000,
        base_increment="0.0001",
        quote_increment="0.01",
        base_min_size=0.0001,
        quote_min_size=1.0,
    )


def test_profit_lock_is_off_below_five_percent_mfe():
    e = ExecutionEngine(NoopGateway(), MemoryDB(), settings())
    levels = e.profit_lock_levels(state(high=104.99))
    assert levels["armed"] is False
    assert levels["trigger_price"] == 0.0


def test_five_percent_mfe_arms_fee_floor_at_3_5_and_limit_at_2_5():
    e = ExecutionEngine(NoopGateway(), MemoryDB(), settings())
    levels = e.profit_lock_levels(state(high=105.0))
    assert levels["armed"] is True
    assert levels["mfe_return"] == pytest.approx(0.05)
    assert levels["protected_return"] == pytest.approx(0.035)
    assert levels["trigger_price"] == pytest.approx(103.5)
    assert levels["limit_return"] == pytest.approx(0.025)
    assert levels["limit_price"] == pytest.approx(102.5)


def test_capture_ratchet_protects_65_percent_of_ten_percent_mfe():
    e = ExecutionEngine(NoopGateway(), MemoryDB(), settings())
    levels = e.profit_lock_levels(state(high=110.0))
    assert levels["armed"] is True
    assert levels["protected_return"] == pytest.approx(0.065)
    assert levels["trigger_price"] == pytest.approx(106.5)
    assert levels["limit_return"] == pytest.approx(0.055)
    assert levels["limit_price"] == pytest.approx(105.5)


def test_profit_limit_floor_overrides_legacy_1_5_percent_buffer_when_needed():
    e = ExecutionEngine(NoopGateway(), MemoryDB(), settings())
    s = state(high=107.45, stop=104.8425)
    # Legacy 1.5% buffer would be ~103.27. V1.4.10 requires about +3.84% here.
    assert e._protective_limit_raw(s) == pytest.approx(103.8425, abs=1e-6)


def test_capture_floor_tightens_medium_winner_without_replacing_chandelier_logic():
    db = MemoryDB()
    e = ExecutionEngine(NoopGateway(), db, settings())
    s = state(high=100.0, stop=96.0, risk=0.04)
    db.state = s
    # New mark creates +7.45% MFE. ATR is deliberately large enough that Chandelier is loose.
    proposed, levels, high = e._proposed_trailing_stop(s, row(price=107.45, atr=4.0))
    expected = 100.0 * (1.0 + 0.65 * 0.0745)
    assert high == pytest.approx(107.45)
    assert levels["armed"] is True
    assert proposed == pytest.approx(expected)


def test_existing_chandelier_wins_when_it_is_tighter_than_capture_floor():
    db = MemoryDB()
    e = ExecutionEngine(NoopGateway(), db, settings())
    s = state(high=100.0, stop=96.0, risk=0.04)
    db.state = s
    # At +25% MFE, 65% capture is +16.25%, while the Chandelier is 125 - 2.5*2 = 120 (+20%).
    proposed, levels, high = e._proposed_trailing_stop(s, row(price=125.0, atr=2.0))
    assert high == pytest.approx(125.0)
    assert levels["armed"] is True
    assert proposed == pytest.approx(120.0)


class BackstopExec:
    def __init__(self, position):
        self.position = position
        self.exits = []
        self.trailed = False

    def reconcile(self):
        return self.position

    def risk_halts(self, products):
        return False, ""

    def ensure_protection(self, state, product):
        return state

    def profit_lock_levels(self, state, observed_price=None):
        e = ExecutionEngine(NoopGateway(), MemoryDB(), settings(live=True))
        return e.profit_lock_levels(state, observed_price=observed_price)

    def exit(self, state, product, reason):
        self.exits.append(reason)
        return True

    def trail_stop(self, state, signal, product_meta):
        self.trailed = True
        return state


class BackstopStrategy:
    def __init__(self, signal, product_meta):
        self.signal = signal
        self.product_meta = product_meta

    def position_monitor(self, product_id):
        return self.signal, self.product_meta


def test_5m_backstop_enforces_persisted_capture_floor_after_restart_or_deferred_replace():
    # Persisted +10% high means the capture floor is +6.5% (106.5), but exchange stop is only 102.
    s = state(high=110.0, stop=102.0)
    r = BotRunner.__new__(BotRunner)
    r.db = MemoryDB(s)
    r.exec = BackstopExec(s)
    r.strategy = BackstopStrategy(row(price=105.0, atr=2.0), product())
    r.s = SimpleNamespace(is_live=True, run_mode="live")

    r.position_check_once()

    assert r.exec.exits == ["PROFIT_LOCK_5M_BACKSTOP"]
    assert r.exec.trailed is False
