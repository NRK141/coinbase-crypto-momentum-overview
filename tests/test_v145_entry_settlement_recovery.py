from __future__ import annotations

from types import SimpleNamespace

import pytest

from crypto_momentum.db import StateDB
from crypto_momentum.execution import ExecutionEngine
from crypto_momentum.models import PositionState, ProductMeta, SignalRow


class LaggingGateway:
    def __init__(self, visible_after: int = 2):
        self.balance_calls = 0
        self.visible_after = visible_after
        self.stop_orders = []

    def balances(self):
        self.balance_calls += 1
        if self.balance_calls <= self.visible_after:
            return {"USD": {"total": 0.50, "available": 0.50, "hold": 0.0}}
        return {
            "USD": {"total": 0.50, "available": 0.50, "hold": 0.0},
            "XLM": {"total": 145.42199059, "available": 145.42199059, "hold": 0.0},
        }

    def place_stop_limit_sell(self, product_id, base_size, stop_price, limit_price, client_order_id):
        self.stop_orders.append((product_id, base_size, stop_price, limit_price, client_order_id))
        return {"success": True, "success_response": {"order_id": "stop-1"}}

    @staticmethod
    def extract_order_id(resp):
        return (resp.get("success_response") or {}).get("order_id")


class OrphanGateway:
    def __init__(self, product: ProductMeta):
        self.product = product
        self.stop_orders = []

    def balances(self):
        return {
            "USD": {"total": 0.50, "available": 0.50, "hold": 0.0},
            "XLM": {"total": 145.42199059, "available": 145.42199059, "hold": 0.0},
        }

    def get_product(self, product_id):
        assert product_id == "XLM-USD"
        return {"product_id": product_id, "price": "0.197", "base_currency_id": "XLM", "quote_currency_id": "USD"}

    def product_meta(self, _raw):
        return self.product

    def open_orders(self, product_id=None):
        return []

    def fills(self, product_id=None, order_id=None, limit=100):
        if order_id is not None and order_id != "maker-1":
            return []
        return [
            {
                "order_id": "maker-1",
                "product_id": "XLM-USD",
                "side": "BUY",
                "size": "145.42199059",
                "price": "0.196131",
                "commission": "0.1711",
                "trade_time": "2026-09-15T02:33:27+00:00",
            }
        ]

    def order(self, order_id):
        assert order_id == "maker-1"
        return {"order": {"order_id": order_id, "product_id": "XLM-USD", "side": "BUY", "status": "FILLED"}}

    def place_stop_limit_sell(self, product_id, base_size, stop_price, limit_price, client_order_id):
        self.stop_orders.append((product_id, base_size, stop_price, limit_price, client_order_id))
        return {"success": True, "success_response": {"order_id": "stop-recovered"}}

    @staticmethod
    def extract_order_id(resp):
        return (resp.get("success_response") or {}).get("order_id")


def settings(*, settlement_wait=0.1):
    return SimpleNamespace(
        is_live=True,
        execution={
            "entry_balance_settlement_wait_seconds": settlement_wait,
            "entry_protection_wait_seconds": 0.1,
            "orphan_recovery_max_age_hours": 24.0,
        },
        risk={
            "stop_limit_buffer_pct": 0.015,
            "atr_stop_multiple": 2.0,
            "min_stop_pct": 0.04,
            "max_stop_pct": 0.07,
            "dust_value_usd": 1.0,
        },
        signal={"top_percentile": 0.90},
        strategy={"cash_equivalent_symbols": ["USDC"]},
    )


def product():
    return ProductMeta(
        product_id="XLM-USD",
        base_currency="XLM",
        quote_currency="USD",
        price=0.196131,
        volume_24h_base=1_000_000,
        quote_volume_24h=10_000_000,
        base_increment="0.00000001",
        quote_increment="0.000001",
        base_min_size=0.01,
        quote_min_size=1.0,
    )


def signal_row():
    return SignalRow(
        product_id="XLM-USD",
        price=0.196131,
        r24=0.095,
        r72=0.087,
        r168=0.0139,
        r336=0.1013,
        vol72=0.02,
        ema_fast=0.190,
        ema_slow=0.185,
        ema_slope=0.001,
        atr=0.00318,
        atr_pct=0.0162,
        one_hour_return=0.01,
        one_hour_sigma=1.0,
        distance_atr=1.0,
        breakout=True,
        trend_pass=True,
        raw_score=1.0,
        percentile=0.981,
        rank=2,
    )


def test_confirmed_fill_survives_initial_zero_balance_and_gets_protected(tmp_path, monkeypatch):
    db = StateDB(tmp_path / "state.db")
    db.set_json(
        "pending_entry",
        {"product_id": "XLM-USD", "base_currency": "XLM", "stop_pct": 0.04, "order_ids": ["maker-1"]},
    )
    g = LaggingGateway(visible_after=1)
    engine = ExecutionEngine(g, db, settings(settlement_wait=1.0))
    monkeypatch.setattr(engine, "_aggregate_orders", lambda _ids: (145.42199059, 0.196131, 0.1711))

    state = engine._finalize_entry(signal_row(), product(), 0.04, ["maker-1"], {"path": "MAKER"})

    assert state is not None
    assert state.quantity == pytest.approx(145.42199059)
    assert state.stop_order_id == "stop-1"
    assert db.get_position() is not None
    assert db.get_json("pending_entry", "missing") is None
    assert g.balance_calls >= 3
    assert len(g.stop_orders) == 1
    assert db.recent_trades(10)[0]["event"] == "PROTECTION_PLACED"


def test_confirmed_fill_timeout_keeps_position_and_pending_intent(tmp_path, monkeypatch):
    db = StateDB(tmp_path / "state.db")
    db.set_json(
        "pending_entry",
        {"product_id": "XLM-USD", "base_currency": "XLM", "stop_pct": 0.04, "order_ids": ["maker-1"]},
    )
    g = LaggingGateway(visible_after=10_000)
    engine = ExecutionEngine(g, db, settings(settlement_wait=0.0))
    monkeypatch.setattr(engine, "_aggregate_orders", lambda _ids: (145.42199059, 0.196131, 0.1711))

    with pytest.raises(RuntimeError, match="has not appeared in Coinbase balances"):
        engine._finalize_entry(signal_row(), product(), 0.04, ["maker-1"], {"path": "MAKER"})

    state = db.get_position()
    assert state is not None
    assert state.product_id == "XLM-USD"
    pending = db.get_json("pending_entry")
    assert pending["confirmed_fill_qty"] == pytest.approx(145.42199059)
    assert pending["stage"] == "FILL_CONFIRMED_WAITING_BALANCE"
    assert db.get_json("manual_halt") is True


def test_explicit_orphan_recovery_matches_recent_fill_and_places_stop(tmp_path, monkeypatch):
    db = StateDB(tmp_path / "state.db")
    row = signal_row()
    db.log_scan("RISK_ON", 0.56, [row])
    g = OrphanGateway(product())
    engine = ExecutionEngine(g, db, settings(settlement_wait=0.0))

    # The fixture uses a fixed historical fill timestamp; scan recency is the stronger guard in
    # this unit test, so skip fill-time age filtering by returning None for that one timestamp.
    original_parse = engine._parse_exchange_time
    def parse_time(value):
        if str(value).startswith("2026-09-15T02:33:27"):
            return None
        return original_parse(value)
    monkeypatch.setattr(engine, "_parse_exchange_time", parse_time)

    state = engine.recover_recent_orphan_position()

    assert state.product_id == "XLM-USD"
    assert state.quantity == pytest.approx(145.42199059)
    assert state.entry_price == pytest.approx(0.196131)
    assert state.initial_risk_pct == pytest.approx(0.04)
    assert state.stop_order_id == "stop-recovered"
    assert len(g.stop_orders) == 1
    events = [x["event"] for x in db.recent_trades(10)]
    assert "ENTRY_RECOVERED_ORPHAN" in events
    assert "PROTECTION_PLACED" in events


def test_reconcile_does_not_clear_confirmed_pending_fill_during_balance_lag(tmp_path, monkeypatch):
    db = StateDB(tmp_path / "state.db")
    state = PositionState(
        product_id="XLM-USD",
        base_currency="XLM",
        quantity=145.42199059,
        entry_price=0.196131,
        entry_time="2026-09-15T02:33:27+00:00",
        initial_stop_price=0.18828576,
        stop_price=0.18828576,
        stop_order_id=None,
        highest_close=0.196131,
        initial_risk_pct=0.04,
        entry_fees=0.1711,
        entry_order_id="maker-1",
    )
    db.set_position(state)
    db.set_json(
        "pending_entry",
        {"product_id": "XLM-USD", "base_currency": "XLM", "stop_pct": 0.04, "order_ids": ["maker-1"]},
    )
    g = LaggingGateway(visible_after=10_000)
    engine = ExecutionEngine(g, db, settings(settlement_wait=0.0))
    monkeypatch.setattr(engine, "_aggregate_orders", lambda _ids: (145.42199059, 0.196131, 0.1711))

    reconciled = engine.reconcile()

    assert reconciled is not None
    assert reconciled.product_id == "XLM-USD"
    assert db.get_position() is not None
    assert db.get_json("pending_entry") is not None
