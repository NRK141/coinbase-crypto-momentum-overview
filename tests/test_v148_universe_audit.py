from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta, timezone
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

from crypto_momentum.coinbase_client import CoinbaseGateway
from crypto_momentum.db import StateDB
from crypto_momentum.models import SignalRow
from crypto_momentum.shadow import Shadow15mEngine, ShadowScanResult
from crypto_momentum.strategy import CandidateDiagnostic, StrategyEngine


def test_products_explicitly_paginates_complete_coinbase_spot_catalog() -> None:
    gateway = CoinbaseGateway.__new__(CoinbaseGateway)
    calls: list[dict] = []

    class Client:
        def get_products(self, **kwargs):
            calls.append(dict(kwargs))
            offset = int(kwargs.get("offset") or 0)
            if offset == 0:
                return {
                    "products": [{"product_id": "AAA-USD"}, {"product_id": "BBB-USD"}],
                    "num_products": 3,
                    "pagination": {"has_next": True, "next_cursor": "ignored-by-sdk-wrapper"},
                }
            assert offset == 2
            return {
                "products": [{"product_id": "CCC-USD"}, {"product_id": "AAA-USD"}],
                "num_products": 3,
                "pagination": {"has_next": False, "next_cursor": ""},
            }

    gateway.client = Client()
    gateway.call = lambda fn, *args, **kwargs: fn(*args, **kwargs)  # type: ignore[method-assign]
    products = gateway.products()
    assert [p["product_id"] for p in products] == ["AAA-USD", "BBB-USD", "CCC-USD"]
    assert len(calls) == 2
    assert calls[0]["product_type"] == "SPOT"
    assert calls[0]["get_all_products"] is False
    assert calls[0]["offset"] == 0
    assert calls[1]["offset"] == 2
    info = gateway.product_catalog_info()
    assert info["pages"] == 2
    assert info["unique_products"] == 3
    assert info["reported_num_products"] == 3
    assert info["pagination_complete"] is True
    assert info["pagination_mode"] == "offset"


def _settings(tmp_path: Path):
    return SimpleNamespace(
        project_root=tmp_path,
        strategy={"quote_currency": "USD"},
        universe={
            "min_quote_volume_24h_usd": 1_000_000,
            "max_spread_pct": 0.005,
            "max_products": 80,
            "stablecoin_symbols": ["USDC", "USDT", "DAI", "PYUSD", "EURC", "GUSD", "USDS"],
            "excluded_symbols": [],
            "require_spot": True,
            "min_history_hours": 337,
        },
        universe_research={
            "enabled": True,
            "log_path": "logs/universe_audit.log",
            "volume_tiers_usd": [1_000_000, 500_000, 250_000],
            "spread_tiers_pct": [0.005, 0.0075, 0.010],
            "history_tiers_hours": [337, 168, 72],
            "top_rejected_examples": 10,
        },
    )


def _product(pid: str, qv: float, bid: float, ask: float, quote: str = "USD") -> dict:
    old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat().replace("+00:00", "Z")
    base = pid.split("-")[0]
    return {
        "product_id": pid,
        "price": str((bid + ask) / 2),
        "volume_24h": "1000000",
        "approximate_quote_24h_volume": str(qv),
        "base_currency_id": base,
        "quote_currency_id": quote,
        "product_type": "SPOT",
        "status": "ONLINE",
        "best_bid_price": str(bid),
        "best_ask_price": str(ask),
        "new_at": old,
    }


class AuditGateway:
    product_meta = staticmethod(CoinbaseGateway.product_meta)
    def product_catalog_info(self):
        return {
            "pages": 2,
            "unique_products": 5,
            "reported_num_products": 5,
            "pagination_complete": True,
            "pagination_mode": "cursor",
        }


def test_universe_audit_identifies_volume_floor_and_quote_scope_without_changing_live_rules(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "data" / "state.db")
    engine = StrategyEngine(AuditGateway(), db, _settings(tmp_path))
    raw = [
        _product("AAA-USD", 2_000_000, 100.0, 100.1),
        _product("BBB-USD", 750_000, 100.0, 100.1),
        _product("CCC-USD", 300_000, 100.0, 100.1),
        _product("WIDE-USD", 2_000_000, 100.0, 100.6),
        _product("ONLY-USDC", 2_000_000, 100.0, 100.1, quote="USDC"),
    ]
    audit = engine._build_universe_audit(raw)
    assert audit["current_eligible_before_cap"] == 1
    assert audit["current_selected_after_cap"] == 1
    assert audit["cap_binding"] is False
    assert audit["volume_counts"]["1000000"] == 1
    assert audit["volume_counts"]["500000"] == 2
    assert audit["volume_counts"]["250000"] == 3
    assert audit["spread_counts"]["0.007500"] == 2  # WIDE joins only when spread is relaxed
    assert audit["usdc_current_quality_bases"] == 1
    assert audit["usdc_quality_bases_not_in_usd"] == 1
    rejected = [x["product_id"] for x in audit["top_volume_rejects"]]
    assert rejected[:2] == ["BBB-USD", "CCC-USD"]
    rendered = engine._render_universe_audit(audit)
    assert "OBSERVATION ONLY" in rendered
    assert "Volume counterfactual" in rendered
    assert "cap_binding=False" in rendered


def test_shadow_log_now_exposes_final_rejection_reasons(tmp_path: Path) -> None:
    settings = SimpleNamespace(
        project_root=tmp_path,
        shadow_15m={"enabled": True, "log_path": "logs/shadow_15m.log"},
    )
    db = StateDB(tmp_path / "data" / "state.db")
    engine = Shadow15mEngine(SimpleNamespace(), db, settings)
    row = SignalRow(
        product_id="TEST-USD", price=100.0,
        r24=0.06, r72=0.08, r168=0.10, r336=0.12,
        vol72=0.01, ema_fast=95.0, ema_slow=90.0, ema_slope=0.01,
        atr=4.0, atr_pct=0.04, one_hour_return=0.01, one_hour_sigma=1.0,
        distance_atr=2.5, breakout=True, trend_pass=True, percentile=1.0, rank=1,
    )
    diag = CandidateDiagnostic(
        product_id="TEST-USD", rank=1, percentile=1.0, persistence=4, persistence_required=3,
        raw_stop_pct=0.08, effective_stop_pct=0.08, rank_pass=True, trend_pass=True,
        persistence_pass=True, breakout_pass=True, sigma_pass=True, distance_pass=False,
        stop_pass=False, cooldown_pass=True, regime_pass=True, actionable=False,
        reasons=["EMA_DISTANCE", "STOP 8.0%>7.0%"],
    )
    result = ShadowScanResult(
        rows=[row], regime="RISK_ON", previous_regime="RISK_ON", breadth=0.8,
        candidates=[], diagnostics=[diag], persistence_counts={"TEST-USD": 4},
        position=None, events=[], funnel={"rank": 1, "trend": 1, "persistence": 1, "breakout": 1, "actionable": 0},
        universe_size=1,
    )
    rendered = engine.render(result, 1)
    assert "Shadow candidate diagnostics" in rendered
    assert "EMA_DISTANCE" in rendered
    assert "STOP 8.0%>7.0%" in rendered
    assert "Rejection counts:" in rendered
