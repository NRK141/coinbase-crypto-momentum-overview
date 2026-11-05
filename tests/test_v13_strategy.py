from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

# The production environment installs Coinbase's official SDK. The build container used for
# offline unit tests may not have it, so provide a tiny import-only stub when absent.
try:
    import coinbase.rest  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    coinbase = types.ModuleType("coinbase")
    rest = types.ModuleType("coinbase.rest")
    class RESTClient:  # pragma: no cover - import shim only
        pass
    rest.RESTClient = RESTClient
    coinbase.rest = rest
    sys.modules["coinbase"] = coinbase
    sys.modules["coinbase.rest"] = rest

from crypto_momentum.coinbase_client import CoinbaseGateway
from crypto_momentum.db import StateDB
from crypto_momentum.strategy import StrategyEngine


class ProductMetaGateway:
    product_meta = staticmethod(CoinbaseGateway.product_meta)


def minimal_settings():
    return SimpleNamespace(
        strategy={"quote_currency": "USD"},
        universe={
            "stablecoin_symbols": ["USDC"],
            "excluded_symbols": [],
            "require_spot": True,
            "min_history_hours": 337,
            "refresh_minutes": 30,
        },
        regime_universe={"min_history_hours": 337, "refresh_minutes": 120},
        regime={
            "btc_product": "BTC-USD",
            "breadth_risk_off_enter": 0.35,
            "breadth_risk_off_exit": 0.40,
            "breadth_risk_on": 0.50,
            "breadth_risk_on_exit": 0.45,
            "block_new_entries_in_risk_off": True,
        },
    )


def test_regime_hysteresis_prevents_boundary_flip(tmp_path: Path):
    db = StateDB(tmp_path / "state.db")
    engine = StrategyEngine(ProductMetaGateway(), db, minimal_settings())

    regime, _ = engine._determine_regime(0.30, btc_above=False)
    assert regime == "RISK_OFF"

    # A recovery above the 35% entry boundary is not enough to exit risk-off.
    regime, previous = engine._determine_regime(0.375, btc_above=False)
    assert previous == "RISK_OFF"
    assert regime == "RISK_OFF"

    # Breadth alone is not enough; BTC must also be healthy.
    regime, previous = engine._determine_regime(0.40, btc_above=False)
    assert previous == "RISK_OFF"
    assert regime == "RISK_OFF"

    # BTC + 40% breadth can exit only to NEUTRAL.
    regime, previous = engine._determine_regime(0.40, btc_above=True)
    assert previous == "RISK_OFF"
    assert regime == "NEUTRAL"


def test_dynamic_universe_filters_volume_spread_and_listing_age(tmp_path: Path):
    db = StateDB(tmp_path / "state.db")
    engine = StrategyEngine(ProductMetaGateway(), db, minimal_settings())
    old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat().replace("+00:00", "Z")
    recent = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat().replace("+00:00", "Z")

    def product(pid: str, qv: float, bid: float, ask: float, new_at: str = old):
        return {
            "product_id": pid,
            "price": str((bid + ask) / 2),
            "volume_24h": "1000000",
            "approximate_quote_24h_volume": str(qv),
            "base_currency_id": pid.split("-")[0],
            "quote_currency_id": "USD",
            "product_type": "SPOT",
            "status": "ONLINE",
            "best_bid_price": str(bid),
            "best_ask_price": str(ask),
            "new_at": new_at,
        }

    raw = [
        product("GOOD-USD", 2_000_000, 100.00, 100.10),
        product("WIDE-USD", 5_000_000, 100.00, 102.00),
        product("NEW-USD", 5_000_000, 100.00, 100.10, recent),
        product("LOW-USD", 100_000, 100.00, 100.10),
    ]
    selected, stats, _ = engine._select_universe(
        raw,
        {
            "min_quote_volume_24h_usd": 1_000_000,
            "max_spread_pct": 0.005,
            "max_products": 80,
            "min_history_hours": 337,
        },
    )
    assert set(selected) == {"GOOD-USD"}
    assert stats["spread"] == 1
    assert stats["history_age"] == 1
    assert stats["volume"] == 1


def test_product_meta_computes_top_of_book_spread_and_listing_time():
    p = {
        "product_id": "ABC-USD",
        "price": "100.05",
        "volume_24h": "20000",
        "approximate_quote_24h_volume": "2000000",
        "base_currency_id": "ABC",
        "quote_currency_id": "USD",
        "best_bid_price": "100.00",
        "best_ask_price": "100.10",
        "new_at": "2025-01-01T00:00:00Z",
    }
    meta = CoinbaseGateway.product_meta(p)
    assert meta is not None
    assert meta.best_bid == 100.00
    assert meta.best_ask == 100.10
    assert 0 < meta.spread_pct < 0.002
    assert meta.new_at == "2025-01-01T00:00:00Z"
