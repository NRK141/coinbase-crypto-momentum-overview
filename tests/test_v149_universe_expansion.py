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

import yaml

from crypto_momentum.coinbase_client import CoinbaseGateway
from crypto_momentum.db import StateDB
from crypto_momentum.strategy import StrategyEngine


def test_v149_config_expands_live_and_shadow_universes_without_touching_regime() -> None:
    root = Path(__file__).resolve().parents[1]
    data = yaml.safe_load((root / "config" / "strategy.yaml").read_text(encoding="utf-8"))

    live = data["universe"]
    assert live["min_quote_volume_24h_usd"] == 500_000
    assert live["max_products"] == 120
    assert live["cache_revision"] == 2

    shadow = data["shadow_universe"]
    assert shadow["min_quote_volume_24h_usd"] == 250_000
    assert shadow["max_products"] == 160
    assert shadow["max_spread_pct"] == live["max_spread_pct"]
    assert shadow["min_history_hours"] == live["min_history_hours"]

    regime = data["regime_universe"]
    assert regime["min_quote_volume_24h_usd"] == 1_000_000
    assert regime["max_products"] == 30


def _product(pid: str, qv: float) -> dict:
    old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat().replace("+00:00", "Z")
    base = pid.split("-")[0]
    return {
        "product_id": pid,
        "price": "100.05",
        "volume_24h": "10000",
        "approximate_quote_24h_volume": str(qv),
        "base_currency_id": base,
        "quote_currency_id": "USD",
        "product_type": "SPOT",
        "status": "ONLINE",
        "best_bid_price": "100.00",
        "best_ask_price": "100.10",
        "new_at": old,
    }


class Gateway:
    product_meta = staticmethod(CoinbaseGateway.product_meta)

    def __init__(self) -> None:
        self.rows = [
            _product("BTC-USD", 20_000_000),
            _product("AAA-USD", 2_000_000),
            _product("BBB-USD", 750_000),
            _product("CCC-USD", 300_000),
            _product("DDD-USD", 200_000),
        ]
        self.books = {row["product_id"]: (100.0, 100.1) for row in self.rows}

    def products(self):
        return list(self.rows)

    def best_bid_asks(self, product_ids=None):
        if product_ids is None:
            return dict(self.books)
        return {pid: self.books[pid] for pid in product_ids if pid in self.books}


def _settings(tmp_path: Path):
    return SimpleNamespace(
        project_root=tmp_path,
        strategy={"quote_currency": "USD"},
        universe={
            "refresh_minutes": 30,
            "min_quote_volume_24h_usd": 500_000,
            "max_spread_pct": 0.005,
            "max_products": 120,
            "cache_revision": 2,
            "stablecoin_symbols": ["USDC", "USDT"],
            "excluded_symbols": [],
            "require_spot": True,
            "min_history_hours": 337,
        },
        shadow_universe={
            "refresh_minutes": 30,
            "min_quote_volume_24h_usd": 250_000,
            "max_spread_pct": 0.005,
            "max_products": 160,
            "min_history_hours": 337,
            "cache_revision": 1,
        },
        regime_universe={
            "refresh_minutes": 120,
            "min_quote_volume_24h_usd": 1_000_000,
            "max_spread_pct": 0.005,
            "max_products": 30,
            "min_history_hours": 337,
            "cache_revision": 2,
        },
        regime={"btc_product": "BTC-USD"},
        universe_research={"enabled": False},
    )


def test_refresh_builds_distinct_live_shadow_and_regime_memberships_and_resets_persistence(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.db")
    db.set_json("persistence_counts", {"AAA-USD": 99})
    db.set_json("persistence_scan_bucket", "old")
    db.set_json("shadow_15m_persistence_counts", {"AAA-USD": 99})
    db.set_json("shadow_15m_persistence_bucket", "old")

    gateway = Gateway()
    engine = StrategyEngine(gateway, db, _settings(tmp_path))
    info = engine.refresh_universes(force_trading=True, force_regime=True)

    live, shadow, regime = engine.universe_snapshot()
    assert set(live) == {"BTC-USD", "AAA-USD", "BBB-USD"}
    assert set(shadow) == {"BTC-USD", "AAA-USD", "BBB-USD", "CCC-USD"}
    assert set(regime) == {"BTC-USD", "AAA-USD"}
    assert "DDD-USD" not in shadow
    assert info["trading_count"] == 3
    assert info["shadow_count"] == 4
    assert info["shadow_stats"]["volume"] == 1
    assert db.get_json("persistence_counts", None) == {}
    assert db.get_json("shadow_15m_persistence_counts", None) == {}


def test_shadow_quote_refresh_updates_broader_shadow_member(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.db")
    gateway = Gateway()
    engine = StrategyEngine(gateway, db, _settings(tmp_path))
    engine.refresh_universes(force_trading=True, force_regime=True)

    gateway.books["CCC-USD"] = (101.0, 103.0)
    engine.refresh_shadow_quotes()
    live, shadow, _ = engine.universe_snapshot()

    assert "CCC-USD" not in live
    assert shadow["CCC-USD"].best_bid == 101.0
    assert shadow["CCC-USD"].best_ask == 103.0
    assert shadow["CCC-USD"].price == 102.0
