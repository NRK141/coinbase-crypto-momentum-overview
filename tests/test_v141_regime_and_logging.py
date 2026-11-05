from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace

# Allow tests to run even when the Coinbase SDK is not installed in the build container.
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

try:
    import coinbase.websocket  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    websocket = types.ModuleType("coinbase.websocket")
    class WSClient:  # pragma: no cover
        pass
    websocket.WSClient = WSClient
    sys.modules["coinbase.websocket"] = websocket
    if "coinbase" in sys.modules:
        setattr(sys.modules["coinbase"], "websocket", websocket)

import yaml

from crypto_momentum.db import StateDB
from crypto_momentum.models import ProductMeta
from crypto_momentum.runner import BotRunner
from crypto_momentum.strategy import ScanResult, StrategyEngine


class ProductGateway:
    @staticmethod
    def product_meta(p):
        bid = float(p["best_bid_price"])
        ask = float(p["best_ask_price"])
        return ProductMeta(
            product_id=p["product_id"],
            base_currency=p["base_currency_id"],
            quote_currency=p["quote_currency_id"],
            price=(bid + ask) / 2,
            volume_24h_base=1_000_000,
            quote_volume_24h=float(p["approximate_quote_24h_volume"]),
            base_increment="0.0001",
            quote_increment="0.01",
            base_min_size=0.0001,
            quote_min_size=1.0,
            best_bid=bid,
            best_ask=ask,
            spread_pct=(ask-bid)/((ask+bid)/2),
            new_at="2025-01-01T00:00:00Z",
        )


def _raw(pid: str, qv: float):
    base = pid.split("-")[0]
    return {
        "product_id": pid,
        "base_currency_id": base,
        "quote_currency_id": "USD",
        "best_bid_price": "99.95",
        "best_ask_price": "100.05",
        "approximate_quote_24h_volume": str(qv),
        "product_type": "SPOT",
        "new_at": "2025-01-01T00:00:00Z",
    }


def test_v141_regime_config_targets_top_30_liquid_assets():
    root = Path(__file__).resolve().parents[1]
    data = yaml.safe_load((root / "config" / "strategy.yaml").read_text())
    cfg = data["regime_universe"]
    assert cfg["refresh_minutes"] == 120
    assert cfg["min_quote_volume_24h_usd"] == 1_000_000
    assert cfg["max_products"] == 30
    assert cfg["min_products_warning"] == 25
    assert cfg["cache_revision"] == 2


def test_regime_selection_remains_exactly_30_when_btc_is_forced_in(tmp_path: Path):
    # BTC is intentionally below the top-30 by volume but should replace the lowest-volume
    # non-forced member rather than expanding the breadth denominator to 31.
    raw = [_raw(f"C{i:02d}-USD", 100_000_000 - i * 1_000_000) for i in range(35)]
    raw.append(_raw("BTC-USD", 1_100_000))
    settings = SimpleNamespace(
        strategy={"quote_currency": "USD"},
        universe={
            "stablecoin_symbols": ["USDC", "USDT"],
            "excluded_symbols": [],
            "require_spot": True,
            "min_history_hours": 337,
        },
        regime={"btc_product": "BTC-USD"},
    )
    engine = StrategyEngine(ProductGateway(), StateDB(tmp_path / "state.db"), settings)
    cfg = {
        "min_quote_volume_24h_usd": 1_000_000,
        "max_spread_pct": 0.005,
        "max_products": 30,
        "min_history_hours": 337,
    }
    selected, _, _ = engine._select_universe(raw, cfg, include_btc=True)
    assert len(selected) == 30
    assert "BTC-USD" in selected


def _empty_scan() -> ScanResult:
    return ScanResult(
        rows=[], products={}, regime="RISK_OFF", previous_regime="UNKNOWN", breadth=0.20,
        candidates=[], diagnostics=[], trading_universe_size=45, regime_universe_size=30,
        regime_rows_available=30, persistence_counts={},
    )


def _log_runner(tmp_path: Path, db: StateDB) -> BotRunner:
    r = BotRunner.__new__(BotRunner)
    r.db = db
    r.s = SimpleNamespace(
        project_root=tmp_path,
        runtime={"screener_log_path": "logs/screener_log.log"},
        signal={"diagnostic_top_n": 10},
    )
    return r


def test_screener_log_is_cumulative_numbered_and_survives_restart(tmp_path: Path):
    db = StateDB(tmp_path / "data" / "state.db")
    r1 = _log_runner(tmp_path, db)
    for _ in range(2):
        n = db.increment_counter("screener_log_scan_number", start=0)
        block = r1._render_scan(_empty_scan(), n)
        r1._append_screener_log(block + "\n")

    # Simulate process restart using the same persistent state DB.
    db2 = StateDB(tmp_path / "data" / "state.db")
    r2 = _log_runner(tmp_path, db2)
    n = db2.increment_counter("screener_log_scan_number", start=0)
    r2._append_screener_log(r2._render_scan(_empty_scan(), n) + "\n")

    text = (tmp_path / "logs" / "screener_log.log").read_text(encoding="utf-8")
    assert text.count("SCAN #1 ") == 1
    assert text.count("SCAN #2 ") == 1
    assert text.count("SCAN #3 ") == 1
    assert text.index("SCAN #1 ") < text.index("SCAN #2 ") < text.index("SCAN #3 ")
    assert text.count("Actionable after persistence/entry filters: none") == 3


def test_scan_once_transcript_captures_scan_logs_and_terminal_block(tmp_path: Path):
    import logging

    db = StateDB(tmp_path / "data" / "state.db")
    runner = _log_runner(tmp_path, db)
    logging.getLogger().setLevel(logging.INFO)

    def fake_impl(scan_number: int):
        logging.getLogger("crypto_momentum.test").info("Signal data: 20/30 products processed")
        runner._print_scan(_empty_scan(), scan_number)
        logging.getLogger("crypto_momentum.test").info("DRY RUN execution plan: no order submitted")

    runner._scan_once_impl = fake_impl  # type: ignore[method-assign]
    runner.scan_once()

    text = (tmp_path / "logs" / "screener_log.log").read_text(encoding="utf-8")
    assert "Signal data: 20/30 products processed" in text
    assert "SCAN #1 " in text
    assert "Actionable after persistence/entry filters: none" in text
    assert "DRY RUN execution plan: no order submitted" in text
