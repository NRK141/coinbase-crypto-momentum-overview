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
    sys.modules["coinbase"] = coinbase
    sys.modules["coinbase.rest"] = rest

from crypto_momentum.db import StateDB
from crypto_momentum.strategy import StrategyEngine


class DummyGateway:
    pass


def settings():
    return SimpleNamespace(
        regime={
            "breadth_risk_off_enter": 0.35,
            "breadth_risk_off_exit": 0.40,
            "breadth_risk_on": 0.50,
            "breadth_risk_on_exit": 0.45,
        }
    )


def engine(tmp_path: Path) -> StrategyEngine:
    return StrategyEngine(DummyGateway(), StateDB(tmp_path / "state.db"), settings())


def test_btc_recovery_alone_cannot_leave_risk_off(tmp_path: Path):
    e = engine(tmp_path)
    e.db.set_json("regime_state", "RISK_OFF")
    regime, prev = e._determine_regime(0.321, btc_above=True)
    assert prev == "RISK_OFF"
    assert regime == "RISK_OFF"


def test_risk_off_requires_btc_and_40pct_breadth_then_only_neutral(tmp_path: Path):
    e = engine(tmp_path)
    e.db.set_json("regime_state", "RISK_OFF")
    assert e._determine_regime(0.45, btc_above=False)[0] == "RISK_OFF"
    # Even very strong breadth exits only one state at a time.
    assert e._determine_regime(0.55, btc_above=True)[0] == "NEUTRAL"


def test_neutral_requires_btc_and_50pct_breadth_for_risk_on(tmp_path: Path):
    e = engine(tmp_path)
    e.db.set_json("regime_state", "NEUTRAL")
    assert e._determine_regime(0.49, btc_above=True)[0] == "NEUTRAL"
    assert e._determine_regime(0.55, btc_above=False)[0] == "NEUTRAL"
    assert e._determine_regime(0.50, btc_above=True)[0] == "RISK_ON"


def test_risk_on_hysteresis_45pct_exit_and_35pct_risk_off(tmp_path: Path):
    e = engine(tmp_path)
    e.db.set_json("regime_state", "RISK_ON")
    assert e._determine_regime(0.47, btc_above=True)[0] == "RISK_ON"
    assert e._determine_regime(0.44, btc_above=True)[0] == "NEUTRAL"

    e.db.set_json("regime_state", "RISK_ON")
    assert e._determine_regime(0.60, btc_above=False)[0] == "NEUTRAL"

    e.db.set_json("regime_state", "RISK_ON")
    assert e._determine_regime(0.34, btc_above=False)[0] == "RISK_OFF"
