from __future__ import annotations

import csv
import json
from pathlib import Path

from crypto_momentum.db import StateDB
from crypto_momentum.models import PositionState


def _db(tmp_path: Path) -> StateDB:
    return StateDB(tmp_path / "data" / "state.db", trade_log_path=tmp_path / "logs" / "trade_log.csv")


def test_trade_ledger_open_update_close_and_csv(tmp_path: Path):
    db = _db(tmp_path)
    db.open_trade_ledger(
        product_id="TEST-USD",
        entry_ts="2026-09-27T12:00:00+00:00",
        entry_price=100.0,
        quantity=0.25,
        entry_fee=0.125,
        initial_stop_price=95.0,
        initial_risk_pct=0.05,
        entry_order_id="entry-1",
        execution_path="MAKER",
        signal={"rank": 2, "percentile": 0.97, "r24": 0.12, "r72": 0.20, "r168": 0.30, "r336": 0.40, "atr_pct": 0.02},
    )
    db.update_trade_excursions("TEST-USD", 108.0)
    db.update_trade_excursions("TEST-USD", 97.0)
    db.close_trade_ledger(
        product_id="TEST-USD",
        exit_ts="2026-09-27T18:00:00+00:00",
        exit_price=106.0,
        exit_quantity=0.25,
        exit_fee=0.265,
        exit_order_id="exit-1",
        exit_reason="EXCHANGE_STOP_FILLED",
    )

    rows = db.trade_ledger_rows()
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "CLOSED"
    assert row["entry_notional"] == 25.0
    assert row["entry_cash_outlay"] == 25.125
    assert row["exit_notional"] == 26.5
    assert row["total_fees"] == 0.39
    assert row["gross_pnl"] == 1.5
    assert abs(row["net_pnl"] - 1.11) < 1e-12
    assert abs(row["mfe"] - 0.08) < 1e-12
    assert abs(row["mae"] + 0.03) < 1e-12
    # Initial price risk is ($100-$95)*0.25 = $1.25; net-PnL R = 1.11/1.25.
    assert abs(row["r_multiple"] - 0.888) < 1e-12
    assert abs(row["capture_efficiency"] - 0.75) < 1e-12
    assert row["holding_minutes"] == 360.0

    csv_path = tmp_path / "logs" / "trade_log.csv"
    with csv_path.open(newline="", encoding="utf-8") as fh:
        csv_rows = list(csv.DictReader(fh))
    assert len(csv_rows) == 1
    assert csv_rows[0]["Asset"] == "TEST-USD"
    assert csv_rows[0]["Exit Reason"] == "EXCHANGE_STOP_FILLED"
    assert csv_rows[0]["MFE (%)"] == "8"
    assert csv_rows[0]["MAE (%)"] == "-3"
    assert csv_rows[0]["Capture Efficiency (%)"] == "75"


def test_existing_event_journal_backfills_without_duplicate(tmp_path: Path):
    db_path = tmp_path / "data" / "state.db"
    db = StateDB(db_path)
    db.log_trade(
        "ENTRY_FILLED", "ABC-USD", "BUY", 10.0, 2.0, 0.10, order_id="entry-old",
        details={
            "execution": {"path": "MAKER"},
            "stop_pct": 0.05,
            "signal": {"rank": 1, "percentile": 0.99, "r24": 0.10, "atr_pct": 0.02},
        },
    )
    db.log_trade(
        "EXIT_FILLED", "ABC-USD", "SELL", 10.0, 2.2, 0.20, 1.70, order_id="exit-old",
        details={"reason": "TREND_FAILURE_5M"},
    )

    ledger_path = tmp_path / "logs" / "trade_log.csv"
    migrated = StateDB(db_path, trade_log_path=ledger_path)
    rows = migrated.trade_ledger_rows()
    assert len(rows) == 1
    assert rows[0]["status"] == "CLOSED"
    assert rows[0]["source"] == "BACKFILLED_DB_EVENTS"
    assert rows[0]["analytics_quality"] == "PARTIAL_LEGACY"
    assert rows[0]["exit_reason"] == "TREND_FAILURE_5M"
    # The exact exit fill is a known observed price, so historical MFE can safely be at least the exit return.
    # The PARTIAL_LEGACY label still warns that intermediate path extrema were not persisted.
    assert abs(rows[0]["mfe"] - 0.10) < 1e-12

    # Opening the same DB again must not duplicate migrated trades.
    migrated_again = StateDB(db_path, trade_log_path=ledger_path)
    assert len(migrated_again.trade_ledger_rows()) == 1


def test_legacy_position_without_low_water_deserializes_and_seeds_open_ledger(tmp_path: Path):
    db_path = tmp_path / "data" / "state.db"
    db = StateDB(db_path)
    # Simulate the V1.4.11 JSON payload: no lowest_close field exists yet.
    legacy = {
        "product_id": "XYZ-USD",
        "base_currency": "XYZ",
        "quantity": 5.0,
        "entry_price": 10.0,
        "entry_time": "2026-09-27T10:00:00+00:00",
        "initial_stop_price": 9.5,
        "stop_price": 10.4,
        "stop_order_id": "stop-1",
        "highest_close": 11.0,
        "initial_risk_pct": 0.05,
        "entry_fees": 0.05,
        "entry_order_id": "entry-xyz",
    }
    db.set_json("position", legacy)
    db.log_trade(
        "ENTRY_FILLED", "XYZ-USD", "BUY", 5.0, 10.0, 0.05, order_id="entry-xyz",
        details={"stop_pct": 0.05, "execution": {"path": "MAKER"}},
    )

    ledger = StateDB(db_path, trade_log_path=tmp_path / "logs" / "trade_log.csv")
    state = ledger.get_position()
    assert isinstance(state, PositionState)
    assert state.lowest_close == 0.0
    row = ledger.trade_ledger_rows()[0]
    assert abs(row["mfe"] - 0.10) < 1e-12
    # Legacy MAE cannot be reconstructed, so seed at entry rather than inventing a drawdown.
    assert row["mae"] == 0.0
