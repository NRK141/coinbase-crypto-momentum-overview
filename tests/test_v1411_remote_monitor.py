from __future__ import annotations

import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from crypto_momentum.db import StateDB
from crypto_momentum.notifications import PushoverNotifier
from crypto_momentum.remote_monitor import build_status_snapshot
from crypto_momentum.models import PositionState


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_dashboard_reports_running_from_fresh_runtime_and_watchdog_heartbeats(tmp_path: Path) -> None:
    data = tmp_path / "data"
    now = datetime.now(timezone.utc)
    _write_json(
        data / "runtime_heartbeat.json",
        {
            "pid": 111,
            "state": "WAITING",
            "updated_utc": (now - timedelta(seconds=5)).isoformat(),
            "last_scan": {"number": 42, "utc": (now - timedelta(minutes=7)).isoformat()},
        },
    )
    _write_json(
        data / "watchdog_heartbeat.json",
        {
            "watchdog_pid": 222,
            "child_pid": 111,
            "state": "CHILD_RUNNING",
            "updated_utc": (now - timedelta(seconds=3)).isoformat(),
        },
    )
    db = StateDB(data / "state.db")
    db.set_position(
        PositionState(
            product_id="DRV-USD",
            base_currency="DRV",
            quantity=1.0,
            entry_price=1.0,
            entry_time=now.isoformat(),
            initial_stop_price=0.95,
            stop_price=0.95,
            stop_order_id="stop-1",
            highest_close=1.0,
            initial_risk_pct=0.05,
        )
    )
    db.log_trade("ENTRY_FILLED", "DRV-USD", "BUY", 1.0, 1.0, details={"reason": "MOMENTUM_TREND_BREAKOUT"})

    snap = build_status_snapshot(tmp_path)
    assert snap["health"] == "RUNNING"
    assert snap["position"] == "DRV-USD"
    assert snap["last_scan_number"] == 42
    assert snap["last_trade"]["event"] == "ENTRY_FILLED"


def test_dashboard_reports_offline_when_runtime_heartbeat_is_stale(tmp_path: Path) -> None:
    data = tmp_path / "data"
    now = datetime.now(timezone.utc)
    _write_json(
        data / "runtime_heartbeat.json",
        {"pid": 111, "state": "WAITING", "updated_utc": (now - timedelta(minutes=3)).isoformat()},
    )
    _write_json(
        data / "watchdog_heartbeat.json",
        {"watchdog_pid": 222, "state": "CHILD_RUNNING", "updated_utc": now.isoformat()},
    )
    snap = build_status_snapshot(tmp_path, runtime_offline_seconds=90)
    assert snap["health"] == "OFFLINE"
    assert "Bot heartbeat" in snap["detail"]


def test_pushover_disabled_is_noop() -> None:
    n = PushoverNotifier(enabled=False, app_token="", user_key="")
    assert n.configured() is False
    assert n.send("test", "message", background=False) is False


def test_pushover_posts_required_fields(monkeypatch) -> None:
    captured = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"status":1,"request":"abc"}'

    def fake_urlopen(req, timeout):
        captured["body"] = req.data.decode("utf-8")
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    n = PushoverNotifier(enabled=True, app_token="token123", user_key="user123", timeout_seconds=4)
    assert n.send("BOT TEST", "hello", background=False) is True
    assert "token=token123" in captured["body"]
    assert "user=user123" in captured["body"]
    assert "title=BOT+TEST" in captured["body"]
    assert "message=hello" in captured["body"]
    assert captured["timeout"] == 4
