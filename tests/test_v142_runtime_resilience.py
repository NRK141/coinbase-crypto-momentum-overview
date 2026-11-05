from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from crypto_momentum.runner import BotRunner
from crypto_momentum import watchdog


class FakeDB:
    def __init__(self):
        self.data = {"last_completed_scan": {"number": 8, "utc": "2026-09-13T08:03:21+00:00"}}

    def get_json(self, key, default=None):
        return self.data.get(key, default)

    def get_position(self):
        return None


def _runner(tmp_path: Path) -> BotRunner:
    r = BotRunner.__new__(BotRunner)
    r.s = SimpleNamespace(
        project_root=tmp_path,
        run_mode="dry_run",
        strategy={"name": "Crypto Cross-Sectional Trend Rotation V1.4.2"},
        runtime={
            "heartbeat_path": "data/runtime_heartbeat.json",
            "runtime_fatal_log_path": "logs/runtime_fatal.log",
        },
    )
    r.db = FakeDB()
    r.stop_requested = False
    return r


def test_heartbeat_is_written_with_last_scan(tmp_path):
    r = _runner(tmp_path)
    r._write_heartbeat(state="WAITING")
    p = tmp_path / "data" / "runtime_heartbeat.json"
    payload = json.loads(p.read_text())
    assert payload["state"] == "WAITING"
    assert payload["mode"] == "dry_run"
    assert payload["last_scan"]["number"] == 8
    assert payload["has_position"] is False


def test_request_stop_is_idempotent(tmp_path, caplog):
    r = _runner(tmp_path)
    r.request_stop()
    r.request_stop()
    assert r.stop_requested is True
    matches = [rec for rec in caplog.records if "Shutdown requested" in rec.getMessage()]
    assert len(matches) == 1


def test_watchdog_clean_exit_codes_include_windows_ctrl_c():
    assert 0 in watchdog.CLEAN_CODES
    assert 130 in watchdog.CLEAN_CODES
    assert -1073741510 in watchdog.CLEAN_CODES


def test_runtime_fatal_log_persists_traceback(tmp_path):
    r = _runner(tmp_path)
    try:
        raise RuntimeError("synthetic scheduler failure")
    except RuntimeError as exc:
        r._append_runtime_fatal("test", exc, recovered=True)
    text = (tmp_path / "logs" / "runtime_fatal.log").read_text()
    assert "synthetic scheduler failure" in text
    assert "context=test" in text
    assert "recovered=True" in text
