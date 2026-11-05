from __future__ import annotations

import json
from pathlib import Path

from crypto_momentum import watchdog


def test_dbg_terminate_process_classification() -> None:
    assert watchdog._is_external_termination(1073807364)
    assert watchdog._is_external_termination(0x40010004)
    assert not watchdog._is_external_termination(1)
    assert watchdog._u32(-1073741510) == 0xC000013A


def test_watchdog_heartbeat_is_atomic_and_contains_child(tmp_path, monkeypatch) -> None:
    hb = tmp_path / "watchdog_heartbeat.json"
    monkeypatch.setattr(watchdog, "WATCHDOG_HEARTBEAT", hb)
    watchdog._heartbeat("CHILD_RUNNING", child_pid=1234, detail="ok")
    data = json.loads(hb.read_text(encoding="utf-8"))
    assert data["version"] == "1.4.12"
    assert data["state"] == "CHILD_RUNNING"
    assert data["child_pid"] == 1234
    assert data["detail"] == "ok"
    assert not hb.with_suffix(hb.suffix + ".tmp").exists()


def test_unattended_task_scripts_present_and_safe() -> None:
    root = Path(__file__).resolve().parents[1]
    install = (root / "scripts" / "install_unattended_task.ps1").read_text(encoding="utf-8")
    stop = (root / "scripts" / "stop_unattended_task.ps1").read_text(encoding="utf-8")
    assert "-LogonType S4U" in install
    assert "-AtLogOn" in install
    assert "-RestartCount 999" in install
    assert "-DontStopIfGoingOnBatteries" in install
    assert "Disable-ScheduledTask" in stop
    assert "Stop-ScheduledTask" in stop
    for name in (
        "install_unattended_task.cmd",
        "start_unattended_task.cmd",
        "stop_unattended_task.cmd",
        "status_unattended_task.cmd",
        "uninstall_unattended_task.cmd",
    ):
        assert (root / name).exists(), name
