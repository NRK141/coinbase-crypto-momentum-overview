from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from filelock import FileLock, Timeout

from .config import load_settings
from .notifications import PushoverNotifier
from .remote_monitor import RemoteHealthDashboard

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
WATCHDOG_LOG = LOG_DIR / "watchdog.log"
FATAL_LOG = LOG_DIR / "runtime_fatal.log"
WATCHDOG_HEARTBEAT = DATA_DIR / "watchdog_heartbeat.json"
BOT_LOCK = DATA_DIR / "bot.lock"

CLEAN_CODES = {0, 130, -1073741510, 3221225786}  # normal + Windows CTRL_C_EVENT variants
DBG_TERMINATE_PROCESS = 0x40010004
MAX_RESTARTS = 5
RESTART_WINDOW_SECONDS = 600
RESTART_DELAY_SECONDS = 10
EXTERNAL_TERMINATION_RESTART_DELAY_SECONDS = 2
HEARTBEAT_SECONDS = 5


def _log(message: str) -> None:
    line = f"{datetime.now().astimezone().isoformat(timespec='seconds')} | {message}"
    print(line, flush=True)
    try:
        with WATCHDOG_LOG.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
    except Exception:
        pass


def _fatal(message: str) -> None:
    _log(message)
    try:
        with FATAL_LOG.open("a", encoding="utf-8") as fh:
            fh.write("\n" + "=" * 100 + "\n")
            fh.write(f"utc={datetime.now(timezone.utc).isoformat()}\n")
            fh.write(f"pid={os.getpid()} context=external_watchdog\n")
            fh.write(message + "\n")
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
    except Exception:
        pass


def _atomic_json(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8", newline="\n") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _heartbeat(state: str, *, child_pid: int | None = None, detail: str | None = None) -> None:
    _atomic_json(
        WATCHDOG_HEARTBEAT,
        {
            "version": "1.4.12",
            "watchdog_pid": os.getpid(),
            "child_pid": child_pid,
            "state": state,
            "detail": detail,
            "updated_utc": datetime.now(timezone.utc).isoformat(),
        },
    )


def _u32(code: int) -> int:
    return code & 0xFFFFFFFF


def _is_external_termination(code: int) -> bool:
    return _u32(code) == DBG_TERMINATE_PROCESS


def _bot_lock_is_held() -> bool:
    probe = FileLock(str(BOT_LOCK))
    try:
        probe.acquire(timeout=0)
    except Timeout:
        return True
    else:
        try:
            probe.release()
        except Exception:
            pass
        return False


def _runtime_child_healthy(child_pid: int, *, fresh_seconds: float = 45.0) -> bool:
    path = DATA_DIR / "runtime_heartbeat.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if int(payload.get("pid") or -1) != int(child_pid):
            return False
        raw = str(payload.get("updated_utc") or "").replace("Z", "+00:00")
        updated = datetime.fromisoformat(raw)
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds()
        state = str(payload.get("state") or "").upper()
        return age <= fresh_seconds and state not in {"STOPPING", "STOPPED"}
    except Exception:
        return False


def _runtime_heartbeat_age_seconds(child_pid: int) -> float | None:
    path = DATA_DIR / "runtime_heartbeat.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if int(payload.get("pid") or -1) != int(child_pid):
            return None
        raw = str(payload.get("updated_utc") or "").replace("Z", "+00:00")
        updated = datetime.fromisoformat(raw)
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return None


def _wait_with_heartbeat(
    child: subprocess.Popen,
    *,
    on_unhealthy=None,
    on_healthy=None,
    runtime_stale_seconds: float = 90.0,
    startup_grace_seconds: float = 120.0,
) -> int:
    started = time.monotonic()
    while True:
        code = child.poll()
        if code is not None:
            return int(code)
        _heartbeat("CHILD_RUNNING", child_pid=child.pid)
        if _runtime_child_healthy(child.pid):
            if on_healthy is not None:
                on_healthy()
        elif time.monotonic() - started >= startup_grace_seconds:
            age = _runtime_heartbeat_age_seconds(child.pid)
            if age is None or age > runtime_stale_seconds:
                if on_unhealthy is not None:
                    age_text = "missing" if age is None else f"{age:.0f}s stale"
                    on_unhealthy(f"Runtime heartbeat is {age_text} while child PID {child.pid} is still running")
        time.sleep(HEARTBEAT_SECONDS)


def main() -> int:
    restarts: deque[float] = deque()
    child: subprocess.Popen | None = None
    notifier = PushoverNotifier.from_env(PROJECT_ROOT)
    dashboard: RemoteHealthDashboard | None = None
    outage_active = False
    outage_started_mono: float | None = None

    try:
        settings = load_settings()
        remote = settings.remote_monitor
        if bool(remote.get("dashboard_enabled", True)):
            dashboard = RemoteHealthDashboard(
                PROJECT_ROOT,
                host=str(remote.get("dashboard_host", "127.0.0.1")),
                port=int(remote.get("dashboard_port", 8765)),
                runtime_degraded_seconds=float(remote.get("runtime_degraded_seconds", 45)),
                runtime_offline_seconds=float(remote.get("runtime_offline_seconds", 90)),
                watchdog_stale_seconds=float(remote.get("watchdog_stale_seconds", 20)),
            )
            dashboard.start()
            _log(
                "Remote health dashboard listening on "
                f"http://{remote.get('dashboard_host', '127.0.0.1')}:{int(remote.get('dashboard_port', 8765))}"
            )
        runtime_stale_seconds = float(remote.get("runtime_offline_seconds", 90))
    except Exception as exc:
        runtime_stale_seconds = 90.0
        _log(f"Remote dashboard startup skipped after error: {type(exc).__name__}: {exc}")

    def mark_unhealthy(detail: str) -> None:
        nonlocal outage_active, outage_started_mono
        if outage_active:
            return
        outage_active = True
        outage_started_mono = time.monotonic()
        notifier.send(
            "BOT OFFLINE",
            f"Coinbase Crypto Momentum needs attention.\n{detail}\nWatchdog remains active and will recover the child when possible.",
            priority=1,
            background=True,
        )

    def mark_healthy() -> None:
        nonlocal outage_active, outage_started_mono
        if not outage_active:
            return
        downtime = max(0.0, time.monotonic() - float(outage_started_mono or time.monotonic()))
        outage_active = False
        outage_started_mono = None
        notifier.send(
            "BOT RECOVERED",
            f"Trading engine heartbeat is healthy again.\nObserved interruption: {downtime:.0f} seconds.",
            background=True,
        )

    _log("Watchdog V1.4.12 started. Unexpected bot-process exits will be restarted; clean Ctrl+C will not restart.")
    _heartbeat("STARTING")

    while True:
        try:
            if _bot_lock_is_held():
                _log(
                    "Trading-engine lock is already held. Another bot instance is active; "
                    "this watchdog will stop instead of launching a duplicate child."
                )
                _heartbeat("DUPLICATE_SUPERVISOR_BLOCKED", detail="data/bot.lock is held")
                return 4

            _log("Launching trading engine child process...")
            child = subprocess.Popen(
                [sys.executable, "-m", "crypto_momentum.cli", "run"],
                cwd=str(PROJECT_ROOT),
            )
            _heartbeat("CHILD_STARTING", child_pid=child.pid)
            code = _wait_with_heartbeat(
                child,
                on_unhealthy=mark_unhealthy,
                on_healthy=mark_healthy,
                runtime_stale_seconds=runtime_stale_seconds,
            )
            child_pid = child.pid
            child = None

            if code in CLEAN_CODES:
                _log(f"Trading engine exited cleanly with code {code}; watchdog stopping.")
                _heartbeat("STOPPED_CLEAN", child_pid=child_pid, detail=f"exit_code={code}")
                return 0 if code == 0 else code

            now = time.monotonic()
            restarts.append(now)
            cutoff = now - RESTART_WINDOW_SECONDS
            while restarts and restarts[0] < cutoff:
                restarts.popleft()

            if _is_external_termination(code):
                detail = (
                    f"Trading engine was externally terminated by Windows/process control: "
                    f"code={code} (0x{_u32(code):08X}, DBG_TERMINATE_PROCESS); "
                    f"restart count={len(restarts)}/{MAX_RESTARTS} inside {RESTART_WINDOW_SECONDS}s."
                )
                delay = EXTERNAL_TERMINATION_RESTART_DELAY_SECONDS
            else:
                detail = (
                    f"Trading engine exited unexpectedly with code {code}; "
                    f"restart count={len(restarts)}/{MAX_RESTARTS} inside {RESTART_WINDOW_SECONDS}s."
                )
                delay = RESTART_DELAY_SECONDS

            _fatal(detail)
            mark_unhealthy(detail)
            if len(restarts) >= MAX_RESTARTS:
                terminal = "Restart-storm guard engaged. Watchdog will stop instead of looping indefinitely."
                _fatal(terminal)
                notifier.send(
                    "BOT OFFLINE",
                    terminal + " Manual intervention is required.",
                    priority=1,
                    background=False,
                )
                _heartbeat("RESTART_STORM_STOP", detail=detail)
                return 2

            _log(f"Restarting in {delay}s. Coinbase will be reconciled on child startup.")
            _heartbeat("RESTART_PENDING", detail=detail)
            for _ in range(delay):
                time.sleep(1)

        except KeyboardInterrupt:
            _log("Ctrl+C received by watchdog; requesting clean stop and disabling automatic restart.")
            _heartbeat("STOP_REQUESTED", child_pid=child.pid if child is not None else None)
            if child is not None and child.poll() is None:
                try:
                    child.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    try:
                        child.terminate()
                        child.wait(timeout=5)
                    except Exception:
                        try:
                            child.kill()
                        except Exception:
                            pass
            _heartbeat("STOPPED_CLEAN", child_pid=child.pid if child is not None else None, detail="Ctrl+C")
            return 130
        except Exception as exc:
            message = f"WATCHDOG INTERNAL ERROR: {type(exc).__name__}: {exc}"
            _fatal(message)
            notifier.send("WATCHDOG FAILED", message, priority=1, background=False)
            _heartbeat("WATCHDOG_INTERNAL_ERROR", detail=f"{type(exc).__name__}: {exc}")
            return 3


if __name__ == "__main__":
    raise SystemExit(main())
