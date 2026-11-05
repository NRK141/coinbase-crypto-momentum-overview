from __future__ import annotations

import html
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


VERSION = "1.4.12"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _age_seconds(value: Any, now: datetime) -> float | None:
    dt = _parse_time(value)
    if dt is None:
        return None
    return max(0.0, (now - dt).total_seconds())


def _human_age(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    if seconds < 60:
        return f"{int(seconds)} sec ago"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes} min ago"
    hours = int(minutes // 60)
    minutes %= 60
    return f"{hours}h {minutes}m ago"


def _db_snapshot(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"position": None, "last_trade": None}
    try:
        conn = sqlite3.connect(db_path, timeout=1.0)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute("SELECT value FROM kv WHERE key='position'").fetchone()
            position = None
            if row is not None:
                raw = json.loads(row["value"])
                if raw:
                    position = str(raw.get("product_id") or "") or None
            trade = conn.execute(
                "SELECT ts,event,product_id,price,realized_pnl,details FROM trades "
                "WHERE event IN ('ENTRY_FILLED','EXIT_FILLED') ORDER BY id DESC LIMIT 1"
            ).fetchone()
            last_trade = dict(trade) if trade is not None else None
            if last_trade is not None:
                try:
                    details = json.loads(last_trade.get("details") or "{}")
                except (TypeError, ValueError):
                    details = {}
                last_trade["reason"] = details.get("reason")
            return {"position": position, "last_trade": last_trade}
        finally:
            conn.close()
    except (sqlite3.Error, ValueError, OSError):
        return {"position": None, "last_trade": None}


def build_status_snapshot(
    project_root: Path,
    *,
    runtime_degraded_seconds: float = 45.0,
    runtime_offline_seconds: float = 90.0,
    watchdog_stale_seconds: float = 20.0,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    runtime = _read_json(project_root / "data" / "runtime_heartbeat.json")
    watchdog = _read_json(project_root / "data" / "watchdog_heartbeat.json")
    db = _db_snapshot(project_root / "data" / "state.db")

    runtime_age = _age_seconds(runtime.get("updated_utc"), now)
    watchdog_age = _age_seconds(watchdog.get("updated_utc"), now)
    runtime_state = str(runtime.get("state") or "UNKNOWN").upper()
    watchdog_state = str(watchdog.get("state") or "UNKNOWN").upper()

    watchdog_fresh = watchdog_age is not None and watchdog_age <= watchdog_stale_seconds
    runtime_fresh = runtime_age is not None and runtime_age <= runtime_degraded_seconds
    runtime_recent = runtime_age is not None and runtime_age <= runtime_offline_seconds
    stopping = runtime_state in {"STOPPING", "STOPPED"}

    if not watchdog_fresh:
        health = "OFFLINE"
        detail = "Supervisor heartbeat is stale"
    elif runtime_fresh and not stopping:
        health = "RUNNING"
        detail = "Bot and supervisor heartbeats are healthy"
    elif runtime_recent and not stopping:
        health = "DEGRADED"
        detail = "Bot heartbeat is delayed; watchdog is still alive"
    else:
        health = "OFFLINE"
        detail = "Bot heartbeat is stale or bot is stopped; watchdog is still alive"

    last_scan = runtime.get("last_scan") or {}
    last_scan_age = _age_seconds(last_scan.get("utc"), now)
    last_trade = db.get("last_trade")
    if last_trade:
        last_trade = dict(last_trade)
        last_trade["age_seconds"] = _age_seconds(last_trade.get("ts"), now)

    return {
        "version": VERSION,
        "health": health,
        "detail": detail,
        "updated_utc": now.isoformat(),
        "runtime_state": runtime_state,
        "runtime_age_seconds": runtime_age,
        "runtime_age_text": _human_age(runtime_age),
        "watchdog_state": watchdog_state,
        "watchdog_age_seconds": watchdog_age,
        "watchdog_age_text": _human_age(watchdog_age),
        "last_scan_number": last_scan.get("number"),
        "last_scan_age_seconds": last_scan_age,
        "last_scan_age_text": _human_age(last_scan_age),
        "position": db.get("position"),
        "last_trade": last_trade,
    }


def _render_page(status: dict[str, Any]) -> str:
    health = str(status.get("health") or "OFFLINE")
    css_class = {"RUNNING": "ok", "DEGRADED": "warn", "OFFLINE": "bad"}.get(health, "bad")
    pos = html.escape(str(status.get("position") or "FLAT"))
    last_trade = status.get("last_trade") or {}
    if last_trade:
        event = "ENTRY" if last_trade.get("event") == "ENTRY_FILLED" else "EXIT"
        pid = html.escape(str(last_trade.get("product_id") or ""))
        reason = last_trade.get("reason")
        suffix = f" · {html.escape(str(reason))}" if reason else ""
        trade_line = f"{event} {pid}{suffix} · {_human_age(last_trade.get('age_seconds'))}"
    else:
        trade_line = "No live trade event recorded"

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crypto Momentum Bot</title>
<style>
:root{{color-scheme:dark;--bg:#101214;--card:#181b1e;--muted:#929aa2;--text:#f4f6f7;--ok:#48c774;--warn:#ffb84d;--bad:#ff5c5c}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}
main{{max-width:520px;margin:0 auto;padding:28px 18px}}.top{{margin:18px 0 28px;text-align:center}}h1{{font-size:16px;font-weight:600;letter-spacing:.08em;margin:0;color:var(--muted)}}
.version{{font-size:12px;color:var(--muted);margin-top:6px}}.status{{font-size:34px;font-weight:700;margin-top:28px}}.dot{{display:inline-block;width:15px;height:15px;border-radius:50%;margin-right:10px;vertical-align:4px}}.ok .dot{{background:var(--ok)}}.warn .dot{{background:var(--warn)}}.bad .dot{{background:var(--bad)}}
.ok .status{{color:var(--ok)}}.warn .status{{color:var(--warn)}}.bad .status{{color:var(--bad)}}.card{{background:var(--card);border-radius:14px;padding:4px 18px;margin-top:16px}}.row{{display:flex;justify-content:space-between;gap:20px;padding:14px 0;border-bottom:1px solid #262a2e}}.row:last-child{{border-bottom:0}}.label{{color:var(--muted)}}.value{{text-align:right}}.event{{padding:16px 18px;background:var(--card);border-radius:14px;margin-top:16px}}.event .label{{margin-bottom:7px;font-size:13px}}.small{{color:var(--muted);font-size:12px;text-align:center;margin-top:18px}}@media(max-width:360px){{.status{{font-size:30px}}}}
</style></head>
<body><main>
<div class="top {css_class}"><h1>CRYPTO MOMENTUM BOT</h1><div class="version">V{VERSION}</div><div class="status"><span class="dot"></span>{health}</div></div>
<div class="card">
<div class="row"><span class="label">Bot heartbeat</span><span class="value">{html.escape(str(status.get('runtime_age_text')))}</span></div>
<div class="row"><span class="label">Watchdog</span><span class="value">{html.escape(str(status.get('watchdog_state')))}</span></div>
<div class="row"><span class="label">Last strategy scan</span><span class="value">{html.escape(str(status.get('last_scan_age_text')))}</span></div>
<div class="row"><span class="label">Position</span><span class="value">{pos}</span></div>
</div>
<div class="event"><div class="label">Last trade event</div><div>{trade_line}</div></div>
<div class="small">Auto-refreshes every 10 seconds · read-only</div>
</main><script>setTimeout(()=>location.reload(),10000);</script></body></html>"""


class RemoteHealthDashboard:
    def __init__(
        self,
        project_root: Path,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
        runtime_degraded_seconds: float = 45.0,
        runtime_offline_seconds: float = 90.0,
        watchdog_stale_seconds: float = 20.0,
    ) -> None:
        self.project_root = project_root
        self.host = host
        self.port = int(port)
        self.runtime_degraded_seconds = float(runtime_degraded_seconds)
        self.runtime_offline_seconds = float(runtime_offline_seconds)
        self.watchdog_stale_seconds = float(watchdog_stale_seconds)
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def _snapshot(self) -> dict[str, Any]:
        return build_status_snapshot(
            self.project_root,
            runtime_degraded_seconds=self.runtime_degraded_seconds,
            runtime_offline_seconds=self.runtime_offline_seconds,
            watchdog_stale_seconds=self.watchdog_stale_seconds,
        )

    def start(self) -> None:
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path.split("?", 1)[0] == "/api/status":
                    body = json.dumps(parent._snapshot(), separators=(",", ":")).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                elif self.path.split("?", 1)[0] in {"/", "/index.html"}:
                    body = _render_page(parent._snapshot()).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                else:
                    body = b"Not found"
                    self.send_response(404)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, _format: str, *_args: Any) -> None:
                return

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, name="remote-health-dashboard", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
