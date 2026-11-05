from __future__ import annotations

import json
import logging
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

log = logging.getLogger(__name__)


class PushoverNotifier:
    """Best-effort Pushover notifications that can never block trading decisions.

    Credentials live only in .env.  The bot never exposes them through the dashboard or logs.
    """

    API_URL = "https://api.pushover.net/1/messages.json"

    def __init__(
        self,
        *,
        enabled: bool,
        app_token: str,
        user_key: str,
        device: str = "",
        timeout_seconds: float = 5.0,
    ) -> None:
        self.enabled = bool(enabled and app_token and user_key)
        self.app_token = app_token.strip()
        self.user_key = user_key.strip()
        self.device = device.strip()
        self.timeout_seconds = max(1.0, float(timeout_seconds))

    @classmethod
    def from_env(cls, project_root: Path | None = None) -> "PushoverNotifier":
        root = project_root or Path(__file__).resolve().parents[2]
        load_dotenv(root / ".env", override=False)
        raw_enabled = os.getenv("PUSHOVER_ENABLED", "false").strip().lower()
        return cls(
            enabled=raw_enabled in {"1", "true", "yes", "on"},
            app_token=os.getenv("PUSHOVER_APP_TOKEN", ""),
            user_key=os.getenv("PUSHOVER_USER_KEY", ""),
            device=os.getenv("PUSHOVER_DEVICE", ""),
            timeout_seconds=float(os.getenv("PUSHOVER_TIMEOUT_SECONDS", "5") or 5),
        )

    def configured(self) -> bool:
        return self.enabled

    def _send_now(self, title: str, message: str, *, priority: int = 0) -> bool:
        if not self.enabled:
            return False
        payload: dict[str, Any] = {
            "token": self.app_token,
            "user": self.user_key,
            "title": title[:250],
            "message": message[:1024],
            "priority": str(int(priority)),
        }
        if self.device:
            payload["device"] = self.device
        body = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(
            self.API_URL,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                data = json.loads(resp.read().decode("utf-8") or "{}")
            if int(data.get("status", 0)) != 1:
                log.warning("Pushover rejected notification: %s", data.get("errors") or data)
                return False
            return True
        except (urllib.error.URLError, TimeoutError, ValueError, OSError) as exc:
            # Notification delivery is observability only.  It must never interfere with orders,
            # protection, reconciliation, or the watchdog restart loop.
            log.warning("Pushover notification failed: %s", exc)
            return False

    def send(self, title: str, message: str, *, priority: int = 0, background: bool = True) -> bool:
        if not self.enabled:
            return False
        if not background:
            return self._send_now(title, message, priority=priority)

        def _worker() -> None:
            self._send_now(title, message, priority=priority)

        threading.Thread(target=_worker, name="pushover-notify", daemon=True).start()
        return True

    def entry(
        self,
        *,
        product_id: str,
        price: float,
        quantity: float,
        reason: str,
        execution_path: str,
        stop_price: float,
    ) -> None:
        message = (
            f"{product_id}\n"
            f"Entry: ${price:.8g}\n"
            f"Reason: {reason}\n"
            f"Execution: {execution_path or 'unknown'}\n"
            f"Initial stop target: ${stop_price:.8g}"
        )
        self.send("BOT ENTRY", message, background=True)

    def exit(
        self,
        *,
        product_id: str,
        price: float,
        reason: str,
        gross_return: float | None,
        realized_pnl: float | None,
    ) -> None:
        lines = [product_id, f"Exit: ${price:.8g}", f"Reason: {reason}"]
        if gross_return is not None:
            lines.append(f"Gross return: {gross_return:+.2%}")
        if realized_pnl is not None:
            lines.append(f"Realized P&L: ${realized_pnl:+.2f}")
        self.send("BOT EXIT", "\n".join(lines), background=True)
