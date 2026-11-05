from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

from coinbase.websocket import WSClient

log = logging.getLogger(__name__)


class WebSocketHealthMonitor:
    """Lightweight public WebSocket health monitor.

    Trading decisions, reconciliation, protection and exits remain REST-authoritative. The
    WebSocket is intentionally *public* (no API credentials are sent) and uses retry=False so
    the Coinbase SDK cannot start a second, nested exponential-backoff loop. We own reconnects
    here with a bounded backoff, which avoids the multi-minute SDK retry stalls and noisy
    background tracebacks seen on transient Windows/DNS/network interruptions.
    """

    def __init__(
        self,
        api_key: str | None,
        api_secret: str | None,
        product_id: str,
        *,
        reconnect_max_seconds: float = 30.0,
    ):
        # api_key/api_secret are accepted for backward compatibility with the runner, but are
        # deliberately not used: heartbeat/ticker_batch are public channels.
        self.product_id = product_id
        self.reconnect_max_seconds = max(1.0, float(reconnect_max_seconds))
        self.last_message_monotonic = 0.0
        self.last_price: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._client: WSClient | None = None
        self._state_lock = threading.Lock()
        self._connected = False
        self._ever_connected = False
        self._last_disconnect_reason: str | None = None
        self._last_disconnect_log_monotonic = 0.0

    @staticmethod
    def quiet_sdk_transport_logs() -> None:
        """Suppress duplicate SDK/websockets tracebacks; this monitor emits concise status logs."""
        logging.getLogger("coinbase.WSClient").setLevel(logging.CRITICAL)
        logging.getLogger("websockets").setLevel(logging.CRITICAL)

    def _on_open(self) -> None:
        with self._state_lock:
            recovered = self._ever_connected and not self._connected
            first = not self._ever_connected
            self._connected = True
            self._ever_connected = True
            self._last_disconnect_reason = None
        if first:
            log.info("WebSocket health feed connected for %s (public feed; REST remains authoritative).", self.product_id)
        elif recovered:
            log.info("WebSocket health feed restored for %s.", self.product_id)

    def _on_close(self) -> None:
        with self._state_lock:
            self._connected = False

    def _on_message(self, msg: str) -> None:
        self.last_message_monotonic = time.monotonic()
        try:
            data: dict[str, Any] = json.loads(msg)
            if data.get("channel") in {"ticker", "ticker_batch"}:
                events = data.get("events", [])
                for event in events:
                    for ticker in event.get("tickers", []):
                        if ticker.get("product_id") == self.product_id:
                            try:
                                self.last_price = float(ticker.get("price"))
                            except (TypeError, ValueError):
                                pass
        except Exception:
            # Health telemetry must never affect trading.
            return

    def _log_disconnect(self, exc: BaseException, backoff: float) -> None:
        reason = f"{type(exc).__name__}: {exc}"
        with self._state_lock:
            self._connected = False
            self._last_disconnect_reason = reason
        now = time.monotonic()
        # Log immediately, then at most once per minute while an outage persists.
        if self._last_disconnect_log_monotonic <= 0 or now - self._last_disconnect_log_monotonic >= 60.0:
            log.warning(
                "WebSocket health feed unavailable for %s (%s). Retrying in %.1fs; REST strategy remains authoritative.",
                self.product_id,
                reason,
                backoff,
            )
            self._last_disconnect_log_monotonic = now

    @staticmethod
    def _dispose_client(client: WSClient | None) -> None:
        """Best-effort cleanup, including the SDK's event-loop thread after broken sockets.

        coinbase-advanced-py 1.8.4 can raise from close() when the underlying socket is already
        closed, before it gets a chance to stop its event loop. The fallback below prevents a
        daemon event-loop thread leak across repeated reconnects.
        """
        if client is None:
            return
        try:
            client.close()
            return
        except Exception:
            pass

        loop = getattr(client, "loop", None)
        thread = getattr(client, "thread", None)
        try:
            if loop is not None and not loop.is_closed():
                loop.call_soon_threadsafe(loop.stop)
        except Exception:
            pass
        try:
            if thread is not None and thread.is_alive():
                thread.join(timeout=2.0)
        except Exception:
            pass
        try:
            if loop is not None and not loop.is_closed() and (thread is None or not thread.is_alive()):
                loop.close()
        except Exception:
            pass

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            client: WSClient | None = None
            opened_at: float | None = None
            try:
                # Public channels need no API credentials. retry=False is intentional: one retry
                # owner (this class) is easier to reason about and keeps retry delay bounded.
                client = WSClient(
                    api_key=None,
                    api_secret=None,
                    on_message=self._on_message,
                    on_open=self._on_open,
                    on_close=self._on_close,
                    retry=False,
                    verbose=False,
                )
                self._client = client
                client.open()
                opened_at = time.monotonic()
                client.subscribe([self.product_id], ["heartbeats", "ticker_batch"])

                while not self._stop.is_set():
                    client.sleep_with_exception_check(1)
            except Exception as exc:
                if self._stop.is_set():
                    break
                # Connections that were healthy for >=30s restart from a short retry; repeated
                # immediate failures exponentially back off, capped at reconnect_max_seconds.
                healthy_for = (time.monotonic() - opened_at) if opened_at is not None else 0.0
                if healthy_for >= 30.0:
                    backoff = 1.0
                self._log_disconnect(exc, backoff)
            finally:
                self._dispose_client(client)
                if self._client is client:
                    self._client = None
                with self._state_lock:
                    self._connected = False

            if self._stop.is_set():
                break
            # Interruptible wait makes Ctrl+C shutdown prompt even during a network outage.
            self._stop.wait(backoff)
            backoff = min(backoff * 2.0, self.reconnect_max_seconds)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.quiet_sdk_transport_logs()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="coinbase-ws-health", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        client = self._client
        if client is not None:
            # Best effort to wake sleep_with_exception_check / socket receive promptly.
            self._dispose_client(client)
        if self._thread:
            self._thread.join(timeout=5.0)

    def age_seconds(self) -> float | None:
        if self.last_message_monotonic <= 0:
            return None
        return time.monotonic() - self.last_message_monotonic

    def snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            return {
                "connected": self._connected,
                "ever_connected": self._ever_connected,
                "last_disconnect_reason": self._last_disconnect_reason,
                "age_seconds": self.age_seconds(),
                "last_price": self.last_price,
            }
