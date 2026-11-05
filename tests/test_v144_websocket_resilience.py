from __future__ import annotations

import logging
import time

import crypto_momentum.websocket_monitor as wm


class _FakeWSClient:
    calls: list[dict] = []
    subscriptions: list[tuple[list[str], list[str]]] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        type(self).calls.append(kwargs)
        self.on_open = kwargs.get("on_open")
        self.on_close = kwargs.get("on_close")
        self.loop = None
        self.thread = None

    def open(self):
        if self.on_open:
            self.on_open()

    def subscribe(self, product_ids, channels):
        type(self).subscriptions.append((list(product_ids), list(channels)))

    def sleep_with_exception_check(self, _seconds):
        raise RuntimeError("simulated transport drop")

    def close(self):
        if self.on_close:
            self.on_close()


def test_ws_health_feed_is_public_and_disables_sdk_nested_retry(monkeypatch):
    _FakeWSClient.calls.clear()
    _FakeWSClient.subscriptions.clear()
    monkeypatch.setattr(wm, "WSClient", _FakeWSClient)

    mon = wm.WebSocketHealthMonitor("secret-key", "secret-private-key", "BTC-USD", reconnect_max_seconds=1)
    mon.start()
    time.sleep(0.05)
    mon.stop()

    assert _FakeWSClient.calls
    first = _FakeWSClient.calls[0]
    assert first["api_key"] is None
    assert first["api_secret"] is None
    assert first["retry"] is False
    assert first["verbose"] is False
    assert _FakeWSClient.subscriptions[0] == (["BTC-USD"], ["heartbeats", "ticker_batch"])


def test_ws_sdk_traceback_loggers_are_quiet_but_monitor_logger_is_not():
    wm.WebSocketHealthMonitor.quiet_sdk_transport_logs()
    assert logging.getLogger("coinbase.WSClient").level == logging.CRITICAL
    assert logging.getLogger("websockets").level == logging.CRITICAL
    assert logging.getLogger("crypto_momentum.websocket_monitor").level != logging.CRITICAL
