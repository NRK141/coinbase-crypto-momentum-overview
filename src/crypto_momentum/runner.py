from __future__ import annotations

import json
import logging
import math
import os
import signal
import time
import traceback
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path

from filelock import FileLock, Timeout

from .coinbase_client import CoinbaseGateway
from .db import StateDB
from .execution import ExecutionEngine
from .logging_setup import configure_logging
from .notifications import PushoverNotifier
from .strategy import ScanResult, StrategyEngine
from .shadow import Shadow15mEngine
from .websocket_monitor import WebSocketHealthMonitor

log = logging.getLogger(__name__)


class BotRunner:
    def __init__(self, settings):
        self.s = settings
        self.s.require_live_confirmation()
        self.logger = configure_logging(settings.project_root, settings.runtime["log_dir"])
        db_path = Path(settings.runtime["sqlite_path"])
        if not db_path.is_absolute():
            db_path = settings.project_root / db_path
        ledger_cfg = settings.trade_ledger
        trade_log_path = None
        if bool(ledger_cfg.get("enabled", True)):
            trade_log_path = Path(ledger_cfg.get("csv_path", "logs/trade_log.csv"))
            if not trade_log_path.is_absolute():
                trade_log_path = settings.project_root / trade_log_path
        self.db = StateDB(db_path, trade_log_path=trade_log_path)
        self.gateway = CoinbaseGateway(settings.api_key, settings.api_secret, settings.execution)
        self.strategy = StrategyEngine(self.gateway, self.db, settings)
        self.exec = ExecutionEngine(self.gateway, self.db, settings)
        self.shadow = Shadow15mEngine(self.gateway, self.db, settings)
        self.stop_requested = False
        self.ws: WebSocketHealthMonitor | None = None
        self.lock = FileLock(str(settings.project_root / "data" / "bot.lock"))
        self._last_ws_stale_warning_monotonic = 0.0

    def request_stop(self, *_args) -> None:
        # Windows can deliver several console-control events for a single Ctrl+C. Log only
        # the first one so shutdown diagnostics remain readable.
        if self.stop_requested:
            return
        self.stop_requested = True
        log.warning("Shutdown requested; current Coinbase-side protection is not cancelled.")

    def doctor(self) -> None:
        print("=== Coinbase Crypto Momentum V1.4.12 - Connection Doctor ===")
        print(f"Run mode: {self.s.run_mode}")
        if not self.s.api_key.startswith("organizations/"):
            print("WARNING: API key name does not look like a current CDP key path.")
        if "BEGIN EC PRIVATE KEY" not in self.s.api_secret and "BEGIN PRIVATE KEY" not in self.s.api_secret:
            print("WARNING: API secret does not look like an ECDSA PEM private key.")

        server = self.gateway.server_time()
        print("Coinbase server time: OK", server)
        balances = self.gateway.balances()
        print("Balances visible to key:")
        for cur, bal in sorted(balances.items()):
            if bal["total"] > 0:
                print(f"  {cur:8s} total={bal['total']:.12g} available={bal['available']:.12g} hold={bal['hold']:.12g}")

        products = self.gateway.products()
        print(f"Products visible: {len(products)}")
        catalog = self.gateway.product_catalog_info()
        if catalog:
            print(
                "Coinbase catalog pagination: "
                f"pages={catalog.get('pages')} unique={catalog.get('unique_products')} "
                f"reported={catalog.get('reported_num_products')} complete={catalog.get('pagination_complete')} "
                f"mode={catalog.get('pagination_mode')}"
            )
        maker, taker = self.gateway.fee_rates()
        print(f"Current fee tier reported by Coinbase: maker={maker:.4%}, taker={taker:.4%}")

        holdings = self.exec.current_non_usd_holdings()
        if holdings:
            print("WARNING: non-cash crypto holdings exist in this API-key portfolio:", holdings)
            print("For live use, move the bot into a dedicated Coinbase Advanced portfolio containing only strategy cash.")
        else:
            print("Portfolio cleanliness: PASS (no non-cash crypto holdings above dust threshold)")

        usd_available = balances.get("USD", {}).get("available", 0.0)
        usdc_available = balances.get("USDC", {}).get("available", 0.0)
        print(f"Estimated strategy equity (USD + configured cash equivalents): ${self.exec.equity():.4f}")
        if usd_available < 2.0 and usdc_available >= 2.0:
            print(
                "ACTION REQUIRED BEFORE LIVE: this V1.4.12 engine submits USD-quoted spot orders and currently "
                f"sees only ${usd_available:.2f} USD but {usdc_available:.6g} USDC. Convert USDC -> USD in "
                "Coinbase before enabling live mode. USDC is treated as 1:1 cash-equivalent for risk/equity "
                "accounting, but the bot deliberately does not auto-convert balances."
            )

        if taker > 0.01:
            print(
                f"FEE NOTE: maker={maker:.2%}, taker={taker:.2%}. V1.4.12 remains maker-first: a successful "
                f"post-only entry + taker/stop exit starts near {maker+taker:.2%} before spread/slippage, "
                f"versus about {2*taker:.2%} for taker/taker."
            )

        print(
            "Runtime schedule: trading universe membership every "
            f"{int(self.s.universe.get('refresh_minutes', 30))}m; regime membership every "
            f"{int(self.s.regime_universe.get('refresh_minutes', 120))}m; live hourly-structure decisions every "
            f"{int(self.s.runtime.get('signal_scan_interval_minutes', 15))}m; open-position exit/trailing check every "
            f"{int(self.s.runtime.get('position_exit_check_minutes', 5))}m."
        )
        if self.shadow.enabled:
            shadow_u = getattr(self.s, "shadow_universe", {}) or self.s.universe
            print(
                "Shadow engine: ENABLED; true 15m-bar simulation every "
                f"{int(self.s.shadow_15m.get('scan_interval_minutes', 15))}m; research universe "
                f">=${float(shadow_u.get('min_quote_volume_24h_usd', 0))/1_000_000:.2f}M / "
                f"max {int(shadow_u.get('max_products', 0))}; no Coinbase orders; log={self.shadow._log_path()}"
            )
        if bool(self.s.universe_research.get("enabled", False)):
            print(
                "Universe research audit: ENABLED; observation-only Coinbase scope/filter analysis; "
                f"log={self.strategy._universe_audit_path()}"
            )
        remote = self.s.remote_monitor
        if bool(remote.get("dashboard_enabled", True)):
            print(
                "Remote health dashboard: ENABLED; local-only http://{}:{} (use Tailscale Serve for phone access)".format(
                    remote.get("dashboard_host", "127.0.0.1"), int(remote.get("dashboard_port", 8765))
                )
            )
        notifier = PushoverNotifier.from_env(self.s.project_root)
        print("Pushover notifications:", "ENABLED" if notifier.configured() else "disabled/not configured")
        print(
            "Screener history: every completed scan block is mirrored cumulatively to "
            f"{self._screener_log_path()}"
        )
        manual_halt = bool(self.db.get_json("manual_halt", False))
        print("Manual entry halt:", "ACTIVE" if manual_halt else "clear")
        if manual_halt:
            print("ACTION REQUIRED FOR LIVE ENTRIES: run resume_new_entries.cmd to clear the persisted manual halt.")
        print("Doctor complete. No order was submitted.")

    def status(self) -> None:
        print("=== Crypto Momentum V1.4.12 Status ===")
        print(f"Mode: {self.s.run_mode}")
        state = self.db.get_position()
        print("Local position:", state.to_dict() if state else "NONE")
        if state:
            lock = self.exec.profit_lock_levels(state)
            print(
                "Profit lock: {} | sampled MFE={:+.2%} | capture trigger={} | limit floor={}".format(
                    "ARMED" if bool(lock["armed"]) else "off",
                    float(lock["mfe_return"]),
                    f"{float(lock['trigger_price']):.8f}" if bool(lock["armed"]) else "n/a",
                    f"{float(lock['limit_price']):.8f}" if bool(lock["armed"]) else "n/a",
                )
            )
        try:
            balances = self.gateway.balances()
            print("Coinbase balances above zero:")
            for cur, bal in sorted(balances.items()):
                if bal["total"] > 0:
                    print(f"  {cur:8s} total={bal['total']:.12g} available={bal['available']:.12g} hold={bal['hold']:.12g}")
            print(f"Estimated equity: ${self.exec.equity():.4f}")
        except Exception as exc:
            print("Coinbase status fetch failed:", exc)
        manual_halt = bool(self.db.get_json("manual_halt", False))
        print("Manual halt:", manual_halt)
        if manual_halt:
            print("WARNING: manual entry halt is ACTIVE. Run resume_new_entries.cmd before live trading if you intend to allow new entries.")
        print("Regime state:", self.db.get_json("regime_state", "UNKNOWN"))
        for label, key in (
            ("Trading universe", "trading_universe_cache"),
            ("Shadow universe", "shadow_universe_cache"),
            ("Regime universe", "regime_universe_cache"),
        ):
            cache = self.db.get_json(key, {}) or {}
            print(f"{label}: {len(cache.get('members', []))} members | refreshed={cache.get('refreshed_at', 'never')}")
        print("Entries last 24h:", self.db.entries_last_24h())
        print(f"Realized P&L today: ${self.db.realized_pnl_today():.4f}")
        if self.shadow.enabled:
            shadow_pos = self.shadow.get_position()
            stats = self.db.shadow_stats()
            print("Shadow 15m position:", shadow_pos.to_dict() if shadow_pos else "NONE")
            print(
                "Shadow 15m stats: scans={scans} actionable={actionable} trades={trades} "
                "win_rate={win_rate:.1%} avg_net={avg_net:+.2%} stopouts={stopouts}".format(**stats)
            )
            print("Shadow 15m log:", self.shadow._log_path())
        if bool(self.s.universe_research.get("enabled", False)):
            audit = self.db.get_json("latest_universe_audit", {}) or {}
            if audit:
                volumes = audit.get("volume_counts", {}) or {}
                print(
                    "Universe audit: catalog={} current={} cap_binding={} volume_tiers={}".format(
                        (audit.get("catalog", {}) or {}).get("unique_products", audit.get("raw_products")),
                        audit.get("current_eligible_before_cap"),
                        audit.get("cap_binding"),
                        volumes,
                    )
                )
            else:
                print("Universe audit: waiting for first V1.4.12 universe refresh")
            print("Universe audit log:", self.strategy._universe_audit_path())
        ledger_cfg = self.s.trade_ledger
        if bool(ledger_cfg.get("enabled", True)):
            ledger_path = Path(ledger_cfg.get("csv_path", "logs/trade_log.csv"))
            if not ledger_path.is_absolute():
                ledger_path = self.s.project_root / ledger_path
            ledger_rows = self.db.trade_ledger_rows()
            closed = [r for r in ledger_rows if r.get("status") == "CLOSED"]
            print(f"Trade ledger: {ledger_path} | rows={len(ledger_rows)} closed={len(closed)}")
            if closed:
                last = closed[-1]
                pnl = last.get("net_pnl")
                print(
                    "Last ledger trade: {} {} net_pnl={} reason={}".format(
                        last.get("product_id"),
                        last.get("exit_ts"),
                        f"${float(pnl):+.4f}" if pnl is not None else "n/a",
                        last.get("exit_reason") or "",
                    )
                )
        print("Recent bot events:")
        for row in self.db.recent_trades(10):
            print(f"  {row['ts']} {row['event']} {row.get('product_id') or ''} {row.get('side') or ''} "
                  f"qty={row.get('quantity')} price={row.get('price')} pnl={row.get('realized_pnl')}")

    def test_notification(self) -> None:
        notifier = PushoverNotifier.from_env(self.s.project_root)
        if not notifier.configured():
            raise RuntimeError(
                "Pushover is not configured. Set PUSHOVER_ENABLED=true, PUSHOVER_APP_TOKEN, and PUSHOVER_USER_KEY in .env."
            )
        ok = notifier.send(
            "BOT TEST",
            "Coinbase Crypto Momentum V1.4.12 notifications are working.",
            background=False,
        )
        if not ok:
            raise RuntimeError("Pushover test notification failed; inspect logs/bot.log for details")
        print("Pushover test notification sent successfully.")

    def recover_orphan(self) -> None:
        print("=== Crypto Momentum V1.4.12 Orphan Entry Recovery ===")
        print("This command does not create a new trade. It can only adopt one recent Coinbase BUY that")
        print("already exists in the dedicated strategy portfolio and matches a recent qualifying scan.")
        state = self.exec.recover_recent_orphan_position()
        print(
            f"Recovered {state.product_id}: qty={state.quantity:.12g} entry={state.entry_price:.8f} "
            f"stop={state.stop_price:.8f} protection_order={state.stop_order_id}"
        )
        print("Recovery complete. Run status.cmd and verify the protective sell order in Coinbase.")

    def _screener_log_path(self) -> Path:
        configured = str(self.s.runtime.get("screener_log_path", "logs/screener_log.log"))
        path = Path(configured)
        if not path.is_absolute():
            path = self.s.project_root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _heartbeat_path(self) -> Path:
        configured = str(self.s.runtime.get("heartbeat_path", "data/runtime_heartbeat.json"))
        path = Path(configured)
        if not path.is_absolute():
            path = self.s.project_root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _runtime_fatal_log_path(self) -> Path:
        configured = str(self.s.runtime.get("runtime_fatal_log_path", "logs/runtime_fatal.log"))
        path = Path(configured)
        if not path.is_absolute():
            path = self.s.project_root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _write_heartbeat(
        self,
        *,
        state: str,
        next_scan: datetime | None = None,
        next_refresh: datetime | None = None,
        next_position_check: datetime | None = None,
        detail: str | None = None,
    ) -> None:
        """Atomically publish liveness/debug state without touching trading logic."""
        try:
            last_scan = self.db.get_json("last_completed_scan", {}) or {}
            position = self.db.get_position()
            shadow_engine = getattr(self, "shadow", None)
            shadow_position = shadow_engine.get_position() if shadow_engine and shadow_engine.enabled else None
            payload = {
                "pid": os.getpid(),
                "strategy": self.s.strategy.get("name"),
                "mode": self.s.run_mode,
                "state": state,
                "updated_utc": datetime.now(timezone.utc).isoformat(),
                "last_scan": last_scan,
                "last_shadow_scan": self.db.get_json("last_shadow_15m_scan", {}) or {},
                "shadow_15m_enabled": bool(shadow_engine and shadow_engine.enabled),
                "shadow_15m_position": shadow_position.product_id if shadow_position else None,
                "next_signal_scan_utc": next_scan.isoformat() if next_scan else None,
                "next_universe_refresh_utc": next_refresh.isoformat() if next_refresh else None,
                "next_position_check_utc": next_position_check.isoformat() if next_position_check else None,
                "has_position": position is not None,
                "position_product": position.product_id if position else None,
                "detail": detail,
            }
            path = self._heartbeat_path()
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            os.replace(tmp, path)
        except Exception as exc:
            # Heartbeat observability must never become a reason for the trading loop to die.
            log.warning("Runtime heartbeat update failed: %s", exc)

    def _append_runtime_fatal(self, context: str, exc: BaseException, *, recovered: bool) -> None:
        """Persist a traceback independently of the rotating application log."""
        try:
            path = self._runtime_fatal_log_path()
            last_scan = self.db.get_json("last_completed_scan", {}) or {}
            position = self.db.get_position()
            with path.open("a", encoding="utf-8", newline="\n") as fh:
                fh.write("\n" + "=" * 100 + "\n")
                fh.write(f"utc={datetime.now(timezone.utc).isoformat()}\n")
                fh.write(f"pid={os.getpid()} strategy={self.s.strategy.get('name')} mode={self.s.run_mode}\n")
                fh.write(f"context={context} recovered={recovered}\n")
                fh.write(f"last_scan={last_scan}\n")
                fh.write(f"position={position.to_dict() if position else None}\n")
                fh.write(f"exception={type(exc).__name__}: {exc}\n")
                fh.write("traceback:\n")
                fh.write("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except OSError:
                    pass
        except Exception as log_exc:
            log.error("Could not persist runtime fatal diagnostics: %s", log_exc)

    def _render_scan(self, result: ScanResult, scan_number: int) -> str:
        lines: list[str] = []
        lines.append("")
        lines.append("=" * 126)
        transition = "" if result.previous_regime in {"UNKNOWN", result.regime} else f" <- {result.previous_regime}"
        lines.append(
            f"SCAN #{scan_number} {datetime.now(timezone.utc).isoformat()} | regime={result.regime}{transition} "
            f"| breadth={result.breadth:.1%} ({result.regime_rows_available}/{result.regime_universe_size} regime rows) "
            f"| trading={result.trading_universe_size}"
        )
        lines.append("rank product          pct    24h      72h      7d       14d      ATR%    trend breakout persist")
        for r in result.rows[:12]:
            persist = int(result.persistence_counts.get(r.product_id, 0))
            lines.append(
                f"{r.rank:>4} {r.product_id:<15} {r.percentile:>5.1%} "
                f"{r.r24:>8.2%} {r.r72:>8.2%} {r.r168:>8.2%} {r.r336:>8.2%} "
                f"{r.atr_pct:>7.2%} {str(r.trend_pass):>6} {str(r.breakout):>8} {persist:>7}"
            )

        top_n = int(self.s.signal.get("diagnostic_top_n", 10))
        lines.append("")
        lines.append("Candidate diagnostics (top ranked):")
        lines.append("product          persist  rawStop  effStop  result / rejection reasons")
        for d in result.diagnostics[:top_n]:
            reasons = "ACTIONABLE" if d.actionable else ", ".join(d.reasons)
            lines.append(
                f"{d.product_id:<16} {d.persistence:>2}/{d.persistence_required:<2} "
                f"{d.raw_stop_pct:>7.2%} {d.effective_stop_pct:>7.2%}  {reasons}"
            )

        lines.append("")
        if result.candidates:
            lines.append("Actionable after persistence/entry filters: " + ", ".join(r.product_id for r in result.candidates[:8]))
        else:
            lines.append("Actionable after persistence/entry filters: none")
        return "\n".join(lines)

    def _append_screener_log(self, block: str) -> None:
        path = self._screener_log_path()
        with path.open("a", encoding="utf-8", newline="\n") as fh:
            fh.write(block)
            if not block.endswith("\n"):
                fh.write("\n")
            fh.flush()
            try:
                import os
                os.fsync(fh.fileno())
            except OSError:
                pass

    def _print_scan(self, result: ScanResult, scan_number: int) -> None:
        block = self._render_scan(result, scan_number)
        print(block, flush=True)
        transcript = getattr(self, "_active_scan_transcript", None)
        if transcript is not None:
            transcript.append(block + "\n")

    def scan_once(self) -> None:
        """Run one live signal cycle, then the independent observation-only shadow cycle.

        `logs/screener_log.log` remains the exact live-engine transcript. Shadow output is kept
        separate in `logs/shadow_15m.log` so hypothetical signals can never be mistaken for
        Coinbase actions. A shadow failure is logged but cannot abort or roll back a live scan.
        """
        scan_number = self.db.increment_counter("screener_log_scan_number", start=0)
        transcript: list[str] = []
        primary_success = False

        class _TranscriptHandler(logging.Handler):
            def emit(inner_self, record: logging.LogRecord) -> None:
                try:
                    transcript.append(inner_self.format(record) + "\n")
                except Exception:
                    inner_self.handleError(record)

        handler = _TranscriptHandler(level=logging.INFO)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S"))
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        self._active_scan_transcript = transcript
        try:
            self._scan_once_impl(scan_number)
            primary_success = True
        except Exception as exc:
            transcript.append(f"SCAN #{scan_number} ABORTED: {type(exc).__name__}: {exc}\n")
            raise
        finally:
            root_logger.removeHandler(handler)
            self._active_scan_transcript = None
            if transcript:
                self._append_screener_log("".join(transcript).rstrip("\n") + "\n")
            self.db.set_json(
                "last_completed_scan",
                {"number": scan_number, "utc": datetime.now(timezone.utc).isoformat()},
            )

        shadow_engine = getattr(self, "shadow", None)
        if primary_success and shadow_engine is not None and shadow_engine.enabled:
            try:
                self._shadow_scan_once()
            except Exception as exc:
                self.db.log_error("shadow_15m_scan", str(exc))
                log.exception("Shadow 15m scan failed; live engine remains authoritative and continues: %s", exc)

    def _shadow_scan_once(self) -> None:
        if not self.shadow.enabled:
            return
        _live, shadow_products, regime = self.strategy.universe_snapshot()
        if not shadow_products or not regime:
            log.warning("Shadow 15m scan skipped: dedicated shadow/regime universe snapshot is not hydrated yet.")
            return
        bucket = self.shadow.bucket_id()
        if self.db.get_json("last_shadow_15m_bucket", None) == bucket:
            log.info("Shadow 15m bucket %s already processed; skipping duplicate restart observation.", bucket)
            return
        # Shadow membership is 30-minute stable, but every 15-minute simulation gets a fresh
        # top-of-book snapshot for current-price overlays and cost/spread diagnostics.
        try:
            self.strategy.refresh_shadow_quotes()
            _live, shadow_products, regime = self.strategy.universe_snapshot()
        except Exception as exc:
            log.warning("Shadow quote refresh failed; using last shadow-universe marks: %s", exc)
        number = self.db.increment_counter("shadow_15m_scan_number", start=0)
        result = self.shadow.scan(shadow_products, regime)
        block = self.shadow.render(result, number)
        self.shadow.append_log(block + "\n")
        self.db.set_json(
            "last_shadow_15m_scan",
            {"number": number, "utc": datetime.now(timezone.utc).isoformat()},
        )
        self.db.set_json("last_shadow_15m_bucket", bucket)
        for event in result.events:
            log.info("%s", event)
        log.info(
            "Shadow 15m scan #%s complete: universe=%s regime=%s breadth=%.1f%% actionable=%s position=%s",
            number, result.universe_size, result.regime, result.breadth * 100, len(result.candidates),
            result.position.product_id if result.position else "FLAT",
        )

    def _scan_once_impl(self, scan_number: int) -> None:
        state = None
        try:
            state = self.exec.reconcile()
        except RuntimeError as exc:
            if self.s.is_live:
                raise
            log.warning("Reconciliation warning in non-live mode: %s", exc)

        current_pid = state.product_id if state else None
        result = self.strategy.scan(current_product=current_pid)
        self._print_scan(result, scan_number)

        if state:
            product = result.products.get(state.product_id)
            row = next((r for r in result.rows if r.product_id == state.product_id), None)
            if product is None:
                product_raw = self.gateway.get_product(state.product_id)
                product = self.gateway.product_meta(product_raw)
            if product is None:
                raise RuntimeError(f"Cannot load product metadata for live position {state.product_id}")
            if row is not None and hasattr(self.exec, "record_position_mark"):
                state = self.exec.record_position_mark(state, row.price)

            # The account-drawdown rule is a true kill switch, not merely an entry filter.
            # Other risk_halts (daily realized loss, entry count, manual halt) block NEW entries only.
            halted, halt_reason = self.exec.risk_halts(result.products)
            if halt_reason.startswith("account drawdown kill switch"):
                log.critical("ACCOUNT KILL SWITCH: %s", halt_reason)
                self.db.set_json("manual_halt", True)
                if self.s.is_live:
                    self.exec.exit(state, product, reason="ACCOUNT_DRAWDOWN_KILL_SWITCH")
                    return
                log.warning("%s mode: account kill switch would force an exit in live mode.", self.s.run_mode.upper())

            state = self.exec.ensure_protection(state, product)
            if state is None:
                return
            if row is None:
                log.warning("No fresh signal row for current position; keeping exchange stop and taking no discretionary action.")
                return

            # Enforce an already-earned V1.4.11 capture floor even if a restart or a deferred
            # cancel/replace left the installed Coinbase stop temporarily below that floor.
            # Use the freshest sell-side bid when available rather than the ranking midpoint.
            lock = self.exec.profit_lock_levels(state)
            sell_mark = product.best_bid if product.best_bid > 0 else row.price
            if (
                bool(lock["armed"])
                and float(lock["trigger_price"]) > state.stop_price
                and sell_mark <= float(lock["trigger_price"])
            ):
                log.warning(
                    "Signal-scan profit-lock backstop for %s: sell mark %.8f <= capture floor %.8f (MFE=%+.2f%%)",
                    state.product_id,
                    sell_mark,
                    float(lock["trigger_price"]),
                    float(lock["mfe_return"]) * 100,
                )
                self.exec.exit(state, product, reason="PROFIT_LOCK_SCAN_BACKSTOP")
                state = None
            # Hard trend failure: require both fast-trend break and non-positive 24h momentum.
            elif row.price < row.ema_fast and row.r24 <= 0:
                self.exec.exit(state, product, reason="TREND_FAILURE")
                state = None
            else:
                state = self.exec.trail_stop(state, row, product)

            # Rotate only when deterioration is meaningful enough to justify two additional trades.
            if state and result.candidates:
                best = result.candidates[0]
                gap = best.percentile - row.percentile
                if (
                    best.product_id != state.product_id
                    and row.percentile < float(self.s.rotation["hold_rank_floor_percentile"])
                    and best.percentile >= float(self.s.rotation["replacement_min_percentile"])
                    and gap >= float(self.s.rotation["replacement_score_gap"])
                ):
                    log.info("Rotation trigger: %s pct=%.1f%% -> %s pct=%.1f%%",
                             state.product_id, row.percentile * 100, best.product_id, best.percentile * 100)
                    exited = self.exec.exit(state, product, reason="MOMENTUM_ROTATION")
                    if exited:
                        state = None

        if state is None and result.candidates:
            halted, reason = self.exec.risk_halts(result.products)
            if halted:
                log.warning("No new entry: %s", reason)
                return
            # Try candidates in score order; a wide spread or bad preview can reject the first one.
            for candidate in result.candidates:
                product = result.products.get(candidate.product_id)
                if not product:
                    continue
                if self.s.run_mode == "observe":
                    log.warning(
                        "OBSERVE: top actionable candidate %s rank=%s pct=%.1f%%; no preview/order sent",
                        candidate.product_id, candidate.rank, candidate.percentile * 100,
                    )
                    break
                before = self.db.get_position()
                self.exec.enter(candidate, product)
                after = self.db.get_position()
                if after is not None or self.s.is_dry_run:
                    break
                if before is not None:
                    break

    def position_check_once(self) -> None:
        """Lightweight 5-minute management pass for an existing position only.

        This does not rank the universe or create entries/rotations. It reconciles Coinbase,
        verifies exchange protection, checks the account kill switch, marks the held asset to
        the current price, provides a market-exit backstop if a stop-limit gaps through, checks
        the existing trend-failure exit, and tightens the trailing stop when warranted.
        """
        state = self.db.get_position()
        if state is None:
            return

        try:
            state = self.exec.reconcile()
        except RuntimeError as exc:
            if self.s.is_live:
                raise
            log.warning("5m position reconciliation warning in non-live mode: %s", exc)
            state = self.db.get_position()
        if state is None:
            return

        row, product = self.strategy.position_monitor(state.product_id)
        if product is None:
            raise RuntimeError(f"5m monitor cannot load product metadata for {state.product_id}")
        if row is not None and hasattr(self.exec, "record_position_mark"):
            state = self.exec.record_position_mark(state, row.price)

        halted, halt_reason = self.exec.risk_halts({state.product_id: product})
        if halt_reason.startswith("account drawdown kill switch"):
            log.critical("ACCOUNT KILL SWITCH (5m monitor): %s", halt_reason)
            self.db.set_json("manual_halt", True)
            if self.s.is_live:
                self.exec.exit(state, product, reason="ACCOUNT_DRAWDOWN_KILL_SWITCH_5M")
                return
            log.warning("%s mode: 5m account kill switch would force an exit in live mode.", self.s.run_mode.upper())

        state = self.exec.ensure_protection(state, product)
        if state is None or row is None:
            if row is None:
                log.warning("5m monitor has no valid signal row for %s; exchange protection remains active.", state.product_id if state else "position")
            return

        lock = self.exec.profit_lock_levels(state)
        log.info(
            "POSITION CHECK 5m %s mark=%.8f ema24=%.8f r24=%+.2f%% stop=%.8f high=%.8f "
            "profitLock=%s captureFloor=%+.2f%%",
            state.product_id,
            row.price,
            row.ema_fast,
            row.r24 * 100,
            state.stop_price,
            state.highest_close,
            "ON" if bool(lock["armed"]) else "OFF",
            float(lock["protected_return"]) * 100,
        )

        # If a restart or the existing anti-churn threshold left the exchange stop below an
        # already-armed V1.4.11 profit floor, enforce the floor with the same market-exit
        # backstop philosophy used for a stop-limit gap-through. This is only evaluated from
        # a previously persisted high-water mark; the current mark cannot arm and instantly
        # trip the rule in the same pass.
        if (
            bool(lock["armed"])
            and float(lock["trigger_price"]) > state.stop_price
            and row.price <= float(lock["trigger_price"])
        ):
            log.warning(
                "5m profit-lock backstop triggered for %s: mark %.8f <= capture floor %.8f (MFE=%+.2f%%)",
                state.product_id,
                row.price,
                float(lock["trigger_price"]),
                float(lock["mfe_return"]) * 100,
            )
            self.exec.exit(state, product, reason="PROFIT_LOCK_5M_BACKSTOP")
            return

        # Coinbase stop-limit protection can theoretically trigger and remain unfilled if price
        # gaps through its limit. While the bot is running, the 5-minute monitor adds a market
        # exit backstop once the current sell-side mark is at/below our stop trigger.
        if row.price <= state.stop_price:
            log.warning(
                "5m stop backstop triggered for %s: mark %.8f <= stop %.8f",
                state.product_id, row.price, state.stop_price,
            )
            self.exec.exit(state, product, reason="FIVE_MIN_STOP_BACKSTOP")
            return

        # Same structural trend-failure rule as the main scan, but marked to current price.
        # This makes exits responsive intrahour without letting 5-minute noise invent a new alpha model.
        if row.price < row.ema_fast and row.r24 <= 0:
            log.warning(
                "5m trend-failure exit for %s: mark %.8f < EMA24 %.8f and r24=%+.2f%%",
                state.product_id, row.price, row.ema_fast, row.r24 * 100,
            )
            self.exec.exit(state, product, reason="TREND_FAILURE_5M")
            return

        self.exec.trail_stop(state, row, product)

    @staticmethod
    def _next_aligned_time(interval_minutes: int, offset_minute_utc: int) -> datetime:
        if interval_minutes <= 0 or 1440 % interval_minutes != 0:
            raise ValueError("refresh interval must be a positive divisor of 1440 minutes")
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elapsed = (now - day_start).total_seconds() / 60.0
        offset = offset_minute_utc % interval_minutes
        slot = math.floor((elapsed - offset) / interval_minutes) + 1
        candidate = day_start + timedelta(minutes=offset + slot * interval_minutes)
        if candidate <= now:
            candidate += timedelta(minutes=interval_minutes)
        return candidate

    def _next_scan_time(self) -> datetime:
        return self._next_aligned_time(
            int(self.s.runtime.get("signal_scan_interval_minutes", 30)),
            int(self.s.runtime.get("signal_scan_offset_minute_utc", self.s.runtime.get("scan_minute_utc", 3))),
        )

    def _next_position_check_time(self) -> datetime:
        return self._next_aligned_time(
            int(self.s.runtime.get("position_exit_check_minutes", 5)),
            int(self.s.runtime.get("position_exit_check_offset_minute_utc", 3)),
        )

    def _next_universe_refresh_time(self) -> datetime:
        return self._next_aligned_time(
            int(self.s.universe.get("refresh_minutes", 30)),
            int(self.s.runtime.get("universe_refresh_offset_minute_utc", self.s.runtime["scan_minute_utc"])),
        )

    def _refresh_universe_only(self) -> None:
        state = self.db.get_position()
        current_pid = state.product_id if state else None
        info = self.strategy.refresh_trading_universe(current_product=current_pid)
        stats = info.get("trading_stats") or {}
        log.info(
            "TRADING UNIVERSE REFRESH: %s eligible | pre-cap=%s | rejected volume=%s spread=%s history_age=%s",
            info.get("trading_count", 0),
            stats.get("eligible_before_cap", "?"),
            stats.get("volume", "?"),
            stats.get("spread", "?"),
            stats.get("history_age", "?"),
        )
        if info.get("shadow_refreshed"):
            ss = info.get("shadow_stats") or {}
            log.info(
                "SHADOW UNIVERSE REFRESH: %s research-only eligible | pre-cap=%s | rejected volume=%s spread=%s history_age=%s",
                info.get("shadow_count", 0),
                ss.get("eligible_before_cap", "?"),
                ss.get("volume", "?"),
                ss.get("spread", "?"),
                ss.get("history_age", "?"),
            )
        audit = info.get("universe_audit") or {}
        if audit:
            volumes = audit.get("volume_counts", {}) or {}
            log.info(
                "UNIVERSE AUDIT: catalog=%s complete=%s | current=%s cap_binding=%s | volume tiers=%s | USDC-only quality bases=%s",
                (audit.get("catalog", {}) or {}).get("unique_products", audit.get("raw_products")),
                (audit.get("catalog", {}) or {}).get("pagination_complete", "?"),
                audit.get("current_eligible_before_cap", "?"),
                audit.get("cap_binding", "?"),
                volumes,
                audit.get("usdc_quality_bases_not_in_usd", "?"),
            )
        if info.get("regime_refreshed"):
            rs = info.get("regime_stats") or {}
            log.info(
                "REGIME UNIVERSE REFRESH: %s stable members | pre-cap=%s | rejected volume=%s spread=%s history_age=%s",
                info.get("regime_count", 0),
                rs.get("eligible_before_cap", "?"),
                rs.get("volume", "?"),
                rs.get("spread", "?"),
                rs.get("history_age", "?"),
            )

    def run(self) -> None:
        try:
            self.lock.acquire(timeout=0)
        except Timeout as exc:
            raise RuntimeError("Another bot instance appears to be running (data/bot.lock is held).") from exc

        signal.signal(signal.SIGINT, self.request_stop)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self.request_stop)

        log.info("Starting %s | mode=%s", self.s.strategy["name"], self.s.run_mode)
        if bool(self.db.get_json("manual_halt", False)):
            log.critical(
                "MANUAL ENTRY HALT ACTIVE: new entries are disabled. Existing positions/protection still run. "
                "Run resume_new_entries.cmd to clear the halt if live entries are intended."
            )
        self._write_heartbeat(state="STARTING")
        scheduler_failures: deque[float] = deque()
        failure_window_seconds = float(self.s.runtime.get("scheduler_failure_window_seconds", 600))
        max_failures = int(self.s.runtime.get("scheduler_max_failures_in_window", 5))
        recovery_sleep = float(self.s.runtime.get("scheduler_recovery_seconds", 5))

        try:
            if bool(self.s.runtime.get("websocket_enabled", True)):
                self.ws = WebSocketHealthMonitor(
                    self.s.api_key,
                    self.s.api_secret,
                    self.s.runtime["websocket_health_product"],
                    reconnect_max_seconds=float(self.s.runtime.get("websocket_reconnect_max_seconds", 30)),
                )
                self.ws.start()

            # Run immediately on startup so gaps are naturally reconstructed from REST candles.
            try:
                self._write_heartbeat(state="STARTUP_SCAN")
                self.scan_once()
            except Exception as exc:
                self.db.log_error("startup_scan", str(exc))
                log.exception("Startup scan failed; bot remains alive and will retry next scan: %s", exc)

            # V1.4.5: an outer recovery boundary protects scheduler plumbing itself. Individual
            # scan/refresh/position jobs still have their own narrower exception handlers below.
            while not self.stop_requested:
                try:
                    next_scan = self._next_scan_time()
                    next_refresh = self._next_universe_refresh_time()
                    has_position = self.db.get_position() is not None
                    next_position_check = self._next_position_check_time() if has_position else None
                    event_times = [next_scan, next_refresh]
                    if next_position_check is not None:
                        event_times.append(next_position_check)
                    next_event = min(event_times)

                    if next_position_check is not None:
                        log.info(
                            "Next universe refresh: %s UTC | next signal scan: %s UTC | next open-position check: %s UTC",
                            next_refresh.isoformat(), next_scan.isoformat(), next_position_check.isoformat(),
                        )
                    else:
                        log.info(
                            "Next universe refresh: %s UTC | next signal scan: %s UTC | open-position 5m monitor: IDLE",
                            next_refresh.isoformat(), next_scan.isoformat(),
                        )

                    self._write_heartbeat(
                        state="WAITING",
                        next_scan=next_scan,
                        next_refresh=next_refresh,
                        next_position_check=next_position_check,
                    )

                    while not self.stop_requested and datetime.now(timezone.utc) < next_event:
                        if self.ws:
                            age = self.ws.age_seconds()
                            stale_after = float(self.s.runtime.get("websocket_stale_after_seconds", 30))
                            repeat_after = float(self.s.runtime.get("websocket_stale_warning_repeat_seconds", 120))
                            if age is not None and age > stale_after:
                                now_mono = time.monotonic()
                                if (
                                    self._last_ws_stale_warning_monotonic <= 0
                                    or now_mono - self._last_ws_stale_warning_monotonic >= repeat_after
                                ):
                                    snap = self.ws.snapshot()
                                    log.warning(
                                        "WebSocket health feed stale (%.1fs, connected=%s); REST strategy remains authoritative.",
                                        age,
                                        snap.get("connected"),
                                    )
                                    self._last_ws_stale_warning_monotonic = now_mono
                            else:
                                self._last_ws_stale_warning_monotonic = 0.0
                        self._write_heartbeat(
                            state="WAITING",
                            next_scan=next_scan,
                            next_refresh=next_refresh,
                            next_position_check=next_position_check,
                        )
                        time.sleep(min(float(self.s.runtime["idle_poll_seconds"]), 15.0))
                    if self.stop_requested:
                        break

                    now = datetime.now(timezone.utc)
                    # Universe membership refresh remains :03/:33 while live decisions run every 15m.
                    # On aligned boundaries refresh membership first; in-between scans refresh quotes only.
                    if now >= next_refresh:
                        try:
                            self._write_heartbeat(state="UNIVERSE_REFRESH", next_scan=next_scan, next_refresh=next_refresh)
                            self._refresh_universe_only()
                        except Exception as exc:
                            self.db.log_error("universe_refresh", str(exc))
                            log.exception("Trading-universe refresh failed; existing cached universe remains: %s", exc)

                    did_signal_scan = False
                    if now >= next_scan:
                        try:
                            self._write_heartbeat(state="SIGNAL_SCAN", next_scan=next_scan, next_refresh=next_refresh)
                            self.scan_once()
                            did_signal_scan = True
                        except Exception as exc:
                            self.db.log_error("scheduled_scan", str(exc))
                            log.exception("Scheduled signal scan failed; will retry at next configured signal cycle: %s", exc)

                    # The full signal scan already manages any open position. Do not immediately
                    # duplicate that work when a 5-minute boundary coincides with a full signal scan.
                    if (not did_signal_scan) and next_position_check is not None and now >= next_position_check:
                        try:
                            self._write_heartbeat(state="POSITION_5M_CHECK", next_position_check=next_position_check)
                            self.position_check_once()
                        except Exception as exc:
                            self.db.log_error("position_5m_check", str(exc))
                            log.exception("5-minute position check failed; exchange-side stop remains authoritative: %s", exc)

                    # Successful scheduler iteration clears stale failures outside the configured window.
                    cutoff = time.monotonic() - failure_window_seconds
                    while scheduler_failures and scheduler_failures[0] < cutoff:
                        scheduler_failures.popleft()

                except Exception as exc:
                    now_mono = time.monotonic()
                    scheduler_failures.append(now_mono)
                    cutoff = now_mono - failure_window_seconds
                    while scheduler_failures and scheduler_failures[0] < cutoff:
                        scheduler_failures.popleft()
                    self.db.log_error("outer_scheduler", f"{type(exc).__name__}: {exc}")
                    log.exception(
                        "OUTER SCHEDULER ERROR recovered (%s/%s inside %.0fs): %s",
                        len(scheduler_failures), max_failures, failure_window_seconds, exc,
                    )
                    self._append_runtime_fatal("outer_scheduler_recovered", exc, recovered=True)
                    self._write_heartbeat(state="RECOVERING", detail=f"{type(exc).__name__}: {exc}")
                    if len(scheduler_failures) >= max_failures:
                        terminal = RuntimeError(
                            f"Scheduler restart-storm guard: {len(scheduler_failures)} failures inside "
                            f"{failure_window_seconds:.0f}s; escalating to external watchdog"
                        )
                        self._append_runtime_fatal("scheduler_restart_storm", terminal, recovered=False)
                        raise terminal from exc
                    # Reconcile from Coinbase on the next normal scan/position pass; do not make
                    # any trading decision while scheduler internals are recovering.
                    end = time.monotonic() + recovery_sleep
                    while not self.stop_requested and time.monotonic() < end:
                        time.sleep(min(1.0, max(0.0, end - time.monotonic())))
        finally:
            self._write_heartbeat(state="STOPPING", detail="clean shutdown path")
            if self.ws:
                self.ws.stop()
            try:
                self.lock.release()
            except Exception:
                pass
            self._write_heartbeat(state="STOPPED", detail="exchange-side protection left intact")
            log.info("Bot stopped. Existing exchange-side stop orders were left intact.")
