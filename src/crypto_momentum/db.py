from __future__ import annotations

import csv
import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable

from .models import PositionState, SignalRow


log = logging.getLogger(__name__)


class StateDB:
    def __init__(self, path: Path, trade_log_path: Path | None = None):
        self.path = path
        self.trade_log_path = trade_log_path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.trade_log_path is not None:
            self.trade_log_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()
        if self.trade_log_path is not None:
            # V1.4.12 migration is additive. Existing event history remains authoritative;
            # build one-position ledger rows from it once, then keep the CSV mirror current.
            self.backfill_trade_ledger_from_events()
            self._seed_open_ledger_from_position()
            self.export_trade_ledger_csv()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connect() as c:
            c.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    regime TEXT NOT NULL,
                    breadth REAL NOT NULL,
                    top_product TEXT,
                    top_percentile REAL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    event TEXT NOT NULL,
                    product_id TEXT,
                    side TEXT,
                    quantity REAL,
                    price REAL,
                    fees REAL,
                    realized_pnl REAL,
                    order_id TEXT,
                    details TEXT
                );
                CREATE TABLE IF NOT EXISTS cooldowns (
                    product_id TEXT PRIMARY KEY,
                    until_ts TEXT NOT NULL,
                    reason TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    context TEXT NOT NULL,
                    message TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS shadow_scans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    regime TEXT NOT NULL,
                    breadth REAL NOT NULL,
                    ranked_count INTEGER NOT NULL,
                    rank_pass_count INTEGER NOT NULL,
                    trend_pass_count INTEGER NOT NULL,
                    persistence_pass_count INTEGER NOT NULL,
                    breakout_pass_count INTEGER NOT NULL,
                    actionable_count INTEGER NOT NULL,
                    top_product TEXT,
                    details TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS shadow_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_ts TEXT NOT NULL,
                    exit_ts TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL NOT NULL,
                    gross_return REAL NOT NULL,
                    net_return REAL NOT NULL,
                    mfe REAL NOT NULL,
                    mae REAL NOT NULL,
                    holding_minutes REAL NOT NULL,
                    exit_reason TEXT NOT NULL,
                    details TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS trade_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_key TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    entry_ts TEXT NOT NULL,
                    entry_order_id TEXT,
                    entry_execution_path TEXT,
                    entry_price REAL NOT NULL,
                    entry_quantity REAL NOT NULL,
                    entry_notional REAL NOT NULL,
                    entry_fee REAL NOT NULL,
                    entry_cash_outlay REAL NOT NULL,
                    initial_stop_price REAL,
                    initial_risk_pct REAL,
                    initial_risk_usd REAL,
                    entry_rank INTEGER,
                    entry_percentile REAL,
                    entry_r24 REAL,
                    entry_r72 REAL,
                    entry_r168 REAL,
                    entry_r336 REAL,
                    entry_atr_pct REAL,
                    highest_mark REAL,
                    lowest_mark REAL,
                    exit_ts TEXT,
                    exit_order_id TEXT,
                    exit_reason TEXT,
                    exit_price REAL,
                    exit_quantity REAL,
                    exit_notional REAL,
                    exit_fee REAL,
                    exit_net_proceeds REAL,
                    total_fees REAL,
                    gross_pnl REAL,
                    net_pnl REAL,
                    gross_return REAL,
                    net_return REAL,
                    mfe REAL,
                    mae REAL,
                    r_multiple REAL,
                    capture_efficiency REAL,
                    holding_minutes REAL,
                    source TEXT NOT NULL,
                    analytics_quality TEXT NOT NULL,
                    details TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_ledger_entry_order
                    ON trade_ledger(entry_order_id) WHERE entry_order_id IS NOT NULL;
                """
            )

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def set_json(self, key: str, value: Any) -> None:
        with self.connect() as c:
            c.execute(
                "INSERT INTO kv(key,value,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (key, json.dumps(value), self.now_iso()),
            )

    def increment_counter(self, key: str, start: int = 0) -> int:
        """Atomically increment an integer counter stored in kv and return the new value."""
        with self.connect() as c:
            row = c.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
            try:
                current = int(json.loads(row["value"])) if row is not None else int(start)
            except (TypeError, ValueError, json.JSONDecodeError):
                current = int(start)
            new_value = current + 1
            c.execute(
                "INSERT INTO kv(key,value,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (key, json.dumps(new_value), self.now_iso()),
            )
        return new_value

    def get_json(self, key: str, default: Any = None) -> Any:
        with self.connect() as c:
            row = c.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return default if row is None else json.loads(row["value"])

    def set_position(self, position: PositionState | None) -> None:
        self.set_json("position", None if position is None else position.to_dict())

    def get_position(self) -> PositionState | None:
        data = self.get_json("position")
        if not data:
            return None
        return PositionState(**data)

    def log_scan(self, regime: str, breadth: float, rows: Iterable[SignalRow]) -> None:
        ordered = sorted(rows, key=lambda x: x.rank if x.rank else 10**9)
        top = ordered[0] if ordered else None
        payload = json.dumps([r.to_dict() for r in ordered[:30]])
        with self.connect() as c:
            c.execute(
                "INSERT INTO scans(ts,regime,breadth,top_product,top_percentile,payload) VALUES(?,?,?,?,?,?)",
                (
                    self.now_iso(), regime, breadth,
                    top.product_id if top else None,
                    top.percentile if top else None,
                    payload,
                ),
            )


    def latest_signal_snapshot(self, product_id: str, lookback_scans: int = 20) -> dict[str, Any] | None:
        """Return the newest persisted scan row for a product, including scan timestamp.

        Used only for explicit orphan-entry recovery after a confirmed live fill where local
        state was lost. Normal trading never reconstructs positions from scans.
        """
        with self.connect() as c:
            rows = c.execute(
                "SELECT ts, payload FROM scans ORDER BY id DESC LIMIT ?",
                (int(lookback_scans),),
            ).fetchall()
        for scan in rows:
            try:
                payload = json.loads(scan["payload"])
            except (TypeError, json.JSONDecodeError):
                continue
            for row in payload if isinstance(payload, list) else []:
                if str(row.get("product_id") or "") == product_id:
                    return {"ts": scan["ts"], "row": row}
        return None

    def log_trade(
        self,
        event: str,
        product_id: str | None = None,
        side: str | None = None,
        quantity: float | None = None,
        price: float | None = None,
        fees: float | None = None,
        realized_pnl: float | None = None,
        order_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.connect() as c:
            c.execute(
                "INSERT INTO trades(ts,event,product_id,side,quantity,price,fees,realized_pnl,order_id,details) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    self.now_iso(), event, product_id, side, quantity, price, fees,
                    realized_pnl, order_id, json.dumps(details or {}),
                ),
            )

    @staticmethod
    def _parse_details(raw: Any) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        try:
            value = json.loads(raw or "{}")
            return value if isinstance(value, dict) else {}
        except (TypeError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _iso_minutes(start: str | None, end: str | None) -> float | None:
        if not start or not end:
            return None
        try:
            s = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
            e = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
            if s.tzinfo is None:
                s = s.replace(tzinfo=timezone.utc)
            if e.tzinfo is None:
                e = e.replace(tzinfo=timezone.utc)
            return max(0.0, (e - s).total_seconds() / 60.0)
        except ValueError:
            return None

    @staticmethod
    def _entry_key(product_id: str, entry_ts: str, entry_order_id: str | None) -> str:
        return f"order:{entry_order_id}" if entry_order_id else f"entry:{product_id}:{entry_ts}"

    def open_trade_ledger(
        self,
        *,
        product_id: str,
        entry_ts: str,
        entry_price: float,
        quantity: float,
        entry_fee: float,
        initial_stop_price: float | None,
        initial_risk_pct: float | None,
        entry_order_id: str | None,
        execution_path: str | None = None,
        signal: dict[str, Any] | None = None,
        source: str = "LIVE",
        analytics_quality: str = "FULL_SAMPLED",
        details: dict[str, Any] | None = None,
        export: bool = True,
    ) -> None:
        if entry_price <= 0 or quantity <= 0:
            return
        sig = signal or {}
        entry_notional = float(entry_price) * float(quantity)
        cash_outlay = entry_notional + float(entry_fee or 0.0)
        risk_usd = None
        if initial_stop_price is not None and float(initial_stop_price) > 0:
            risk_usd = max(0.0, (float(entry_price) - float(initial_stop_price)) * float(quantity))
        key = self._entry_key(product_id, entry_ts, entry_order_id)
        payload = json.dumps(details or {})
        with self.connect() as c:
            existing = None
            if entry_order_id:
                existing = c.execute(
                    "SELECT id FROM trade_ledger WHERE entry_order_id=?", (entry_order_id,)
                ).fetchone()
            if existing is None:
                existing = c.execute(
                    "SELECT id FROM trade_ledger WHERE entry_key=?", (key,)
                ).fetchone()
            if existing is not None:
                # Recovery/restart paths can report the same Coinbase BUY more than once.
                # Preserve the original ledger row instead of creating a duplicate trade.
                return
            c.execute(
                "INSERT INTO trade_ledger("
                "entry_key,status,product_id,entry_ts,entry_order_id,entry_execution_path,entry_price,entry_quantity,"
                "entry_notional,entry_fee,entry_cash_outlay,initial_stop_price,initial_risk_pct,initial_risk_usd,"
                "entry_rank,entry_percentile,entry_r24,entry_r72,entry_r168,entry_r336,entry_atr_pct,"
                "highest_mark,lowest_mark,mfe,mae,source,analytics_quality,details) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    key, "OPEN", product_id, entry_ts, entry_order_id, execution_path,
                    float(entry_price), float(quantity), entry_notional, float(entry_fee or 0.0), cash_outlay,
                    float(initial_stop_price) if initial_stop_price is not None else None,
                    float(initial_risk_pct) if initial_risk_pct is not None else None,
                    risk_usd,
                    int(sig.get("rank")) if sig.get("rank") is not None else None,
                    float(sig.get("percentile")) if sig.get("percentile") is not None else None,
                    float(sig.get("r24")) if sig.get("r24") is not None else None,
                    float(sig.get("r72")) if sig.get("r72") is not None else None,
                    float(sig.get("r168")) if sig.get("r168") is not None else None,
                    float(sig.get("r336")) if sig.get("r336") is not None else None,
                    float(sig.get("atr_pct")) if sig.get("atr_pct") is not None else None,
                    float(entry_price), float(entry_price), 0.0, 0.0,
                    source, analytics_quality, payload,
                ),
            )
        if export:
            self.export_trade_ledger_csv()

    def update_trade_excursions(self, product_id: str, mark: float, *, export: bool = True) -> None:
        if mark <= 0:
            return
        with self.connect() as c:
            row = c.execute(
                "SELECT id,entry_price,highest_mark,lowest_mark FROM trade_ledger "
                "WHERE status='OPEN' AND product_id=? ORDER BY id DESC LIMIT 1",
                (product_id,),
            ).fetchone()
            if row is None:
                return
            entry = float(row["entry_price"])
            high = max(float(row["highest_mark"] or entry), float(mark), entry)
            low_raw = float(row["lowest_mark"] or entry)
            if low_raw <= 0:
                low_raw = entry
            low = min(low_raw, float(mark), entry)
            mfe = high / entry - 1.0 if entry > 0 else None
            mae = low / entry - 1.0 if entry > 0 else None
            c.execute(
                "UPDATE trade_ledger SET highest_mark=?,lowest_mark=?,mfe=?,mae=? WHERE id=?",
                (high, low, mfe, mae, int(row["id"])),
            )
        if export:
            self.export_trade_ledger_csv()

    def close_trade_ledger(
        self,
        *,
        product_id: str,
        exit_ts: str,
        exit_price: float,
        exit_quantity: float,
        exit_fee: float,
        exit_order_id: str | None,
        exit_reason: str,
        export: bool = True,
    ) -> None:
        if exit_price <= 0 or exit_quantity <= 0:
            return
        # Include the actual fill in excursion extremes before final analytics are frozen.
        self.update_trade_excursions(product_id, exit_price, export=False)
        with self.connect() as c:
            row = c.execute(
                "SELECT * FROM trade_ledger WHERE status='OPEN' AND product_id=? ORDER BY id DESC LIMIT 1",
                (product_id,),
            ).fetchone()
            if row is None:
                log.warning("Trade ledger could not find an open row for %s exit; event log remains authoritative.", product_id)
                return
            entry_price = float(row["entry_price"])
            entry_qty = float(row["entry_quantity"])
            qty = min(float(exit_quantity), entry_qty) if entry_qty > 0 else float(exit_quantity)
            entry_fee = float(row["entry_fee"] or 0.0)
            entry_notional = entry_price * qty
            exit_notional = float(exit_price) * float(exit_quantity)
            exit_fee_f = float(exit_fee or 0.0)
            gross_pnl = (float(exit_price) - entry_price) * qty
            net_pnl = gross_pnl - entry_fee - exit_fee_f
            gross_return = gross_pnl / entry_notional if entry_notional > 0 else None
            cash_outlay = entry_notional + entry_fee
            net_return = net_pnl / cash_outlay if cash_outlay > 0 else None
            risk_usd = float(row["initial_risk_usd"] or 0.0)
            r_multiple = net_pnl / risk_usd if risk_usd > 0 else None
            mfe = float(row["mfe"] or 0.0)
            capture = gross_return / mfe if mfe > 0 and gross_return is not None else None
            hold = self._iso_minutes(row["entry_ts"], exit_ts)
            c.execute(
                "UPDATE trade_ledger SET status='CLOSED',exit_ts=?,exit_order_id=?,exit_reason=?,exit_price=?,"
                "exit_quantity=?,exit_notional=?,exit_fee=?,exit_net_proceeds=?,total_fees=?,gross_pnl=?,net_pnl=?,"
                "gross_return=?,net_return=?,r_multiple=?,capture_efficiency=?,holding_minutes=? WHERE id=?",
                (
                    exit_ts, exit_order_id, exit_reason, float(exit_price), float(exit_quantity), exit_notional,
                    exit_fee_f, exit_notional - exit_fee_f, entry_fee + exit_fee_f, gross_pnl, net_pnl,
                    gross_return, net_return, r_multiple, capture, hold, int(row["id"]),
                ),
            )
        if export:
            self.export_trade_ledger_csv()

    def trade_ledger_rows(self) -> list[dict[str, Any]]:
        with self.connect() as c:
            rows = c.execute("SELECT * FROM trade_ledger ORDER BY id ASC").fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _fmt_csv(value: Any, digits: int = 10) -> Any:
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.{digits}f}".rstrip("0").rstrip(".")
        return value

    def export_trade_ledger_csv(self) -> None:
        if self.trade_log_path is None:
            return
        rows = self.trade_ledger_rows()
        headers = [
            "Trade #", "Asset", "Status", "Entry Time UTC", "Entry Price", "Entry Qty",
            "Entry Notional ($, ex fees)", "Entry Fee ($)", "Entry Cash Outlay ($, incl fees)",
            "Entry Execution", "Initial Stop Price", "Initial Risk (%)", "Initial Risk ($)",
            "Entry Rank", "Entry Percentile (%)", "24h Return at Entry (%)", "72h Return at Entry (%)",
            "7d Return at Entry (%)", "14d Return at Entry (%)", "ATR at Entry (%)",
            "Exit Time UTC", "Exit Price", "Exit Qty", "Exit Gross Proceeds ($)", "Exit Fee ($)",
            "Exit Net Proceeds ($)", "Total Fees ($)", "Exit Reason", "Gross PnL ($)", "Net PnL ($)",
            "Gross PnL (%)", "Net PnL (%)", "Highest Sampled Mark", "Lowest Sampled Mark", "MFE (%)",
            "MAE (%)", "R-Multiple (net PnL / initial risk)", "Capture Efficiency (%)", "Holding Minutes",
            "Entry Order ID", "Exit Order ID", "Source", "Analytics Quality",
        ]
        tmp = self.trade_log_path.with_suffix(self.trade_log_path.suffix + ".tmp")
        try:
            with tmp.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=headers)
                writer.writeheader()
                for r in rows:
                    pct = lambda x: "" if x is None else self._fmt_csv(float(x) * 100.0, 6)
                    writer.writerow({
                        "Trade #": r["id"],
                        "Asset": r["product_id"],
                        "Status": r["status"],
                        "Entry Time UTC": r["entry_ts"],
                        "Entry Price": self._fmt_csv(r["entry_price"]),
                        "Entry Qty": self._fmt_csv(r["entry_quantity"]),
                        "Entry Notional ($, ex fees)": self._fmt_csv(r["entry_notional"], 6),
                        "Entry Fee ($)": self._fmt_csv(r["entry_fee"], 6),
                        "Entry Cash Outlay ($, incl fees)": self._fmt_csv(r["entry_cash_outlay"], 6),
                        "Entry Execution": r["entry_execution_path"] or "",
                        "Initial Stop Price": self._fmt_csv(r["initial_stop_price"]),
                        "Initial Risk (%)": pct(r["initial_risk_pct"]),
                        "Initial Risk ($)": self._fmt_csv(r["initial_risk_usd"], 6),
                        "Entry Rank": r["entry_rank"] if r["entry_rank"] is not None else "",
                        "Entry Percentile (%)": pct(r["entry_percentile"]),
                        "24h Return at Entry (%)": pct(r["entry_r24"]),
                        "72h Return at Entry (%)": pct(r["entry_r72"]),
                        "7d Return at Entry (%)": pct(r["entry_r168"]),
                        "14d Return at Entry (%)": pct(r["entry_r336"]),
                        "ATR at Entry (%)": pct(r["entry_atr_pct"]),
                        "Exit Time UTC": r["exit_ts"] or "",
                        "Exit Price": self._fmt_csv(r["exit_price"]),
                        "Exit Qty": self._fmt_csv(r["exit_quantity"]),
                        "Exit Gross Proceeds ($)": self._fmt_csv(r["exit_notional"], 6),
                        "Exit Fee ($)": self._fmt_csv(r["exit_fee"], 6),
                        "Exit Net Proceeds ($)": self._fmt_csv(r["exit_net_proceeds"], 6),
                        "Total Fees ($)": self._fmt_csv(r["total_fees"], 6),
                        "Exit Reason": r["exit_reason"] or "",
                        "Gross PnL ($)": self._fmt_csv(r["gross_pnl"], 6),
                        "Net PnL ($)": self._fmt_csv(r["net_pnl"], 6),
                        "Gross PnL (%)": pct(r["gross_return"]),
                        "Net PnL (%)": pct(r["net_return"]),
                        "Highest Sampled Mark": self._fmt_csv(r["highest_mark"]),
                        "Lowest Sampled Mark": self._fmt_csv(r["lowest_mark"]),
                        "MFE (%)": pct(r["mfe"]),
                        "MAE (%)": pct(r["mae"]),
                        "R-Multiple (net PnL / initial risk)": self._fmt_csv(r["r_multiple"], 6),
                        "Capture Efficiency (%)": pct(r["capture_efficiency"]),
                        "Holding Minutes": self._fmt_csv(r["holding_minutes"], 2),
                        "Entry Order ID": r["entry_order_id"] or "",
                        "Exit Order ID": r["exit_order_id"] or "",
                        "Source": r["source"],
                        "Analytics Quality": r["analytics_quality"],
                    })
            os.replace(tmp, self.trade_log_path)
        except Exception as exc:
            log.warning("Trade ledger CSV export failed; SQLite ledger remains authoritative: %s", exc)
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass

    def _seed_open_ledger_from_position(self) -> None:
        position = self.get_position()
        with self.connect() as c:
            # Historical event journals can contain an entry whose matching exit was never
            # persisted (older exchange-side stop reconciliation was intentionally sparse).
            # Do not leave those rows masquerading as current OPEN positions forever.
            if position is None:
                c.execute(
                    "UPDATE trade_ledger SET status='LEGACY_UNRESOLVED' "
                    "WHERE status='OPEN' AND source='BACKFILLED_DB_EVENTS'"
                )
                return
            c.execute(
                "UPDATE trade_ledger SET status='LEGACY_UNRESOLVED' "
                "WHERE status='OPEN' AND source='BACKFILLED_DB_EVENTS' AND product_id<>?",
                (position.product_id,),
            )
            row = c.execute(
                "SELECT id,entry_price,highest_mark,lowest_mark,analytics_quality FROM trade_ledger "
                "WHERE status='OPEN' AND product_id=? ORDER BY id DESC LIMIT 1",
                (position.product_id,),
            ).fetchone()
            if row is None:
                return
            low = float(position.lowest_close or 0.0)
            if low <= 0:
                low = float(position.entry_price)
            entry = float(row["entry_price"])
            high = max(float(row["highest_mark"] or entry), float(position.highest_close or entry), entry)
            low = min(float(row["lowest_mark"] or entry), low, entry)
            mfe = high / entry - 1.0 if entry > 0 else None
            mae = low / entry - 1.0 if entry > 0 else None
            c.execute(
                "UPDATE trade_ledger SET highest_mark=?,lowest_mark=?,mfe=?,mae=? WHERE id=?",
                (high, low, mfe, mae, int(row["id"])),
            )

    def backfill_trade_ledger_from_events(self) -> None:
        """Best-effort one-time migration from the existing event journal.

        Historical fills/fees/PnL can be reconstructed from ``trades``. Historical MFE/MAE
        generally cannot, because old closed positions did not persist their sampled low-water
        marks. Those rows are explicitly labeled PARTIAL_LEGACY instead of inventing data.
        """
        with self.connect() as c:
            events = c.execute("SELECT * FROM trades ORDER BY id ASC").fetchall()
        entry_events = {"ENTRY_FILLED", "ENTRY_RECOVERED", "ENTRY_RECOVERED_ORPHAN"}
        for ev in events:
            event = str(ev["event"] or "")
            pid = str(ev["product_id"] or "")
            if not pid:
                continue
            details = self._parse_details(ev["details"])
            if event in entry_events:
                oid = str(ev["order_id"] or "") or None
                with self.connect() as c:
                    if oid and c.execute("SELECT 1 FROM trade_ledger WHERE entry_order_id=?", (oid,)).fetchone():
                        continue
                    prior_open = c.execute(
                        "SELECT id,source FROM trade_ledger WHERE status='OPEN' AND product_id=? ORDER BY id DESC LIMIT 1",
                        (pid,),
                    ).fetchone()
                    if prior_open is not None:
                        if str(prior_open["source"] or "") == "BACKFILLED_DB_EVENTS":
                            c.execute(
                                "UPDATE trade_ledger SET status='LEGACY_UNRESOLVED' WHERE id=?",
                                (int(prior_open["id"]),),
                            )
                        else:
                            continue
                signal = details.get("signal") if isinstance(details.get("signal"), dict) else {}
                execution = details.get("execution") if isinstance(details.get("execution"), dict) else {}
                stop_pct = details.get("stop_pct")
                try:
                    stop_pct_f = float(stop_pct) if stop_pct is not None else None
                except (TypeError, ValueError):
                    stop_pct_f = None
                price = float(ev["price"] or 0.0)
                qty = float(ev["quantity"] or 0.0)
                stop_price = price * (1.0 - stop_pct_f) if price > 0 and stop_pct_f is not None else None
                self.open_trade_ledger(
                    product_id=pid,
                    entry_ts=str(ev["ts"]),
                    entry_price=price,
                    quantity=qty,
                    entry_fee=float(ev["fees"] or 0.0),
                    initial_stop_price=stop_price,
                    initial_risk_pct=stop_pct_f,
                    entry_order_id=oid,
                    execution_path=str(execution.get("path") or event),
                    signal=signal,
                    source="BACKFILLED_DB_EVENTS",
                    analytics_quality="PARTIAL_LEGACY",
                    details={"event_id": int(ev["id"]), "event": event},
                    export=False,
                )
            elif event == "EXIT_FILLED":
                with self.connect() as c:
                    open_row = c.execute(
                        "SELECT id FROM trade_ledger WHERE status='OPEN' AND product_id=? ORDER BY id DESC LIMIT 1",
                        (pid,),
                    ).fetchone()
                if open_row is None:
                    continue
                self.close_trade_ledger(
                    product_id=pid,
                    exit_ts=str(ev["ts"]),
                    exit_price=float(ev["price"] or 0.0),
                    exit_quantity=float(ev["quantity"] or 0.0),
                    exit_fee=float(ev["fees"] or 0.0),
                    exit_order_id=str(ev["order_id"] or "") or None,
                    exit_reason=str(details.get("reason") or "EXIT_FILLED"),
                    export=False,
                )

    def set_cooldown(self, product_id: str, hours: int, reason: str) -> None:
        until = datetime.now(timezone.utc) + timedelta(hours=hours)
        with self.connect() as c:
            c.execute(
                "INSERT INTO cooldowns(product_id,until_ts,reason) VALUES(?,?,?) "
                "ON CONFLICT(product_id) DO UPDATE SET until_ts=excluded.until_ts, reason=excluded.reason",
                (product_id, until.isoformat(), reason),
            )

    def in_cooldown(self, product_id: str) -> bool:
        with self.connect() as c:
            row = c.execute("SELECT until_ts FROM cooldowns WHERE product_id=?", (product_id,)).fetchone()
        if not row:
            return False
        try:
            return datetime.fromisoformat(row["until_ts"]) > datetime.now(timezone.utc)
        except ValueError:
            return False

    def entries_last_24h(self) -> int:
        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        with self.connect() as c:
            row = c.execute(
                "SELECT COUNT(*) AS n FROM trades WHERE event='ENTRY_FILLED' AND ts>=?", (since,)
            ).fetchone()
        return int(row["n"])

    def realized_pnl_today(self) -> float:
        start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        with self.connect() as c:
            row = c.execute(
                "SELECT COALESCE(SUM(realized_pnl),0) AS pnl FROM trades WHERE event='EXIT_FILLED' AND ts>=?",
                (start,),
            ).fetchone()
        return float(row["pnl"] or 0.0)

    def recent_trades(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as c:
            rows = c.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def log_error(self, context: str, message: str) -> None:
        with self.connect() as c:
            c.execute(
                "INSERT INTO errors(ts,context,message) VALUES(?,?,?)",
                (self.now_iso(), context, message),
            )

    def log_shadow_scan(
        self,
        *,
        regime: str,
        breadth: float,
        ranked_count: int,
        rank_pass_count: int,
        trend_pass_count: int,
        persistence_pass_count: int,
        breakout_pass_count: int,
        actionable_count: int,
        top_product: str | None,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.connect() as c:
            c.execute(
                "INSERT INTO shadow_scans(ts,regime,breadth,ranked_count,rank_pass_count,trend_pass_count,"
                "persistence_pass_count,breakout_pass_count,actionable_count,top_product,details) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.now_iso(), regime, breadth, int(ranked_count), int(rank_pass_count),
                    int(trend_pass_count), int(persistence_pass_count), int(breakout_pass_count),
                    int(actionable_count), top_product, json.dumps(details or {}),
                ),
            )

    def log_shadow_trade(
        self,
        *,
        entry_ts: str,
        exit_ts: str,
        product_id: str,
        entry_price: float,
        exit_price: float,
        gross_return: float,
        net_return: float,
        mfe: float,
        mae: float,
        holding_minutes: float,
        exit_reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.connect() as c:
            c.execute(
                "INSERT INTO shadow_trades(entry_ts,exit_ts,product_id,entry_price,exit_price,gross_return,"
                "net_return,mfe,mae,holding_minutes,exit_reason,details) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    entry_ts, exit_ts, product_id, float(entry_price), float(exit_price), float(gross_return),
                    float(net_return), float(mfe), float(mae), float(holding_minutes), exit_reason,
                    json.dumps(details or {}),
                ),
            )

    def shadow_stats(self) -> dict[str, Any]:
        with self.connect() as c:
            scan = c.execute(
                "SELECT COUNT(*) AS scans, COALESCE(SUM(rank_pass_count),0) AS rank_pass, "
                "COALESCE(SUM(trend_pass_count),0) AS trend_pass, "
                "COALESCE(SUM(persistence_pass_count),0) AS persistence_pass, "
                "COALESCE(SUM(breakout_pass_count),0) AS breakout_pass, "
                "COALESCE(SUM(actionable_count),0) AS actionable FROM shadow_scans"
            ).fetchone()
            trade = c.execute(
                "SELECT COUNT(*) AS trades, COALESCE(SUM(CASE WHEN net_return>0 THEN 1 ELSE 0 END),0) AS wins, "
                "COALESCE(AVG(gross_return),0) AS avg_gross, COALESCE(AVG(net_return),0) AS avg_net, "
                "COALESCE(AVG(mfe),0) AS avg_mfe, COALESCE(AVG(mae),0) AS avg_mae, "
                "COALESCE(AVG(holding_minutes),0) AS avg_hold, "
                "COALESCE(SUM(CASE WHEN exit_reason='STOP' THEN 1 ELSE 0 END),0) AS stopouts "
                "FROM shadow_trades"
            ).fetchone()
        trades = int(trade["trades"] or 0)
        wins = int(trade["wins"] or 0)
        return {
            "scans": int(scan["scans"] or 0),
            "rank_pass": int(scan["rank_pass"] or 0),
            "trend_pass": int(scan["trend_pass"] or 0),
            "persistence_pass": int(scan["persistence_pass"] or 0),
            "breakout_pass": int(scan["breakout_pass"] or 0),
            "actionable": int(scan["actionable"] or 0),
            "trades": trades,
            "wins": wins,
            "win_rate": (wins / trades) if trades else 0.0,
            "avg_gross": float(trade["avg_gross"] or 0.0),
            "avg_net": float(trade["avg_net"] or 0.0),
            "avg_mfe": float(trade["avg_mfe"] or 0.0),
            "avg_mae": float(trade["avg_mae"] or 0.0),
            "avg_hold_minutes": float(trade["avg_hold"] or 0.0),
            "stopouts": int(trade["stopouts"] or 0),
        }
