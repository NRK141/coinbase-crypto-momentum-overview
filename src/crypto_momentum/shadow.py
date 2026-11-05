from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import math
import os
from pathlib import Path
from typing import Any

from .coinbase_client import CoinbaseGateway
from .db import StateDB
from .indicators import assign_percentiles, build_signal_row_15m
from .models import ProductMeta, SignalRow
from .strategy import CandidateDiagnostic

log = logging.getLogger(__name__)


@dataclass
class ShadowPositionState:
    product_id: str
    entry_price: float
    entry_time: str
    initial_stop_price: float
    stop_price: float
    initial_risk_pct: float
    highest_price: float
    lowest_price: float
    entry_atr: float
    entry_percentile: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ShadowScanResult:
    rows: list[SignalRow]
    regime: str
    previous_regime: str
    breadth: float
    candidates: list[SignalRow]
    diagnostics: list[CandidateDiagnostic]
    persistence_counts: dict[str, int]
    position: ShadowPositionState | None
    events: list[str]
    funnel: dict[str, int]
    universe_size: int


class Shadow15mEngine:
    """Observation-only true 15-minute strategy simulator.

    This component has no reference to ExecutionEngine and never calls any Coinbase order method.
    It shares only Coinbase market data with the live engine. V1.4.9 gives it a separate, broader
    research universe, while simulated position/state remains isolated under `shadow_15m_*` keys/tables.
    """

    POSITION_KEY = "shadow_15m_position"
    PERSISTENCE_KEY = "shadow_15m_persistence_counts"
    BUCKET_KEY = "shadow_15m_persistence_bucket"
    REGIME_KEY = "shadow_15m_regime_state"
    COOLDOWNS_KEY = "shadow_15m_cooldowns"
    RECENT_ENTRIES_KEY = "shadow_15m_recent_entries"

    def __init__(self, gateway: CoinbaseGateway, db: StateDB, settings):
        self.gateway = gateway
        self.db = db
        self.s = settings
        self.cfg = settings.shadow_15m

    @property
    def enabled(self) -> bool:
        return bool(self.cfg.get("enabled", False))

    def _log_path(self) -> Path:
        configured = str(self.cfg.get("log_path", "logs/shadow_15m.log"))
        path = Path(configured)
        if not path.is_absolute():
            path = self.s.project_root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def append_log(self, text: str) -> None:
        path = self._log_path()
        with path.open("a", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
            if not text.endswith("\n"):
                fh.write("\n")
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass

    def get_position(self) -> ShadowPositionState | None:
        data = self.db.get_json(self.POSITION_KEY, None)
        return ShadowPositionState(**data) if data else None

    def set_position(self, position: ShadowPositionState | None) -> None:
        self.db.set_json(self.POSITION_KEY, None if position is None else position.to_dict())

    def _in_cooldown(self, product_id: str, now: datetime) -> bool:
        payload = self.db.get_json(self.COOLDOWNS_KEY, {}) or {}
        raw = payload.get(product_id)
        if not raw:
            return False
        try:
            return datetime.fromisoformat(raw) > now
        except (TypeError, ValueError):
            return False

    def _set_cooldown(self, product_id: str, now: datetime) -> None:
        payload = self.db.get_json(self.COOLDOWNS_KEY, {}) or {}
        payload[product_id] = (now + timedelta(hours=int(self.s.risk["stopped_token_cooldown_hours"]))).isoformat()
        # prune expired entries while writing
        clean: dict[str, str] = {}
        for pid, raw in payload.items():
            try:
                if datetime.fromisoformat(raw) > now:
                    clean[pid] = raw
            except (TypeError, ValueError):
                pass
        self.db.set_json(self.COOLDOWNS_KEY, clean)

    def _recent_entry_count(self, now: datetime) -> int:
        raw = self.db.get_json(self.RECENT_ENTRIES_KEY, []) or []
        cutoff = now - timedelta(hours=24)
        kept: list[str] = []
        for item in raw:
            try:
                dt = datetime.fromisoformat(item)
                if dt >= cutoff:
                    kept.append(dt.isoformat())
            except (TypeError, ValueError):
                pass
        self.db.set_json(self.RECENT_ENTRIES_KEY, kept)
        return len(kept)

    def _record_entry_time(self, now: datetime) -> None:
        self._recent_entry_count(now)
        raw = self.db.get_json(self.RECENT_ENTRIES_KEY, []) or []
        raw.append(now.isoformat())
        self.db.set_json(self.RECENT_ENTRIES_KEY, raw)

    def _determine_regime(self, breadth: float, btc_above: bool) -> tuple[str, str]:
        previous = str(self.db.get_json(self.REGIME_KEY, "UNKNOWN") or "UNKNOWN")
        risk_off_enter = float(self.s.regime["breadth_risk_off_enter"])
        risk_off_exit = float(self.s.regime["breadth_risk_off_exit"])
        risk_on_enter = float(self.s.regime["breadth_risk_on"])
        risk_on_exit = float(self.s.regime.get("breadth_risk_on_exit", 0.45))
        if previous == "RISK_OFF":
            regime = "NEUTRAL" if (btc_above and breadth >= risk_off_exit) else "RISK_OFF"
        elif previous == "RISK_ON":
            if (not btc_above) and breadth < risk_off_enter:
                regime = "RISK_OFF"
            elif (not btc_above) or breadth < risk_on_exit:
                regime = "NEUTRAL"
            else:
                regime = "RISK_ON"
        else:
            if (not btc_above) and breadth < risk_off_enter:
                regime = "RISK_OFF"
            elif btc_above and breadth >= risk_on_enter:
                regime = "RISK_ON"
            else:
                regime = "NEUTRAL"
        self.db.set_json(self.REGIME_KEY, regime)
        return regime, previous

    def bucket_id(self, now: datetime | None = None) -> str:
        now = now or datetime.now(timezone.utc)
        interval = int(self.cfg.get("scan_interval_minutes", 15))
        offset = int(self.cfg.get("scan_offset_minute_utc", 3)) % interval
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elapsed_minutes = int((now - day_start).total_seconds() // 60)
        slot = (elapsed_minutes - offset) // interval
        bucket_dt = day_start + timedelta(minutes=offset + slot * interval)
        if bucket_dt > now:
            bucket_dt -= timedelta(minutes=interval)
        return bucket_dt.isoformat()

    def _persistence(self, ranked: list[SignalRow], now: datetime) -> dict[str, int]:
        interval = int(self.cfg.get("scan_interval_minutes", 15))
        bucket = self.bucket_id(now)
        bucket_dt = datetime.fromisoformat(bucket)
        previous_bucket = self.db.get_json(self.BUCKET_KEY, None)
        old_counts: dict[str, int] = self.db.get_json(self.PERSISTENCE_KEY, {}) or {}
        same = previous_bucket == bucket
        try:
            previous_dt = datetime.fromisoformat(previous_bucket) if previous_bucket else None
            consecutive = bool((not same) and previous_dt and bucket_dt - previous_dt == timedelta(minutes=interval))
        except (TypeError, ValueError):
            consecutive = False

        threshold = float(self.cfg["top_percentile"])
        new_counts: dict[str, int] = {}
        for row in ranked:
            if row.trend_pass and row.percentile >= threshold:
                if same:
                    new_counts[row.product_id] = int(old_counts.get(row.product_id, 1))
                elif consecutive:
                    new_counts[row.product_id] = int(old_counts.get(row.product_id, 0)) + 1
                else:
                    new_counts[row.product_id] = 1
        self.db.set_json(self.PERSISTENCE_KEY, new_counts)
        self.db.set_json(self.BUCKET_KEY, bucket)
        return new_counts

    def _entry_cost_pass(self, row: SignalRow, product: ProductMeta) -> tuple[bool, str | None]:
        spread = max(0.0, float(product.spread_pct))
        if spread > float(self.s.execution["max_spread_pct"]):
            return False, f"SPREAD {spread:.2%}"
        maker = float(self.cfg.get("estimated_maker_fee_pct", 0.006))
        taker = float(self.cfg.get("estimated_taker_fee_pct", 0.012))
        friction = maker + taker + spread
        if friction > float(self.s.execution["max_maker_roundtrip_friction_pct"]):
            return False, f"FRICTION {friction:.2%}"
        multiple = float(self.s.execution["min_24h_return_to_maker_friction_multiple"])
        if row.r24 < friction * multiple:
            return False, f"COST_HURDLE {row.r24:.2%}<{friction*multiple:.2%}"
        return True, None

    def _exit_position(
        self,
        position: ShadowPositionState,
        exit_price: float,
        reason: str,
        now: datetime,
        events: list[str],
    ) -> None:
        maker = float(self.cfg.get("estimated_maker_fee_pct", 0.006))
        taker = float(self.cfg.get("estimated_taker_fee_pct", 0.012))
        gross = exit_price / position.entry_price - 1.0
        net = ((1.0 - maker) * (exit_price / position.entry_price) * (1.0 - taker)) - 1.0
        mfe = position.highest_price / position.entry_price - 1.0
        mae = position.lowest_price / position.entry_price - 1.0
        try:
            entered = datetime.fromisoformat(position.entry_time)
            hold_minutes = max(0.0, (now - entered).total_seconds() / 60.0)
        except ValueError:
            hold_minutes = 0.0
        self.db.log_shadow_trade(
            entry_ts=position.entry_time,
            exit_ts=now.isoformat(),
            product_id=position.product_id,
            entry_price=position.entry_price,
            exit_price=exit_price,
            gross_return=gross,
            net_return=net,
            mfe=mfe,
            mae=mae,
            holding_minutes=hold_minutes,
            exit_reason=reason,
            details={"estimated_maker_fee_pct": maker, "estimated_taker_fee_pct": taker},
        )
        if reason == "STOP":
            self._set_cooldown(position.product_id, now)
        events.append(
            f"SHADOW EXIT {position.product_id} reason={reason} exit={exit_price:.8f} "
            f"gross={gross:+.2%} estNet={net:+.2%} MFE={mfe:+.2%} MAE={mae:+.2%} hold={hold_minutes:.0f}m"
        )
        self.set_position(None)

    def _enter_position(self, row: SignalRow, now: datetime, events: list[str]) -> ShadowPositionState:
        raw_stop = float(self.s.risk["atr_stop_multiple"]) * row.atr_pct
        risk_pct = max(raw_stop, float(self.s.risk["min_stop_pct"]))
        stop_price = row.price * (1.0 - risk_pct)
        position = ShadowPositionState(
            product_id=row.product_id,
            entry_price=row.price,
            entry_time=now.isoformat(),
            initial_stop_price=stop_price,
            stop_price=stop_price,
            initial_risk_pct=risk_pct,
            highest_price=row.price,
            lowest_price=row.price,
            entry_atr=row.atr,
            entry_percentile=row.percentile,
        )
        self.set_position(position)
        self._record_entry_time(now)
        events.append(
            f"SHADOW ENTRY {row.product_id} entry={row.price:.8f} stop={stop_price:.8f} "
            f"risk={risk_pct:.2%} rank={row.rank} pct={row.percentile:.1%}"
        )
        return position

    def _manage_position(
        self,
        position: ShadowPositionState,
        row: SignalRow | None,
        candidates: list[SignalRow],
        now: datetime,
        events: list[str],
    ) -> ShadowPositionState | None:
        if row is None:
            events.append(f"SHADOW HOLD {position.product_id}: no fresh 15m row")
            return position

        position.highest_price = max(position.highest_price, row.price)
        position.lowest_price = min(position.lowest_price, row.price)

        # Stop is checked before any same-scan trail tightening to avoid look-ahead ordering.
        if row.price <= position.stop_price:
            self._exit_position(position, position.stop_price, "STOP", now, events)
            return None
        if row.price < row.ema_fast and row.r24 <= 0:
            self._exit_position(position, row.price, "TREND_FAILURE", now, events)
            return None

        if row.price >= position.entry_price * (1.0 + position.initial_risk_pct):
            trail = position.highest_price - float(self.s.risk["trail_atr_multiple"]) * row.atr
            if trail > position.stop_price and trail < row.price:
                old = position.stop_price
                position.stop_price = trail
                events.append(
                    f"SHADOW TRAIL {position.product_id} {old:.8f}->{trail:.8f} high={position.highest_price:.8f}"
                )

        if candidates:
            best = candidates[0]
            gap = best.percentile - row.percentile
            if (
                best.product_id != position.product_id
                and row.percentile < float(self.s.rotation["hold_rank_floor_percentile"])
                and best.percentile >= float(self.s.rotation["replacement_min_percentile"])
                and gap >= float(self.s.rotation["replacement_score_gap"])
            ):
                self._exit_position(position, row.price, "ROTATION", now, events)
                return None

        self.set_position(position)
        return position

    def scan(
        self,
        shadow_products: dict[str, ProductMeta],
        regime_products: dict[str, ProductMeta],
        *,
        now: datetime | None = None,
    ) -> ShadowScanResult:
        if not self.enabled:
            raise RuntimeError("15m shadow engine is disabled")
        now = now or datetime.now(timezone.utc)
        cfg = self.cfg
        position = self.get_position()

        products = dict(shadow_products)
        if position and position.product_id not in products:
            try:
                raw = self.gateway.get_product(position.product_id)
                meta = self.gateway.product_meta(raw)
                if meta:
                    products[position.product_id] = meta
            except Exception as exc:
                log.warning("Shadow could not refresh held simulated product %s: %s", position.product_id, exc)

        all_ids = list(dict.fromkeys([
            *shadow_products.keys(),
            *regime_products.keys(),
            *([position.product_id] if position else []),
        ]))
        rows_by_pid: dict[str, SignalRow] = {}
        total = len(all_ids)
        requested_bars = int(cfg.get("candle_bars", 349))
        min_bars = int(cfg.get("min_history_bars", 337))
        for idx, pid in enumerate(all_ids, start=1):
            try:
                candles = self.gateway.candles_15m(pid, bars=requested_bars)
                if len(candles) < min_bars:
                    continue
                current_price = None
                meta = products.get(pid) or shadow_products.get(pid) or regime_products.get(pid)
                if pid in shadow_products or (position and pid == position.product_id):
                    if meta:
                        if meta.best_bid > 0 and meta.best_ask > meta.best_bid:
                            current_price = (meta.best_bid + meta.best_ask) / 2.0
                        elif meta.price > 0:
                            current_price = meta.price
                row = build_signal_row_15m(pid, candles, cfg, current_price=current_price)
                if row:
                    rows_by_pid[pid] = row
            except Exception as exc:
                log.warning("Shadow 15m market-data skip %s: %s", pid, exc)
            if idx % 20 == 0 or idx == total:
                log.info("Shadow 15m data: %s/%s products processed", idx, total)

        ranked = assign_percentiles([rows_by_pid[pid] for pid in shadow_products if pid in rows_by_pid])
        regime_rows = [rows_by_pid[pid] for pid in regime_products if pid in rows_by_pid]
        breadth = (sum(1 for r in regime_rows if r.breadth_member) / len(regime_rows)) if regime_rows else 0.0
        btc = rows_by_pid.get(self.s.regime["btc_product"])
        btc_above = bool(btc and btc.price > btc.ema_slow)
        regime, previous_regime = self._determine_regime(breadth, btc_above)
        persistence = self._persistence(ranked, now)

        threshold = float(cfg["top_percentile"])
        persistence_required = int(cfg["persistence_scans"])
        regime_pass = not (regime == "RISK_OFF" and self.s.regime.get("block_new_entries_in_risk_off", True))
        diagnostics: list[CandidateDiagnostic] = []
        candidates: list[SignalRow] = []
        funnel = {"rank": 0, "trend": 0, "persistence": 0, "breakout": 0, "actionable": 0}

        for row in ranked:
            count = int(persistence.get(row.product_id, 0))
            raw_stop = float(self.s.risk["atr_stop_multiple"]) * row.atr_pct
            effective_stop = max(raw_stop, float(self.s.risk["min_stop_pct"]))
            rank_pass = row.percentile >= threshold
            trend_pass = bool(row.trend_pass)
            persistence_pass = count >= persistence_required
            breakout_pass = bool(row.breakout)
            sigma_pass = row.one_hour_sigma <= float(cfg["max_one_bar_sigma"])
            distance_pass = row.distance_atr <= float(cfg["max_distance_atr"])
            stop_pass = raw_stop <= float(self.s.risk["max_stop_pct"])
            cooldown_pass = not self._in_cooldown(row.product_id, now)

            if rank_pass:
                funnel["rank"] += 1
            if rank_pass and trend_pass:
                funnel["trend"] += 1
            if rank_pass and trend_pass and persistence_pass:
                funnel["persistence"] += 1
            if rank_pass and trend_pass and persistence_pass and breakout_pass:
                funnel["breakout"] += 1

            reasons: list[str] = []
            if not regime_pass:
                reasons.append("REGIME")
            if not rank_pass:
                reasons.append("RANK")
            if not trend_pass:
                reasons.append("TREND")
            if not persistence_pass:
                reasons.append(f"PERSIST {count}/{persistence_required}")
            if not breakout_pass:
                reasons.append("BREAKOUT")
            if not sigma_pass:
                reasons.append("15M_EXTENDED")
            if not distance_pass:
                reasons.append("EMA_DISTANCE")
            if not stop_pass:
                reasons.append(f"STOP {raw_stop:.1%}>{float(self.s.risk['max_stop_pct']):.1%}")
            if not cooldown_pass:
                reasons.append("COOLDOWN")
            product = shadow_products.get(row.product_id)
            if not reasons and product is not None:
                cost_pass, cost_reason = self._entry_cost_pass(row, product)
                if not cost_pass and cost_reason:
                    reasons.append(cost_reason)

            actionable = not reasons
            if actionable:
                candidates.append(row)
                funnel["actionable"] += 1
            diagnostics.append(CandidateDiagnostic(
                product_id=row.product_id,
                rank=row.rank,
                percentile=row.percentile,
                persistence=count,
                persistence_required=persistence_required,
                raw_stop_pct=raw_stop,
                effective_stop_pct=effective_stop,
                rank_pass=rank_pass,
                trend_pass=trend_pass,
                persistence_pass=persistence_pass,
                breakout_pass=breakout_pass,
                sigma_pass=sigma_pass,
                distance_pass=distance_pass,
                stop_pass=stop_pass,
                cooldown_pass=cooldown_pass,
                regime_pass=regime_pass,
                actionable=actionable,
                reasons=reasons,
            ))

        events: list[str] = []
        if position:
            position = self._manage_position(position, rows_by_pid.get(position.product_id), candidates, now, events)

        if position is None and candidates:
            if self._recent_entry_count(now) >= int(self.s.risk["max_new_entries_per_24h"]):
                events.append("SHADOW ENTRY BLOCKED: max entries/24h")
            else:
                position = self._enter_position(candidates[0], now, events)

        self.db.log_shadow_scan(
            regime=regime,
            breadth=breadth,
            ranked_count=len(ranked),
            rank_pass_count=funnel["rank"],
            trend_pass_count=funnel["trend"],
            persistence_pass_count=funnel["persistence"],
            breakout_pass_count=funnel["breakout"],
            actionable_count=funnel["actionable"],
            top_product=ranked[0].product_id if ranked else None,
            details={"position": position.to_dict() if position else None, "universe_size": len(shadow_products)},
        )
        return ShadowScanResult(
            rows=ranked,
            regime=regime,
            previous_regime=previous_regime,
            breadth=breadth,
            candidates=candidates,
            diagnostics=diagnostics,
            persistence_counts=persistence,
            position=position,
            events=events,
            funnel=funnel,
            universe_size=len(shadow_products),
        )

    def render(self, result: ShadowScanResult, scan_number: int) -> str:
        transition = "" if result.previous_regime in {"UNKNOWN", result.regime} else f" <- {result.previous_regime}"
        lines = [
            "",
            "=" * 126,
            f"SHADOW 15M SCAN #{scan_number} {datetime.now(timezone.utc).isoformat()} | "
            f"regime={result.regime}{transition} | breadth={result.breadth:.1%} | "
            f"universe={result.universe_size} | position={result.position.product_id if result.position else 'FLAT'}",
            "rank product          pct     6h      18h      42h      84h      ATR%    trend breakout persist",
        ]
        for row in result.rows[:12]:
            persist = int(result.persistence_counts.get(row.product_id, 0))
            lines.append(
                f"{row.rank:>4} {row.product_id:<15} {row.percentile:>5.1%} "
                f"{row.r24:>8.2%} {row.r72:>8.2%} {row.r168:>8.2%} {row.r336:>8.2%} "
                f"{row.atr_pct:>7.2%} {str(row.trend_pass):>6} {str(row.breakout):>8} {persist:>7}"
            )
        lines.append("")
        lines.append("Shadow candidate diagnostics (top ranked):")
        lines.append("product          persist  rawStop  effStop  result / rejection reasons")
        for diag in result.diagnostics[:10]:
            result_text = "ACTIONABLE" if diag.actionable else ", ".join(diag.reasons)
            lines.append(
                f"{diag.product_id:<15} {diag.persistence:>2}/{diag.persistence_required:<2} "
                f"{diag.raw_stop_pct:>8.2%} {diag.effective_stop_pct:>8.2%}  {result_text}"
            )

        canonical = {
            "REGIME", "RANK", "TREND", "BREAKOUT", "15M_EXTENDED", "EMA_DISTANCE",
            "COOLDOWN", "SPREAD", "FRICTION", "COST_HURDLE",
        }
        rejection_counts: dict[str, int] = {}
        for diag in result.diagnostics:
            for reason in diag.reasons:
                key = reason.split()[0]
                if key.startswith("PERSIST"):
                    key = "PERSIST"
                elif key.startswith("STOP"):
                    key = "STOP"
                elif key not in canonical:
                    key = key.split("<", 1)[0]
                rejection_counts[key] = rejection_counts.get(key, 0) + 1
        if rejection_counts:
            ordered = sorted(rejection_counts.items(), key=lambda kv: (-kv[1], kv[0]))
            lines.append("Rejection counts: " + ", ".join(f"{k}={v}" for k, v in ordered))

        lines.append("")
        lines.append(
            "Funnel: rank=%d trend=%d persistence=%d breakout=%d actionable=%d" % (
                result.funnel["rank"], result.funnel["trend"], result.funnel["persistence"],
                result.funnel["breakout"], result.funnel["actionable"],
            )
        )
        for event in result.events:
            lines.append(event)
        stats = self.db.shadow_stats()
        lines.append(
            "Cumulative: scans={scans} actionable={actionable} trades={trades} winRate={win_rate:.1%} "
            "avgGross={avg_gross:+.2%} avgNet={avg_net:+.2%} avgMFE={avg_mfe:+.2%} "
            "avgMAE={avg_mae:+.2%} avgHold={avg_hold_minutes:.0f}m stopouts={stopouts}".format(**stats)
        )
        lines.append("NOTE: shadow results are hypothetical; MFE/MAE are sampled at 15m decision marks and fees are estimated.")
        return "\n".join(lines)
