from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
import os
from pathlib import Path
from typing import Any

from .coinbase_client import CoinbaseGateway
from .db import StateDB
from .indicators import assign_percentiles, build_signal_row
from .models import ProductMeta, SignalRow

log = logging.getLogger(__name__)


@dataclass
class CandidateDiagnostic:
    product_id: str
    rank: int
    percentile: float
    persistence: int
    persistence_required: int
    raw_stop_pct: float
    effective_stop_pct: float
    rank_pass: bool
    trend_pass: bool
    persistence_pass: bool
    breakout_pass: bool
    sigma_pass: bool
    distance_pass: bool
    stop_pass: bool
    cooldown_pass: bool
    regime_pass: bool
    actionable: bool
    reasons: list[str]


@dataclass
class ScanResult:
    rows: list[SignalRow]
    products: dict[str, ProductMeta]
    regime: str
    previous_regime: str
    breadth: float
    candidates: list[SignalRow]
    diagnostics: list[CandidateDiagnostic]
    trading_universe_size: int
    regime_universe_size: int
    regime_rows_available: int
    persistence_counts: dict[str, int]


class StrategyEngine:
    def __init__(self, gateway: CoinbaseGateway, db: StateDB, settings):
        self.gateway = gateway
        self.db = db
        self.s = settings
        self._trading_products: dict[str, ProductMeta] = {}
        self._shadow_products: dict[str, ProductMeta] = {}
        self._regime_products: dict[str, ProductMeta] = {}
        self._trading_refreshed_at: datetime | None = None
        self._shadow_refreshed_at: datetime | None = None
        self._regime_refreshed_at: datetime | None = None

    @staticmethod
    def _tradable(p: dict[str, Any]) -> bool:
        # Coinbase has evolved the exact field set; reject only explicit non-tradable flags.
        if p.get("trading_disabled") is True:
            return False
        if p.get("cancel_only") is True:
            return False
        if p.get("view_only") is True:
            return False
        if p.get("is_disabled") is True:
            return False
        status = str(p.get("status") or "").upper()
        if status and status not in {"ONLINE", "TRADING", "UNKNOWN"}:
            return False
        return True

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            value = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _spread_pct(bid: float, ask: float) -> float:
        if bid <= 0 or ask <= bid:
            return 0.0
        mid = (bid + ask) / 2.0
        return (ask - bid) / mid if mid > 0 else 0.0

    def _cache_age_minutes(self, when: datetime | None) -> float:
        if when is None:
            return float("inf")
        return max(0.0, (datetime.now(timezone.utc) - when).total_seconds() / 60.0)

    def _load_cached_members(self, key: str) -> tuple[list[str], datetime | None]:
        payload = self.db.get_json(key, {}) or {}
        members = [str(x) for x in payload.get("members", [])]
        refreshed = self._parse_dt(payload.get("refreshed_at"))
        return members, refreshed

    def _save_cached_members(
        self, key: str, members: list[str], refreshed: datetime, stats: dict[str, Any], *, cache_revision: int = 1
    ) -> None:
        self.db.set_json(
            key,
            {
                "refreshed_at": refreshed.isoformat(),
                "members": members,
                "stats": stats,
                "cache_revision": int(cache_revision),
            },
        )

    def _enrich_raw_products(self, raw_products: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Attach current top-of-book quotes to List Products snapshots.

        Coinbase List Products currently exposes best_bid_price/best_ask_price, but V1.4.1 also
        uses the dedicated Best Bid/Ask endpoint once per universe refresh as a robust source.
        This keeps the half-hour universe refresh lightweight: two REST requests, no candles.
        """
        try:
            books = self.gateway.best_bid_asks()
        except Exception as exc:
            log.warning("Best-bid/ask universe snapshot failed; using List Products quotes where present: %s", exc)
            books = {}

        enriched: list[dict[str, Any]] = []
        for original in raw_products:
            p = dict(original)
            pid = str(p.get("product_id") or "")
            quote = books.get(pid)
            if quote:
                p["best_bid_price"] = str(quote[0])
                p["best_ask_price"] = str(quote[1])
            enriched.append(p)
        return enriched

    def _universe_audit_path(self) -> Path:
        cfg = getattr(self.s, "universe_research", {}) or {}
        configured = str(cfg.get("log_path", "logs/universe_audit.log"))
        path = Path(configured)
        if not path.is_absolute():
            path = self.s.project_root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _append_universe_audit(self, text: str) -> None:
        path = self._universe_audit_path()
        with path.open("a", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
            if not text.endswith("\n"):
                fh.write("\n")
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass

    def _build_universe_audit(self, raw_products: list[dict[str, Any]]) -> dict[str, Any]:
        """Measure which Coinbase-universe constraints are actually binding.

        This is observation-only. It deliberately reuses the live filter semantics but does not
        mutate trading/regime membership. Counterfactual tiers isolate volume, spread, history,
        quote-currency, and max-products effects before any later global-universe expansion.
        """
        research = getattr(self.s, "universe_research", {}) or {}
        live = self.s.universe
        quote = self.s.strategy["quote_currency"].upper()
        stable = {x.upper() for x in live.get("stablecoin_symbols", [])}
        excluded = {x.upper() for x in live.get("excluded_symbols", [])}
        require_spot = bool(live.get("require_spot", True))
        current_volume = float(live["min_quote_volume_24h_usd"])
        current_spread = float(live.get("max_spread_pct", 1.0))
        current_history = float(live.get("min_history_hours", 0))
        max_products = int(live["max_products"])
        volume_tiers = sorted({float(x) for x in research.get("volume_tiers_usd", [current_volume, 500000, 250000])}, reverse=True)
        if current_volume not in volume_tiers:
            volume_tiers.append(current_volume)
            volume_tiers.sort(reverse=True)
        spread_tiers = sorted({float(x) for x in research.get("spread_tiers_pct", [current_spread, 0.0075, 0.0100])})
        if current_spread not in spread_tiers:
            spread_tiers.append(current_spread)
            spread_tiers.sort()
        history_tiers = sorted({float(x) for x in research.get("history_tiers_hours", [current_history, 168, 72])}, reverse=True)
        if current_history not in history_tiers:
            history_tiers.append(current_history)
            history_tiers.sort(reverse=True)
        now = datetime.now(timezone.utc)

        raw_count = len(raw_products)
        parseable = 0
        tradable = 0
        spot = 0
        quote_counts: dict[str, int] = {}
        quote_base_sets: dict[str, set[str]] = {}
        usd_quality_rows: list[ProductMeta] = []
        usd_two_sided: list[ProductMeta] = []
        usd_history_ok_any_spread: list[ProductMeta] = []
        current_nonvolume: list[ProductMeta] = []
        wide_spread_current_volume: list[ProductMeta] = []
        too_new_current_quality: list[ProductMeta] = []

        def old_enough(meta: ProductMeta, hours: float) -> bool:
            listed = self._parse_dt(meta.new_at)
            return listed is None or (now - listed).total_seconds() >= hours * 3600

        for p in raw_products:
            if not self._tradable(p):
                continue
            tradable += 1
            meta = self.gateway.product_meta(p)
            if not meta:
                continue
            parseable += 1
            ptype = str(p.get("product_type") or "SPOT").upper()
            if require_spot and ptype not in {"SPOT", "UNKNOWN", ""}:
                continue
            spot += 1
            if meta.base_currency in stable or meta.base_currency in excluded:
                continue
            quote_counts[meta.quote_currency] = quote_counts.get(meta.quote_currency, 0) + 1
            quote_base_sets.setdefault(meta.quote_currency, set()).add(meta.base_currency)
            if meta.quote_currency != quote:
                continue
            usd_quality_rows.append(meta)
            if meta.best_bid <= 0 or meta.best_ask <= meta.best_bid:
                continue
            usd_two_sided.append(meta)
            if old_enough(meta, current_history):
                usd_history_ok_any_spread.append(meta)
            if meta.quote_volume_24h >= current_volume and old_enough(meta, current_history) and meta.spread_pct > current_spread:
                wide_spread_current_volume.append(meta)
            if meta.quote_volume_24h >= current_volume and meta.spread_pct <= current_spread and not old_enough(meta, current_history):
                too_new_current_quality.append(meta)
            if meta.spread_pct <= current_spread and old_enough(meta, current_history):
                current_nonvolume.append(meta)

        volume_counts = {
            str(int(tier)): sum(1 for m in current_nonvolume if m.quote_volume_24h >= tier)
            for tier in volume_tiers
        }
        current_eligible_before_cap = sum(1 for m in current_nonvolume if m.quote_volume_24h >= current_volume)
        current_selected = min(current_eligible_before_cap, max_products)

        spread_counts: dict[str, int] = {}
        for tier in spread_tiers:
            count = sum(
                1 for m in usd_two_sided
                if m.quote_volume_24h >= current_volume and old_enough(m, current_history) and m.spread_pct <= tier
            )
            spread_counts[f"{tier:.6f}"] = count

        history_counts: dict[str, int] = {}
        for tier in history_tiers:
            count = sum(
                1 for m in usd_two_sided
                if m.quote_volume_24h >= current_volume and m.spread_pct <= current_spread and old_enough(m, tier)
            )
            history_counts[str(int(tier))] = count

        # Comparable USDC books are informative for future scope expansion, but V1.4.8 does
        # not trade them. Apply the same volume/spread/history quality checks.
        usdc_quality_bases: set[str] = set()
        usd_quality_bases: set[str] = {m.base_currency for m in current_nonvolume if m.quote_volume_24h >= current_volume}
        for p in raw_products:
            if not self._tradable(p):
                continue
            meta = self.gateway.product_meta(p)
            if not meta or meta.base_currency in stable or meta.base_currency in excluded or meta.quote_currency != "USDC":
                continue
            ptype = str(p.get("product_type") or "SPOT").upper()
            if require_spot and ptype not in {"SPOT", "UNKNOWN", ""}:
                continue
            if meta.best_bid <= 0 or meta.best_ask <= meta.best_bid:
                continue
            if meta.quote_volume_24h < current_volume or meta.spread_pct > current_spread or not old_enough(meta, current_history):
                continue
            usdc_quality_bases.add(meta.base_currency)

        top_n = int(research.get("top_rejected_examples", 10))
        volume_rejects = sorted(
            (m for m in current_nonvolume if m.quote_volume_24h < current_volume),
            key=lambda m: m.quote_volume_24h,
            reverse=True,
        )[:top_n]
        spread_rejects = sorted(wide_spread_current_volume, key=lambda m: m.spread_pct)[:top_n]
        history_rejects = sorted(
            too_new_current_quality,
            key=lambda m: self._parse_dt(m.new_at) or datetime.min.replace(tzinfo=timezone.utc),
        )[:top_n]

        catalog = self.gateway.product_catalog_info() if hasattr(self.gateway, "product_catalog_info") else {}
        audit = {
            "utc": now.isoformat(),
            "catalog": catalog,
            "raw_products": raw_count,
            "parseable_tradable_products": parseable,
            "tradable_products": tradable,
            "spot_products": spot,
            "quote_counts_nonstable": dict(sorted(quote_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
            "unique_nonstable_bases": len(set().union(*quote_base_sets.values())) if quote_base_sets else 0,
            "usd_nonstable_spot": len(usd_quality_rows),
            "usd_two_sided": len(usd_two_sided),
            "usd_current_spread_and_history_no_volume": len(current_nonvolume),
            "current_eligible_before_cap": current_eligible_before_cap,
            "current_selected_after_cap": current_selected,
            "max_products": max_products,
            "cap_binding": current_eligible_before_cap > max_products,
            "volume_counts": volume_counts,
            "spread_counts": spread_counts,
            "history_counts": history_counts,
            "usdc_current_quality_bases": len(usdc_quality_bases),
            "usdc_quality_bases_not_in_usd": len(usdc_quality_bases - usd_quality_bases),
            "top_volume_rejects": [
                {"product_id": m.product_id, "quote_volume_24h": m.quote_volume_24h, "spread_pct": m.spread_pct}
                for m in volume_rejects
            ],
            "top_spread_rejects": [
                {"product_id": m.product_id, "quote_volume_24h": m.quote_volume_24h, "spread_pct": m.spread_pct}
                for m in spread_rejects
            ],
            "top_history_rejects": [
                {"product_id": m.product_id, "quote_volume_24h": m.quote_volume_24h, "new_at": m.new_at}
                for m in history_rejects
            ],
        }
        return audit

    def _render_universe_audit(self, audit: dict[str, Any]) -> str:
        live = self.s.universe
        current_volume = int(float(live["min_quote_volume_24h_usd"]))
        current_spread = float(live.get("max_spread_pct", 1.0))
        current_history = int(float(live.get("min_history_hours", 0)))
        v = audit.get("volume_counts", {})
        s = audit.get("spread_counts", {})
        h = audit.get("history_counts", {})
        catalog = audit.get("catalog", {}) or {}
        volume_keys = sorted((int(k) for k in v), reverse=True)
        volume_text = " | ".join(f">=${k/1_000_000:.2f}M: {v[str(k)]}" for k in volume_keys)
        spread_text = " | ".join(f"<={float(k):.2%}: {value}" for k, value in sorted(s.items(), key=lambda kv: float(kv[0])))
        history_text = " | ".join(f">={int(k)}h: {value}" for k, value in sorted(h.items(), key=lambda kv: -int(kv[0])))
        quote_counts = audit.get("quote_counts_nonstable", {}) or {}
        top_quotes = ", ".join(f"{q}={n}" for q, n in list(quote_counts.items())[:8]) or "none"
        lines = [
            "",
            "=" * 126,
            f"UNIVERSE AUDIT {audit.get('utc')} | OBSERVATION ONLY - AUDIT DOES NOT MUTATE MEMBERSHIP",
            f"Catalog: unique={catalog.get('unique_products', audit.get('raw_products'))} pages={catalog.get('pages', '?')} "
            f"reported={catalog.get('reported_num_products', '?')} complete={catalog.get('pagination_complete', '?')} "
            f"mode={catalog.get('pagination_mode', '?')}",
            f"Scope: raw={audit.get('raw_products')} tradable={audit.get('tradable_products')} spot={audit.get('spot_products')} "
            f"unique_nonstable_bases={audit.get('unique_nonstable_bases')} | quotes: {top_quotes}",
            f"USD chain: nonstable_spot={audit.get('usd_nonstable_spot')} two_sided={audit.get('usd_two_sided')} "
            f"spread<={current_spread:.2%}+history>={current_history}h(no volume)={audit.get('usd_current_spread_and_history_no_volume')} "
            f"current>={current_volume/1_000_000:.2f}M={audit.get('current_eligible_before_cap')} "
            f"after_cap{audit.get('max_products')}={audit.get('current_selected_after_cap')} cap_binding={audit.get('cap_binding')}",
            f"Volume counterfactual: {volume_text}",
            f"Spread counterfactual (volume>={current_volume/1_000_000:.2f}M, history fixed): {spread_text}",
            f"History counterfactual (volume/spread fixed): {history_text}",
            f"USDC future-scope check: same-quality bases={audit.get('usdc_current_quality_bases')} "
            f"bases_not_already_in_USD={audit.get('usdc_quality_bases_not_in_usd')}",
        ]
        vr = audit.get("top_volume_rejects", []) or []
        if vr:
            lines.append("Closest volume-floor rejects: " + ", ".join(
                f"{x['product_id']}=${x['quote_volume_24h']/1_000_000:.3f}M" for x in vr
            ))
        sr = audit.get("top_spread_rejects", []) or []
        if sr:
            lines.append("Closest spread rejects: " + ", ".join(
                f"{x['product_id']}={x['spread_pct']:.2%}" for x in sr
            ))
        hr = audit.get("top_history_rejects", []) or []
        if hr:
            lines.append("History-age rejects: " + ", ".join(str(x["product_id"]) for x in hr))
        lines.append(
            "NOTE: audit is research-only. V1.4.9 live membership uses the configured $500k tier; "
            "the true-15m shadow universe is separately configured at $250k."
        )
        return "\n".join(lines)

    def _select_universe(
        self,
        raw_products: list[dict[str, Any]],
        cfg: dict[str, Any],
        *,
        current_product: str | None = None,
        include_btc: bool = False,
    ) -> tuple[dict[str, ProductMeta], dict[str, int], dict[str, dict[str, Any]]]:
        quote = self.s.strategy["quote_currency"].upper()
        stable = {x.upper() for x in self.s.universe.get("stablecoin_symbols", [])}
        excluded = {x.upper() for x in self.s.universe.get("excluded_symbols", [])}
        min_qv = float(cfg["min_quote_volume_24h_usd"])
        max_spread = float(cfg.get("max_spread_pct", 1.0))
        max_products = int(cfg["max_products"])
        min_hist = float(cfg.get("min_history_hours", self.s.universe["min_history_hours"]))
        now = datetime.now(timezone.utc)

        stats = {
            "raw": len(raw_products),
            "non_tradable": 0,
            "wrong_quote_or_type": 0,
            "stable_or_excluded": 0,
            "volume": 0,
            "spread": 0,
            "history_age": 0,
            "eligible_before_cap": 0,
        }
        raw_by_pid: dict[str, dict[str, Any]] = {}
        rows: list[ProductMeta] = []

        for p in raw_products:
            pid = str(p.get("product_id") or "")
            if pid:
                raw_by_pid[pid] = p
            if not self._tradable(p):
                stats["non_tradable"] += 1
                continue
            meta = self.gateway.product_meta(p)
            if not meta:
                continue
            if meta.quote_currency != quote:
                stats["wrong_quote_or_type"] += 1
                continue
            if meta.base_currency in stable or meta.base_currency in excluded:
                stats["stable_or_excluded"] += 1
                continue
            ptype = str(p.get("product_type") or "SPOT").upper()
            if self.s.universe.get("require_spot", True) and ptype not in {"SPOT", "UNKNOWN", ""}:
                stats["wrong_quote_or_type"] += 1
                continue
            if meta.quote_volume_24h < min_qv:
                stats["volume"] += 1
                continue

            # A current two-sided quote is required for universe eligibility. The dedicated
            # batch Best Bid/Ask snapshot normally supplies it even when List Products does not.
            if meta.best_bid <= 0 or meta.best_ask <= meta.best_bid:
                stats["spread"] += 1
                continue
            if meta.spread_pct > max_spread:
                stats["spread"] += 1
                continue

            # Listing age is the inexpensive half-hour history filter. The hourly signal scan
            # still verifies that >=min_history_hours actual completed candles exist.
            listed = self._parse_dt(meta.new_at)
            if listed is not None and (now - listed).total_seconds() < min_hist * 3600:
                stats["history_age"] += 1
                continue
            rows.append(meta)

        rows.sort(key=lambda x: x.quote_volume_24h, reverse=True)
        stats["eligible_before_cap"] = len(rows)
        selected = {m.product_id: m for m in rows[:max_products]}

        # Preserve benchmark/current holding for signal/risk management even when a transient
        # spread/volume filter would otherwise remove them. These forced inclusions are not
        # automatically entry candidates; ranking/entry rules still apply.
        must_have: set[str] = set()
        if include_btc:
            must_have.add(self.s.regime["btc_product"])
        if current_product:
            must_have.add(current_product)
        for pid in must_have:
            if pid in selected:
                continue
            p = raw_by_pid.get(pid)
            if p is None:
                continue
            meta = self.gateway.product_meta(p)
            if meta:
                selected[pid] = meta

        # Keep the configured cap exact even when a forced benchmark/holding had to be
        # inserted. Remove the lowest-volume non-forced member first. This makes the
        # regime breadth denominator a stable top-N basket rather than N or N+1.
        if len(selected) > max_products:
            removable = sorted(
                (m for pid, m in selected.items() if pid not in must_have),
                key=lambda x: x.quote_volume_24h,
            )
            while len(selected) > max_products and removable:
                victim = removable.pop(0)
                selected.pop(victim.product_id, None)

        return selected, stats, raw_by_pid

    def _hydrate_cached_ids(
        self,
        ids: list[str],
        raw_by_pid: dict[str, dict[str, Any]],
        *,
        include_btc: bool = False,
        current_product: str | None = None,
    ) -> dict[str, ProductMeta]:
        wanted = set(ids)
        if include_btc:
            wanted.add(self.s.regime["btc_product"])
        if current_product:
            wanted.add(current_product)
        out: dict[str, ProductMeta] = {}
        for pid in wanted:
            p = raw_by_pid.get(pid)
            if not p:
                continue
            meta = self.gateway.product_meta(p)
            if meta:
                out[pid] = meta
        return out

    def refresh_universes(
        self,
        current_product: str | None = None,
        *,
        force_trading: bool = False,
        force_regime: bool = False,
    ) -> dict[str, Any]:
        """Refresh lightweight metadata universes.

        V1.4.9 maintains three separate memberships from one Coinbase catalog snapshot:
        live trading ($500k default), true-15m shadow research ($250k default), and the
        slower/stable regime basket ($1M default). The shadow membership is observation-only
        and is never exposed to ExecutionEngine.
        """
        now = datetime.now(timezone.utc)
        trading_cfg = self.s.universe
        shadow_cfg = getattr(self.s, "shadow_universe", None) or trading_cfg
        regime_cfg = self.s.regime_universe

        trading_cached_ids, trading_cached_dt = self._load_cached_members("trading_universe_cache")
        shadow_cached_ids, shadow_cached_dt = self._load_cached_members("shadow_universe_cache")
        regime_cached_ids, regime_cached_dt = self._load_cached_members("regime_universe_cache")
        trading_cache_payload = self.db.get_json("trading_universe_cache", {}) or {}
        shadow_cache_payload = self.db.get_json("shadow_universe_cache", {}) or {}
        regime_cache_payload = self.db.get_json("regime_universe_cache", {}) or {}
        trading_revision_ok = int(trading_cache_payload.get("cache_revision", 1)) == int(trading_cfg.get("cache_revision", 1))
        shadow_revision_ok = int(shadow_cache_payload.get("cache_revision", 0)) == int(shadow_cfg.get("cache_revision", 1))
        regime_revision_ok = int(regime_cache_payload.get("cache_revision", 1)) == int(regime_cfg.get("cache_revision", 1))

        if self._trading_refreshed_at is None:
            self._trading_refreshed_at = trading_cached_dt
        if self._shadow_refreshed_at is None:
            self._shadow_refreshed_at = shadow_cached_dt
        if self._regime_refreshed_at is None:
            self._regime_refreshed_at = regime_cached_dt

        # A membership-revision change invalidates persistence that was computed under a
        # different cross-sectional ranking set. Reset only persistence; positions/risk state
        # remain intact and continue to be managed normally.
        if not trading_revision_ok:
            self.db.set_json("persistence_counts", {})
            self.db.set_json("persistence_scan_bucket", None)
        if not shadow_revision_ok:
            self.db.set_json("shadow_15m_persistence_counts", {})
            self.db.set_json("shadow_15m_persistence_bucket", None)

        # Trading and shadow memberships intentionally refresh together every 30 minutes.
        # force_trading is used by the aligned universe scheduler and therefore forces both.
        trading_due = force_trading or (not trading_revision_ok) or (
            self._cache_age_minutes(self._trading_refreshed_at) >= float(trading_cfg.get("refresh_minutes", 30))
        ) or (not self._trading_products and not trading_cached_ids)
        shadow_due = force_trading or (not shadow_revision_ok) or (
            self._cache_age_minutes(self._shadow_refreshed_at) >= float(shadow_cfg.get("refresh_minutes", 30))
        ) or (not self._shadow_products and not shadow_cached_ids)
        regime_due = force_regime or (not regime_revision_ok) or (
            self._cache_age_minutes(self._regime_refreshed_at) >= float(regime_cfg.get("refresh_minutes", 120))
        ) or (not self._regime_products and not regime_cached_ids)

        need_hydration = (not self._trading_products and bool(trading_cached_ids)) or (
            not self._shadow_products and bool(shadow_cached_ids)
        ) or (not self._regime_products and bool(regime_cached_ids))
        if not trading_due and not shadow_due and not regime_due and not need_hydration:
            return {
                "trading_refreshed": False,
                "shadow_refreshed": False,
                "regime_refreshed": False,
                "trading_count": len(self._trading_products),
                "shadow_count": len(self._shadow_products),
                "regime_count": len(self._regime_products),
            }

        raw = self._enrich_raw_products(self.gateway.products())
        raw_by_pid = {str(p.get("product_id") or ""): p for p in raw if p.get("product_id")}
        out: dict[str, Any] = {
            "trading_refreshed": False,
            "shadow_refreshed": False,
            "regime_refreshed": False,
            "trading_count": len(self._trading_products),
            "shadow_count": len(self._shadow_products),
            "regime_count": len(self._regime_products),
        }

        research_cfg = getattr(self.s, "universe_research", {}) or {}
        if trading_due and bool(research_cfg.get("enabled", False)):
            try:
                audit = self._build_universe_audit(raw)
                self.db.set_json("latest_universe_audit", audit)
                self._append_universe_audit(self._render_universe_audit(audit) + "\n")
                out["universe_audit"] = audit
            except Exception as exc:
                # Research diagnostics can never block live universe maintenance.
                log.exception("Universe research audit failed; live membership refresh continues: %s", exc)

        if trading_due:
            selected, stats, _ = self._select_universe(raw, trading_cfg, current_product=None, include_btc=False)
            self._trading_products = selected
            self._trading_refreshed_at = now
            self._save_cached_members(
                "trading_universe_cache", list(selected), now, stats,
                cache_revision=int(trading_cfg.get("cache_revision", 1)),
            )
            out.update({
                "trading_refreshed": True,
                "trading_count": len(selected),
                "trading_stats": stats,
            })
        elif not self._trading_products:
            self._trading_products = self._hydrate_cached_ids(
                trading_cached_ids, raw_by_pid, include_btc=False, current_product=None
            )
            self._trading_refreshed_at = trading_cached_dt

        if shadow_due:
            selected, stats, _ = self._select_universe(raw, shadow_cfg, current_product=None, include_btc=False)
            self._shadow_products = selected
            self._shadow_refreshed_at = now
            self._save_cached_members(
                "shadow_universe_cache", list(selected), now, stats,
                cache_revision=int(shadow_cfg.get("cache_revision", 1)),
            )
            out.update({
                "shadow_refreshed": True,
                "shadow_count": len(selected),
                "shadow_stats": stats,
            })
        elif not self._shadow_products:
            self._shadow_products = self._hydrate_cached_ids(
                shadow_cached_ids, raw_by_pid, include_btc=False, current_product=None
            )
            self._shadow_refreshed_at = shadow_cached_dt

        if regime_due:
            selected, stats, _ = self._select_universe(raw, regime_cfg, include_btc=True)
            self._regime_products = selected
            self._regime_refreshed_at = now
            self._save_cached_members(
                "regime_universe_cache", list(selected), now, stats,
                cache_revision=int(regime_cfg.get("cache_revision", 1)),
            )
            out.update({
                "regime_refreshed": True,
                "regime_count": len(selected),
                "regime_stats": stats,
            })
        elif not self._regime_products:
            self._regime_products = self._hydrate_cached_ids(regime_cached_ids, raw_by_pid, include_btc=True)
            self._regime_refreshed_at = regime_cached_dt

        return out

    def refresh_trading_universe(self, current_product: str | None = None) -> dict[str, Any]:
        return self.refresh_universes(current_product=current_product, force_trading=True)


    def universe_snapshot(self) -> tuple[dict[str, ProductMeta], dict[str, ProductMeta], dict[str, ProductMeta]]:
        """Return copies of live, shadow, and regime membership.

        The second mapping is observation-only and must never be sent to ExecutionEngine.
        """
        return dict(self._trading_products), dict(self._shadow_products), dict(self._regime_products)

    def _refresh_trading_quotes(self) -> None:
        """Refresh top-of-book marks without changing membership.

        V1.4.7 evaluates hourly structure every 15 minutes while membership still refreshes
        every 30 minutes. This lightweight quote refresh prevents the in-between scans from
        using 15-minute-old bid/ask data.
        """
        if not self._trading_products:
            return
        books = self.gateway.best_bid_asks(list(self._trading_products.keys()))
        for pid, meta in self._trading_products.items():
            quote = books.get(pid)
            if not quote:
                continue
            bid, ask = quote
            meta.best_bid = float(bid)
            meta.best_ask = float(ask)
            meta.spread_pct = self._spread_pct(meta.best_bid, meta.best_ask)
            if meta.best_bid > 0 and meta.best_ask > meta.best_bid:
                meta.price = (meta.best_bid + meta.best_ask) / 2.0

    def refresh_shadow_quotes(self) -> None:
        """Refresh top-of-book marks for the broader research-only shadow universe."""
        if not self._shadow_products:
            return
        books = self.gateway.best_bid_asks(list(self._shadow_products.keys()))
        for pid, meta in self._shadow_products.items():
            quote = books.get(pid)
            if not quote:
                continue
            bid, ask = quote
            meta.best_bid = float(bid)
            meta.best_ask = float(ask)
            meta.spread_pct = self._spread_pct(meta.best_bid, meta.best_ask)
            if meta.best_bid > 0 and meta.best_ask > meta.best_bid:
                meta.price = (meta.best_bid + meta.best_ask) / 2.0

    def _determine_regime(self, breadth: float, btc_above: bool) -> tuple[str, str]:
        previous = str(self.db.get_json("regime_state", "UNKNOWN") or "UNKNOWN")
        risk_off_enter = float(self.s.regime["breadth_risk_off_enter"])
        risk_off_exit = float(self.s.regime["breadth_risk_off_exit"])
        risk_on_enter = float(self.s.regime["breadth_risk_on"])
        risk_on_exit = float(self.s.regime.get("breadth_risk_on_exit", 0.45))

        # V1.4.3 state machine. BTC is no longer allowed to override weak breadth by itself.
        # Transitions are deliberately one step at a time:
        #   RISK_OFF -> NEUTRAL only when BTC is healthy AND breadth >= 40%.
        #   NEUTRAL  -> RISK_ON  only when BTC is healthy AND breadth >= 50%.
        #   RISK_ON  -> NEUTRAL when BTC weakens OR breadth < 45%.
        #   Any non-RISK_OFF state -> RISK_OFF only when BTC is weak AND breadth < 35%.
        if previous == "RISK_OFF":
            regime = "NEUTRAL" if (btc_above and breadth >= risk_off_exit) else "RISK_OFF"
        elif previous == "RISK_ON":
            if (not btc_above) and breadth < risk_off_enter:
                regime = "RISK_OFF"
            elif (not btc_above) or breadth < risk_on_exit:
                regime = "NEUTRAL"
            else:
                regime = "RISK_ON"
        else:  # NEUTRAL or UNKNOWN
            if (not btc_above) and breadth < risk_off_enter:
                regime = "RISK_OFF"
            elif btc_above and breadth >= risk_on_enter:
                regime = "RISK_ON"
            else:
                regime = "NEUTRAL"

        self.db.set_json("regime_state", regime)
        return regime, previous

    def position_monitor(self, product_id: str) -> tuple[SignalRow | None, ProductMeta | None]:
        """Build a lightweight mark-to-market row for an open position.

        This is used by the 5-minute open-position exit monitor. It makes only the requests needed
        for the held product: current product metadata plus completed hourly candles. The
        current Coinbase price is overlaid on stable hourly EMA/ATR history, allowing faster
        trend-failure/trailing-stop decisions without running the full cross-sectional scan.
        """
        raw = self.gateway.get_product(product_id)
        product = self.gateway.product_meta(raw)
        if product is None:
            return None, None
        candles = self.gateway.candles(product_id, hours=349)
        if len(candles) < int(self.s.universe["min_history_hours"]):
            return None, product
        # For an exit monitor, the current best bid is the conservative executable mark.
        # Fall back to Coinbase's product price only if top-of-book is unavailable.
        mark_price = product.best_bid if product.best_bid > 0 else product.price
        row = build_signal_row(product_id, candles, self.s.signal, current_price=mark_price)
        return row, product

    def scan(self, current_product: str | None = None) -> ScanResult:
        refresh = self.refresh_universes(current_product=current_product)
        if refresh.get("trading_refreshed") or refresh.get("shadow_refreshed") or refresh.get("regime_refreshed"):
            log.info(
                "Universe refresh during scan: trading=%s%s shadow=%s%s regime=%s%s",
                refresh.get("trading_count", 0),
                "*" if refresh.get("trading_refreshed") else "",
                refresh.get("shadow_count", 0),
                "*" if refresh.get("shadow_refreshed") else "",
                refresh.get("regime_count", 0),
                "*" if refresh.get("regime_refreshed") else "",
            )
        else:
            # Membership remains 30-minute stable, but 15-minute decision scans need a fresh
            # executable midpoint. One batched best-bid/ask request updates marks cheaply.
            try:
                self._refresh_trading_quotes()
            except Exception as exc:
                log.warning("15m quote refresh failed; using last universe snapshot marks: %s", exc)

        products = dict(self._trading_products)
        sig_cfg = self.s.signal
        trading_min_hist = int(self.s.universe["min_history_hours"])
        regime_min_hist = int(self.s.regime_universe.get("min_history_hours", trading_min_hist))

        # A held asset is monitored even if current spread/volume removes it from the entry universe.
        # It is NOT reintroduced into cross-sectional ranking merely because we already own it.
        if current_product and current_product not in products:
            try:
                held_raw = self.gateway.get_product(current_product)
                held_meta = self.gateway.product_meta(held_raw)
                if held_meta:
                    products[current_product] = held_meta
            except Exception as exc:
                log.warning("Could not refresh held-product metadata %s: %s", current_product, exc)

        # Pull each product's candles once even if it belongs to both universes.
        all_ids = list(dict.fromkeys([
            *self._trading_products.keys(),
            *self._regime_products.keys(),
            *([current_product] if current_product else []),
        ]))
        rows_by_pid: dict[str, SignalRow] = {}
        total = len(all_ids)
        for idx, pid in enumerate(all_ids, start=1):
            try:
                candles = self.gateway.candles(pid, hours=349)
                min_needed = min(
                    trading_min_hist if pid in self._trading_products else 10**9,
                    regime_min_hist if pid in self._regime_products else 10**9,
                    trading_min_hist if current_product and pid == current_product else 10**9,
                )
                if len(candles) < min_needed:
                    log.debug("Skip %-15s history=%s < %s", pid, len(candles), min_needed)
                    continue
                # Trading candidates and the currently held product are marked to a fresh
                # Coinbase product price. Regime-only members remain based on completed hourly
                # candles so breadth is not overly sensitive to intrahour noise.
                current_price = None
                if pid in self._trading_products or (current_product and pid == current_product):
                    meta = products.get(pid) or self._trading_products.get(pid)
                    if meta:
                        if meta.best_bid > 0 and meta.best_ask > meta.best_bid:
                            current_price = (meta.best_bid + meta.best_ask) / 2.0
                        elif meta.price > 0:
                            current_price = meta.price
                row = build_signal_row(pid, candles, sig_cfg, current_price=current_price)
                if row:
                    rows_by_pid[pid] = row
            except Exception as exc:
                log.warning("Market-data skip %s: %s", pid, exc)
            if idx % 20 == 0 or idx == total:
                log.info("Signal data: %s/%s products processed", idx, total)

        trading_rows = [rows_by_pid[pid] for pid in self._trading_products if pid in rows_by_pid]
        ranked = assign_percentiles(trading_rows)
        if not ranked:
            return ScanResult(
                [], products, "UNKNOWN", str(self.db.get_json("regime_state", "UNKNOWN")), 0.0, [], [],
                len(self._trading_products), len(self._regime_products), 0, {},
            )

        regime_rows = [rows_by_pid[pid] for pid in self._regime_products if pid in rows_by_pid]
        min_regime_members = int(self.s.regime_universe.get("min_products_warning", 0))
        if min_regime_members and len(regime_rows) < min_regime_members:
            log.warning(
                "Regime breadth has only %s valid members (preferred minimum %s); regime remains conservative.",
                len(regime_rows), min_regime_members,
            )
        breadth = (sum(1 for r in regime_rows if r.breadth_member) / len(regime_rows)) if regime_rows else 0.0
        btc = rows_by_pid.get(self.s.regime["btc_product"])
        btc_above = bool(btc and btc.price > btc.ema_slow)
        regime, previous_regime = self._determine_regime(breadth, btc_above)

        # V1.4.7 signal decisions are aligned every 15 minutes. Persistence still represents
        # roughly 30 minutes of sustained qualification via three consecutive 15m buckets.
        # Restarting repeatedly inside the same bucket cannot manufacture persistence.
        interval = int(self.s.runtime.get("signal_scan_interval_minutes", 30))
        offset = int(self.s.runtime.get("signal_scan_offset_minute_utc", 3)) % interval
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elapsed_minutes = int((now - day_start).total_seconds() // 60)
        slot = (elapsed_minutes - offset) // interval
        current_bucket_dt = day_start + timedelta(minutes=offset + slot * interval)
        if current_bucket_dt > now:
            current_bucket_dt -= timedelta(minutes=interval)
        current_bucket = current_bucket_dt.isoformat()
        previous_bucket = self.db.get_json("persistence_scan_bucket", None)
        # Backward compatibility: ignore the old hourly key rather than letting a V1.3 value
        # accidentally count as another scheduled persistence observation.
        old_counts: dict[str, int] = self.db.get_json("persistence_counts", {}) or {}
        if previous_bucket == current_bucket:
            same_bucket = True
            consecutive_bucket = False
        else:
            same_bucket = False
            try:
                previous_dt = datetime.fromisoformat(previous_bucket) if previous_bucket else None
                consecutive_bucket = bool(previous_dt and current_bucket_dt - previous_dt == timedelta(minutes=interval))
            except (TypeError, ValueError):
                consecutive_bucket = False

        new_counts: dict[str, int] = {}
        threshold = float(sig_cfg["top_percentile"])
        for r in ranked:
            if r.trend_pass and r.percentile >= threshold:
                if same_bucket:
                    new_counts[r.product_id] = int(old_counts.get(r.product_id, 1))
                elif consecutive_bucket:
                    new_counts[r.product_id] = int(old_counts.get(r.product_id, 0)) + 1
                else:
                    new_counts[r.product_id] = 1
        self.db.set_json("persistence_counts", new_counts)
        self.db.set_json("persistence_scan_bucket", current_bucket)

        regime_pass = not (regime == "RISK_OFF" and self.s.regime.get("block_new_entries_in_risk_off", True))
        persistence_required = int(sig_cfg["persistence_scans"])
        candidates: list[SignalRow] = []
        diagnostics: list[CandidateDiagnostic] = []

        for r in ranked:
            persistence = int(new_counts.get(r.product_id, 0))
            raw_stop_pct = float(self.s.risk["atr_stop_multiple"]) * r.atr_pct
            effective_stop_pct = max(raw_stop_pct, float(self.s.risk["min_stop_pct"]))
            rank_pass = r.percentile >= threshold
            trend_pass = bool(r.trend_pass)
            persistence_pass = persistence >= persistence_required
            breakout_pass = bool(r.breakout)
            sigma_pass = r.one_hour_sigma <= float(sig_cfg["max_one_hour_sigma"])
            distance_pass = r.distance_atr <= float(sig_cfg["max_distance_atr"])
            stop_pass = raw_stop_pct <= float(self.s.risk["max_stop_pct"])
            cooldown_pass = not self.db.in_cooldown(r.product_id)

            reasons: list[str] = []
            if not regime_pass:
                reasons.append("REGIME")
            if not rank_pass:
                reasons.append("RANK")
            if not trend_pass:
                reasons.append("TREND")
            if not persistence_pass:
                reasons.append(f"PERSIST {persistence}/{persistence_required}")
            if not breakout_pass:
                reasons.append("BREAKOUT")
            if not sigma_pass:
                reasons.append("1H_EXTENDED")
            if not distance_pass:
                reasons.append("EMA_DISTANCE")
            if not stop_pass:
                reasons.append(f"STOP {raw_stop_pct:.1%}>{float(self.s.risk['max_stop_pct']):.1%}")
            if not cooldown_pass:
                reasons.append("COOLDOWN")

            actionable = not reasons
            if actionable:
                candidates.append(r)

            diagnostics.append(CandidateDiagnostic(
                product_id=r.product_id,
                rank=r.rank,
                percentile=r.percentile,
                persistence=persistence,
                persistence_required=persistence_required,
                raw_stop_pct=raw_stop_pct,
                effective_stop_pct=effective_stop_pct,
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

        self.db.log_scan(regime, breadth, ranked)
        return ScanResult(
            ranked,
            products,
            regime,
            previous_regime,
            breadth,
            candidates,
            diagnostics,
            len(self._trading_products),
            len(self._regime_products),
            len(regime_rows),
            new_counts,
        )
