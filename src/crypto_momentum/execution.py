from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any

from .coinbase_client import CoinbaseGateway, _f
from .utils import round_down, maker_buy_limit_price, base_size_for_cash
from .db import StateDB
from .models import PositionState, ProductMeta, SignalRow
from .notifications import PushoverNotifier

log = logging.getLogger(__name__)


class ExecutionEngine:
    def __init__(self, gateway: CoinbaseGateway, db: StateDB, settings):
        self.g = gateway
        self.db = db
        self.s = settings
        self.notifier = PushoverNotifier.from_env(getattr(settings, "project_root", None))

    @staticmethod
    def _uuid(_prefix: str) -> str:
        # Coinbase requires uniqueness; keeping a true UUID preserves idempotency semantics.
        return str(uuid.uuid4())

    def _cash_equivalents(self) -> set[str]:
        return {str(x).upper() for x in self.s.strategy.get("cash_equivalent_symbols", ["USDC"])}

    def equity(self, products: dict[str, ProductMeta] | None = None) -> float:
        balances = self.g.balances()
        total = balances.get("USD", {}).get("total", 0.0)
        cash_equiv = self._cash_equivalents()
        for currency, bal in balances.items():
            if currency == "USD" or bal["total"] <= 0:
                continue
            if currency in cash_equiv:
                total += bal["total"]
                continue
            pid = f"{currency}-USD"
            try:
                price = products[pid].price if products and pid in products else _f(self.g.get_product(pid).get("price"))
            except Exception:
                price = 0.0
            if price > 0:
                total += bal["total"] * price
        return total

    def risk_halts(self, products: dict[str, ProductMeta] | None = None) -> tuple[bool, str]:
        equity = self.equity(products)
        now_date = datetime.now(timezone.utc).date().isoformat()
        day_key = self.db.get_json("day_start", {}) or {}
        if day_key.get("date") != now_date:
            self.db.set_json("day_start", {"date": now_date, "equity": equity})
            day_start = equity
        else:
            day_start = float(day_key.get("equity", equity))

        stored_peak = self.db.get_json("peak_equity", None)
        if stored_peak is None or float(stored_peak or 0.0) <= 0:
            peak = equity
            self.db.set_json("peak_equity", peak)
        else:
            peak = float(stored_peak)
            if equity > peak:
                peak = equity
                self.db.set_json("peak_equity", peak)

        if peak > 0 and equity <= peak * (1.0 - float(self.s.risk["max_account_drawdown_pct"])):
            return True, f"account drawdown kill switch: equity ${equity:.2f}, peak ${peak:.2f}"

        pnl_today = self.db.realized_pnl_today()
        max_daily = day_start * float(self.s.risk["daily_realized_loss_halt_pct"])
        if max_daily > 0 and pnl_today <= -max_daily:
            return True, f"daily realized loss halt: P&L ${pnl_today:.2f} vs start equity ${day_start:.2f}"

        if self.db.entries_last_24h() >= int(self.s.risk["max_new_entries_per_24h"]):
            return True, "maximum new entries in rolling 24h reached"

        if self.db.get_json("manual_halt", False):
            return True, "manual halt flag is set"

        return False, ""

    def current_non_usd_holdings(self) -> dict[str, float]:
        balances = self.g.balances()
        holdings: dict[str, float] = {}
        dust_usd = float(self.s.risk["dust_value_usd"])
        cash_equiv = self._cash_equivalents()
        for currency, bal in balances.items():
            if currency == "USD" or currency in cash_equiv or bal["total"] <= 0:
                continue
            pid = f"{currency}-USD"
            try:
                price = _f(self.g.get_product(pid).get("price"))
            except Exception:
                price = 0.0
            if price > 0 and bal["total"] * price >= dust_usd:
                holdings[currency] = bal["total"]
        return holdings

    @staticmethod
    def _order_status(payload: dict[str, Any]) -> str:
        order = payload.get("order", payload)
        return str(order.get("status") or "").upper()

    def _aggregate_orders(self, order_ids: list[str]) -> tuple[float, float, float]:
        """Aggregate fills across one or more entry orders.

        The fills endpoint is preferred. Order summary fields are a fallback for brief
        fill-reporting lag so a completed entry can still be protected immediately.
        """
        qty = 0.0
        notional = 0.0
        fees = 0.0
        for oid in order_ids:
            if not oid:
                continue
            fills = self.g.fills(order_id=oid)
            fq, fp, ff = self.g.fill_summary(fills)
            if fq > 0 and fp > 0:
                qty += fq
                notional += fq * fp
                fees += ff
                continue
            try:
                payload = self.g.order(oid)
                order = payload.get("order", payload)
                oq = _f(order.get("filled_size"))
                op = _f(order.get("average_filled_price"))
                of = _f(order.get("total_fees"))
                if oq > 0 and op > 0:
                    qty += oq
                    notional += oq * op
                    fees += of
            except Exception as exc:
                log.warning("Could not read order summary %s while aggregating entry: %s", oid, exc)
        return qty, (notional / qty if qty > 0 else 0.0), fees

    def _recover_pending_entry(self, holdings: dict[str, float]) -> PositionState | None:
        pending = self.db.get_json("pending_entry", None)
        if not pending:
            return None

        pid = str(pending.get("product_id") or "")
        base = str(pending.get("base_currency") or "")
        order_ids = [str(x) for x in (pending.get("order_ids") or []) if x]
        log.warning("Recovering interrupted entry intent for %s; cancelling any orphan entry orders first.", pid)

        # Never leave a maker entry sitting on the book after a restart. Cancel first,
        # then determine whether any fills occurred before deciding how to recover.
        if self.s.is_live:
            for oid in order_ids:
                try:
                    payload = self.g.order(oid)
                    if self._order_status(payload) in {"OPEN", "PENDING", "ACTIVE"}:
                        self.g.cancel(oid)
                except Exception as exc:
                    log.warning("Pending-entry order cleanup failed for %s: %s", oid, exc)
            time.sleep(0.75)

        # V1.4.5: order/fill state is authoritative for whether an entry happened. Coinbase
        # account balances can lag a just-filled order briefly, so never erase a confirmed
        # pending fill merely because the first balance snapshot still shows zero.
        qty, avg_price, fees = self._aggregate_orders(order_ids)
        holdings = self.current_non_usd_holdings()
        held_qty = holdings.get(base, 0.0)

        if qty <= 0 and held_qty <= 0:
            self.db.set_json("pending_entry", None)
            log.info("Interrupted entry had no fill and no material Coinbase holding; pending intent cleared.")
            return None

        extras = {k: v for k, v in holdings.items() if k != base}
        if extras:
            raise RuntimeError(f"Pending-entry recovery found ambiguous holdings: {holdings}")

        if avg_price <= 0:
            raise RuntimeError(
                f"Strategy-created holding {base} exists after interrupted entry, but entry price could not be reconstructed. "
                "Do not trade manually in this portfolio; inspect Coinbase fills before resuming."
            )

        if held_qty <= 0 and qty > 0:
            held_qty = self._wait_for_entry_holding(base, qty)
        if held_qty <= 0:
            # Keep pending_entry intact so the next recovery pass still knows this was a bot
            # order. Clearing it here would convert a temporary balance-visibility lag into an
            # 'unknown manual holding' failure like the V1.4.4 incident.
            raise RuntimeError(
                f"Entry fill for {pid} is confirmed (qty={qty:.12g}) but Coinbase balances have not "
                "materialized the holding yet. Pending entry retained; retry recovery shortly."
            )

        stop_pct = float(pending.get("stop_pct") or 0.0)
        if stop_pct <= 0:
            raise RuntimeError("Pending-entry recovery is missing stop_pct; refusing to adopt an unprotected position")
        stop_price = avg_price * (1.0 - stop_pct)
        state = PositionState(
            product_id=pid,
            base_currency=base,
            quantity=held_qty,
            entry_price=avg_price,
            entry_time=str(pending.get("started_at") or datetime.now(timezone.utc).isoformat()),
            initial_stop_price=stop_price,
            stop_price=stop_price,
            stop_order_id=None,
            highest_close=avg_price,
            initial_risk_pct=stop_pct,
            entry_fees=fees,
            entry_order_id=order_ids[-1] if order_ids else None,
            lowest_close=avg_price,
        )
        self.db.set_position(state)
        self.db.log_trade(
            "ENTRY_RECOVERED", pid, "BUY", held_qty, avg_price, fees,
            order_id=state.entry_order_id,
            details={"order_ids": order_ids, "recovered_after_restart": True},
        )
        self.db.open_trade_ledger(
            product_id=pid,
            entry_ts=state.entry_time,
            entry_price=avg_price,
            quantity=held_qty,
            entry_fee=fees,
            initial_stop_price=state.initial_stop_price,
            initial_risk_pct=stop_pct,
            entry_order_id=state.entry_order_id,
            execution_path="RECOVERED_PENDING_ENTRY",
            signal=dict(pending.get("signal") or {}),
            source="RECOVERED_PENDING_ENTRY",
            analytics_quality="FULL_SAMPLED",
            details={"order_ids": order_ids},
        )
        product = self.g.product_meta(self.g.get_product(pid))
        if product is None:
            raise RuntimeError(f"Could not load product metadata while recovering {pid}")
        if self.s.is_live:
            state = self.ensure_protection(state, product)
        self.db.set_json("pending_entry", None)
        return state

    def reconcile(self) -> PositionState | None:
        """Reconcile local state against Coinbase. Coinbase is always source of truth."""
        state = self.db.get_position()
        holdings = self.current_non_usd_holdings()

        if state is None:
            recovered = self._recover_pending_entry(holdings)
            if recovered is not None:
                return recovered
            if holdings:
                raise RuntimeError(
                    "The strategy portfolio contains non-USD holdings but local state has no position: "
                    f"{holdings}. Move/sell them before live operation. The bot refuses to adopt manual holdings."
                )
            return None

        held_qty = holdings.get(state.base_currency, 0.0)
        if held_qty <= 0:
            pending = self.db.get_json("pending_entry", None) or {}
            same_pending = str(pending.get("product_id") or "") == state.product_id
            pending_ids = [str(x) for x in (pending.get("order_ids") or []) if x]
            if same_pending and pending_ids:
                fill_qty, fill_avg, _fill_fees = self._aggregate_orders(pending_ids)
                if fill_qty > 0 and fill_avg > 0:
                    # A confirmed entry fill plus a still-present pending intent means Coinbase's
                    # account-balance view is lagging. Preserve the local position and retry
                    # protection; never downgrade this into an unknown/manual-holding state.
                    log.warning(
                        "Coinbase balance visibility is lagging confirmed entry %s (fill qty=%.12g); "
                        "local position retained and protection will be retried.",
                        state.product_id, fill_qty,
                    )
                    return state

            if state.stop_order_id:
                try:
                    order = self.g.order(state.stop_order_id)
                    if self._order_status(order) == "FILLED":
                        fills = self.g.fills(order_id=state.stop_order_id)
                        qty, price, fees = self.g.fill_summary(fills)
                        pnl = (price - state.entry_price) * qty - state.entry_fees - fees if qty and price else None
                        exit_reason = "EXCHANGE_STOP_FILLED"
                        self.db.log_trade(
                            "EXIT_FILLED", state.product_id, "SELL", qty, price, fees, pnl,
                            state.stop_order_id, {"reason": exit_reason}
                        )
                        self.db.close_trade_ledger(
                            product_id=state.product_id,
                            exit_ts=self._latest_fill_time_iso(fills),
                            exit_price=price,
                            exit_quantity=qty,
                            exit_fee=fees,
                            exit_order_id=state.stop_order_id,
                            exit_reason=exit_reason,
                        )
                        log.info(
                            "EXCHANGE STOP filled %s qty=%.12g avg=%.8f fee=$%.4f P&L=%s order=%s",
                            state.product_id, qty, price, fees,
                            f"${pnl:.4f}" if pnl is not None else "unknown", state.stop_order_id,
                        )
                        self.notifier.exit(
                            product_id=state.product_id,
                            price=price,
                            reason=exit_reason,
                            gross_return=((price / state.entry_price) - 1.0) if price and state.entry_price else None,
                            realized_pnl=pnl,
                        )
                        self.db.set_cooldown(
                            state.product_id,
                            int(self.s.risk["stopped_token_cooldown_hours"]),
                            "stop filled",
                        )
                except Exception as exc:
                    log.warning("Could not inspect prior protection order: %s", exc)
            log.warning("Local position no longer exists on Coinbase; clearing local state.")
            self.db.set_position(None)
            return None

        extras = {k: v for k, v in holdings.items() if k != state.base_currency}
        if extras:
            raise RuntimeError(f"Multiple non-USD holdings found in strategy portfolio: {holdings}")

        state.quantity = held_qty
        self.db.set_position(state)
        return state

    def record_position_mark(self, state: PositionState, mark: float) -> PositionState:
        """Persist sampled MAE/MFE analytics without changing stop-decision sequencing.

        ``highest_close`` remains owned by trail_stop so V1.4.10/11 profit-lock semantics are
        unchanged. ``lowest_close`` and the dedicated trade ledger can safely observe every
        management mark before an exit decision.
        """
        if mark <= 0:
            return state
        low = float(state.lowest_close or 0.0)
        if low <= 0:
            low = float(state.entry_price)
        state.lowest_close = min(low, float(mark), float(state.entry_price))
        self.db.set_position(state)
        self.db.update_trade_excursions(state.product_id, float(mark))
        return state

    def profit_lock_levels(
        self,
        state: PositionState,
        *,
        observed_price: float | None = None,
    ) -> dict[str, float | bool]:
        """Return the V1.4.10 fee-lock / peak-capture levels for a position.

        The rule is deliberately based on the persisted high-water mark so it survives
        restarts and can never loosen after arming. ``observed_price`` is used only when
        evaluating a fresh mark before it has been persisted into ``highest_close``.
        """
        entry = float(state.entry_price)
        high = max(float(state.highest_close), float(observed_price or 0.0), entry)
        mfe = (high / entry - 1.0) if entry > 0 else 0.0

        activation = float(self.s.risk.get("profit_lock_activation_pct", 0.05))
        min_trigger = float(self.s.risk.get("profit_lock_min_trigger_pct", 0.035))
        min_limit = float(self.s.risk.get("profit_lock_min_limit_pct", 0.025))
        capture_ratio = float(self.s.risk.get("profit_capture_ratio", 0.65))
        limit_gap = float(self.s.risk.get("profit_capture_limit_gap_pct", 0.01))

        armed = mfe >= activation
        if not armed:
            return {
                "armed": False,
                "mfe_return": mfe,
                "protected_return": 0.0,
                "trigger_price": 0.0,
                "limit_return": 0.0,
                "limit_price": 0.0,
            }

        protected_return = max(min_trigger, capture_ratio * mfe)
        limit_return = max(min_limit, protected_return - limit_gap)
        return {
            "armed": True,
            "mfe_return": mfe,
            "protected_return": protected_return,
            "trigger_price": entry * (1.0 + protected_return),
            "limit_return": limit_return,
            "limit_price": entry * (1.0 + limit_return),
        }

    def _protective_limit_raw(self, state: PositionState) -> float:
        """Compute the sell stop-limit limit price before exchange tick rounding.

        Normal protection keeps the legacy percentage buffer. After the +5% profit lock
        arms, the limit is never intentionally placed below the configured fee-aware
        profit floor. The value is capped at the stop trigger so a restoration of older
        protection can never create an invalid sell stop-limit with limit > stop.
        """
        default_limit = float(state.stop_price) * (1.0 - float(self.s.risk["stop_limit_buffer_pct"]))
        lock = self.profit_lock_levels(state)
        if bool(lock["armed"]):
            default_limit = max(default_limit, float(lock["limit_price"]))
        return min(float(state.stop_price), default_limit)

    def _proposed_trailing_stop(
        self,
        state: PositionState,
        row: SignalRow,
    ) -> tuple[float, dict[str, float | bool], float]:
        """Return (proposed_stop, profit_lock_levels, updated_high_water)."""
        high_water = max(float(state.highest_close), float(row.price))

        activation_price = state.entry_price * (
            1.0 + state.initial_risk_pct * float(self.s.risk["trail_activation_r"])
        )
        chandelier = state.initial_stop_price
        if high_water >= activation_price:
            chandelier = high_water - float(self.s.risk["trail_atr_multiple"]) * row.atr
            chandelier = max(chandelier, state.initial_stop_price)

        lock = self.profit_lock_levels(state, observed_price=high_water)
        profit_floor = (
            float(lock["trigger_price"]) if bool(lock["armed"]) else state.initial_stop_price
        )
        proposed = max(chandelier, profit_floor, state.initial_stop_price)
        return proposed, lock, high_water

    def ensure_protection(self, state: PositionState, product: ProductMeta) -> PositionState:
        if state.stop_order_id:
            try:
                payload = self.g.order(state.stop_order_id)
                status = self._order_status(payload)
                if status in {"OPEN", "PENDING", "ACTIVE"}:
                    return state
                if status == "FILLED":
                    return self.reconcile()  # type: ignore[return-value]
            except Exception as exc:
                log.warning("Protection order check failed: %s", exc)

        if not self.s.is_live:
            return state

        balances = self.g.balances()
        qty = balances.get(state.base_currency, {}).get("available", 0.0)
        if qty <= 0:
            # Immediately after a live fill, Coinbase's order endpoint can show FILLED before
            # the account endpoint exposes the new base balance. Give settlement a bounded
            # window rather than declaring the position nonexistent/unprotectable.
            qty = self._wait_available_base(
                state.base_currency,
                timeout_seconds=float(self.s.execution.get("entry_protection_wait_seconds", 20.0)),
            )
        if qty <= 0:
            qty = self.g.balances().get(state.base_currency, {}).get("total", 0.0)
        base_size = round_down(qty, product.base_increment)
        if float(base_size) <= 0:
            raise RuntimeError("Cannot place protection: confirmed entry exists but no sellable base quantity is visible yet")

        stop_price = round_down(state.stop_price, product.quote_increment)
        limit = self._protective_limit_raw(state)
        limit_price = round_down(limit, product.quote_increment)
        resp = self.g.place_stop_limit_sell(
            state.product_id, base_size, stop_price, limit_price, self._uuid("stop")
        )
        oid = self.g.extract_order_id(resp)
        if not oid:
            raise RuntimeError(f"Failed to place exchange protection: {self.g.response_error(resp)}")
        state.stop_order_id = oid
        self.db.set_position(state)
        self.db.log_trade("PROTECTION_PLACED", state.product_id, "SELL", float(base_size), state.stop_price, order_id=oid)
        log.info("Protection placed: %s stop %.8f (limit %.8f)", state.product_id, state.stop_price, float(limit_price))
        return state

    def _target_cash_budget(self, product: ProductMeta) -> tuple[float, str]:
        usd = self.g.balances().get("USD", {}).get("available", 0.0)
        reserve = float(self.s.execution["min_cash_reserve_usd"])
        amount = min(usd * float(self.s.strategy["deployment_pct"]), max(0.0, usd - reserve))
        quote_size = round_down(amount, product.quote_increment)
        return usd, quote_size

    def _market_preview_metrics(self, product_id: str, quote_size: str) -> dict[str, Any]:
        preview = self.g.preview_market_buy(product_id, quote_size)
        metrics = self.g.preview_metrics(preview)
        requested_quote = float(quote_size)
        if requested_quote > 0:
            metrics["commission_pct"] = float(metrics["commission"]) / requested_quote
        return metrics

    def _taker_fallback_allowed(
        self,
        row: SignalRow,
        quote_size: str,
    ) -> tuple[bool, dict[str, Any], str]:
        metrics = self._market_preview_metrics(row.product_id, quote_size)
        if metrics["errs"]:
            return False, metrics, f"preview errors: {metrics['errs']}"
        if float(metrics["spread_pct"]) > float(self.s.execution["max_spread_pct"]):
            return False, metrics, f"spread {float(metrics['spread_pct']):.3%} too wide"
        if float(metrics["slippage_pct"]) > float(self.s.execution["max_preview_slippage_pct"]):
            return False, metrics, f"preview slippage {float(metrics['slippage_pct']):.3%} too high"
        _maker_fee, taker_fee = self.g.fee_rates()
        friction = (
            float(metrics["commission_pct"])
            + taker_fee
            + float(metrics["spread_pct"])
            + 2.0 * float(metrics["slippage_pct"])
        )
        metrics["taker_roundtrip_friction_pct"] = friction
        max_friction = float(self.s.execution["max_taker_roundtrip_friction_pct"])
        if friction > max_friction:
            return False, metrics, f"taker round-trip friction {friction:.3%} > {max_friction:.3%}"
        multiple = float(self.s.execution["min_24h_return_to_taker_friction_multiple"])
        if friction > 0 and row.r24 < friction * multiple:
            return False, metrics, f"24h momentum {row.r24:.2%} < {multiple:.1f}x taker friction {friction:.2%}"
        return True, metrics, ""

    def preview_candidate(self, row: SignalRow, product: ProductMeta) -> tuple[bool, str, dict[str, Any]]:
        bid, ask = self.g.best_bid_ask(row.product_id)
        if bid <= 0 or ask <= 0 or ask <= bid:
            return False, "could not obtain valid bid/ask", {}
        spread = (ask - bid) / ((ask + bid) / 2.0)
        if spread > float(self.s.execution["max_spread_pct"]):
            return False, f"spread {spread:.3%} too wide", {"spread_pct": spread}

        usd, quote_size = self._target_cash_budget(product)
        if float(quote_size) <= 0 or float(quote_size) < max(product.quote_min_size, 0.0):
            return False, f"insufficient USD for minimum order (available ${usd:.2f})", {}

        maker_fee, taker_fee = self.g.fee_rates()
        maker_price = maker_buy_limit_price(
            bid,
            ask,
            product.quote_increment,
            float(self.s.execution["maker_inside_spread_fraction"]),
        )
        maker_base = base_size_for_cash(float(quote_size), float(maker_price), maker_fee, product.base_increment)
        maker_notional = float(maker_base) * float(maker_price)
        if float(maker_base) <= 0 or float(maker_base) < product.base_min_size:
            return False, "maker order would be below base minimum", {}
        if product.quote_min_size > 0 and maker_notional < product.quote_min_size:
            return False, "maker order would be below quote minimum", {}

        taker_ok, taker_metrics, taker_reason = self._taker_fallback_allowed(row, quote_size)
        # The market preview also serves as a depth sanity check for the product even when
        # the actual primary order is maker-only.
        if taker_metrics.get("errs"):
            return False, f"market-depth preview failed: {taker_metrics['errs']}", taker_metrics
        if float(taker_metrics.get("spread_pct", spread)) > float(self.s.execution["max_spread_pct"]):
            return False, f"spread {float(taker_metrics.get('spread_pct', spread)):.3%} too wide", taker_metrics
        if float(taker_metrics.get("slippage_pct", 0.0)) > float(self.s.execution["max_preview_slippage_pct"]):
            return False, f"market depth too thin: preview slippage {float(taker_metrics.get('slippage_pct', 0.0)):.3%}", taker_metrics

        # Maker entry avoids paying the entry spread and uses the lower maker fee. We still
        # budget a conservative current full spread + one preview slippage unit for the future
        # taker/stop exit so the edge test is not artificially optimistic.
        maker_friction = (
            maker_fee
            + taker_fee
            + spread
            + float(taker_metrics.get("slippage_pct", 0.0))
        )
        maker_max = float(self.s.execution["max_maker_roundtrip_friction_pct"])
        maker_multiple = float(self.s.execution["min_24h_return_to_maker_friction_multiple"])
        if maker_friction > maker_max:
            return False, f"maker-path round-trip friction {maker_friction:.3%} > {maker_max:.3%}", {}
        if maker_friction > 0 and row.r24 < maker_friction * maker_multiple:
            return (
                False,
                f"24h momentum {row.r24:.2%} < {maker_multiple:.1f}x maker-path friction {maker_friction:.2%}",
                {},
            )

        metrics: dict[str, Any] = dict(taker_metrics)
        metrics.update({
            "quote_size": float(quote_size),
            "target_cash_usd": float(quote_size),
            "initial_bid": bid,
            "initial_ask": ask,
            "maker_fee_pct": maker_fee,
            "taker_fee_pct": taker_fee,
            "maker_limit_price": float(maker_price),
            "maker_limit_price_str": maker_price,
            "maker_base_size": float(maker_base),
            "maker_base_size_str": maker_base,
            "maker_roundtrip_friction_pct": maker_friction,
            "taker_roundtrip_friction_pct": float(taker_metrics.get("taker_roundtrip_friction_pct", 0.0)),
            "fallback_allowed": bool(self.s.execution.get("fallback_taker_enabled", True) and taker_ok),
            "fallback_block_reason": "" if taker_ok else taker_reason,
        })
        return True, "", metrics

    def _cancel_entry_order(self, order_id: str) -> dict[str, Any]:
        try:
            self.g.cancel(order_id)
        except Exception as exc:
            log.warning("Cancel request for maker entry %s returned an error: %s", order_id, exc)
        # Let cancel/fill races settle, then read authoritative order state.
        time.sleep(0.5)
        order = self.g.wait_for_fill(order_id, 4)
        status = str(order.get("status") or "").upper()
        if status in {"OPEN", "PENDING", "ACTIVE"}:
            raise RuntimeError(f"Maker entry {order_id} remained {status} after cancel; refusing fallback to avoid overbuy")
        return order

    def _finalize_entry(
        self,
        row: SignalRow,
        product: ProductMeta,
        stop_pct: float,
        order_ids: list[str],
        execution_details: dict[str, Any],
    ) -> PositionState | None:
        # Fill history can lag order state very briefly. Give it one short chance to settle.
        qty, avg_price, fees = self._aggregate_orders(order_ids)
        if qty <= 0:
            time.sleep(0.75)
            qty, avg_price, fees = self._aggregate_orders(order_ids)

        if qty <= 0:
            # Only a confirmed absence of fills may clear the pending intent. A balance snapshot
            # alone is not sufficient because Coinbase can expose FILLED before balances update.
            self.db.set_json("pending_entry", None)
            log.info("Entry attempt for %s produced no confirmed fill; no position opened.", row.product_id)
            return None
        if avg_price <= 0:
            raise RuntimeError(f"Entry fill for {row.product_id} is confirmed but fill price could not be reconstructed")

        pending = self.db.get_json("pending_entry", None) or {}
        pending.update({
            "order_ids": order_ids.copy(),
            "confirmed_fill_qty": qty,
            "confirmed_avg_price": avg_price,
            "confirmed_fees": fees,
            "fill_confirmed_at": datetime.now(timezone.utc).isoformat(),
            "stage": "FILL_CONFIRMED_WAITING_BALANCE",
        })
        self.db.set_json("pending_entry", pending)

        stop_price = avg_price * (1.0 - stop_pct)
        # Persist local ownership immediately from the authoritative fill. This is deliberately
        # done before waiting on balances so a crash/restart cannot forget a real filled trade.
        state = PositionState(
            product_id=row.product_id,
            base_currency=product.base_currency,
            quantity=qty,
            entry_price=avg_price,
            entry_time=datetime.now(timezone.utc).isoformat(),
            initial_stop_price=stop_price,
            stop_price=stop_price,
            stop_order_id=None,
            highest_close=avg_price,
            initial_risk_pct=stop_pct,
            entry_fees=fees,
            entry_order_id=order_ids[-1] if order_ids else None,
            lowest_close=avg_price,
        )
        self.db.set_position(state)
        self.db.log_trade(
            "ENTRY_FILLED", row.product_id, "BUY", qty, avg_price, fees,
            order_id=state.entry_order_id,
            details={
                "reason": "MOMENTUM_TREND_BREAKOUT",
                "execution": execution_details,
                "order_ids": order_ids,
                "signal": row.to_dict(),
                "stop_pct": stop_pct,
                "balance_settlement_pending": True,
            },
        )
        self.db.open_trade_ledger(
            product_id=row.product_id,
            entry_ts=state.entry_time,
            entry_price=avg_price,
            quantity=qty,
            entry_fee=fees,
            initial_stop_price=state.initial_stop_price,
            initial_risk_pct=stop_pct,
            entry_order_id=state.entry_order_id,
            execution_path=str(execution_details.get("path", "unknown")),
            signal=row.to_dict(),
            source="LIVE",
            analytics_quality="FULL_SAMPLED",
            details={"order_ids": order_ids},
        )
        log.info(
            "ENTRY filled %s qty=%.12g avg=%.8f fee=$%.4f via %s; waiting for Coinbase balance settlement",
            row.product_id,
            qty,
            avg_price,
            fees,
            execution_details.get("path", "unknown"),
        )
        self.notifier.entry(
            product_id=row.product_id,
            price=avg_price,
            quantity=qty,
            reason="MOMENTUM_TREND_BREAKOUT",
            execution_path=str(execution_details.get("path", "unknown")),
            stop_price=stop_price,
        )

        held_qty = self._wait_for_entry_holding(product.base_currency, qty)
        if held_qty <= 0:
            # Do not erase state or pending_entry. The order fill is real. Halt additional entries
            # and let the 5-minute position monitor/restart recovery retry protection.
            self.db.set_json("manual_halt", True)
            log.critical(
                "ENTRY SETTLEMENT DELAY: %s fill qty=%.12g is confirmed but Coinbase balances still show no holding. "
                "Local position/pending entry retained and new entries halted; protection will be retried.",
                row.product_id, qty,
            )
            raise RuntimeError(
                f"Confirmed {row.product_id} entry fill has not appeared in Coinbase balances within settlement timeout"
            )

        state.quantity = held_qty
        self.db.set_position(state)
        pending["stage"] = "BALANCE_VISIBLE_PENDING_PROTECTION"
        pending["holding_qty"] = held_qty
        self.db.set_json("pending_entry", pending)

        try:
            state = self.ensure_protection(state, product)
        except Exception as protection_exc:
            log.critical("PROTECTION FAILED after entry: %s. Emergency market exit.", protection_exc)
            self.db.log_error("entry_protection", str(protection_exc))
            exited = False
            try:
                exited = self.exit(
                    state,
                    product,
                    reason="PROTECTION_PLACEMENT_FAILED",
                    protection_already_cancelled=True,
                )
            finally:
                if exited:
                    self.db.set_json("pending_entry", None)
                else:
                    self.db.set_json("manual_halt", True)
            raise RuntimeError("Entry filled but protection could not be established; emergency exit attempted") from protection_exc

        # Only after exchange-side protection exists is the crash-recovery intent complete.
        self.db.set_json("pending_entry", None)
        return state

    @staticmethod
    def _parse_exchange_time(value: Any) -> datetime | None:
        if not value:
            return None
        try:
            raw = str(value).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except (TypeError, ValueError):
            return None

    def _latest_fill_time_iso(self, fills: list[dict[str, Any]] | None) -> str:
        times: list[datetime] = []
        for fill in fills or []:
            dt = self._parse_exchange_time(
                fill.get("trade_time") or fill.get("sequence_timestamp") or fill.get("created_time")
            )
            if dt is not None:
                times.append(dt)
        return (max(times).isoformat() if times else self.db.now_iso())

    def recover_recent_orphan_position(self) -> PositionState:
        """Explicitly recover one recent bot-created holding after a lost local entry state.

        This command is intentionally strict and is never called by normal reconciliation.
        It exists for the V1.4.4 balance-settlement race: a Coinbase order was confirmed filled,
        the account balance appeared later, but pending/local state had already been cleared.
        Recovery requires exactly one holding, no local position, no open order, a recent matching
        BUY fill whose quantity matches the holding, and a recent persisted qualifying signal row.
        """
        if not self.s.is_live:
            raise RuntimeError("Orphan live-position recovery is available only in RUN_MODE=live")
        if self.db.get_position() is not None:
            raise RuntimeError("Local position already exists; orphan recovery is not applicable")
        if self.db.get_json("pending_entry", None):
            recovered = self._recover_pending_entry(self.current_non_usd_holdings())
            if recovered is None:
                raise RuntimeError("Pending entry existed but did not recover a position")
            return recovered

        holdings = self.current_non_usd_holdings()
        if len(holdings) != 1:
            raise RuntimeError(
                f"Recovery requires exactly one material non-USD holding; found {holdings or 'none'}"
            )
        base, held_qty = next(iter(holdings.items()))
        pid = f"{base}-USD"
        product = self.g.product_meta(self.g.get_product(pid))
        if product is None:
            raise RuntimeError(f"Could not load product metadata for orphan holding {pid}")

        open_orders = self.g.open_orders(pid)
        if open_orders:
            raise RuntimeError(
                f"Open Coinbase order(s) already exist for {pid}; refusing automatic recovery to avoid duplicate protection"
            )

        snapshot = self.db.latest_signal_snapshot(pid)
        if not snapshot:
            raise RuntimeError(f"No recent persisted scan row exists for {pid}; cannot prove this was a strategy entry")
        try:
            scan_ts = self._parse_exchange_time(snapshot.get("ts"))
            row = dict(snapshot.get("row") or {})
            atr_pct = float(row.get("atr_pct") or 0.0)
            percentile = float(row.get("percentile") or 0.0)
            trend_pass = bool(row.get("trend_pass"))
            breakout = bool(row.get("breakout"))
        except (TypeError, ValueError):
            raise RuntimeError(f"Recent scan snapshot for {pid} is malformed")

        max_age_hours = float(self.s.execution.get("orphan_recovery_max_age_hours", 6.0))
        if scan_ts is None or datetime.now(timezone.utc) - scan_ts > timedelta(hours=max_age_hours):
            raise RuntimeError(f"Latest qualifying scan for {pid} is older than {max_age_hours:.1f}h; refusing recovery")
        if not trend_pass or not breakout or percentile < float(self.s.signal.get("top_percentile", 0.90)):
            raise RuntimeError(
                f"Latest scan row for {pid} does not match a strategy-quality entry signal; refusing recovery"
            )

        fills = self.g.fills(product_id=pid, limit=100)
        groups: dict[str, dict[str, Any]] = {}
        for fill in fills:
            side = str(fill.get("side") or "").upper()
            if side and side != "BUY":
                continue
            oid = str(fill.get("order_id") or "")
            if not oid:
                continue
            q = _f(fill.get("size"))
            px = _f(fill.get("price"))
            fee = _f(fill.get("commission"))
            if q <= 0 or px <= 0:
                continue
            g = groups.setdefault(oid, {"qty": 0.0, "notional": 0.0, "fees": 0.0, "times": []})
            g["qty"] += q
            g["notional"] += q * px
            g["fees"] += fee
            dt = self._parse_exchange_time(
                fill.get("trade_time") or fill.get("sequence_timestamp") or fill.get("created_time")
            )
            if dt is not None:
                g["times"].append(dt)

        tolerance = max(float(product.base_increment) * 2.0, held_qty * 0.001)
        matches: list[tuple[str, dict[str, Any]]] = []
        now = datetime.now(timezone.utc)
        for oid, group in groups.items():
            q = float(group["qty"])
            if abs(q - held_qty) > tolerance:
                continue
            times = list(group.get("times") or [])
            latest_fill_time = max(times) if times else None
            if latest_fill_time is not None and now - latest_fill_time > timedelta(hours=max_age_hours):
                continue
            try:
                payload = self.g.order(oid)
                order = payload.get("order", payload)
                order_pid = str(order.get("product_id") or pid)
                order_side = str(order.get("side") or "BUY").upper()
                if order_pid != pid or order_side != "BUY":
                    continue
            except Exception as exc:
                log.warning("Could not verify candidate orphan order %s: %s", oid, exc)
                continue
            matches.append((oid, group))

        if len(matches) != 1:
            raise RuntimeError(
                f"Could not uniquely match {held_qty:.12g} {base} to one recent Coinbase BUY fill; "
                f"matching_orders={len(matches)}. Inspect Coinbase manually before recovery."
            )

        oid, group = matches[0]
        qty = float(group["qty"])
        avg_price = float(group["notional"]) / qty
        fees = float(group["fees"])
        raw_stop = float(self.s.risk["atr_stop_multiple"]) * atr_pct
        stop_pct = max(float(self.s.risk["min_stop_pct"]), raw_stop)
        if stop_pct > float(self.s.risk["max_stop_pct"]):
            raise RuntimeError(
                f"Recovered signal now implies stop {stop_pct:.2%} > configured maximum; refusing automatic adoption"
            )

        times = list(group.get("times") or [])
        entry_time = (max(times) if times else scan_ts or now).isoformat()
        stop_price = avg_price * (1.0 - stop_pct)
        state = PositionState(
            product_id=pid,
            base_currency=base,
            quantity=held_qty,
            entry_price=avg_price,
            entry_time=entry_time,
            initial_stop_price=stop_price,
            stop_price=stop_price,
            stop_order_id=None,
            highest_close=avg_price,
            initial_risk_pct=stop_pct,
            entry_fees=fees,
            entry_order_id=oid,
            lowest_close=avg_price,
        )
        self.db.set_position(state)
        self.db.log_trade(
            "ENTRY_RECOVERED_ORPHAN", pid, "BUY", held_qty, avg_price, fees,
            order_id=oid,
            details={
                "reason": "V1.4.4_BALANCE_SETTLEMENT_RACE",
                "scan_ts": snapshot.get("ts"),
                "stop_pct": stop_pct,
            },
        )
        self.db.open_trade_ledger(
            product_id=pid,
            entry_ts=entry_time,
            entry_price=avg_price,
            quantity=held_qty,
            entry_fee=fees,
            initial_stop_price=stop_price,
            initial_risk_pct=stop_pct,
            entry_order_id=oid,
            execution_path="RECOVERED_ORPHAN_ENTRY",
            signal=row,
            source="RECOVERED_ORPHAN_ENTRY",
            analytics_quality="FULL_SAMPLED",
            details={"scan_ts": snapshot.get("ts")},
        )
        try:
            state = self.ensure_protection(state, product)
        except Exception:
            self.db.set_json("manual_halt", True)
            raise
        self.db.set_json("pending_entry", None)
        log.warning(
            "RECOVERED orphan bot entry %s qty=%.12g avg=%.8f; exchange protection established at %.8f",
            pid, held_qty, avg_price, state.stop_price,
        )
        return state

    def enter(self, row: SignalRow, product: ProductMeta) -> PositionState | None:
        halted, reason = self.risk_halts({product.product_id: product})
        if halted:
            log.warning("Entry blocked: %s", reason)
            return None

        ok, reason, metrics = self.preview_candidate(row, product)
        if not ok:
            log.info("Candidate %s rejected by execution gate: %s", row.product_id, reason)
            return None

        raw_stop = float(self.s.risk["atr_stop_multiple"]) * row.atr_pct
        stop_pct = max(float(self.s.risk["min_stop_pct"]), raw_stop)
        if stop_pct > float(self.s.risk["max_stop_pct"]):
            log.info("Candidate %s rejected: required stop %.2f%% > max", row.product_id, stop_pct * 100)
            return None

        maker_price = float(metrics["maker_limit_price"])
        maker_base = float(metrics["maker_base_size"])
        target_cash = float(metrics["target_cash_usd"])
        fallback_allowed = bool(metrics["fallback_allowed"])
        log.info(
            "Execution plan %s: target=$%.2f maker_limit=%.8f makerRT=%.2f%% takerRT=%.2f%% fallback=%s%s",
            row.product_id,
            target_cash,
            maker_price,
            float(metrics["maker_roundtrip_friction_pct"]) * 100,
            float(metrics["taker_roundtrip_friction_pct"]) * 100,
            "YES" if fallback_allowed else "NO",
            "" if fallback_allowed else f" ({metrics.get('fallback_block_reason', '')})",
        )

        if not self.s.is_live:
            details = {
                "metrics": metrics,
                "stop_pct": stop_pct,
                "plan": "POST_ONLY_MAKER_THEN_CAPPED_FOK_FALLBACK",
                "maker_wait_seconds": int(self.s.execution["maker_wait_seconds"]),
            }
            self.db.log_trade("DRY_ENTRY", row.product_id, "BUY", price=row.price, details=details)
            log.warning(
                "%s mode: would post-only BUY ~%.12g %s at %.8f (~$%.2f), wait %ss; taker fallback=%s; stop %.2f%%",
                self.s.run_mode.upper(),
                maker_base,
                row.product_id,
                maker_price,
                target_cash,
                int(self.s.execution["maker_wait_seconds"]),
                "YES" if fallback_allowed else "NO",
                stop_pct * 100,
            )
            return None

        pending: dict[str, Any] = {
            "product_id": row.product_id,
            "base_currency": product.base_currency,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "stop_pct": stop_pct,
            "target_cash_usd": target_cash,
            "order_ids": [],
            "signal": row.to_dict(),
        }
        self.db.set_json("pending_entry", pending)

        order_ids: list[str] = []
        path_parts: list[str] = []
        maker_spend = 0.0

        # Stage 1: maker-only. Coinbase's post_only flag is the final guard against
        # accidentally turning this order into a taker fill if the book moves.
        maker_resp = self.g.limit_gtc_buy_post_only(
            row.product_id,
            str(metrics["maker_base_size_str"]),
            str(metrics["maker_limit_price_str"]),
            self._uuid("maker-entry"),
        )
        maker_oid = self.g.extract_order_id(maker_resp)
        if maker_oid:
            order_ids.append(maker_oid)
            pending["order_ids"] = order_ids.copy()
            self.db.set_json("pending_entry", pending)
            path_parts.append("MAKER")
            maker_order = self.g.wait_for_fill(maker_oid, int(self.s.execution["maker_wait_seconds"]))
            maker_status = str(maker_order.get("status") or "").upper()
            if maker_status not in {"FILLED", "CANCELLED", "FAILED", "EXPIRED"}:
                self._cancel_entry_order(maker_oid)
            maker_qty, maker_avg, maker_fees = self._aggregate_orders([maker_oid])
            maker_spend = maker_qty * maker_avg + maker_fees if maker_qty > 0 and maker_avg > 0 else 0.0
            fill_fraction = maker_spend / target_cash if target_cash > 0 else 0.0
            log.info(
                "Maker stage %s: filled=$%.2f (%.1f%% of target), qty=%.12g fee=$%.4f",
                row.product_id, maker_spend, fill_fraction * 100, maker_qty, maker_fees,
            )
        else:
            log.info("Post-only maker order for %s was not accepted: %s", row.product_id, self.g.response_error(maker_resp))
            fill_fraction = 0.0

        # Stage 2: only pay taker if the maker stage did not get close enough to target,
        # the signal still covers taker costs, and price has not run away during the wait.
        sufficient = float(self.s.execution["maker_sufficient_fill_fraction"])
        if fill_fraction < sufficient and fallback_allowed and bool(self.s.execution.get("fallback_taker_enabled", True)):
            balances = self.g.balances()
            usd_available = balances.get("USD", {}).get("available", 0.0)
            reserve = float(self.s.execution["min_cash_reserve_usd"])
            remaining_budget = min(max(0.0, target_cash - maker_spend), max(0.0, usd_available - reserve))
            min_fallback = float(self.s.execution["fallback_min_cash_usd"])
            if remaining_budget >= min_fallback:
                bid2, ask2 = self.g.best_bid_ask(row.product_id)
                initial_ask = float(metrics["initial_ask"])
                max_chase = initial_ask * (1.0 + float(self.s.execution["fallback_max_chase_pct"]))
                if bid2 > 0 and ask2 > 0 and ask2 <= max_chase:
                    fallback_quote = round_down(remaining_budget, product.quote_increment)
                    fb_ok, fb_metrics, fb_reason = self._taker_fallback_allowed(row, fallback_quote)
                    if fb_ok:
                        raw_limit = min(
                            ask2 * (1.0 + float(self.s.execution["fallback_limit_buffer_pct"])),
                            max_chase,
                        )
                        limit_price = round_down(raw_limit, product.quote_increment)
                        taker_fee = float(metrics["taker_fee_pct"])
                        base_size = base_size_for_cash(
                            float(fallback_quote),
                            float(limit_price),
                            taker_fee,
                            product.base_increment,
                        )
                        fallback_notional = float(base_size) * float(limit_price)
                        if (
                            float(base_size) >= product.base_min_size
                            and (product.quote_min_size <= 0 or fallback_notional >= product.quote_min_size)
                            and float(base_size) > 0
                        ):
                            resp = self.g.limit_fok_buy(
                                row.product_id,
                                base_size,
                                limit_price,
                                self._uuid("fok-entry"),
                            )
                            oid = self.g.extract_order_id(resp)
                            if oid:
                                order_ids.append(oid)
                                pending["order_ids"] = order_ids.copy()
                                self.db.set_json("pending_entry", pending)
                                path_parts.append("FOK_TAKER")
                                self.g.wait_for_fill(oid, int(self.s.execution["order_fill_wait_seconds"]))
                                log.info(
                                    "Fallback FOK sent for %s: budget=$%.2f limit=%s current_takerRT=%.2f%%",
                                    row.product_id,
                                    float(fallback_quote),
                                    limit_price,
                                    float(fb_metrics.get("taker_roundtrip_friction_pct", 0.0)) * 100,
                                )
                            else:
                                log.info("Fallback FOK for %s was rejected: %s", row.product_id, self.g.response_error(resp))
                        else:
                            log.info("Fallback skipped for %s: remaining order is below product minimums", row.product_id)
                    else:
                        log.info("Fallback skipped for %s: refreshed taker gate failed: %s", row.product_id, fb_reason)
                else:
                    log.info(
                        "Fallback skipped for %s: ask moved from %.8f to %.8f (max chase %.8f)",
                        row.product_id, initial_ask, ask2, max_chase,
                    )
            else:
                log.info("Fallback skipped for %s: only $%.2f remains (< $%.2f threshold)", row.product_id, remaining_budget, min_fallback)
        elif fill_fraction < sufficient and not fallback_allowed:
            log.info("No taker fallback for %s: %s", row.product_id, metrics.get("fallback_block_reason", "cost gate blocked"))

        return self._finalize_entry(
            row,
            product,
            stop_pct,
            order_ids,
            {
                "path": "+".join(path_parts) if path_parts else "NO_FILL",
                "target_cash_usd": target_cash,
                "maker_plan": metrics,
                "maker_spend_usd": maker_spend,
            },
        )

    def _wait_for_entry_holding(
        self,
        currency: str,
        expected_qty: float,
        timeout_seconds: float | None = None,
    ) -> float:
        """Wait for a confirmed buy fill to appear in Coinbase account balances.

        Coinbase order/fill state can lead the account-balance view by a short interval.
        For entry finalisation we treat the confirmed fill as authoritative and wait for the
        corresponding holding instead of clearing crash-recovery state.
        """
        if timeout_seconds is None:
            timeout_seconds = float(self.s.execution.get("entry_balance_settlement_wait_seconds", 20.0))
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        latest = 0.0
        # Allow tiny product-increment / reporting differences while still requiring essentially
        # the complete confirmed fill before protection is sized.
        threshold = max(0.0, expected_qty * 0.999)
        while time.monotonic() < deadline:
            balances = self.g.balances()
            latest = float(balances.get(currency, {}).get("total", 0.0) or 0.0)
            if latest > 0 and (expected_qty <= 0 or latest >= threshold):
                return latest
            time.sleep(0.5)
        return latest

    def _wait_available_base(self, currency: str, timeout_seconds: float = 6.0) -> float:
        """Wait briefly for Coinbase to expose sellable base balance."""
        deadline = time.monotonic() + timeout_seconds
        latest = 0.0
        while time.monotonic() < deadline:
            balances = self.g.balances()
            latest = balances.get(currency, {}).get("available", 0.0)
            if latest > 0:
                return latest
            time.sleep(0.5)
        return latest

    def exit(
        self,
        state: PositionState,
        product: ProductMeta,
        reason: str,
        protection_already_cancelled: bool = False,
    ) -> bool:
        if not self.s.is_live:
            self.db.log_trade("DRY_EXIT", state.product_id, "SELL", state.quantity, details={"reason": reason})
            log.warning("%s mode: would EXIT %s because %s", self.s.run_mode.upper(), state.product_id, reason)
            return False

        old_stop = state.stop_order_id
        if old_stop and not protection_already_cancelled:
            try:
                self.g.cancel(old_stop)
                time.sleep(0.5)
            except Exception as exc:
                log.warning("Stop cancel returned an error before exit: %s", exc)

        qty = self._wait_available_base(state.base_currency)
        base_size = round_down(qty, product.base_increment)
        if float(base_size) <= 0:
            reconciled = self.reconcile()
            return reconciled is None

        # Exits prioritize certainty over maker-fee savings. Trend failure, kill-switch, and
        # protection recovery are precisely the cases where waiting on a passive sell is risky.
        preview = self.g.preview_market_sell(state.product_id, base_size)
        if preview.get("errs"):
            raise RuntimeError(f"Exit preview failed: {preview.get('errs')}")
        resp = self.g.market_sell(state.product_id, base_size, self._uuid("exit"))
        oid = self.g.extract_order_id(resp)
        if not oid:
            state.stop_order_id = None
            try:
                self.ensure_protection(state, product)
            except Exception as restore_exc:
                log.critical("Exit failed AND protection restoration failed: %s", restore_exc)
            raise RuntimeError(f"Exit order rejected: {self.g.response_error(resp)}")

        order = self.g.wait_for_fill(oid, int(self.s.execution["order_fill_wait_seconds"]))
        fills = self.g.fills(order_id=oid)
        sold_qty, avg_price, fees = self.g.fill_summary(fills)
        if sold_qty <= 0:
            sold_qty = _f(order.get("filled_size"), float(base_size))
            avg_price = _f(order.get("average_filled_price"))
            fees = _f(order.get("total_fees"))
        if avg_price <= 0:
            avg_price = _f(self.g.get_product(state.product_id).get("price"))
        pnl = (avg_price - state.entry_price) * sold_qty - state.entry_fees - fees
        self.db.log_trade(
            "EXIT_FILLED", state.product_id, "SELL", sold_qty, avg_price, fees, pnl, oid,
            {"reason": reason, "entry_price": state.entry_price},
        )
        self.db.close_trade_ledger(
            product_id=state.product_id,
            exit_ts=self._latest_fill_time_iso(fills),
            exit_price=avg_price,
            exit_quantity=sold_qty,
            exit_fee=fees,
            exit_order_id=oid,
            exit_reason=reason,
        )
        self.notifier.exit(
            product_id=state.product_id,
            price=avg_price,
            reason=reason,
            gross_return=((avg_price / state.entry_price) - 1.0) if avg_price and state.entry_price else None,
            realized_pnl=pnl,
        )
        self.db.set_position(None)
        if "STOP" in reason.upper():
            self.db.set_cooldown(
                state.product_id,
                int(self.s.risk["stopped_token_cooldown_hours"]),
                reason,
            )
        log.info("EXIT filled %s qty=%.12g avg=%.8f P&L=$%.4f reason=%s", state.product_id, sold_qty, avg_price, pnl, reason)
        return True

    def trail_stop(self, state: PositionState, row: SignalRow, product: ProductMeta) -> PositionState:
        # ``highest_close`` is the persisted sampled high-water mark (current sell-side mark
        # in the live 5-minute monitor). It is intentionally monotonic across restarts.
        proposed, lock, high_water = self._proposed_trailing_stop(state, row)
        state.highest_close = high_water
        self.db.update_trade_excursions(state.product_id, high_water)

        # Keep the existing anti-churn threshold: Coinbase stop replacement temporarily
        # releases/relocks the base asset, so tiny stop raises are not worth extra order churn.
        if proposed <= state.stop_price * 1.002:
            self.db.set_position(state)
            return state

        # A desired capture floor can be above the current mark after a restart/deferred
        # replacement. Never submit a fresh stop trigger above market here. Runner-level
        # sell-side backstops handle the already-breached floor with an urgent exit instead.
        if proposed >= row.price:
            self.db.set_position(state)
            log.warning(
                "Desired trailing stop %.8f is at/above current mark %.8f for %s; "
                "leaving existing protection in place for profit-lock backstop handling.",
                proposed,
                row.price,
                state.product_id,
            )
            return state

        if not self.s.is_live:
            self.db.set_position(state)
            log.info(
                "%s mode: would trail %s stop to %.8f (MFE=%+.2f%% lock=%s captureFloor=%+.2f%%)",
                self.s.run_mode.upper(),
                state.product_id,
                proposed,
                float(lock["mfe_return"]) * 100,
                "ON" if bool(lock["armed"]) else "OFF",
                float(lock["protected_return"]) * 100,
            )
            return state

        old_price = state.stop_price
        old_oid = state.stop_order_id
        if old_oid:
            self.g.cancel(old_oid)
            released = self._wait_available_base(state.base_currency)
            if released <= 0:
                raise RuntimeError("Cancelled prior stop, but Coinbase did not release sellable base balance in time")
        state.stop_order_id = None
        state.stop_price = proposed
        self.db.set_position(state)
        try:
            state = self.ensure_protection(state, product)
            limit_raw = self._protective_limit_raw(state)
            log.info(
                "Trailed %s stop %.8f -> %.8f (limitFloorAware=%.8f MFE=%+.2f%% lock=%s captureFloor=%+.2f%%)",
                state.product_id,
                old_price,
                proposed,
                limit_raw,
                float(lock["mfe_return"]) * 100,
                "ON" if bool(lock["armed"]) else "OFF",
                float(lock["protected_return"]) * 100,
            )
        except Exception as exc:
            log.error("New trailing stop placement failed; attempting to restore old protection: %s", exc)
            state.stop_price = old_price
            state.stop_order_id = None
            self.db.set_position(state)
            try:
                state = self.ensure_protection(state, product)
            except Exception as restore_exc:
                log.critical("Could not restore protection: %s. Emergency market exit.", restore_exc)
                self.exit(state, product, reason="PROTECTION_REPLACEMENT_FAILED", protection_already_cancelled=True)
                raise
        return state
