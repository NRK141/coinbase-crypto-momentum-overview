from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Callable

from coinbase.rest import RESTClient

from .models import Candle, ProductMeta
from .utils import round_down, completed_hour_window, completed_bucket_window

log = logging.getLogger(__name__)


def as_dict(obj: Any) -> dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    try:
        return dict(obj)
    except Exception:
        return vars(obj) if hasattr(obj, "__dict__") else {}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default




class CoinbaseGateway:
    def __init__(self, api_key: str, api_secret: str, execution_cfg: dict[str, Any]):
        if not api_key or not api_secret:
            raise RuntimeError("COINBASE_API_KEY and COINBASE_API_SECRET must be configured in .env")
        self.client = RESTClient(api_key=api_key, api_secret=api_secret)
        self.retry_attempts = int(execution_cfg["api_retry_attempts"])
        self.retry_base = float(execution_cfg["api_retry_base_seconds"])
        self.spacing = float(execution_cfg["request_spacing_seconds"])
        self._last_request = 0.0

    def _paced(self) -> None:
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.spacing:
            time.sleep(self.spacing - elapsed)

    @staticmethod
    def _non_retryable(exc: Exception) -> bool:
        """Do not waste time retrying deterministic client/request failures."""
        text = str(exc).upper()
        deterministic = (
            "INVALID_ARGUMENT",
            "NOT_FOUND",
            "PERMISSION_DENIED",
            "400 CLIENT ERROR",
            "403 CLIENT ERROR",
            "404 CLIENT ERROR",
        )
        return any(token in text for token in deterministic)

    def call(self, fn: Callable, *args, **kwargs):
        last_exc: Exception | None = None
        for attempt in range(self.retry_attempts):
            try:
                self._paced()
                out = fn(*args, **kwargs)
                self._last_request = time.monotonic()
                return out
            except Exception as exc:  # SDK raises several HTTP / auth exception types across versions
                last_exc = exc
                if self._non_retryable(exc):
                    raise RuntimeError(f"Coinbase API request rejected: {exc}") from exc
                sleep_for = self.retry_base * (2 ** attempt)
                log.warning("Coinbase API call failed (%s/%s): %s; retrying in %.2fs",
                            attempt + 1, self.retry_attempts, exc, sleep_for)
                time.sleep(sleep_for)
        raise RuntimeError(f"Coinbase API call failed after {self.retry_attempts} attempts: {last_exc}") from last_exc

    def server_time(self) -> dict[str, Any]:
        return as_dict(self.call(self.client.get_unix_time))

    def accounts(self) -> list[dict[str, Any]]:
        # A dedicated strategy portfolio should be small; 250 comfortably covers its currencies.
        payload = as_dict(self.call(self.client.get_accounts, limit=250))
        return list(payload.get("accounts", []))

    def balances(self) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for acct in self.accounts():
            currency = str(acct.get("currency") or "").upper()
            if not currency:
                continue
            available = _f((acct.get("available_balance") or {}).get("value"))
            hold = _f((acct.get("hold") or {}).get("value"))
            out[currency] = {
                "available": available,
                "hold": hold,
                "total": available + hold,
            }
        return out

    def products(self) -> list[dict[str, Any]]:
        """Return the complete Coinbase SPOT catalog visible to this API key.

        V1.4.8 explicitly paginates List Products instead of assuming one 250-row response is
        complete. The pinned Coinbase SDK exposes the endpoint's ``offset`` parameter directly,
        so pagination uses deterministic offsets and de-duplicates product ids defensively.
        """
        page_limit = 250
        offset = 0
        seen_ids: set[str] = set()
        products: list[dict[str, Any]] = []
        pages = 0
        reported_num_products = 0
        pagination_complete = False
        max_pages = 20

        while pages < max_pages:
            payload = as_dict(self.call(
                self.client.get_products,
                limit=page_limit,
                offset=offset,
                product_type="SPOT",
                get_tradability_status=True,
                # product_type=SPOT is the intended scope. Coinbase documents
                # get_all_products=True as broadening the response to all product types.
                get_all_products=False,
            ))
            page = [as_dict(x) for x in payload.get("products", [])]
            pages += 1
            try:
                reported_num_products = max(reported_num_products, int(payload.get("num_products") or 0))
            except (TypeError, ValueError):
                pass

            new_unique = 0
            for product in page:
                pid = str(product.get("product_id") or "")
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)
                products.append(product)
                new_unique += 1

            pagination = as_dict(payload.get("pagination"))
            has_next = bool(pagination.get("has_next"))

            if has_next:
                if not page or new_unique == 0:
                    pagination_complete = False
                    break
                offset += len(page)
                continue

            # If pagination metadata is absent but the page is exactly full, probe the next
            # offset rather than silently assuming that the first page was complete.
            if (not pagination) and len(page) >= page_limit and new_unique > 0:
                offset += len(page)
                continue

            pagination_complete = True
            break

        if pages >= max_pages and not pagination_complete:
            log.warning("Coinbase product catalog pagination hit the %s-page safety cap", max_pages)

        self._last_product_catalog_info = {
            "pages": pages,
            "unique_products": len(products),
            "reported_num_products": reported_num_products,
            "pagination_complete": pagination_complete,
            "pagination_mode": "offset",
            "page_limit": page_limit,
        }
        if not pagination_complete:
            log.warning(
                "Coinbase product catalog completeness could not be proven: pages=%s unique=%s reported=%s",
                pages, len(products), reported_num_products,
            )
        elif pages > 1:
            log.info(
                "Coinbase product catalog paginated: pages=%s unique=%s reported=%s mode=offset",
                pages, len(products), reported_num_products,
            )
        return products

    def product_catalog_info(self) -> dict[str, Any]:
        return dict(getattr(self, "_last_product_catalog_info", {}) or {})

    @staticmethod
    def product_meta(p: dict[str, Any]) -> ProductMeta | None:
        pid = str(p.get("product_id") or "")
        if not pid or "-" not in pid:
            return None
        base = str(p.get("base_currency_id") or p.get("base_currency") or pid.split("-")[0]).upper()
        quote = str(p.get("quote_currency_id") or p.get("quote_currency") or pid.split("-")[-1]).upper()
        price = _f(p.get("price"))
        volume = _f(p.get("volume_24h"))
        approximate_quote_volume = _f(p.get("approximate_quote_24h_volume"))
        bid = _f(p.get("best_bid_price") or p.get("best_bid") or p.get("bid"))
        ask = _f(p.get("best_ask_price") or p.get("best_ask") or p.get("ask"))
        spread_pct = 0.0
        if bid > 0 and ask > bid:
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid if mid > 0 else 0.0
        if price <= 0:
            return None
        return ProductMeta(
            product_id=pid,
            base_currency=base,
            quote_currency=quote,
            price=price,
            volume_24h_base=volume,
            quote_volume_24h=approximate_quote_volume if approximate_quote_volume > 0 else price * volume,
            base_increment=str(p.get("base_increment") or "0.00000001"),
            quote_increment=str(p.get("quote_increment") or "0.01"),
            base_min_size=_f(p.get("base_min_size"), 0.0),
            quote_min_size=_f(p.get("quote_min_size"), 0.0),
            best_bid=bid,
            best_ask=ask,
            spread_pct=spread_pct,
            new_at=str(p.get("new_at")) if p.get("new_at") else None,
        )

    def get_product(self, product_id: str) -> dict[str, Any]:
        return as_dict(self.call(self.client.get_product, product_id, get_tradability_status=True))

    def best_bid_asks(self, product_ids: list[str] | None = None) -> dict[str, tuple[float, float]]:
        """Return current top-of-book quotes in one Coinbase request where possible."""
        kwargs = {} if not product_ids else {"product_ids": product_ids}
        payload = as_dict(self.call(self.client.get_best_bid_ask, **kwargs))
        out: dict[str, tuple[float, float]] = {}
        for book in payload.get("pricebooks", []):
            pid = str(book.get("product_id") or "")
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            if not pid or not bids or not asks:
                continue
            bid = _f((bids[0] or {}).get("price"))
            ask = _f((asks[0] or {}).get("price"))
            if bid > 0 and ask > bid:
                out[pid] = (bid, ask)
        return out

    def _candles_by_granularity(
        self,
        product_id: str,
        *,
        bucket_seconds: int,
        granularity: str,
        requested_buckets: int = 349,
    ) -> list[Candle]:
        # Keep every request below Coinbase's practical 350-bucket boundary and exclude the
        # currently forming candle. This helper is shared by the live 1h engine and the
        # observation-only 15m shadow engine.
        start_ts, end_ts, bucket_count = completed_bucket_window(
            bucket_seconds, requested_buckets=requested_buckets, safe_max_buckets=349
        )
        payload = as_dict(self.call(
            self.client.get_candles,
            product_id=product_id,
            start=str(start_ts),
            end=str(end_ts),
            granularity=granularity,
            limit=bucket_count,
        ))
        candles: list[Candle] = []
        for c in payload.get("candles", []):
            try:
                candles.append(Candle(
                    start=int(c["start"]),
                    low=float(c["low"]),
                    high=float(c["high"]),
                    open=float(c["open"]),
                    close=float(c["close"]),
                    volume=float(c.get("volume", 0)),
                ))
            except (KeyError, TypeError, ValueError):
                continue
        candles.sort(key=lambda x: x.start)
        candles = [c for c in candles if c.start + bucket_seconds <= end_ts]
        return candles[-bucket_count:]

    def candles(self, product_id: str, hours: int = 349) -> list[Candle]:
        # Production currently rejects a start/end interval spanning >=350 buckets even though
        # the public docs describe 350 as the max limit. Use 349 completed, hour-aligned bars.
        return self._candles_by_granularity(
            product_id, bucket_seconds=3600, granularity="ONE_HOUR", requested_buckets=hours
        )

    def candles_15m(self, product_id: str, bars: int = 349) -> list[Candle]:
        """Return completed 15-minute candles for the observation-only shadow strategy."""
        return self._candles_by_granularity(
            product_id, bucket_seconds=900, granularity="FIFTEEN_MINUTE", requested_buckets=bars
        )

    def best_bid_ask(self, product_id: str) -> tuple[float, float]:
        # First try product snapshot, which usually exposes best bid/ask.
        p = self.get_product(product_id)
        bid = _f(p.get("best_bid_price") or p.get("best_bid") or p.get("bid"))
        ask = _f(p.get("best_ask_price") or p.get("best_ask") or p.get("ask"))
        if bid > 0 and ask > 0:
            return bid, ask

        payload = as_dict(self.call(self.client.get_best_bid_ask, product_ids=[product_id]))
        pricebooks = payload.get("pricebooks", [])
        if pricebooks:
            book = pricebooks[0]
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if bids and asks:
                return _f(bids[0].get("price")), _f(asks[0].get("price"))
        return 0.0, 0.0

    def fee_rates(self) -> tuple[float, float]:
        payload = as_dict(self.call(self.client.get_transaction_summary, product_type="SPOT"))
        tier = payload.get("fee_tier") or {}
        return _f(tier.get("maker_fee_rate")), _f(tier.get("taker_fee_rate"))

    def preview_market_buy(self, product_id: str, quote_size: str) -> dict[str, Any]:
        return as_dict(self.call(self.client.preview_market_order_buy, product_id=product_id, quote_size=quote_size))

    def preview_market_sell(self, product_id: str, base_size: str) -> dict[str, Any]:
        return as_dict(self.call(self.client.preview_market_order_sell, product_id=product_id, base_size=base_size))

    def market_buy(self, product_id: str, quote_size: str, client_order_id: str) -> dict[str, Any]:
        return as_dict(self.call(
            self.client.market_order_buy,
            client_order_id=client_order_id,
            product_id=product_id,
            quote_size=quote_size,
        ))

    def limit_gtc_buy_post_only(
        self,
        product_id: str,
        base_size: str,
        limit_price: str,
        client_order_id: str,
    ) -> dict[str, Any]:
        return as_dict(self.call(
            self.client.limit_order_gtc_buy,
            client_order_id=client_order_id,
            product_id=product_id,
            base_size=base_size,
            limit_price=limit_price,
            post_only=True,
        ))

    def limit_fok_buy(
        self,
        product_id: str,
        base_size: str,
        limit_price: str,
        client_order_id: str,
    ) -> dict[str, Any]:
        return as_dict(self.call(
            self.client.limit_order_fok_buy,
            client_order_id=client_order_id,
            product_id=product_id,
            base_size=base_size,
            limit_price=limit_price,
        ))

    def market_sell(self, product_id: str, base_size: str, client_order_id: str) -> dict[str, Any]:
        return as_dict(self.call(
            self.client.market_order_sell,
            client_order_id=client_order_id,
            product_id=product_id,
            base_size=base_size,
        ))

    def place_stop_limit_sell(
        self,
        product_id: str,
        base_size: str,
        stop_price: str,
        limit_price: str,
        client_order_id: str,
    ) -> dict[str, Any]:
        return as_dict(self.call(
            self.client.stop_limit_order_gtc_sell,
            client_order_id=client_order_id,
            product_id=product_id,
            base_size=base_size,
            limit_price=limit_price,
            stop_price=stop_price,
            stop_direction="STOP_DIRECTION_STOP_DOWN",
        ))

    def cancel(self, order_id: str) -> dict[str, Any]:
        return as_dict(self.call(self.client.cancel_orders, order_ids=[order_id]))

    def order(self, order_id: str) -> dict[str, Any]:
        return as_dict(self.call(self.client.get_order, order_id=order_id))

    def open_orders(self, product_id: str | None = None) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {"order_status": ["OPEN"], "limit": 100}
        if product_id:
            kwargs["product_ids"] = [product_id]
        payload = as_dict(self.call(self.client.list_orders, **kwargs))
        return list(payload.get("orders", []))

    def fills(self, product_id: str | None = None, order_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {"limit": limit}
        if product_id:
            kwargs["product_ids"] = [product_id]
        if order_id:
            kwargs["order_ids"] = [order_id]
        payload = as_dict(self.call(self.client.get_fills, **kwargs))
        return list(payload.get("fills", []))

    def wait_for_fill(self, order_id: str, timeout_seconds: int) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            payload = self.order(order_id)
            last = payload.get("order", payload)
            status = str(last.get("status") or "").upper()
            if status in {"FILLED", "CANCELLED", "FAILED", "EXPIRED"}:
                return last
            time.sleep(0.5)
        return last

    @staticmethod
    def extract_order_id(response: dict[str, Any]) -> str | None:
        if response.get("success") is False:
            return None
        sr = response.get("success_response") or {}
        return sr.get("order_id") or response.get("order_id")

    @staticmethod
    def response_error(response: dict[str, Any]) -> str:
        if response.get("success") is False:
            return str(response.get("error_response") or response)
        return ""

    @staticmethod
    def fill_summary(fills: list[dict[str, Any]]) -> tuple[float, float, float]:
        """Return (base_qty, weighted_price, total_commission)."""
        qty = 0.0
        notional = 0.0
        fees = 0.0
        for f in fills:
            q = _f(f.get("size"))
            p = _f(f.get("price"))
            fee = _f(f.get("commission"))
            qty += q
            notional += q * p
            fees += fee
        return qty, (notional / qty if qty > 0 else 0.0), fees

    @staticmethod
    def preview_metrics(preview: dict[str, Any]) -> dict[str, float | list]:
        bid = _f(preview.get("best_bid"))
        ask = _f(preview.get("best_ask"))
        avg = _f(preview.get("est_average_filled_price"))
        quote = _f(preview.get("quote_size"))
        commission = _f(preview.get("commission_total"))
        spread = 0.0
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            spread = (ask - bid) / mid if mid > 0 else 0.0
        slippage = 0.0
        if ask > 0 and avg > 0:
            slippage = max(0.0, avg / ask - 1.0)
        return {
            "best_bid": bid,
            "best_ask": ask,
            "estimated_fill": avg,
            "spread_pct": spread,
            "slippage_pct": slippage,
            "commission": commission,
            "commission_pct": commission / quote if quote > 0 else 0.0,
            "errs": preview.get("errs") or [],
            "warnings": preview.get("warning") or preview.get("warnings") or [],
        }
