from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP


def round_down(value: float | str, increment: str) -> str:
    inc = Decimal(str(increment))
    if inc <= 0:
        return str(value)
    val = Decimal(str(value))
    units = (val / inc).to_integral_value(rounding=ROUND_DOWN)
    result = units * inc
    places = max(0, -inc.as_tuple().exponent)
    return f"{result:.{places}f}"


def round_up(value: float | str, increment: str) -> str:
    inc = Decimal(str(increment))
    if inc <= 0:
        return str(value)
    val = Decimal(str(value))
    units = (val / inc).to_integral_value(rounding=ROUND_UP)
    result = units * inc
    places = max(0, -inc.as_tuple().exponent)
    return f"{result:.{places}f}"


def maker_buy_limit_price(
    bid: float,
    ask: float,
    quote_increment: str,
    inside_spread_fraction: float = 0.50,
) -> str:
    """Return a buy price intended to remain post-only while improving fill odds.

    The price starts at the best bid and may improve part-way into the spread, but
    it is always capped at least one quote tick below the current ask. Coinbase's
    post_only flag remains the final authority: if the book moves and the order
    would take liquidity, Coinbase can reject the order rather than charge taker.
    """
    if bid <= 0 or ask <= 0 or ask <= bid:
        raise ValueError("maker_buy_limit_price requires 0 < bid < ask")
    inc = Decimal(str(quote_increment))
    if inc <= 0:
        raise ValueError("quote_increment must be positive")
    b = Decimal(str(bid))
    a = Decimal(str(ask))
    frac = Decimal(str(max(0.0, min(float(inside_spread_fraction), 1.0))))
    candidate = b + (a - b) * frac
    max_post = a - inc
    candidate = min(candidate, max_post)
    candidate = max(candidate, b)
    units = (candidate / inc).to_integral_value(rounding=ROUND_DOWN)
    price = units * inc
    # Defensive cap in case the market's supplied bid/ask were not tick-aligned.
    if price >= a:
        price = ((a - inc) / inc).to_integral_value(rounding=ROUND_DOWN) * inc
    if price <= 0:
        raise ValueError("computed maker price is non-positive")
    places = max(0, -inc.as_tuple().exponent)
    return f"{price:.{places}f}"


def base_size_for_cash(
    cash_budget: float,
    price: float,
    fee_rate: float,
    base_increment: str,
) -> str:
    """Size a buy so worst-case price*size plus its fee stays inside cash_budget."""
    if cash_budget <= 0 or price <= 0:
        return round_down(0, base_increment)
    gross_per_unit = price * (1.0 + max(0.0, fee_rate))
    return round_down(cash_budget / gross_per_unit, base_increment)


def completed_bucket_window(
    bucket_seconds: int,
    *,
    now: datetime | None = None,
    requested_buckets: int = 349,
    safe_max_buckets: int = 349,
) -> tuple[int, int, int]:
    """Return an aligned [start, end) window containing completed candles only.

    Coinbase production has historically rejected ranges spanning the documented 350-bucket
    ceiling, so the bot deliberately caps requests at 349 buckets for every granularity.
    """
    if bucket_seconds <= 0:
        raise ValueError("bucket_seconds must be positive")
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    buckets = max(1, min(int(requested_buckets), int(safe_max_buckets)))
    end = int(now.timestamp()) // int(bucket_seconds) * int(bucket_seconds)
    start = end - buckets * int(bucket_seconds)
    return start, end, buckets


def completed_hour_window(now: datetime | None = None, requested_hours: int = 349) -> tuple[int, int, int]:
    """Return an hour-aligned [start, end) window containing only completed 1h bars.

    Coinbase's docs currently say 350 is the maximum candle count, but production
    has returned INVALID_ARGUMENT when the start/end interval spans 350 or more
    buckets. 349 is therefore the deliberately conservative live-safe cap.
    """
    return completed_bucket_window(
        3600, now=now, requested_buckets=requested_hours, safe_max_buckets=349
    )
