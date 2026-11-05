from __future__ import annotations

import math
import numpy as np

from .models import Candle, SignalRow


def ema(values: np.ndarray, span: int) -> np.ndarray:
    if len(values) == 0:
        return np.array([], dtype=float)
    alpha = 2.0 / (span + 1.0)
    out = np.empty_like(values, dtype=float)
    out[0] = values[0]
    for i in range(1, len(values)):
        out[i] = alpha * values[i] + (1.0 - alpha) * out[i - 1]
    return out


def atr(candles: list[Candle], period: int) -> float:
    if len(candles) < period + 1:
        return float("nan")
    highs = np.array([c.high for c in candles], dtype=float)
    lows = np.array([c.low for c in candles], dtype=float)
    closes = np.array([c.close for c in candles], dtype=float)
    prev = closes[:-1]
    tr = np.maximum.reduce([
        highs[1:] - lows[1:],
        np.abs(highs[1:] - prev),
        np.abs(lows[1:] - prev),
    ])
    return float(np.mean(tr[-period:]))


def safe_return(current: float, past: float) -> float:
    if current <= 0 or past <= 0:
        return float("nan")
    return current / past - 1.0


def _build_signal_row_generic(
    product_id: str,
    candles: list[Candle],
    *,
    lookbacks: list[int],
    weights: list[float],
    fast_span: int,
    slow_span: int,
    slope_lookback: int,
    atr_period: int,
    vol_window: int,
    breakout_bars: int,
    current_price: float | None = None,
) -> SignalRow | None:
    """Build a signal row where all lookbacks/spans are expressed in candle bars."""
    need = max(lookbacks) + 1
    if len(candles) < need:
        return None

    closes = np.array([c.close for c in candles], dtype=float)
    highs = np.array([c.high for c in candles], dtype=float)
    if np.any(closes <= 0) or not np.all(np.isfinite(closes)):
        return None

    completed_price = float(closes[-1])
    price = float(current_price) if current_price is not None and current_price > 0 else completed_price

    returns = {lb: safe_return(price, float(closes[-1-lb])) for lb in lookbacks}
    if any(not math.isfinite(v) for v in returns.values()):
        return None

    fast_arr = ema(closes, int(fast_span))
    slow_arr = ema(closes, int(slow_span))
    ema_fast = float(fast_arr[-1])
    ema_slow = float(slow_arr[-1])
    ema_slope = (
        ema_slow / float(slow_arr[-1-int(slope_lookback)]) - 1.0
        if len(slow_arr) > int(slope_lookback) else 0.0
    )

    bar_returns = np.diff(np.log(closes))
    vol72 = float(np.std(bar_returns[-int(vol_window):], ddof=1)) if len(bar_returns) >= int(vol_window) else float("nan")

    completed_bar_return = safe_return(completed_price, float(closes[-2]))
    if current_price is not None:
        developing_return = safe_return(price, completed_price)
        one_hour_return = max(completed_bar_return, developing_return)
    else:
        one_hour_return = completed_bar_return
    one_hour_sigma = one_hour_return / vol72 if vol72 > 1e-12 else 0.0

    atr_val = atr(candles, int(atr_period))
    atr_pct = atr_val / price if price > 0 else float("nan")
    distance_atr = (price - ema_fast) / atr_val if atr_val > 1e-12 else 0.0

    if current_price is not None:
        prior_high = float(np.max(highs[-int(breakout_bars):]))
    else:
        prior_high = float(np.max(highs[-1-int(breakout_bars):-1]))
    breakout = price > prior_high

    trend_pass = (
        all(v > 0 for v in returns.values())
        and price > ema_fast > ema_slow
        and ema_slope > 0
    )

    vol_floor = max(vol72, 0.001)
    risk_adjusted = [returns[lb] / (vol_floor * math.sqrt(lb)) for lb in lookbacks]
    raw_score = float(sum(w * x for w, x in zip(weights, risk_adjusted)))

    # SignalRow retains the historical r24/r72/r168/r336 field names for compatibility.
    # For the 15m shadow strategy those fields represent 24/72/168/336 *bars*, i.e.
    # approximately 6h/18h/42h/84h. Shadow logs label them with wall-clock horizons.
    return SignalRow(
        product_id=product_id,
        price=price,
        r24=returns[lookbacks[0]],
        r72=returns[lookbacks[1]],
        r168=returns[lookbacks[2]],
        r336=returns[lookbacks[3]],
        vol72=vol72,
        ema_fast=ema_fast,
        ema_slow=ema_slow,
        ema_slope=ema_slope,
        atr=atr_val,
        atr_pct=atr_pct,
        one_hour_return=one_hour_return,
        one_hour_sigma=one_hour_sigma,
        distance_atr=distance_atr,
        breakout=breakout,
        trend_pass=trend_pass,
        raw_score=raw_score,
        breadth_member=price > ema_slow,
    )


def build_signal_row(
    product_id: str,
    candles: list[Candle],
    cfg: dict,
    *,
    current_price: float | None = None,
) -> SignalRow | None:
    """Build the production multi-horizon signal from completed 1h candles.

    `current_price` does not mutate the completed-candle EMA/ATR/volatility history. It
    overlays a fresh tradable mark for returns, trend confirmation, breakout, extension,
    and distance, allowing the 15-minute decision cadence to retain hourly structure.
    """
    return _build_signal_row_generic(
        product_id,
        candles,
        lookbacks=list(cfg["lookbacks_hours"]),
        weights=list(cfg["weights"]),
        fast_span=int(cfg["ema_fast_hours"]),
        slow_span=int(cfg["ema_slow_hours"]),
        slope_lookback=int(cfg["ema_slope_lookback_hours"]),
        atr_period=int(cfg["atr_hours"]),
        vol_window=int(cfg["realized_vol_hours"]),
        breakout_bars=int(cfg["breakout_hours"]),
        current_price=current_price,
    )


def build_signal_row_15m(
    product_id: str,
    candles: list[Candle],
    cfg: dict,
    *,
    current_price: float | None = None,
) -> SignalRow | None:
    """Build the observation-only true 15-minute variant.

    Numeric periods are interpreted as 15-minute bars, deliberately making this a genuinely
    faster experimental strategy rather than another hourly strategy with more frequent polling.
    No caller of this function has order-execution authority.
    """
    return _build_signal_row_generic(
        product_id,
        candles,
        lookbacks=list(cfg["lookback_bars"]),
        weights=list(cfg["weights"]),
        fast_span=int(cfg["ema_fast_bars"]),
        slow_span=int(cfg["ema_slow_bars"]),
        slope_lookback=int(cfg["ema_slope_lookback_bars"]),
        atr_period=int(cfg["atr_bars"]),
        vol_window=int(cfg["realized_vol_bars"]),
        breakout_bars=int(cfg["breakout_bars"]),
        current_price=current_price,
    )


def assign_percentiles(rows: list[SignalRow]) -> list[SignalRow]:
    if not rows:
        return rows
    ordered = sorted(rows, key=lambda r: r.raw_score)
    n = len(ordered)
    if n == 1:
        ordered[0].percentile = 1.0
    else:
        for i, r in enumerate(ordered):
            r.percentile = i / (n - 1)
    ranked = sorted(rows, key=lambda r: r.raw_score, reverse=True)
    for i, r in enumerate(ranked, start=1):
        r.rank = i
    return ranked
