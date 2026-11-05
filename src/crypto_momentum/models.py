from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any


@dataclass
class Candle:
    start: int
    low: float
    high: float
    open: float
    close: float
    volume: float


@dataclass
class ProductMeta:
    product_id: str
    base_currency: str
    quote_currency: str
    price: float
    volume_24h_base: float
    quote_volume_24h: float
    base_increment: str
    quote_increment: str
    base_min_size: float
    quote_min_size: float
    best_bid: float = 0.0
    best_ask: float = 0.0
    spread_pct: float = 0.0
    new_at: str | None = None


@dataclass
class SignalRow:
    product_id: str
    price: float
    r24: float
    r72: float
    r168: float
    r336: float
    vol72: float
    ema_fast: float
    ema_slow: float
    ema_slope: float
    atr: float
    atr_pct: float
    one_hour_return: float
    one_hour_sigma: float
    distance_atr: float
    breakout: bool
    trend_pass: bool
    raw_score: float = 0.0
    percentile: float = 0.0
    rank: int = 0
    breadth_member: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PositionState:
    product_id: str
    base_currency: str
    quantity: float
    entry_price: float
    entry_time: str
    initial_stop_price: float
    stop_price: float
    stop_order_id: str | None
    highest_close: float
    initial_risk_pct: float
    entry_fees: float = 0.0
    entry_order_id: str | None = None
    # V1.4.12 sampled low-water mark for MAE analytics. Legacy positions deserialize
    # with 0.0 and are normalized to entry price before the first new observation.
    lowest_close: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
