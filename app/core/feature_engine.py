from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Deque, Tuple


@dataclass
class FeatureVector:
    price: float
    spread: float
    btc_gap: float
    btc_velocity: float
    btc_volatility: float
    momentum_score: float
    price_change_rate: float
    orderbook_imbalance: float
    time_left: int
    time_bucket: str
    regime: str
    price_bucket: str
    btc_gap_bucket: str
    price_time: float
    gap_velocity: float

    def ml_row(self) -> dict[str, float]:
        return {
            "price": self.price,
            "spread": self.spread,
            "btc_gap": self.btc_gap,
            "btc_velocity": self.btc_velocity,
            "btc_volatility": self.btc_volatility,
            "momentum_score": self.momentum_score,
            "price_change_rate": self.price_change_rate,
            "orderbook_imbalance": self.orderbook_imbalance,
            "time_left": float(self.time_left),
            "regime_volatile": 1.0 if self.regime == "volatile" else 0.0,
            "price_time": self.price_time,
            "gap_velocity": self.gap_velocity,
        }

    def meta_row(self, decision_rule: bool, decision_ml: bool, prob_ml: float) -> dict[str, float]:
        row = self.ml_row()
        row["prob_ml"] = prob_ml
        row["decision_rule"] = 1.0 if decision_rule else 0.0
        row["decision_ml"] = 1.0 if decision_ml else 0.0
        return row


class FeatureEngine:
    def __init__(self) -> None:
        self._btc_history: Deque[Tuple[datetime, float]] = deque()
        self._market_price_history: Deque[Tuple[datetime, float]] = deque()

    def build(
        self,
        *,
        market_price: float,
        bid: float,
        ask: float,
        time_left_seconds: int,
        orderbook_imbalance: float,
        btc_price: float,
        now: datetime | None = None,
    ) -> FeatureVector:
        now = now or datetime.now(timezone.utc)
        self._append_history(now, market_price, btc_price)

        spread = max(ask - bid, 0.0)
        btc_gap = self._pct_change_since(self._btc_history, now, seconds=300)
        btc_velocity = self._pct_change_since(self._btc_history, now, seconds=5)
        btc_volatility = self._range_over_mean(self._btc_history, now, seconds=60)
        price_change_rate = self._pct_change_since(self._market_price_history, now, seconds=15)

        momentum_score = (0.55 * btc_velocity) + (0.30 * price_change_rate) + (0.15 * orderbook_imbalance)
        regime = "volatile" if (btc_volatility > 0.003 or abs(btc_velocity) > 0.0015) else "trending"
        time_bucket = self._time_bucket(time_left_seconds)

        price_bucket = self._bucketize(market_price, 0.02, 0.0, 1.0)
        btc_gap_bucket = self._bucketize(btc_gap, 0.0025, -0.05, 0.05)
        price_time = market_price * float(time_left_seconds)
        gap_velocity = btc_gap * btc_velocity

        return FeatureVector(
            price=market_price,
            spread=spread,
            btc_gap=btc_gap,
            btc_velocity=btc_velocity,
            btc_volatility=btc_volatility,
            momentum_score=momentum_score,
            price_change_rate=price_change_rate,
            orderbook_imbalance=orderbook_imbalance,
            time_left=time_left_seconds,
            time_bucket=time_bucket,
            regime=regime,
            price_bucket=price_bucket,
            btc_gap_bucket=btc_gap_bucket,
            price_time=price_time,
            gap_velocity=gap_velocity,
        )

    def _append_history(self, now: datetime, market_price: float, btc_price: float) -> None:
        self._btc_history.append((now, btc_price))
        self._market_price_history.append((now, market_price))
        self._trim_deque(self._btc_history, now, keep_seconds=900)
        self._trim_deque(self._market_price_history, now, keep_seconds=900)

    @staticmethod
    def _trim_deque(items: Deque[Tuple[datetime, float]], now: datetime, keep_seconds: int) -> None:
        cutoff = now - timedelta(seconds=keep_seconds)
        while items and items[0][0] < cutoff:
            items.popleft()

    @staticmethod
    def _pct_change_since(history: Deque[Tuple[datetime, float]], now: datetime, seconds: int) -> float:
        if len(history) < 2:
            return 0.0
        target = now - timedelta(seconds=seconds)
        base = history[0][1]
        for ts, value in history:
            if ts >= target:
                base = value
                break
        current = history[-1][1]
        if base == 0:
            return 0.0
        return (current - base) / base

    @staticmethod
    def _range_over_mean(history: Deque[Tuple[datetime, float]], now: datetime, seconds: int) -> float:
        if len(history) < 2:
            return 0.0
        target = now - timedelta(seconds=seconds)
        values = [value for ts, value in history if ts >= target]
        if len(values) < 2:
            return 0.0
        high = max(values)
        low = min(values)
        mean = sum(values) / len(values)
        if mean == 0:
            return 0.0
        return (high - low) / mean

    @staticmethod
    def _time_bucket(time_left_seconds: int) -> str:
        minutes = max(time_left_seconds, 0) / 60.0
        if minutes > 12:
            return "15-12"
        if minutes > 9:
            return "12-9"
        if minutes > 6:
            return "9-6"
        if minutes > 3:
            return "6-3"
        return "lt3"

    @staticmethod
    def _bucketize(value: float, step: float, min_value: float, max_value: float) -> str:
        value = max(min_value, min(max_value, value))
        bucket_start = min_value + int((value - min_value) / step) * step
        bucket_end = bucket_start + step
        if bucket_end > max_value:
            bucket_end = max_value
        return f"{bucket_start:.4f}_{bucket_end:.4f}"
