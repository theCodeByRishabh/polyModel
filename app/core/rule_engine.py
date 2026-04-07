from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable

from app.core.feature_engine import FeatureVector
from app.db.repository import BucketPerformance

logger = logging.getLogger(__name__)


@dataclass
class RuleState:
    threshold: float = 0.93
    min_momentum: float = 0.0
    max_spread: float = 0.03
    min_btc_gap: float = -0.02


class RuleEngine:
    def __init__(self, baseline_threshold: float = 0.93):
        self.state = RuleState(threshold=baseline_threshold)

    def decide(self, features: FeatureVector) -> bool:
        threshold = self._adjust_threshold_for_regime(features)
        decision = (
            features.price <= threshold
            and features.spread <= self.state.max_spread
            and features.momentum_score >= self.state.min_momentum
            and features.btc_gap >= self.state.min_btc_gap
            and features.time_left >= 180
        )
        return bool(decision)

    def _adjust_threshold_for_regime(self, features: FeatureVector) -> float:
        if features.regime == "volatile":
            return max(0.80, self.state.threshold - 0.01)
        return self.state.threshold

    def update_thresholds(self, price_bucket_stats: Iterable[BucketPerformance]) -> float:
        best = None
        for stat in price_bucket_stats:
            if stat.count < 30:
                continue
            if best is None:
                best = stat
                continue
            if stat.win_rate > best.win_rate:
                best = stat
            elif stat.win_rate == best.win_rate and stat.count > best.count:
                best = stat

        if best is None:
            return self.state.threshold

        new_threshold = self._upper_bound_from_bucket(best.price_bucket)
        if new_threshold is None:
            return self.state.threshold

        self.state.threshold = (0.8 * self.state.threshold) + (0.2 * new_threshold)
        self.state.threshold = max(0.75, min(0.99, self.state.threshold))
        logger.info(
            "Updated rule threshold from aggregated stats: bucket=%s win_rate=%.4f count=%d threshold=%.4f",
            best.price_bucket,
            best.win_rate,
            best.count,
            self.state.threshold,
        )
        return self.state.threshold

    @staticmethod
    def _upper_bound_from_bucket(bucket: str) -> float | None:
        try:
            _, hi = bucket.split("_", maxsplit=1)
            return float(hi)
        except Exception:
            return None
