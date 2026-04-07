from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.core.meta_engine import MetaEngine
from app.core.ml_engine import MLEngine
from app.core.rule_engine import RuleEngine
from app.db.repository import Repository

logger = logging.getLogger(__name__)


@dataclass
class TrainingStatus:
    trained_ml: bool
    trained_meta: bool
    resolved_count: int
    reason: str
    threshold: float


class LearningEngine:
    def __init__(
        self,
        repository: Repository,
        ml_engine: MLEngine,
        meta_engine: MetaEngine,
        rule_engine: RuleEngine,
        min_new_resolved: int = 50,
        min_interval_seconds: int = 600,
    ):
        self._repository = repository
        self._ml_engine = ml_engine
        self._meta_engine = meta_engine
        self._rule_engine = rule_engine
        self._min_new_resolved = min_new_resolved
        self._min_interval = timedelta(seconds=min_interval_seconds)
        self._last_resolved_count = 0
        self._last_train_at = datetime.now(timezone.utc) - self._min_interval

    async def maybe_train(self, force: bool = False) -> TrainingStatus:
        resolved_count = await self._repository.get_resolved_count()
        now = datetime.now(timezone.utc)
        enough_new = (resolved_count - self._last_resolved_count) >= self._min_new_resolved
        interval_elapsed = (now - self._last_train_at) >= self._min_interval

        if not force and not (enough_new or interval_elapsed):
            return TrainingStatus(
                trained_ml=False,
                trained_meta=False,
                resolved_count=resolved_count,
                reason="cooldown",
                threshold=self._rule_engine.state.threshold,
            )

        rows = await self._repository.get_training_rows(limit=7000)
        trained_ml, trained_meta = await asyncio.gather(
            asyncio.to_thread(self._ml_engine.train, rows),
            asyncio.to_thread(self._meta_engine.train, rows),
        )

        bucket_stats = await self._repository.get_price_bucket_performance(last_days=7, min_count=30)
        threshold = self._rule_engine.update_thresholds(bucket_stats)

        self._last_resolved_count = resolved_count
        self._last_train_at = now
        reason = "new_data" if enough_new else "time_interval"
        logger.info(
            "Learning cycle finished: ml=%s meta=%s resolved_count=%d reason=%s threshold=%.4f",
            trained_ml,
            trained_meta,
            resolved_count,
            reason,
            threshold,
        )
        return TrainingStatus(
            trained_ml=trained_ml,
            trained_meta=trained_meta,
            resolved_count=resolved_count,
            reason=reason,
            threshold=threshold,
        )
