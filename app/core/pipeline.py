from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.config import Settings
from app.core.cleanup import CleanupEngine
from app.core.data_clients import DataSourceError, PolymarketClient
from app.core.feature_engine import FeatureEngine
from app.core.learning_engine import LearningEngine
from app.core.meta_engine import MetaEngine
from app.core.ml_engine import MLEngine
from app.core.outcome_tracker import OutcomeTracker
from app.core.rule_engine import RuleEngine
from app.db.repository import Repository

logger = logging.getLogger(__name__)


@dataclass
class RuntimeState:
    started_at: datetime
    last_observation_at: datetime | None = None
    last_error: str | None = None
    mode: str = "learning"


class PredictionPipeline:
    def __init__(
        self,
        *,
        settings: Settings,
        repository: Repository,
        feature_engine: FeatureEngine,
        rule_engine: RuleEngine,
        ml_engine: MLEngine,
        meta_engine: MetaEngine,
        learning_engine: LearningEngine,
        outcome_tracker: OutcomeTracker,
        cleanup_engine: CleanupEngine,
        market_client: PolymarketClient,
    ):
        self._settings = settings
        self._repository = repository
        self._feature_engine = feature_engine
        self._rule_engine = rule_engine
        self._ml_engine = ml_engine
        self._meta_engine = meta_engine
        self._learning_engine = learning_engine
        self._outcome_tracker = outcome_tracker
        self._cleanup_engine = cleanup_engine
        self._market_client = market_client

        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self.state = RuntimeState(started_at=datetime.now(timezone.utc), mode=settings.mode)

    async def start(self) -> None:
        self._stop_event.clear()
        await self._market_client.start()
        self._tasks = [
            asyncio.create_task(self._observation_loop(), name="observation-loop"),
            asyncio.create_task(self._outcome_loop(), name="outcome-loop"),
            asyncio.create_task(self._learning_loop(), name="learning-loop"),
            asyncio.create_task(self._cleanup_loop(), name="cleanup-loop"),
            asyncio.create_task(self._metrics_loop(), name="metrics-loop"),
        ]
        logger.info("Prediction pipeline started in mode=%s", self._settings.mode)

    async def stop(self) -> None:
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._market_client.close()
        logger.info("Prediction pipeline stopped")

    async def _observation_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                market_snapshot = await self._market_client.fetch_market(self._settings.market_slug)

                if market_snapshot.resolved and market_snapshot.outcome is not None:
                    await self._repository.mark_market_resolved(
                        market_snapshot.market_slug,
                        market_snapshot.outcome,
                    )

                features = self._feature_engine.build(
                    market_price=market_snapshot.price,
                    bid=market_snapshot.bid,
                    ask=market_snapshot.ask,
                    time_left_seconds=market_snapshot.time_left_seconds,
                    orderbook_imbalance=market_snapshot.orderbook_imbalance,
                    btc_price=market_snapshot.btc_reference_price,
                    now=market_snapshot.timestamp,
                )

                decision_rule = self._rule_engine.decide(features)
                decision_ml, prob_ml, expected_value = self._ml_engine.decide(features)
                meta_choice, prob_meta = self._meta_engine.choose(
                    features=features,
                    decision_rule=decision_rule,
                    decision_ml=decision_ml,
                    prob_ml=prob_ml,
                )
                final_decision = self._resolve_final_decision(
                    mode=self._settings.mode,
                    decision_rule=decision_rule,
                    decision_ml=decision_ml,
                    meta_choice=meta_choice,
                )

                payload = {
                    "timestamp": market_snapshot.timestamp,
                    "market_slug": market_snapshot.market_slug,
                    "price": features.price,
                    "spread": features.spread,
                    "btc_reference_price": market_snapshot.btc_reference_price,
                    "btc_gap": features.btc_gap,
                    "btc_velocity": features.btc_velocity,
                    "btc_volatility": features.btc_volatility,
                    "momentum_score": features.momentum_score,
                    "price_change_rate": features.price_change_rate,
                    "orderbook_imbalance": features.orderbook_imbalance,
                    "time_left": features.time_left,
                    "time_bucket": features.time_bucket,
                    "regime": features.regime,
                    "price_bucket": features.price_bucket,
                    "btc_gap_bucket": features.btc_gap_bucket,
                    "price_time": features.price_time,
                    "gap_velocity": features.gap_velocity,
                    "decision_rule": decision_rule,
                    "decision_ml": decision_ml,
                    "final_decision": final_decision,
                    "prob_ml": prob_ml,
                    "prob_meta": prob_meta,
                    "expected_value": expected_value,
                    "meta_choice": meta_choice,
                    "data_source": market_snapshot.source,
                    "resolved": bool(market_snapshot.resolved and market_snapshot.outcome is not None),
                    "outcome": market_snapshot.outcome if market_snapshot.resolved else None,
                }
                await self._repository.add_observation(payload)
                self.state.last_observation_at = datetime.now(timezone.utc)
                self.state.last_error = None
            except DataSourceError as exc:
                self.state.last_error = str(exc)
                logger.warning("Observation loop data source warning: %s", exc)
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                self.state.last_error = str(exc)
                logger.exception("Observation loop error: %s", exc)

            await self._sleep_with_stop(self._settings.poll_interval_seconds)

    async def _outcome_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._outcome_tracker.check_and_update()
            except Exception as exc:  # pragma: no cover
                logger.exception("Outcome loop error: %s", exc)
            await self._sleep_with_stop(30)

    async def _learning_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if self._settings.mode == "learning":
                    await self._learning_engine.maybe_train()
            except Exception as exc:  # pragma: no cover
                logger.exception("Learning loop error: %s", exc)
            await self._sleep_with_stop(30)

    async def _cleanup_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._cleanup_engine.run_once()
            except Exception as exc:  # pragma: no cover
                logger.exception("Cleanup loop error: %s", exc)
            await self._sleep_with_stop(self._settings.cleanup_interval_seconds)

    async def _metrics_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._repository.update_metrics_snapshot()
            except Exception as exc:  # pragma: no cover
                logger.exception("Metrics loop error: %s", exc)
            await self._sleep_with_stop(60)

    async def _sleep_with_stop(self, seconds: int) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return

    @staticmethod
    def _resolve_final_decision(
        *,
        mode: str,
        decision_rule: bool,
        decision_ml: bool,
        meta_choice: str,
    ) -> bool:
        if mode == "observation":
            return False
        if meta_choice == "ml":
            return decision_ml
        if meta_choice == "rule":
            return decision_rule
        return False

    def health(self) -> dict[str, Any]:
        return {
            "mode": self.state.mode,
            "started_at": self.state.started_at.isoformat(),
            "last_observation_at": self.state.last_observation_at.isoformat()
            if self.state.last_observation_at
            else None,
            "last_error": self.state.last_error,
            "btc_feed": self._market_client.btc_feed_status(),
        }
