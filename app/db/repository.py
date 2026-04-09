from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import and_, desc, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AggregatedStat, Base, Metric, Observation


@dataclass
class BucketPerformance:
    price_bucket: str
    count: int
    wins: int
    losses: int
    win_rate: float


class Repository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def init_tables(self, engine) -> None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def healthcheck(self) -> bool:
        async with self._session_factory() as session:
            result = await session.execute(text("SELECT 1"))
            return bool(result.scalar_one() == 1)

    async def add_observation(self, payload: dict[str, Any]) -> int:
        async with self._session_factory() as session:
            obs = Observation(**payload)
            session.add(obs)
            await session.commit()
            await session.refresh(obs)
            return obs.id

    async def mark_market_resolved(self, market_slug: str, outcome: bool) -> int:
        async with self._session_factory() as session:
            result = await session.execute(
                update(Observation)
                .where(and_(Observation.market_slug == market_slug, Observation.resolved.is_(False)))
                .values(resolved=True, outcome=outcome)
            )
            await session.commit()
            return int(result.rowcount or 0)

    async def get_recent_observations(self, limit: int = 50) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(Observation).order_by(desc(Observation.timestamp)).limit(limit)
                )
            ).scalars()
            return [self._observation_to_dict(row) for row in rows]

    async def get_bucket_stats(self, limit: int = 300) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(AggregatedStat)
                    .order_by(desc(AggregatedStat.bucket_day), desc(AggregatedStat.updated_at))
                    .limit(limit)
                )
            ).scalars()
            return [
                {
                    "id": row.id,
                    "bucket_day": row.bucket_day.isoformat(),
                    "price_bucket": row.price_bucket,
                    "time_bucket": row.time_bucket,
                    "btc_gap_bucket": row.btc_gap_bucket,
                    "count": row.count,
                    "wins": row.wins,
                    "losses": row.losses,
                    "win_rate": (row.wins / row.count) if row.count else 0.0,
                    "updated_at": row.updated_at.isoformat(),
                }
                for row in rows
            ]

    async def get_resolved_count(self) -> int:
        async with self._session_factory() as session:
            value = await session.scalar(
                select(func.count(Observation.id)).where(
                    and_(Observation.resolved.is_(True), Observation.outcome.is_not(None))
                )
            )
            return int(value or 0)

    async def get_unresolved_market_slugs(self, limit: int = 25) -> list[str]:
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(Observation.market_slug)
                    .where(Observation.resolved.is_(False))
                    .group_by(Observation.market_slug)
                    .order_by(desc(func.max(Observation.timestamp)))
                    .limit(limit)
                )
            ).scalars().all()
            return [str(slug) for slug in rows if slug]

    async def get_training_rows(self, limit: int = 6000) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(Observation)
                    .where(and_(Observation.resolved.is_(True), Observation.outcome.is_not(None)))
                    .order_by(desc(Observation.timestamp))
                    .limit(limit)
                )
            ).scalars()
            data = [self._observation_to_dict(row) for row in rows]
            data.reverse()
            return data

    async def get_comparison_stats(self) -> dict[str, Any]:
        async with self._session_factory() as session:
            row = (
                await session.execute(
                    text(
                        """
                        SELECT
                          COUNT(*)::int AS resolved_count,
                          COALESCE(SUM(CASE WHEN decision_rule = outcome THEN 1 ELSE 0 END), 0)::int AS rule_correct,
                          COALESCE(SUM(CASE WHEN decision_ml = outcome THEN 1 ELSE 0 END), 0)::int AS ml_correct,
                          COALESCE(SUM(CASE WHEN decision_rule = outcome AND decision_ml <> outcome THEN 1 ELSE 0 END), 0)::int AS rule_only_correct,
                          COALESCE(SUM(CASE WHEN decision_ml = outcome AND decision_rule <> outcome THEN 1 ELSE 0 END), 0)::int AS ml_only_correct
                        FROM observations
                        WHERE resolved = TRUE AND outcome IS NOT NULL
                        """
                    )
                )
            ).mappings().one()

            resolved_count = int(row["resolved_count"])
            rule_correct = int(row["rule_correct"])
            ml_correct = int(row["ml_correct"])
            return {
                "resolved_count": resolved_count,
                "rule_correct": rule_correct,
                "ml_correct": ml_correct,
                "rule_accuracy": (rule_correct / resolved_count) if resolved_count else 0.0,
                "ml_accuracy": (ml_correct / resolved_count) if resolved_count else 0.0,
                "rule_only_correct": int(row["rule_only_correct"]),
                "ml_only_correct": int(row["ml_only_correct"]),
            }

    async def get_price_bucket_performance(self, last_days: int = 7, min_count: int = 30) -> list[BucketPerformance]:
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    text(
                        """
                        SELECT
                          price_bucket,
                          SUM(count)::int AS count,
                          SUM(wins)::int AS wins,
                          SUM(losses)::int AS losses
                        FROM aggregated_stats
                        WHERE bucket_day >= (CURRENT_DATE - (:last_days * INTERVAL '1 day'))::date
                        GROUP BY price_bucket
                        HAVING SUM(count) >= :min_count
                        ORDER BY (SUM(wins)::float / NULLIF(SUM(count), 0)) DESC, SUM(count) DESC
                        """
                    ),
                    {"last_days": last_days, "min_count": min_count},
                )
            ).mappings()

            perf: list[BucketPerformance] = []
            for row in rows:
                count = int(row["count"])
                wins = int(row["wins"])
                losses = int(row["losses"])
                perf.append(
                    BucketPerformance(
                        price_bucket=str(row["price_bucket"]),
                        count=count,
                        wins=wins,
                        losses=losses,
                        win_rate=(wins / count) if count else 0.0,
                    )
                )
            return perf

    async def update_metrics_snapshot(self) -> dict[str, Any]:
        async with self._session_factory() as session:
            agg = (
                await session.execute(
                    text(
                        """
                        SELECT
                          COUNT(*) FILTER (WHERE resolved = TRUE AND outcome IS NOT NULL AND final_decision = TRUE)::int AS total_trades,
                          COALESCE(AVG(CASE WHEN resolved = TRUE AND outcome IS NOT NULL THEN CASE WHEN decision_rule = outcome THEN 1.0 ELSE 0.0 END END), 0.0) AS rule_accuracy,
                          COALESCE(AVG(CASE WHEN resolved = TRUE AND outcome IS NOT NULL THEN CASE WHEN decision_ml = outcome THEN 1.0 ELSE 0.0 END END), 0.0) AS ml_accuracy,
                          COALESCE(SUM(CASE WHEN resolved = TRUE AND outcome IS NOT NULL AND final_decision = TRUE AND outcome = TRUE THEN (1 - price) ELSE 0 END), 0.0) AS total_profit,
                          COALESCE(SUM(CASE WHEN resolved = TRUE AND outcome IS NOT NULL AND final_decision = TRUE AND outcome = FALSE THEN price ELSE 0 END), 0.0) AS total_loss,
                          COALESCE(AVG(CASE WHEN resolved = TRUE AND outcome IS NOT NULL AND final_decision = TRUE THEN CASE WHEN outcome = TRUE THEN (1 - price) ELSE (-price) END END), 0.0) AS ev,
                          COALESCE(AVG(
                            CASE
                              WHEN resolved = TRUE AND outcome IS NOT NULL AND meta_choice = 'ml' THEN CASE WHEN decision_ml = outcome THEN 1.0 ELSE 0.0 END
                              WHEN resolved = TRUE AND outcome IS NOT NULL AND meta_choice = 'rule' THEN CASE WHEN decision_rule = outcome THEN 1.0 ELSE 0.0 END
                              ELSE NULL
                            END
                          ), 0.0) AS meta_accuracy
                        FROM observations
                        """
                    )
                )
            ).mappings().one()

            pnl_rows = (
                await session.execute(
                    text(
                        """
                        SELECT
                          timestamp,
                          CASE
                            WHEN final_decision = TRUE AND outcome IS NOT NULL THEN
                              CASE WHEN outcome = TRUE THEN (1 - price) ELSE (-price) END
                            ELSE 0
                          END AS pnl
                        FROM observations
                        WHERE resolved = TRUE AND outcome IS NOT NULL
                        ORDER BY timestamp ASC
                        """
                    )
                )
            ).mappings().all()

            cumulative = 0.0
            peak = 0.0
            max_drawdown = 0.0
            for row in pnl_rows:
                cumulative += float(row["pnl"])
                peak = max(peak, cumulative)
                max_drawdown = max(max_drawdown, peak - cumulative)

            decision_rows = (
                await session.execute(
                    text(
                        """
                        SELECT meta_choice, COUNT(*)::int AS c
                        FROM observations
                        WHERE resolved = TRUE AND outcome IS NOT NULL AND meta_choice IS NOT NULL
                        GROUP BY meta_choice
                        """
                    )
                )
            ).mappings().all()
            distribution = {str(row["meta_choice"]): int(row["c"]) for row in decision_rows}

            payload = {
                "id": 1,
                "total_trades": int(agg["total_trades"] or 0),
                "win_rate_rule": float(agg["rule_accuracy"] or 0.0),
                "win_rate_ml": float(agg["ml_accuracy"] or 0.0),
                "total_profit": float(agg["total_profit"] or 0.0),
                "total_loss": float(agg["total_loss"] or 0.0),
                "ev": float(agg["ev"] or 0.0),
                "max_drawdown": float(max_drawdown),
                "meta_accuracy": float(agg["meta_accuracy"] or 0.0),
                "ml_accuracy": float(agg["ml_accuracy"] or 0.0),
                "rule_accuracy": float(agg["rule_accuracy"] or 0.0),
                "meta_decision_distribution": json.dumps(distribution),
            }

            await session.execute(
                text(
                    """
                    INSERT INTO metrics (
                      id, total_trades, win_rate_rule, win_rate_ml, total_profit, total_loss, ev, max_drawdown,
                      meta_accuracy, ml_accuracy, rule_accuracy, meta_decision_distribution, updated_at
                    )
                    VALUES (
                      :id, :total_trades, :win_rate_rule, :win_rate_ml, :total_profit, :total_loss, :ev, :max_drawdown,
                      :meta_accuracy, :ml_accuracy, :rule_accuracy, CAST(:meta_decision_distribution AS jsonb), NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                      total_trades = EXCLUDED.total_trades,
                      win_rate_rule = EXCLUDED.win_rate_rule,
                      win_rate_ml = EXCLUDED.win_rate_ml,
                      total_profit = EXCLUDED.total_profit,
                      total_loss = EXCLUDED.total_loss,
                      ev = EXCLUDED.ev,
                      max_drawdown = EXCLUDED.max_drawdown,
                      meta_accuracy = EXCLUDED.meta_accuracy,
                      ml_accuracy = EXCLUDED.ml_accuracy,
                      rule_accuracy = EXCLUDED.rule_accuracy,
                      meta_decision_distribution = EXCLUDED.meta_decision_distribution,
                      updated_at = NOW()
                    """
                ),
                payload,
            )
            await session.commit()
            return payload

    async def get_metrics(self) -> dict[str, Any]:
        async with self._session_factory() as session:
            row = (
                await session.execute(select(Metric).where(Metric.id == 1))
            ).scalar_one_or_none()
            if row is None:
                return await self.update_metrics_snapshot()
            return {
                "total_trades": row.total_trades,
                "win_rate_rule": row.win_rate_rule,
                "win_rate_ml": row.win_rate_ml,
                "total_profit": row.total_profit,
                "total_loss": row.total_loss,
                "ev": row.ev,
                "max_drawdown": row.max_drawdown,
                "meta_accuracy": row.meta_accuracy,
                "ml_accuracy": row.ml_accuracy,
                "rule_accuracy": row.rule_accuracy,
                "meta_decision_distribution": row.meta_decision_distribution,
                "updated_at": row.updated_at.isoformat(),
            }

    async def cleanup_and_compress(
        self,
        keep_raw_hours: int,
        keep_aggregated_days: int,
        max_db_size_mb: int,
    ) -> dict[str, Any]:
        async with self._session_factory() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO aggregated_stats (
                      bucket_day, price_bucket, time_bucket, btc_gap_bucket, count, wins, losses, updated_at
                    )
                    SELECT
                      date_trunc('day', timestamp)::date AS bucket_day,
                      price_bucket,
                      time_bucket,
                      btc_gap_bucket,
                      COUNT(*)::int AS count,
                      SUM(CASE WHEN outcome = TRUE THEN 1 ELSE 0 END)::int AS wins,
                      SUM(CASE WHEN outcome = FALSE THEN 1 ELSE 0 END)::int AS losses,
                      NOW() AS updated_at
                    FROM observations
                    WHERE
                      timestamp < (NOW() - make_interval(hours => :keep_raw_hours))
                      AND timestamp >= (NOW() - make_interval(days => :keep_aggregated_days))
                      AND resolved = TRUE
                      AND outcome IS NOT NULL
                    GROUP BY 1, 2, 3, 4
                    ON CONFLICT (bucket_day, price_bucket, time_bucket, btc_gap_bucket) DO UPDATE SET
                      count = aggregated_stats.count + EXCLUDED.count,
                      wins = aggregated_stats.wins + EXCLUDED.wins,
                      losses = aggregated_stats.losses + EXCLUDED.losses,
                      updated_at = NOW()
                    """
                ),
                {
                    "keep_raw_hours": keep_raw_hours,
                    "keep_aggregated_days": keep_aggregated_days,
                },
            )

            delete_resolved = await session.execute(
                text(
                    """
                    DELETE FROM observations
                    WHERE
                      timestamp < (NOW() - make_interval(hours => :keep_raw_hours))
                      AND resolved = TRUE
                      AND outcome IS NOT NULL
                    """
                ),
                {"keep_raw_hours": keep_raw_hours},
            )

            delete_agg = await session.execute(
                text(
                    """
                    DELETE FROM aggregated_stats
                    WHERE bucket_day < (CURRENT_DATE - (:keep_aggregated_days * INTERVAL '1 day'))::date
                    """
                ),
                {"keep_aggregated_days": keep_aggregated_days},
            )

            await session.commit()

            current_size = await self.get_database_size_mb(session)
            emergency_deleted = 0
            if current_size > float(max_db_size_mb):
                emergency_deleted = await self._emergency_prune(session, max_db_size_mb)
                current_size = await self.get_database_size_mb(session)

            return {
                "deleted_observations": int(delete_resolved.rowcount or 0),
                "deleted_aggregates": int(delete_agg.rowcount or 0),
                "database_size_mb": current_size,
                "emergency_deleted": emergency_deleted,
            }

    async def get_database_size_mb(self, existing_session: AsyncSession | None = None) -> float:
        if existing_session is not None:
            result = await existing_session.execute(
                text("SELECT pg_database_size(current_database()) / (1024.0 * 1024.0)")
            )
            return float(result.scalar_one())

        async with self._session_factory() as session:
            result = await session.execute(
                text("SELECT pg_database_size(current_database()) / (1024.0 * 1024.0)")
            )
            return float(result.scalar_one())

    async def _emergency_prune(self, session: AsyncSession, max_db_size_mb: int) -> int:
        deleted_total = 0
        for _ in range(5):
            size_mb = await self.get_database_size_mb(session)
            if size_mb <= float(max_db_size_mb):
                break

            deleted = await session.execute(
                text(
                    """
                    WITH doomed AS (
                      SELECT id
                      FROM observations
                      WHERE resolved = TRUE
                        AND outcome IS NOT NULL
                        AND timestamp < (NOW() - INTERVAL '6 hours')
                      ORDER BY timestamp ASC
                      LIMIT 20000
                    )
                    DELETE FROM observations
                    WHERE id IN (SELECT id FROM doomed)
                    """
                )
            )
            step_deleted = int(deleted.rowcount or 0)
            deleted_total += step_deleted
            await session.commit()
            if step_deleted == 0:
                break
        return deleted_total

    @staticmethod
    def _observation_to_dict(row: Observation) -> dict[str, Any]:
        return {
            "id": row.id,
            "timestamp": row.timestamp.isoformat(),
            "market_slug": row.market_slug,
            "price": row.price,
            "spread": row.spread,
            "btc_reference_price": row.btc_reference_price,
            "btc_gap": row.btc_gap,
            "btc_velocity": row.btc_velocity,
            "btc_volatility": row.btc_volatility,
            "momentum_score": row.momentum_score,
            "price_change_rate": row.price_change_rate,
            "orderbook_imbalance": row.orderbook_imbalance,
            "time_left": row.time_left,
            "time_bucket": row.time_bucket,
            "regime": row.regime,
            "price_bucket": row.price_bucket,
            "btc_gap_bucket": row.btc_gap_bucket,
            "price_time": row.price_time,
            "gap_velocity": row.gap_velocity,
            "decision_rule": row.decision_rule,
            "decision_ml": row.decision_ml,
            "final_decision": row.final_decision,
            "prob_ml": row.prob_ml,
            "prob_meta": row.prob_meta,
            "expected_value": row.expected_value,
            "meta_choice": row.meta_choice,
            "data_source": row.data_source,
            "outcome": row.outcome,
            "resolved": row.resolved,
        }
