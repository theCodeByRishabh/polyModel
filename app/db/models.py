from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import JSON, BigInteger, Boolean, Date, DateTime, Float, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class Observation(Base):
    __tablename__ = "observations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True, server_default=func.now())
    market_slug: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    price: Mapped[float] = mapped_column(Float, nullable=False)
    spread: Mapped[float] = mapped_column(Float, nullable=False)
    btc_reference_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0, server_default="0")
    btc_gap: Mapped[float] = mapped_column(Float, nullable=False)
    btc_velocity: Mapped[float] = mapped_column(Float, nullable=False)
    btc_volatility: Mapped[float] = mapped_column(Float, nullable=False)
    momentum_score: Mapped[float] = mapped_column(Float, nullable=False)
    price_change_rate: Mapped[float] = mapped_column(Float, nullable=False)
    orderbook_imbalance: Mapped[float] = mapped_column(Float, nullable=False)
    time_left: Mapped[int] = mapped_column(Integer, nullable=False)
    time_bucket: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    regime: Mapped[str] = mapped_column(String(32), nullable=False)

    price_bucket: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    btc_gap_bucket: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    price_time: Mapped[float] = mapped_column(Float, nullable=False)
    gap_velocity: Mapped[float] = mapped_column(Float, nullable=False)

    decision_rule: Mapped[bool] = mapped_column(Boolean, nullable=False)
    decision_ml: Mapped[bool] = mapped_column(Boolean, nullable=False)
    final_decision: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    prob_ml: Mapped[float | None] = mapped_column(Float, nullable=True)
    prob_meta: Mapped[float | None] = mapped_column(Float, nullable=True)
    expected_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    meta_choice: Mapped[str | None] = mapped_column(String(16), nullable=True)
    data_source: Mapped[str | None] = mapped_column(String(255), nullable=True)

    outcome: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false", index=True)

    __table_args__ = (
        Index("ix_observations_market_resolved_ts", "market_slug", "resolved", "timestamp"),
    )


class AggregatedStat(Base):
    __tablename__ = "aggregated_stats"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    bucket_day: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    price_bucket: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    time_bucket: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    btc_gap_bucket: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint(
            "bucket_day",
            "price_bucket",
            "time_bucket",
            "btc_gap_bucket",
            name="uq_aggregated_stats_bucket",
        ),
    )


class Metric(Base):
    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    total_trades: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    win_rate_rule: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    win_rate_ml: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    total_profit: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    total_loss: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    ev: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    max_drawdown: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    meta_accuracy: Mapped[float | None] = mapped_column(Float, nullable=True)
    ml_accuracy: Mapped[float | None] = mapped_column(Float, nullable=True)
    rule_accuracy: Mapped[float | None] = mapped_column(Float, nullable=True)
    meta_decision_distribution: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
