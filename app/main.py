from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse

from app.config import Settings, get_settings
from app.core.cleanup import CleanupEngine
from app.core.data_clients import PolymarketClient
from app.dashboard import render_dashboard_html
from app.core.feature_engine import FeatureEngine
from app.core.learning_engine import LearningEngine
from app.core.meta_engine import MetaEngine
from app.core.ml_engine import MLEngine
from app.core.outcome_tracker import OutcomeTracker
from app.core.pipeline import PredictionPipeline
from app.core.rule_engine import RuleEngine
from app.db.repository import Repository
from app.db.session import build_engine, build_session_factory
from app.logging_config import setup_logging

logger = logging.getLogger(__name__)


@dataclass
class AppContainer:
    settings: Settings
    repository: Repository
    pipeline: PredictionPipeline
    engine: Any


def _get_container(request: Request) -> AppContainer:
    return request.app.state.container


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    setup_logging(settings.log_level)

    engine = build_engine(settings)
    session_factory = build_session_factory(engine)
    repository = Repository(session_factory)
    await repository.init_tables(engine)

    feature_engine = FeatureEngine()
    rule_engine = RuleEngine(baseline_threshold=0.93)
    ml_engine = MLEngine(settings.model_file)
    meta_engine = MetaEngine(settings.meta_model_file)
    ml_engine.load()
    meta_engine.load()

    market_client = PolymarketClient(settings)
    learning_engine = LearningEngine(
        repository=repository,
        ml_engine=ml_engine,
        meta_engine=meta_engine,
        rule_engine=rule_engine,
        min_new_resolved=settings.retrain_min_new_resolved,
        min_interval_seconds=settings.retrain_min_interval_seconds,
    )
    outcome_tracker = OutcomeTracker(repository, market_client, settings.market_slug)
    cleanup_engine = CleanupEngine(
        repository=repository,
        keep_raw_hours=settings.keep_raw_hours,
        keep_aggregated_days=settings.keep_aggregated_days,
        max_db_size_mb=settings.max_db_size_mb,
    )

    pipeline = PredictionPipeline(
        settings=settings,
        repository=repository,
        feature_engine=feature_engine,
        rule_engine=rule_engine,
        ml_engine=ml_engine,
        meta_engine=meta_engine,
        learning_engine=learning_engine,
        outcome_tracker=outcome_tracker,
        cleanup_engine=cleanup_engine,
        market_client=market_client,
    )
    await pipeline.start()
    app.state.container = AppContainer(settings=settings, repository=repository, pipeline=pipeline, engine=engine)
    try:
        yield
    finally:
        await pipeline.stop()
        await engine.dispose()


app = FastAPI(
    title="Polymarket BTC 5m Prediction Engine",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse)
async def root() -> HTMLResponse:
    return HTMLResponse(content=render_dashboard_html())


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page() -> HTMLResponse:
    return HTMLResponse(content=render_dashboard_html())


@app.get("/health")
async def health(request: Request) -> dict[str, Any]:
    container = _get_container(request)
    db_ok = await container.repository.healthcheck()
    runtime = container.pipeline.health()
    runtime["resolved_count"] = await container.repository.get_resolved_count()
    status = "ok" if db_ok else "degraded"
    return {"status": status, "db_ok": db_ok, "runtime": runtime}


@app.get("/stats")
async def stats(request: Request) -> dict[str, Any]:
    container = _get_container(request)
    return await container.repository.get_metrics()


@app.get("/comparison")
async def comparison(request: Request) -> dict[str, Any]:
    container = _get_container(request)
    return await container.repository.get_comparison_stats()


@app.get("/recent")
async def recent(request: Request) -> list[dict[str, Any]]:
    container = _get_container(request)
    return await container.repository.get_recent_observations(limit=50)


@app.get("/buckets")
async def buckets(request: Request) -> list[dict[str, Any]]:
    container = _get_container(request)
    return await container.repository.get_bucket_stats(limit=300)
