from __future__ import annotations

from typing import Any

from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings


def _normalize_asyncpg_url(database_url: str) -> tuple[str, dict[str, Any]]:
    """
    asyncpg expects `ssl`, not `sslmode`.
    Railway/Neon URLs often include `?sslmode=require`; map that safely.
    """
    normalized_input = database_url
    if database_url.startswith("postgresql://"):
        normalized_input = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    url: URL = make_url(normalized_input)
    query = dict(url.query)
    connect_args: dict[str, Any] = {}

    sslmode = query.pop("sslmode", None)
    # asyncpg does not accept this psycopg-style argument when routed through SQLAlchemy URL query.
    query.pop("channel_binding", None)
    if sslmode is not None and "ssl" not in query:
        normalized = str(sslmode).strip().lower()
        if normalized in {"disable", "allow"}:
            connect_args["ssl"] = False
        else:
            # require / prefer / verify-ca / verify-full -> use TLS
            connect_args["ssl"] = True

    # NOTE: str(URL) hides password as ***; use explicit render to keep credentials.
    normalized_url = url.set(query=query).render_as_string(hide_password=False)
    return normalized_url, connect_args


def build_engine(settings: Settings) -> AsyncEngine:
    normalized_url, connect_args = _normalize_asyncpg_url(settings.database_url)
    return create_async_engine(
        normalized_url,
        echo=False,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        connect_args=connect_args,
    )


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)
