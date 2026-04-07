from __future__ import annotations

import logging
from dataclasses import asdict, dataclass

from app.core.data_clients import DataSourceError, PolymarketClient
from app.db.repository import Repository

logger = logging.getLogger(__name__)


@dataclass
class OutcomeUpdateResult:
    resolved: bool
    updated_rows: int
    outcome: bool | None
    source: str | None


class OutcomeTracker:
    def __init__(self, repository: Repository, market_client: PolymarketClient, market_slug: str):
        self._repository = repository
        self._market_client = market_client
        self._market_slug = market_slug

    async def check_and_update(self) -> OutcomeUpdateResult:
        try:
            snapshot = await self._market_client.fetch_market(self._market_slug)
        except DataSourceError as exc:
            logger.warning("Outcome tracker skipped: %s", exc)
            return OutcomeUpdateResult(resolved=False, updated_rows=0, outcome=None, source=None)

        if snapshot.resolved and snapshot.outcome is not None:
            updated_rows = await self._repository.mark_market_resolved(snapshot.market_slug, snapshot.outcome)
            if updated_rows > 0:
                logger.info(
                    "Marked %d observations resolved for %s with outcome=%s",
                    updated_rows,
                    snapshot.market_slug,
                    snapshot.outcome,
                )
            return OutcomeUpdateResult(
                resolved=True,
                updated_rows=updated_rows,
                outcome=snapshot.outcome,
                source=snapshot.source,
            )
        return OutcomeUpdateResult(resolved=False, updated_rows=0, outcome=None, source=snapshot.source)
