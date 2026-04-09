from __future__ import annotations

import logging
from dataclasses import dataclass

from app.core.data_clients import DataSourceError, PolymarketClient
from app.core.market_slug import resolve_market_slug
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
        active_slug = resolve_market_slug(self._market_slug)
        unresolved_slugs = await self._repository.get_unresolved_market_slugs(limit=25)

        # Keep active slug first, then unresolved history, de-duplicated.
        ordered_slugs: list[str] = []
        for slug in [active_slug, *unresolved_slugs]:
            if slug and slug not in ordered_slugs:
                ordered_slugs.append(slug)

        total_updated = 0
        last_outcome: bool | None = None
        last_source: str | None = None
        resolved_any = False

        for slug in ordered_slugs:
            try:
                snapshot = await self._market_client.fetch_market(slug)
            except DataSourceError as exc:
                if slug == active_slug:
                    logger.warning("Outcome tracker skipped active slug (%s): %s", slug, exc)
                continue

            last_source = snapshot.source
            if snapshot.resolved and snapshot.outcome is not None:
                updated_rows = await self._repository.mark_market_resolved(snapshot.market_slug, snapshot.outcome)
                if updated_rows > 0:
                    logger.info(
                        "Marked %d observations resolved for %s with outcome=%s",
                        updated_rows,
                        snapshot.market_slug,
                        snapshot.outcome,
                    )
                    total_updated += updated_rows
                    resolved_any = True
                    last_outcome = snapshot.outcome

        return OutcomeUpdateResult(
            resolved=resolved_any,
            updated_rows=total_updated,
            outcome=last_outcome,
            source=last_source,
        )
