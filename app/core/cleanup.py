from __future__ import annotations

import logging
from dataclasses import dataclass

from app.db.repository import Repository

logger = logging.getLogger(__name__)


@dataclass
class CleanupResult:
    deleted_observations: int
    deleted_aggregates: int
    database_size_mb: float
    emergency_deleted: int


class CleanupEngine:
    def __init__(self, repository: Repository, keep_raw_hours: int, keep_aggregated_days: int, max_db_size_mb: int):
        self._repository = repository
        self._keep_raw_hours = keep_raw_hours
        self._keep_aggregated_days = keep_aggregated_days
        self._max_db_size_mb = max_db_size_mb

    async def run_once(self) -> CleanupResult:
        result = await self._repository.cleanup_and_compress(
            keep_raw_hours=self._keep_raw_hours,
            keep_aggregated_days=self._keep_aggregated_days,
            max_db_size_mb=self._max_db_size_mb,
        )
        cleanup = CleanupResult(**result)
        logger.info(
            "Cleanup done: deleted_raw=%d deleted_agg=%d db_size_mb=%.2f emergency_deleted=%d",
            cleanup.deleted_observations,
            cleanup.deleted_aggregates,
            cleanup.database_size_mb,
            cleanup.emergency_deleted,
        )
        return cleanup
