from __future__ import annotations

from datetime import datetime, timezone


def resolve_market_slug(raw_slug: str, now: datetime | None = None) -> str:
    """
    Resolve dynamic Polymarket BTC 5m slugs when placeholders are used.

    Supported placeholders:
    - <WINDOW_START_TS>
    - {WINDOW_START_TS}
    """
    if not raw_slug:
        return raw_slug

    if "<WINDOW_START_TS>" not in raw_slug and "{WINDOW_START_TS}" not in raw_slug:
        return raw_slug

    now = now or datetime.now(timezone.utc)
    window_start_ts = (int(now.timestamp()) // 300) * 300
    resolved = raw_slug.replace("<WINDOW_START_TS>", str(window_start_ts))
    resolved = resolved.replace("{WINDOW_START_TS}", str(window_start_ts))
    return resolved
