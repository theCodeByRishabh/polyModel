from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import asdict

from app.config import get_settings
from app.core.data_clients import PolymarketClient


async def main() -> int:
    settings = get_settings()
    market_client = PolymarketClient(settings)
    code = 0
    await market_client.start()
    try:
        await asyncio.sleep(2)
        market_result = await market_client.fetch_market(settings.market_slug)
        print("Polymarket source: OK")
        print(json.dumps(asdict(market_result), indent=2, default=str))
        print("BTC proxy feed status:")
        print(json.dumps(market_client.btc_feed_status(), indent=2, default=str))
    except Exception as exc:
        code = 1
        print("Polymarket source: FAIL")
        print(str(exc))

    await market_client.close()
    return code


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
