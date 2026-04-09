from __future__ import annotations

import asyncio
import json
from dataclasses import asdict

import httpx
import websockets
from httpx import Response

from app.config import Settings
from app.core.data_clients import PolymarketClient


class MockBackend:
    def __init__(self, mode: str):
        self.mode = mode

    def handler(self, request: httpx.Request) -> Response:
        path = request.url.path
        query = dict(request.url.params)

        if path == "/book":
            token_id = query.get("token_id", "")
            if self.mode == "book_fail":
                return Response(503, json={"error": "book unavailable"})
            if self.mode == "events_reversed_outcomes":
                return Response(404, json={"error": "book unavailable for reversed-outcome case"})
            if token_id:
                return Response(
                    200,
                    json={
                        "bids": [{"price": "0.93", "size": "100"}, {"price": "0.92", "size": "75"}],
                        "asks": [{"price": "0.94", "size": "120"}, {"price": "0.95", "size": "60"}],
                    },
                )
            return Response(404, json={"error": "missing token"})

        if path == "/events":
            if self.mode in {"markets_fallback", "template_fallback"}:
                return Response(500, json={"error": "events unavailable"})

            if self.mode == "events_multi":
                return Response(
                    200,
                    json=[
                        {
                            "slug": "btc-updown-5m-test",
                            "markets": [
                                {
                                    "slug": "some-other-market",
                                    "endDate": "2099-01-01T00:05:00Z",
                                    "clobTokenIds": '["333","444"]',
                                    "outcomes": '["Up","Down"]',
                                    "active": True,
                                    "outcomePrices": '["0.11","0.89"]',
                                },
                                {
                                    "slug": "btc-updown-5m-test",
                                    "endDate": "2099-01-01T00:05:00Z",
                                    "clobTokenIds": '["111","222"]',
                                    "outcomes": '["Up","Down"]',
                                    "active": True,
                                    "outcomePrices": '["0.94","0.06"]',
                                },
                            ],
                        }
                    ],
                )

            if self.mode == "events_reversed_outcomes":
                return Response(
                    200,
                    json=[
                        {
                            "slug": "btc-updown-5m-test",
                            "markets": [
                                {
                                    "slug": "btc-updown-5m-test",
                                    "endDate": "2099-01-01T00:05:00Z",
                                    "clobTokenIds": '["111","222"]',
                                    "outcomes": '["Down","Up"]',
                                    "active": True,
                                    "outcomePrices": '["0.94","0.06"]',
                                }
                            ],
                        }
                    ],
                )

            return Response(
                200,
                json=[
                    {
                        "slug": "btc-updown-5m-test",
                        "markets": [
                            {
                                "slug": "btc-updown-5m-test",
                                "endDate": "2099-01-01T00:05:00Z",
                                "clobTokenIds": '["111","222"]',
                                "outcomes": '["Up","Down"]',
                                "active": True,
                                "outcomePrices": '["0.94","0.06"]',
                            }
                        ],
                    }
                ],
            )

        if path == "/markets":
            if self.mode == "template_fallback":
                return Response(500, json={"error": "markets unavailable"})
            return Response(
                200,
                json=[
                    {
                        "slug": "btc-updown-5m-test",
                        "endDate": "2099-01-01T00:05:00Z",
                        "clobTokenIds": '["111","222"]',
                        "outcomes": '["Up","Down"]',
                        "active": True,
                        "outcomePrices": '["0.93","0.07"]',
                    }
                ],
            )

        if path.startswith("/fallback/"):
            return Response(
                200,
                json={
                    "slug": "btc-updown-5m-test",
                    "endDate": "2099-01-01T00:05:00Z",
                    "clobTokenIds": '["111","222"]',
                    "outcomes": '["Up","Down"]',
                    "active": True,
                    "outcomePrices": '["0.92","0.08"]',
                },
            )

        return Response(404, json={"error": f"unhandled path {path}"})


async def ws_handler(websocket):
    try:
        await websocket.recv()
        await websocket.send(
            json.dumps(
                {
                    "topic": "crypto_prices_chainlink",
                    "payload": {"symbol": "btc/usd", "value": "70999.12"},
                }
            )
        )
        await asyncio.sleep(2)
    except Exception:
        return


def build_settings() -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/db",
        MODE="observation",
        MARKET_SLUG="btc-updown-5m-test",
        GAMMA_API_BASE="https://mock.local",
        CLOB_HOST="https://mock.local",
        POLYMARKET_MARKET_URL_TEMPLATE="https://mock.local/fallback/{slug}",
        POLYMARKET_FALLBACK_URL_TEMPLATE="https://mock.local/fallback/{slug}",
        POLYMARKET_RTDS_WS_URL="ws://127.0.0.1:8765",
    )


async def run_case(mode: str, expected_source_contains: str, expected_price: float | None = None) -> tuple[bool, str]:
    settings = build_settings()
    backend = MockBackend(mode)
    client = PolymarketClient(settings)
    await client._http.aclose()
    client._http = httpx.AsyncClient(transport=httpx.MockTransport(backend.handler), base_url="https://mock.local")

    ws_server = await websockets.serve(ws_handler, "127.0.0.1", 8765)

    try:
        await client.start()
        await asyncio.sleep(1.0)
        snapshot = await client.fetch_market(settings.market_slug)
        feed_status = client.btc_feed_status()

        checks = [
            expected_source_contains in snapshot.source,
            snapshot.ask >= snapshot.bid,
            snapshot.btc_reference_price > 1000,
            feed_status.get("latest_price") is not None,
        ]
        if expected_price is not None:
            checks.append(abs(snapshot.price - expected_price) < 1e-9)
        passed = all(checks)
        message = (
            f"mode={mode} source={snapshot.source} price={snapshot.price:.4f} ask={snapshot.ask:.4f} "
            f"bid={snapshot.bid:.4f} btc_ref={snapshot.btc_reference_price:.2f}"
        )
        return passed, message
    finally:
        await client.close()
        ws_server.close()
        await ws_server.wait_closed()


async def main() -> int:
    scenarios = [
        ("events_success", "/events", None),
        ("events_multi", "/events", None),
        ("events_reversed_outcomes", "/events", 0.06),
        ("markets_fallback", "/markets", None),
        ("template_fallback", "/fallback/", None),
    ]

    ok = True
    for mode, expected, expected_price in scenarios:
        passed, info = await run_case(mode, expected, expected_price=expected_price)
        print(f"[{'PASS' if passed else 'FAIL'}] {info}")
        ok = ok and passed
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
