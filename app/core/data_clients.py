from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
import websockets

from app.config import Settings

logger = logging.getLogger(__name__)

GAMMA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/",
}


class DataSourceError(RuntimeError):
    pass


@dataclass
class MarketSnapshot:
    market_slug: str
    price: float
    bid: float
    ask: float
    orderbook_imbalance: float
    time_left_seconds: int
    resolved: bool
    outcome: bool | None
    btc_reference_price: float
    timestamp: datetime
    source: str

    @property
    def spread(self) -> float:
        return max(self.ask - self.bid, 0.0)


class PolymarketClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._http = httpx.AsyncClient(timeout=settings.request_timeout_seconds)
        self._gamma_api = settings.gamma_api_base.rstrip("/")
        self._clob_host = settings.clob_host.rstrip("/")
        self._fallback_templates = [
            settings.polymarket_market_url_template,
            settings.polymarket_fallback_url_template,
        ]
        self._rtds_ws = settings.polymarket_rtds_ws_url

        self._btc_task: asyncio.Task | None = None
        self._btc_stop = asyncio.Event()
        self._latest_btc_price: float | None = None
        self._latest_btc_timestamp: datetime | None = None

    async def start(self) -> None:
        if self._btc_task is None:
            self._btc_stop.clear()
            self._btc_task = asyncio.create_task(self._run_chainlink_wss(), name="chainlink-feed")

    async def close(self) -> None:
        self._btc_stop.set()
        if self._btc_task is not None:
            self._btc_task.cancel()
            await asyncio.gather(self._btc_task, return_exceptions=True)
            self._btc_task = None
        await self._http.aclose()

    async def fetch_market(self, market_slug: str) -> MarketSnapshot:
        record, source = await self._fetch_raw_market_record(market_slug)
        snapshot = await self._build_market_snapshot(record, market_slug, source)

        proxy_slug = self._settings.btc_reference_market_slug.strip()
        if proxy_slug and proxy_slug != market_slug and self._latest_btc_price is None:
            try:
                proxy_record, _ = await self._fetch_raw_market_record(proxy_slug)
                proxy_price = self._extract_btc_reference(proxy_record)
                if proxy_price is not None:
                    snapshot.btc_reference_price = proxy_price
            except Exception:
                pass

        return snapshot

    def btc_feed_status(self) -> dict[str, Any]:
        return {
            "source": "polymarket_chainlink_ws",
            "latest_price": self._latest_btc_price,
            "latest_timestamp": self._latest_btc_timestamp.isoformat() if self._latest_btc_timestamp else None,
            "connected": self._btc_task is not None and not self._btc_task.done(),
        }

    async def _run_chainlink_wss(self) -> None:
        subscribe_msg = json.dumps(
            {
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": "crypto_prices_chainlink",
                        "type": "*",
                        "filters": "{\"symbol\":\"btc/usd\"}",
                    }
                ],
            }
        )

        while not self._btc_stop.is_set():
            try:
                async with websockets.connect(self._rtds_ws, ping_interval=None, open_timeout=10) as ws:
                    await ws.send(subscribe_msg)
                    logger.info("Subscribed to Polymarket Chainlink BTC feed.")

                    async for raw in ws:
                        if self._btc_stop.is_set():
                            break
                        if raw in ("PING", "PONG"):
                            continue
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if msg.get("topic") != "crypto_prices_chainlink":
                            continue
                        payload = msg.get("payload", {})
                        if str(payload.get("symbol", "")).lower() != "btc/usd":
                            continue

                        value = self._safe_float(payload.get("value"))
                        if value is None or value <= 0:
                            continue
                        self._latest_btc_price = value
                        self._latest_btc_timestamp = datetime.now(timezone.utc)
            except Exception as exc:
                if not self._btc_stop.is_set():
                    logger.warning("Chainlink feed disconnected. Reconnecting in 3s: %s", exc)
                    await asyncio.sleep(3)

    async def _fetch_raw_market_record(self, slug: str) -> tuple[dict[str, Any], str]:
        errors: list[str] = []

        try:
            response = await self._http.get(
                f"{self._gamma_api}/events",
                params={"slug": slug},
                headers=GAMMA_HEADERS,
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list) and data:
                event = data[0]
                markets = event.get("markets", [])
                if isinstance(markets, list) and markets:
                    return markets[0], f"{self._gamma_api}/events?slug={slug}"
        except Exception as exc:
            errors.append(f"events -> {exc}")

        try:
            response = await self._http.get(
                f"{self._gamma_api}/markets",
                params={"slug": slug},
                headers=GAMMA_HEADERS,
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list) and data:
                return data[0], f"{self._gamma_api}/markets?slug={slug}"
            if isinstance(data, dict):
                record = self._extract_record(data)
                if record is not None:
                    return record, f"{self._gamma_api}/markets?slug={slug}"
        except Exception as exc:
            errors.append(f"markets -> {exc}")

        for template in self._fallback_templates:
            url = template.format(slug=slug)
            try:
                response = await self._http.get(url)
                response.raise_for_status()
                record = self._extract_record(response.json())
                if record is not None:
                    return record, url
            except Exception as exc:
                errors.append(f"{url} -> {exc}")

        raise DataSourceError(f"Failed to fetch market {slug}. Errors: {' | '.join(errors)}")

    async def _build_market_snapshot(
        self,
        record: dict[str, Any],
        market_slug: str,
        source: str,
    ) -> MarketSnapshot:
        yes_token_id = self._extract_yes_token_id(record)
        book = await self._fetch_orderbook(yes_token_id) if yes_token_id else {}

        bids = book.get("bids", []) if isinstance(book, dict) else []
        asks = book.get("asks", []) if isinstance(book, dict) else []
        best_bid = self._best_price(bids, side="bid")
        best_ask = self._best_price(asks, side="ask")

        fallback_price = self._extract_price(record)
        bid = best_bid if best_bid is not None else fallback_price
        ask = best_ask if best_ask is not None else fallback_price
        if ask < bid:
            ask = bid

        price = ask if best_ask is not None else fallback_price
        orderbook_imbalance = self._compute_imbalance(bids, asks)

        now = datetime.now(timezone.utc)
        end_time = self._extract_datetime(record, ["endDate", "endTime", "end_date_iso", "closeTime", "expiresAt"])
        time_left_seconds = max(int((end_time - now).total_seconds()), 0) if end_time else 300

        resolved = self._extract_bool(record, ["resolved", "isResolved", "closed", "isClosed"])
        if resolved is None:
            active = self._extract_bool(record, ["active", "isActive"])
            resolved = bool(active is False and time_left_seconds == 0)

        outcome = self._extract_outcome(record, yes_token_id=yes_token_id, resolved=bool(resolved))

        btc_reference = self._latest_btc_price
        if btc_reference is None:
            btc_reference = self._extract_btc_reference(record)
        if btc_reference is None:
            btc_reference = price

        return MarketSnapshot(
            market_slug=market_slug,
            price=float(max(min(price, 1.0), 0.0)),
            bid=float(max(min(bid, 1.0), 0.0)),
            ask=float(max(min(ask, 1.0), 0.0)),
            orderbook_imbalance=orderbook_imbalance,
            time_left_seconds=time_left_seconds,
            resolved=bool(resolved),
            outcome=outcome,
            btc_reference_price=float(btc_reference),
            timestamp=now,
            source=source,
        )

    async def _fetch_orderbook(self, token_id: str | None) -> dict[str, Any]:
        if not token_id:
            return {}
        try:
            response = await self._http.get(f"{self._clob_host}/book", params={"token_id": token_id})
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _extract_yes_token_id(self, record: dict[str, Any]) -> str | None:
        clob_ids = self._parse_json_or_csv(record.get("clobTokenIds"))
        outcomes = [str(o).strip().strip('"').strip("'").lower() for o in self._parse_json_or_csv(record.get("outcomes"))]

        yes_idx = 0
        for idx, outcome in enumerate(outcomes):
            if outcome in {"up", "yes", "true"}:
                yes_idx = idx
                break
        if clob_ids and len(clob_ids) > yes_idx:
            return str(clob_ids[yes_idx])

        tokens = record.get("tokens")
        if isinstance(tokens, list):
            for token in tokens:
                outcome = str(token.get("outcome", "")).lower()
                if outcome in {"up", "yes", "true"}:
                    tid = token.get("token_id") or token.get("id")
                    if tid:
                        return str(tid)
            if tokens:
                tid = tokens[0].get("token_id") or tokens[0].get("id")
                if tid:
                    return str(tid)
        return None

    def _extract_price(self, record: dict[str, Any]) -> float:
        direct = self._extract_float(record, ["price", "lastTradePrice", "last_price", "yesPrice", "probability"])
        if direct is not None:
            return direct

        outcome_prices = self._parse_json_or_csv(record.get("outcomePrices"))
        if outcome_prices:
            value = self._safe_float(outcome_prices[0])
            if value is not None:
                return value

        token_prices = self._parse_json_or_csv(record.get("tokenPrices"))
        if token_prices:
            value = self._safe_float(token_prices[0])
            if value is not None:
                return value

        raise DataSourceError("Unable to parse market price.")

    def _extract_btc_reference(self, record: dict[str, Any]) -> float | None:
        return self._extract_float(
            record,
            [
                "oraclePrice",
                "indexPrice",
                "underlyingPrice",
                "referencePrice",
                "assetPrice",
                "btcPrice",
                "underlying_price",
            ],
        )

    def _extract_outcome(self, record: dict[str, Any], yes_token_id: str | None, resolved: bool) -> bool | None:
        for key in ["outcome", "winningOutcome", "winner", "result", "winning_outcome"]:
            if key not in record:
                continue
            parsed = self._parse_outcome_value(record[key], yes_token_id=yes_token_id)
            if parsed is not None:
                return parsed

        tokens = record.get("tokens")
        if isinstance(tokens, list):
            for token in tokens:
                winner = token.get("winner")
                if bool(winner):
                    token_outcome = token.get("outcome")
                    parsed = self._parse_outcome_value(token_outcome, yes_token_id=yes_token_id)
                    if parsed is not None:
                        return parsed
                    token_id = token.get("token_id") or token.get("id")
                    if yes_token_id and token_id:
                        return str(token_id) == str(yes_token_id)

        if resolved:
            outcome_prices = self._parse_json_or_csv(record.get("outcomePrices"))
            if len(outcome_prices) >= 2:
                p0 = self._safe_float(outcome_prices[0])
                p1 = self._safe_float(outcome_prices[1])
                if p0 is not None and p1 is not None and p0 != p1:
                    return p0 > p1
        return None

    def _parse_outcome_value(self, value: Any, yes_token_id: str | None) -> bool | None:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.lower().strip()
            if lowered in {"yes", "true", "1", "up", "above"}:
                return True
            if lowered in {"no", "false", "0", "down", "below"}:
                return False
            if yes_token_id and lowered == str(yes_token_id).lower():
                return True
        return None

    @staticmethod
    def _best_price(levels: list[Any], side: str) -> float | None:
        prices: list[float] = []
        for level in levels:
            if not isinstance(level, dict):
                continue
            value = PolymarketClient._safe_float(level.get("price"))
            if value is not None:
                prices.append(value)
        if not prices:
            return None
        return max(prices) if side == "bid" else min(prices)

    @staticmethod
    def _compute_imbalance(bids: list[Any], asks: list[Any], depth: int = 10) -> float:
        bid_depth = 0.0
        ask_depth = 0.0
        for level in bids[:depth]:
            if isinstance(level, dict):
                p = PolymarketClient._safe_float(level.get("price"))
                s = PolymarketClient._safe_float(level.get("size"))
                if p is not None and s is not None:
                    bid_depth += p * s
        for level in asks[:depth]:
            if isinstance(level, dict):
                p = PolymarketClient._safe_float(level.get("price"))
                s = PolymarketClient._safe_float(level.get("size"))
                if p is not None and s is not None:
                    ask_depth += p * s
        total = bid_depth + ask_depth
        if total <= 0:
            return 0.0
        return (bid_depth - ask_depth) / total

    @staticmethod
    def _extract_record(payload: Any) -> dict[str, Any] | None:
        if isinstance(payload, list):
            return payload[0] if payload else None
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), list):
                data = payload.get("data")
                return data[0] if data else None
            if isinstance(payload.get("data"), dict):
                return payload["data"]
            if isinstance(payload.get("markets"), list):
                markets = payload["markets"]
                return markets[0] if markets else None
            return payload
        return None

    @staticmethod
    def _extract_float(record: dict[str, Any], keys: list[str]) -> float | None:
        for key in keys:
            if key in record:
                value = PolymarketClient._safe_float(record[key])
                if value is not None:
                    return value
        return None

    @staticmethod
    def _extract_datetime(record: dict[str, Any], keys: list[str]) -> datetime | None:
        for key in keys:
            if key not in record:
                continue
            raw = record[key]
            if raw is None:
                continue
            if isinstance(raw, (int, float)):
                if raw > 1e12:
                    return datetime.fromtimestamp(raw / 1000.0, tz=timezone.utc)
                return datetime.fromtimestamp(raw, tz=timezone.utc)
            if isinstance(raw, str):
                try:
                    normalized = raw.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(normalized)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc)
                except ValueError:
                    continue
        return None

    @staticmethod
    def _extract_bool(record: dict[str, Any], keys: list[str]) -> bool | None:
        for key in keys:
            if key not in record:
                continue
            raw = record[key]
            if isinstance(raw, bool):
                return raw
            if isinstance(raw, (int, float)):
                return bool(raw)
            if isinstance(raw, str):
                lowered = raw.lower().strip()
                if lowered in {"true", "yes", "1"}:
                    return True
                if lowered in {"false", "no", "0"}:
                    return False
        return None

    @staticmethod
    def _parse_json_or_csv(value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                    if isinstance(parsed, list):
                        return parsed
                except Exception:
                    pass
            return [part.strip().strip('"').strip("'") for part in stripped.split(",") if part.strip()]
        return []

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
