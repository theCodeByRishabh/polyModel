from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


ModeType = Literal["observation", "shadow", "learning"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = Field(alias="DATABASE_URL")
    mode: ModeType = Field(default="learning", alias="MODE")
    market_slug: str = Field(default="bitcoin-above-0-in-5-minutes", alias="MARKET_SLUG")

    poll_interval_seconds: int = Field(default=5, alias="POLL_INTERVAL_SECONDS")
    retrain_min_new_resolved: int = Field(default=50, alias="RETRAIN_MIN_NEW_RESOLVED")
    retrain_min_interval_seconds: int = Field(default=600, alias="RETRAIN_MIN_INTERVAL_SECONDS")
    cleanup_interval_seconds: int = Field(default=1800, alias="CLEANUP_INTERVAL_SECONDS")

    keep_raw_hours: int = Field(default=24, alias="KEEP_RAW_HOURS")
    keep_aggregated_days: int = Field(default=7, alias="KEEP_AGGREGATED_DAYS")
    max_db_size_mb: int = Field(default=500, alias="MAX_DB_SIZE_MB")

    model_path: str = Field(default="models/model.pkl", alias="MODEL_PATH")
    meta_model_path: str = Field(default="models/meta_model.pkl", alias="META_MODEL_PATH")

    polymarket_market_url_template: str = Field(
        default="https://clob.polymarket.com/markets/{slug}",
        alias="POLYMARKET_MARKET_URL_TEMPLATE",
    )
    polymarket_fallback_url_template: str = Field(
        default="https://strapi-matic.poly.market/markets?slug={slug}",
        alias="POLYMARKET_FALLBACK_URL_TEMPLATE",
    )
    gamma_api_base: str = Field(default="https://gamma-api.polymarket.com", alias="GAMMA_API_BASE")
    clob_host: str = Field(default="https://clob.polymarket.com", alias="CLOB_HOST")
    polymarket_rtds_ws_url: str = Field(
        default="wss://ws-live-data.polymarket.com",
        alias="POLYMARKET_RTDS_WS_URL",
    )
    btc_reference_market_slug: str = Field(
        default="",
        alias="BTC_REFERENCE_MARKET_SLUG",
    )

    request_timeout_seconds: float = Field(default=8.0, alias="REQUEST_TIMEOUT_SECONDS")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @property
    def model_file(self) -> Path:
        return Path(self.model_path)

    @property
    def meta_model_file(self) -> Path:
        return Path(self.meta_model_path)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
