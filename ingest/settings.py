"""Configuration, read from the environment and from indicators.yml.

Everything the pipeline needs to know that differs between a laptop, CI and a
server lives here, and nothing else reads os.environ directly. That is what makes
the fixture mode possible: CI flips one variable and the whole pipeline runs with
no network.
"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PACKAGE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_DIR.parent


class Indicator(BaseModel):
    """One World Bank series, as declared in indicators.yml."""

    id: str
    label: str
    non_null_of_330: int | None = None


class SourceCatalogue(BaseModel):
    countries: list[str]
    indicators: list[Indicator]

    @field_validator("countries")
    @classmethod
    def _iso3_only(cls, values: list[str]) -> list[str]:
        # The observation endpoint returns both an ISO2 code (under country.id)
        # and an ISO3 code (under countryiso3code). The dimension table is keyed on
        # ISO3, so a two-letter code slipping in here would produce rows that can
        # never join. Cheaper to reject at config load than to debug later.
        bad = [v for v in values if len(v) != 3 or not v.isalpha() or v != v.upper()]
        if bad:
            raise ValueError(f"countries must be uppercase ISO3 codes, got: {bad}")
        return values

    @property
    def country_path(self) -> str:
        """The API separates countries in the URL path with semicolons."""
        return ";".join(self.countries)


class Settings(BaseSettings):
    """Runtime configuration.

    Defaults here match .env.example so a developer who forgets to source .env
    gets a working local setup rather than a confusing failure, EXCEPT for the
    passwords, which have no default on purpose: a database password that silently
    falls back to a default is how a pipeline ends up connecting somewhere nobody
    intended.
    """

    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- source API ---------------------------------------------------------
    wb_api_base: str = "https://api.worldbank.org/v2"
    wb_per_page: int = 100
    wb_request_timeout_seconds: float = 30.0
    wb_max_retries: int = 5

    # live hits the public API; fixture replays committed JSON so the pipeline is
    # runnable with no network at all. CI uses fixture, which is what makes the
    # ingestion tests deterministic instead of dependent on a third party.
    source_api_mode: Literal["live", "fixture"] = "live"

    # --- PostgreSQL --------------------------------------------------------
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "wbsource"
    postgres_user: str = "wbapp"
    postgres_password: str = Field(...)

    # --- ClickHouse (read only, for the verification checks) ---------------
    clickhouse_host: str = "clickhouse"
    clickhouse_http_port: int = 8123
    clickhouse_user: str = "analytics"
    clickhouse_password: str = Field(...)

    # --- thresholds --------------------------------------------------------
    cdc_lag_error_seconds: int = 300

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


@functools.lru_cache(maxsize=1)
def get_catalogue() -> SourceCatalogue:
    raw = yaml.safe_load((PACKAGE_DIR / "indicators.yml").read_text(encoding="utf-8"))
    return SourceCatalogue.model_validate(raw)
