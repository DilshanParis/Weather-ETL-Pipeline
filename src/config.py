from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import find_dotenv, load_dotenv


@dataclass(frozen=True)
class Settings:
    openweather_api_key: str
    openweather_base_url: str = "https://api.openweathermap.org/data/2.5/weather"
    openweather_units: str = "metric"
    default_city: str = "Colombo"

    database_url: str | None = None
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "weather"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"


def get_settings() -> Settings:
    """Load configuration from environment variables.

    Reads from a local `.env` file if present (via python-dotenv).
    """

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    # Allow a host-specific override file for local development.
    # Useful when `.env` is tuned for Docker (e.g., POSTGRES_HOST=host.docker.internal).
    load_dotenv(os.path.join(project_root, ".env.local"), override=False)

    # Load `.env` starting from the current working directory and walking up.
    # This works well for local runs and also for Airflow (repo is mounted into the container).
    load_dotenv(find_dotenv(usecwd=True), override=False)

    # NOTE: The API key is required only when calling the weather API.
    # Other modules (DB load/dashboard) should still work without it.
    api_key = os.getenv("OPENWEATHER_API_KEY", "").strip()

    database_url = os.getenv("DATABASE_URL")

    return Settings(
        openweather_api_key=api_key,
        openweather_base_url=os.getenv(
            "OPENWEATHER_BASE_URL", "https://api.openweathermap.org/data/2.5/weather"
        ).strip(),
        openweather_units=os.getenv("OPENWEATHER_UNITS", "metric").strip(),
        default_city=os.getenv("DEFAULT_CITY", "Colombo").strip(),
        database_url=database_url.strip() if database_url else None,
        postgres_host=os.getenv("POSTGRES_HOST", "localhost").strip(),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "weather").strip(),
        postgres_user=os.getenv("POSTGRES_USER", "postgres").strip(),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "postgres").strip(),
    )


def build_database_url(settings: Settings) -> str:
    """Build a SQLAlchemy Postgres URL from settings.

    If `settings.database_url` is provided, returns it as-is.
    """

    if settings.database_url:
        return settings.database_url

    # Percent-encoding is omitted for simplicity; keep passwords URL-safe or set DATABASE_URL directly.
    return (
        "postgresql+psycopg2://"
        f"{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
    )
