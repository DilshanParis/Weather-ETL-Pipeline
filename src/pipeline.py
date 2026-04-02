from __future__ import annotations

import logging

from .config import Settings, get_settings
from .extract import WeatherApiError, fetch_weather_json
from .load import DatabaseLoadError, load_weather_dataframe
from .transform import weather_json_to_dataframe

logger = logging.getLogger(__name__)


def run_weather_etl(
    *,
    city: str | None = None,
    table_name: str = "weather_data",
    settings: Settings | None = None,
) -> None:
    """Run the end-to-end ETL: extract -> transform -> load.

    Args:
        city: Optional city name. If omitted, uses `DEFAULT_CITY` from env.
        table_name: Target table name in Postgres.
        settings: Optional Settings object.
    """

    effective_settings = settings or get_settings()
    effective_city = (city or effective_settings.default_city).strip()
    if not effective_city:
        raise ValueError("city must be provided or DEFAULT_CITY must be set")

    logger.info("Starting ETL for city=%s", effective_city)

    try:
        payload = fetch_weather_json(effective_city, settings=effective_settings)
        df = weather_json_to_dataframe(payload)
        load_weather_dataframe(df, table_name=table_name, settings=effective_settings)
    except (WeatherApiError, DatabaseLoadError):
        logger.exception("ETL failed")
        raise

    logger.info("ETL succeeded: rows_loaded=%s table=%s", len(df.index), table_name)
