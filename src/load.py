from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.types import DateTime, Float, Integer, Text

from .config import Settings, build_database_url, get_settings


@dataclass(frozen=True)
class DatabaseLoadError(Exception):
    message: str
    details: dict | None = None

    def __str__(self) -> str:  # pragma: no cover
        return self.message


def load_weather_dataframe(
    df: pd.DataFrame,
    *,
    table_name: str = "weather_data",
    settings: Settings | None = None,
    engine: Engine | None = None,
) -> None:
    """Load a weather DataFrame into PostgreSQL.

    - If the table does not exist, it will be created.
    - New rows are appended.

    Args:
        df: DataFrame produced by `weather_json_to_dataframe`.
        table_name: Target table name (default: weather_data).
        settings: Optional Settings; if omitted, loaded from environment.
        engine: Optional SQLAlchemy Engine (useful for tests).
    """

    if df is None or df.empty:
        return

    required_columns = [
        "city",
        "temperature",
        "humidity",
        "pressure",
        "weather_description",
        "timestamp",
    ]
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame is missing required columns: {missing}")

    # Keep schema stable and ignore unexpected columns.
    df_to_load = df.loc[:, required_columns].copy()

    effective_settings = settings or get_settings()
    effective_engine = engine or create_engine(build_database_url(effective_settings))

    dtype_map = {
        "city": Text(),
        "temperature": Float(),
        "humidity": Integer(),
        "pressure": Integer(),
        "weather_description": Text(),
        "timestamp": DateTime(timezone=True),
    }

    try:
        df_to_load.to_sql(
            name=table_name,
            con=effective_engine,
            if_exists="append",
            index=False,
            dtype=dtype_map,
            method="multi",
        )
    except Exception as exc:  # noqa: BLE001
        raise DatabaseLoadError(
            "Failed to load DataFrame into PostgreSQL",
            details={"table": table_name, "error": str(exc)},
        ) from exc
