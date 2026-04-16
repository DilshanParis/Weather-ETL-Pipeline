from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import Settings, build_database_url, get_settings


@dataclass(frozen=True)
class FeatureEngineeringError(Exception):
    message: str
    details: dict | None = None

    def __str__(self) -> str:  # pragma: no cover
        return self.message


FEATURE_COLUMNS: list[str] = [
    "temp_roll3",
    "temp_diff_prev",
    "humidity_roll3",
]


def load_weather_data(
    *,
    table_name: str = "weather_data",
    settings: Settings | None = None,
    engine: Engine | None = None,
    city: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Load weather observations from PostgreSQL.

    Args:
        table_name: Source table name (default: weather_data).
        settings: Optional Settings; if omitted, loaded from environment.
        engine: Optional SQLAlchemy Engine (useful for tests).
        city: Optional city filter.
        limit: Optional maximum number of rows (applied after ordering).

    Returns:
        DataFrame with at least: city, temperature, humidity, pressure, timestamp.
    """

    # Avoid SQL injection by only allowing simple identifier names.
    # (SQLAlchemy parameterization does not apply to identifiers like table names.)
    if not isinstance(table_name, str) or not table_name:
        raise ValueError("table_name must be a non-empty string")
    if not table_name.replace("_", "a").isalnum() or table_name[0].isdigit():
        raise ValueError("table_name must be a simple identifier (letters/numbers/underscore)")

    effective_settings = settings or get_settings()
    effective_engine = engine or create_engine(build_database_url(effective_settings))

    where = ""
    params: dict[str, object] = {}
    if city is not None and (city := city.strip()):
        where = "WHERE city = :city"
        params["city"] = city

    limit_sql = ""
    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        limit_sql = "LIMIT :limit"
        params["limit"] = int(limit)

    query = text(
        f"""
        SELECT city, temperature, humidity, pressure, weather_description, timestamp
        FROM {table_name}
        {where}
        ORDER BY city, timestamp
        {limit_sql}
        """
    )

    return pd.read_sql_query(query, con=effective_engine, params=params)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create ML-ready features from raw weather observations.

    Features created (per-city, ordered by timestamp):
    - rolling average temperature over the last 3 records
    - temperature difference from previous record
    - rolling average humidity over the last 3 records

    Also creates a supervised learning target for *next* temperature:
    - target_temperature = temperature shifted by -1 (next record)

    This matches the Phase 2 goal of predicting a future temperature without
    leaking future information into training features.
    """

    if df is None or df.empty:
        return pd.DataFrame()

    required = {"city", "temperature", "humidity", "timestamp"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Input DataFrame is missing required columns: {missing}")

    out = df.copy()

    out["city"] = out["city"].astype("string")
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out["temperature"] = pd.to_numeric(out["temperature"], errors="coerce")
    out["humidity"] = pd.to_numeric(out["humidity"], errors="coerce")

    # Drop rows without a usable timestamp; ordering is undefined otherwise.
    out = out.dropna(subset=["timestamp"]).copy()
    if out.empty:
        return pd.DataFrame()

    out = out.sort_values(["city", "timestamp"], ascending=True).reset_index(drop=True)

    # Handle missing values using per-city forward/back fill.
    for col in ["temperature", "humidity"]:
        out[col] = (
            out.groupby("city", sort=False)[col]
            .apply(lambda s: s.ffill().bfill())
            .reset_index(level=0, drop=True)
        )

    # If an entire city partition is NaN, ffill/bfill won't help; drop those rows.
    out = out.dropna(subset=["temperature", "humidity"]).copy()
    if out.empty:
        return pd.DataFrame()

    temp = out.groupby("city", sort=False)["temperature"]
    hum = out.groupby("city", sort=False)["humidity"]

    out["temp_roll3"] = (
        temp.rolling(window=3, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    out["temp_diff_prev"] = temp.diff().reset_index(level=0, drop=True).fillna(0.0)
    out["humidity_roll3"] = (
        hum.rolling(window=3, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    # Predict the *next* temperature.
    out["target_temperature"] = temp.shift(-1).reset_index(level=0, drop=True)

    # Final cleanup.
    out["temp_roll3"] = pd.to_numeric(out["temp_roll3"], errors="coerce")
    out["temp_diff_prev"] = pd.to_numeric(out["temp_diff_prev"], errors="coerce")
    out["humidity_roll3"] = pd.to_numeric(out["humidity_roll3"], errors="coerce")

    out = out.dropna(subset=FEATURE_COLUMNS + ["target_temperature"]).copy()

    return out


def build_feature_dataframe(
    *,
    table_name: str = "weather_data",
    settings: Settings | None = None,
    engine: Engine | None = None,
    city: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Load raw data from Postgres and return an ML-ready DataFrame."""

    try:
        raw = load_weather_data(
            table_name=table_name,
            settings=settings,
            engine=engine,
            city=city,
            limit=limit,
        )
        return engineer_features(raw)
    except Exception as exc:  # noqa: BLE001
        raise FeatureEngineeringError(
            "Failed to build feature DataFrame",
            details={"table": table_name, "city": city, "error": str(exc)},
        ) from exc
