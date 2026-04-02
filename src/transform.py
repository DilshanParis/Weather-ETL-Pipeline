from __future__ import annotations

import pandas as pd


def weather_json_to_dataframe(payload: dict) -> pd.DataFrame:
    """Transform OpenWeatherMap JSON payload into a normalized DataFrame.
    """

    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")

    city = payload.get("name")
    main = payload.get("main") or {}
    weather = payload.get("weather") or []

    temperature = main.get("temp")
    humidity = main.get("humidity")
    pressure = main.get("pressure")

    description = None
    if isinstance(weather, list) and weather:
        first = weather[0] or {}
        if isinstance(first, dict):
            description = first.get("description")

    # OpenWeatherMap uses `dt` (UTC epoch seconds).
    dt_epoch = payload.get("dt")
    timestamp = (
        pd.to_datetime(dt_epoch, unit="s", utc=True, errors="coerce")
        if dt_epoch is not None
        else pd.NaT
    )

    df = pd.DataFrame(
        [
            {
                "city": city,
                "temperature": temperature,
                "humidity": humidity,
                "pressure": pressure,
                "weather_description": description,
                "timestamp": timestamp,
            }
        ]
    )

    # Normalize dtypes while keeping missing values.
    df["city"] = df["city"].astype("string")
    df["weather_description"] = df["weather_description"].astype("string")
    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce").astype("Float64")
    df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce").astype("Int64")
    df["pressure"] = pd.to_numeric(df["pressure"], errors="coerce").astype("Int64")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    return df
