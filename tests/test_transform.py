import pandas as pd

from src.transform import weather_json_to_dataframe


def test_weather_json_to_dataframe_basic() -> None:
    payload = {
        "name": "Colombo",
        "main": {"temp": 30.5, "humidity": 70, "pressure": 1012},
        "weather": [{"description": "clear sky"}],
        "dt": 1735689600,
    }

    df = weather_json_to_dataframe(payload)

    assert list(df.columns) == [
        "city",
        "temperature",
        "humidity",
        "pressure",
        "weather_description",
        "timestamp",
    ]
    assert len(df) == 1

    row = df.iloc[0]
    assert row["city"] == "Colombo"
    assert float(row["temperature"]) == 30.5
    assert int(row["humidity"]) == 70
    assert int(row["pressure"]) == 1012
    assert row["weather_description"] == "clear sky"

    ts = row["timestamp"]
    assert isinstance(ts, pd.Timestamp)
    assert ts.tz is not None
