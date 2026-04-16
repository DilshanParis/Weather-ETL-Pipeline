from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import joblib
import pandas as pd

from .features import FEATURE_COLUMNS, build_feature_dataframe

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PredictionError(Exception):
    message: str
    details: dict | None = None

    def __str__(self) -> str:  # pragma: no cover
        return self.message


def predict_next_temperature(
    *,
    model_path: str | Path = "model.pkl",
    table_name: str = "weather_data",
    city: str | None = None,
) -> pd.DataFrame:
    """Load a trained model and predict the next temperature.

    Returns a DataFrame with columns: city, timestamp, predicted_temperature.
    The prediction is made from the latest available record per city.
    """

    model_path = Path(model_path)
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")

    artifact = joblib.load(model_path)
    model = artifact.get("model")
    feature_columns = artifact.get("feature_columns") or FEATURE_COLUMNS

    df = build_feature_dataframe(table_name=table_name, city=city)
    if df.empty:
        raise PredictionError(
            "No data available for prediction. Run ETL to populate Postgres first.",
            details={"table": table_name, "city": city},
        )

    # Use the latest row per city as the basis for predicting the next temperature.
    latest_idx = df.groupby("city", sort=False)["timestamp"].idxmax()
    latest = df.loc[latest_idx].sort_values(["city", "timestamp"]).reset_index(drop=True)

    X_latest = latest.loc[:, list(feature_columns)].astype(float)

    try:
        preds = model.predict(X_latest)
    except Exception as exc:  # noqa: BLE001
        raise PredictionError(
            "Prediction failed",
            details={"error": str(exc), "rows": int(len(latest))},
        ) from exc

    result = latest.loc[:, ["city", "timestamp"]].copy()
    result["predicted_temperature"] = pd.to_numeric(preds, errors="coerce")

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Predict next temperature using a trained model")
    parser.add_argument("--model-path", default="model.pkl", help="Path to model.pkl")
    parser.add_argument("--table", default="weather_data", help="Source table (default: weather_data)")
    parser.add_argument("--city", default=None, help="Optional city filter")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    df = predict_next_temperature(
        model_path=args.model_path,
        table_name=args.table,
        city=args.city,
    )

    # Print minimal output for CLI usage.
    if args.city:
        row = df.iloc[0]
        print(
            f"city={row['city']} timestamp={row['timestamp']} predicted_temperature={row['predicted_temperature']:.2f}"
        )
    else:
        print(df.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
