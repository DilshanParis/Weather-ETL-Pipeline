from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

from .features import FEATURE_COLUMNS, build_feature_dataframe

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModelTrainingError(Exception):
    message: str
    details: dict | None = None

    def __str__(self) -> str:  # pragma: no cover
        return self.message


def train_and_save_model(
    *,
    model_path: str | Path = "model.pkl",
    table_name: str = "weather_data",
    city: str | None = None,
    estimator: str = "random_forest",
    test_size: float = 0.2,
    random_state: int = 42,
) -> dict:
    """Train a regression model to predict the next temperature and save it.

    The dataset is built from PostgreSQL using `build_feature_dataframe`.

    Args:
        model_path: Where to save the artifact (joblib).
        table_name: Source table in Postgres.
        city: Optional city filter.
        estimator: `random_forest` or `linear`.
        test_size: Fraction of rows used for evaluation (time-based split).
        random_state: Random seed for the estimator (where applicable).

    Returns:
        The saved artifact dictionary.
    """

    df = build_feature_dataframe(table_name=table_name, city=city)
    if df.empty:
        raise ModelTrainingError(
            "No training data available. Run ETL to populate Postgres first.",
            details={"table": table_name, "city": city},
        )

    X = df.loc[:, FEATURE_COLUMNS].astype(float)
    y = df["target_temperature"].astype(float)

    if not 0.0 < test_size < 1.0:
        raise ValueError("test_size must be between 0 and 1")

    # Time-based split (no shuffle) to avoid leaking future into training.
    split_idx = max(1, int(len(df) * (1.0 - test_size)))
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    if estimator == "linear":
        model = LinearRegression()
    elif estimator == "random_forest":
        model = RandomForestRegressor(
            n_estimators=200,
            random_state=random_state,
            n_jobs=-1,
        )
    else:
        raise ValueError("estimator must be 'random_forest' or 'linear'")

    try:
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test) if len(X_test) else []
        mse = float(mean_squared_error(y_test, y_pred)) if len(X_test) else float("nan")
    except Exception as exc:  # noqa: BLE001
        raise ModelTrainingError(
            "Model training failed",
            details={"error": str(exc), "rows": len(df)},
        ) from exc

    artifact = {
        "model": model,
        "feature_columns": FEATURE_COLUMNS,
        "trained_at": datetime.now(tz=timezone.utc).isoformat(),
        "table_name": table_name,
        "city": city,
        "estimator": estimator,
        "row_count": int(len(df)),
        "mse": mse,
        "sklearn_version": getattr(sklearn, "__version__", None),
    }

    model_path = Path(model_path)
    joblib.dump(artifact, model_path)

    logger.info("Trained %s model; mse=%.4f; saved=%s", estimator, mse, model_path)
    return artifact


def main() -> int:
    parser = argparse.ArgumentParser(description="Train a weather temperature prediction model")
    parser.add_argument("--model-path", default="model.pkl", help="Output path for model.pkl")
    parser.add_argument("--table", default="weather_data", help="Source table (default: weather_data)")
    parser.add_argument("--city", default=None, help="Optional city filter")
    parser.add_argument(
        "--estimator",
        default="random_forest",
        choices=["random_forest", "linear"],
        help="Model type",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Holdout fraction for evaluation (time-based split)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    artifact = train_and_save_model(
        model_path=args.model_path,
        table_name=args.table,
        city=args.city,
        estimator=args.estimator,
        test_size=args.test_size,
    )

    # Print a tiny, human-friendly summary.
    mse = artifact.get("mse")
    rows = artifact.get("row_count")
    print(f"rows={rows} mse={mse} saved={args.model_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
