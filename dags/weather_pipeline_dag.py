"""Weather ETL Airflow DAG.

DAG id: `weather_pipeline`
Schedule: hourly

Notes for Windows:
- Apache Airflow is not officially supported on native Windows.
- Run this DAG using WSL2 or Docker, and configure environment variables there.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta

logger = logging.getLogger(__name__)


# Make project root importable when Airflow parses DAGs.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except Exception:  # noqa: BLE001
    # Allows importing this file on machines/environments without Airflow installed.
    DAG = None  # type: ignore[assignment]
    PythonOperator = None  # type: ignore[assignment]
else:

    import pendulum

    def _run_weather_etl() -> None:
        from src.pipeline import run_weather_etl

        logger.info("Triggering weather ETL")
        run_weather_etl()


    def _run_feature_engineering() -> None:
        from src.features import build_feature_dataframe

        df = build_feature_dataframe()
        logger.info("Feature engineering done: rows=%s", len(df.index))


    def _run_train_model() -> None:
        from src.train_model import train_and_save_model

        artifact = train_and_save_model()
        logger.info(
            "Model trained: estimator=%s mse=%s rows=%s",
            artifact.get("estimator"),
            artifact.get("mse"),
            artifact.get("row_count"),
        )


    def _run_predict() -> None:
        from src.predict import predict_next_temperature

        df = predict_next_temperature()
        logger.info("Predictions generated: rows=%s", len(df.index))


    default_args = {
        "owner": "weather-etl",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id="weather_pipeline",
        description="Extract weather data from OpenWeatherMap and load into Postgres",
        default_args=default_args,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        schedule="@hourly",
        catchup=False,
        max_active_runs=1,
        tags=["weather", "etl"],
    ) as dag:
        run_etl = PythonOperator(
            task_id="run_weather_etl",
            python_callable=_run_weather_etl,
        )

        run_features = PythonOperator(
            task_id="run_feature_engineering",
            python_callable=_run_feature_engineering,
        )

        train_model = PythonOperator(
            task_id="train_model",
            python_callable=_run_train_model,
        )

        predict = PythonOperator(
            task_id="predict_temperature",
            python_callable=_run_predict,
        )

        run_etl >> run_features >> train_model >> predict

