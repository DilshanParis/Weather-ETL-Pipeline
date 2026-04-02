from __future__ import annotations

import argparse
import logging

from src.pipeline import run_weather_etl


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the weather ETL pipeline")
    parser.add_argument(
        "--city",
        help="City name (defaults to DEFAULT_CITY from .env)",
        default=None,
    )
    parser.add_argument(
        "--table",
        help="Postgres table name (default: weather_data)",
        default="weather_data",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    run_weather_etl(city=args.city, table_name=args.table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
