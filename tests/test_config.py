from src.config import Settings, build_database_url


def test_build_database_url_prefers_database_url() -> None:
    settings = Settings(openweather_api_key="x", database_url="postgresql+psycopg2://u:p@h:5432/db")
    assert build_database_url(settings) == "postgresql+psycopg2://u:p@h:5432/db"


def test_build_database_url_from_parts() -> None:
    settings = Settings(
        openweather_api_key="x",
        database_url=None,
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="weather",
        postgres_user="postgres",
        postgres_password="postgres",
    )

    assert (
        build_database_url(settings)
        == "postgresql+psycopg2://postgres:postgres@localhost:5432/weather"
    )
