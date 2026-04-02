from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from .config import Settings, get_settings


@dataclass(frozen=True)
class WeatherApiError(Exception):
    message: str
    status_code: int | None = None
    details: dict[str, Any] | None = None

    def __str__(self) -> str:  # pragma: no cover
        base = self.message
        if self.status_code is not None:
            base = f"{base} (status_code={self.status_code})"
        return base


def fetch_weather_json(
    city: str,
    *,
    settings: Settings | None = None,
    timeout_s: float = 15.0,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Fetch weather data from OpenWeatherMap.

    Args:
        city: City name, e.g. "Colombo".
        settings: Optional Settings; if omitted, loaded from environment.
        timeout_s: Requests timeout in seconds.
        session: Optional requests session (useful for tests).

    Returns:
        Parsed JSON payload as a dictionary.

    Raises:
        WeatherApiError: For network errors, non-2xx responses, or invalid JSON.
        RuntimeError: If `OPENWEATHER_API_KEY` is missing when settings are loaded.
    """

    city = (city or "").strip()
    if not city:
        raise ValueError("city must be a non-empty string")

    effective_settings = settings or get_settings()
    if not (effective_settings.openweather_api_key or "").strip():
        raise RuntimeError(
            "Missing OPENWEATHER_API_KEY. Create a .env file (see .env.example) or set it in your environment."
        )

    http = session or requests.Session()
    params = {
        "q": city,
        "appid": effective_settings.openweather_api_key,
        "units": effective_settings.openweather_units,
    }

    try:
        response = http.get(
            effective_settings.openweather_base_url,
            params=params,
            timeout=timeout_s,
        )
    except requests.RequestException as exc:
        raise WeatherApiError(
            "Failed to call OpenWeatherMap API",
            details={"error": str(exc), "city": city},
        ) from exc

    if not response.ok:
        details: dict[str, Any] | None = None
        try:
            details = response.json()
        except ValueError:
            details = {"body": (response.text or "").strip()[:500]}

        raise WeatherApiError(
            "OpenWeatherMap API returned an error",
            status_code=response.status_code,
            details=details,
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise WeatherApiError(
            "OpenWeatherMap response was not valid JSON",
            status_code=response.status_code,
            details={"body": (response.text or "").strip()[:500]},
        ) from exc

    if not isinstance(payload, dict):
        raise WeatherApiError(
            "Unexpected OpenWeatherMap payload type",
            status_code=response.status_code,
            details={"type": type(payload).__name__},
        )

    return payload
