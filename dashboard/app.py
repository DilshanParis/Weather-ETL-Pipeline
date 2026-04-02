from __future__ import annotations

import os
import sys

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


# Ensure `import src...` works even if Streamlit is launched from the dashboard folder.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
	sys.path.insert(0, PROJECT_ROOT)

from src.config import build_database_url, get_settings  # noqa: E402


st.set_page_config(page_title="Weather Dashboard", layout="wide")

st.title("Weather Dashboard")
st.caption("PostgreSQL-backed weather trends and latest reading")


@st.cache_data(ttl=300)
def load_weather_data(limit: int = 2000) -> pd.DataFrame:
	settings = get_settings()
	engine = create_engine(build_database_url(settings))

	query = text(
		"""
		SELECT
			city,
			temperature,
			humidity,
			pressure,
			weather_description,
			timestamp
		FROM weather_data
		ORDER BY timestamp DESC
		LIMIT :limit
		"""
	)

	with engine.connect() as conn:
		df = pd.read_sql(query, conn, params={"limit": int(limit)})

	if not df.empty:
		df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
		df = df.dropna(subset=["timestamp"]).sort_values("timestamp")

	return df


with st.sidebar:
	st.header("Settings")
	limit = st.number_input(
		"Rows to load",
		min_value=100,
		max_value=20000,
		value=2000,
		step=100,
		help="Loads the most recent rows (sorted for plotting).",
	)
	refresh = st.button("Refresh")

if refresh:
	load_weather_data.clear()

try:
	df = load_weather_data(limit=int(limit))
except Exception as exc:  # noqa: BLE001
	st.error("Failed to load data from PostgreSQL.")

	# Common pitfall: using Docker-only hostnames for a locally-run dashboard.
	if os.getenv("POSTGRES_HOST", "").strip().lower() == "host.docker.internal":
		st.info(
			"Your POSTGRES_HOST is set to `host.docker.internal`. "
			"That is meant for containers reaching the host. "
			"If you are running Streamlit locally, set POSTGRES_HOST=localhost (or create a `.env.local` override)."
		)

	st.code(str(exc))
	st.stop()

if df.empty:
	st.info("No rows found in `weather_data` yet. Run the ETL to load data.")
	st.stop()

latest = df.iloc[-1:].copy()

st.subheader("Latest record")
st.dataframe(latest, use_container_width=True, hide_index=True)

st.subheader("Trends")

col1, col2 = st.columns(2)

with col1:
	st.markdown("**Temperature (°C)**")
	temp_series = df.set_index("timestamp")["temperature"].dropna()
	st.line_chart(temp_series)

with col2:
	st.markdown("**Humidity (%)**")
	humidity_series = df.set_index("timestamp")["humidity"].dropna()
	st.line_chart(humidity_series)
