FROM apache/airflow:2.9.3-python3.11

# Install OS deps if needed (kept minimal)
USER root
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install project runtime dependencies (NOT apache-airflow itself)
COPY docker/airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt --constraint "${CONSTRAINT_URL}"
