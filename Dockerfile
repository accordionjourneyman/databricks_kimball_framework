# Dockerfile for local Spark + Delta Lake testing environment.
# This image provides a full PySpark + Delta Lake setup for running
# integration tests locally without needing a Databricks cluster.
#
# IMPORTANT: We deliberately do NOT install databricks-connect here.
# When databricks-connect is installed alongside pyspark, it hijacks
# SparkSession.builder.getOrCreate() and refuses to create local sessions.
# By keeping them separate, the local Docker environment gets a clean
# PySpark + Delta setup where SparkSession.builder works as documented.
#
# For the databricks target, install databricks-connect on the host
# machine (outside Docker) and run: python tools/run_tests.py -t databricks
#
# Usage:
#   docker-compose build
#   docker-compose run --rm kimball-tests python -m pytest tests/ -v
#   docker-compose run --rm kimball-tests python -m pytest tests/unit/ -q
#   docker-compose run --rm kimball-tests python tools/run_tests.py -t local --integration

FROM python:3.11-bookworm

# Install Java 17 (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        bash \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml .
COPY src/ src/
COPY tests/ tests/
COPY tools/ tools/

# Install Python dependencies.
# We install pyspark and delta-spark directly (not via pyproject.toml [dev])
# because the dev extras also pull in databricks-connect which hijacks
# the local SparkSession builder.
RUN pip install --no-cache-dir \
    pyspark==3.5.5 \
    delta-spark==3.2.0 \
    pyyaml \
    jinja2 \
    pydantic \
    pytest \
    ruff

# Install the framework package itself (without remote/databricks extras)
RUN pip install --no-cache-dir --no-deps -e .

# Default command runs all tests locally
CMD ["python", "-m", "pytest", "tests/", "-v", "--tb=short"]