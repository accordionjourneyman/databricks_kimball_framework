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

FROM python:3.14-bookworm

LABEL org.opencontainers.image.title="Kimball framework integration-test image" \
      org.opencontainers.image.description="Local Spark/Delta test image; not a production runtime image"

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
RUN pip install --no-cache-dir \
    pyspark==4.0.1 \
    delta-spark==4.2.0 \
    pyyaml \
    jinja2 \
    pydantic \
    jsonschema \
    pytest \
    pytest-benchmark \
    ruff \
    databricks-sdk

# databricks-sdk is the pure-Python SDK (NOT databricks-connect). tests/conftest.py
# does `import databricks` and then mocks `databricks.sdk.runtime`, so the SDK
# package must be present for the test session to import. It does not hijack
# SparkSession.builder (the reason databricks-connect is deliberately omitted).

# Download Delta Lake JARs at build time so they're available offline at runtime.
# Delta 4.x is built against Spark 4.0 / 4.1 and Scala 2.13.
RUN SPARK_JARS_DIR=/usr/local/lib/python3.11/site-packages/pyspark/jars && \
    curl -fsSL -o "$SPARK_JARS_DIR/delta-spark_4.0_2.13-4.2.0.jar" \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_4.0_2.13/4.2.0/delta-spark_4.0_2.13-4.2.0.jar" && \
    curl -fsSL -o "$SPARK_JARS_DIR/delta-storage-4.2.0.jar" \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/4.2.0/delta-storage-4.2.0.jar"

# Install the framework package itself (without remote/databricks extras)
RUN pip install --no-cache-dir --no-deps -e .

# Tests do not need root. Keeping this image non-privileged also catches code
# that accidentally assumes writable system directories.
RUN useradd --create-home --uid 10001 kimball && chown -R kimball:kimball /app
USER kimball

# Default command runs all tests locally
CMD ["python", "-m", "pytest", "tests/", "-v", "--tb=short"]
