"""
NYC Taxi TLC dataset integration tests.

Models the Kimball challenges from the NYC Taxi trip record data:
  - Partitioned fact table: partition by pickup date for query performance
  - SCD2 on zones: taxi zones can be renamed or reassigned
  - effective_at business time: use pickup timestamp (not processing time)
    for SCD2 valid_from/valid_to boundaries
  - SCD2 on taxis (medallion): medallion owner can transfer
  - Large fact volume: 200M+ rows/year, so late-arriving trips matter

Reference: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
These tests use small synthetic samples (3-5 rows per table) that exercise
the same Kimball patterns as the full dataset.
"""

import os
import uuid

import pytest
from pyspark.sql import SparkSession

from kimball.common.config import ConfigLoader
from kimball.orchestration.orchestrator import Orchestrator

pytestmark = pytest.mark.usefixtures("spark")


@pytest.fixture
def test_db(spark: SparkSession):
    db_name = f"kimball_taxi_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    os.environ["KIMBALL_ETL_SCHEMA"] = db_name
    yield db_name
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")


@pytest.fixture
def config_loader():
    return ConfigLoader()


@pytest.fixture
def tmp_config(tmp_path, config_loader):
    def _write(content: str) -> str:
        path = tmp_path / f"taxi_{uuid.uuid4().hex[:8]}.yml"
        path.write_text(content, encoding="utf-8")
        return str(path)

    return _write


class TestNYCTaxiEffectiveAt:
    """Test that effective_at uses business time, not processing time."""

    def test_scd2_uses_pickup_timestamp_for_validity(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.taxi_zones (
                location_id INT, zone_name STRING, borough STRING,
                update_timestamp TIMESTAMP
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.taxi_zones VALUES
            (1, 'Midtown Center', 'Manhattan', TIMESTAMP '2024-01-01 00:00:00'),
            (2, 'JFK Airport',    'Queens',    TIMESTAMP '2024-01-01 00:00:00')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_zone
table_type: dimension
scd_type: 2
keys:
  surrogate_key: zone_sk
  natural_keys: [location_id]
track_history_columns: [zone_name, borough]
effective_at: update_timestamp
sources:
  - name: {test_db}.taxi_zones
    alias: z
    cdc_strategy: full
transformation_sql: |
  SELECT location_id, zone_name, borough, update_timestamp FROM z
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        initial_rows = (
            spark.table(f"{test_db}.dim_zone").filter("location_id = 1").collect()
        )
        assert len(initial_rows) == 1
        assert initial_rows[0]["__is_current"]
        initial_valid_from = initial_rows[0]["__valid_from"]

        spark.sql(f"""
            UPDATE {test_db}.taxi_zones
            SET zone_name = 'Midtown CBD', update_timestamp = TIMESTAMP '2024-06-15 00:00:00'
            WHERE location_id = 1
        """)

        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        all_rows = (
            spark.table(f"{test_db}.dim_zone")
            .filter("location_id = 1")
            .orderBy("zone_sk")
            .collect()
        )
        assert len(all_rows) == 2
        old, new = all_rows[0], all_rows[1]
        assert not old["__is_current"]
        assert new["__is_current"]
        assert new["__valid_from"] >= old["__valid_to"]
        assert old["__valid_to"] >= initial_valid_from

        for t in [f"{test_db}.dim_zone", f"{test_db}.taxi_zones"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestNYCTaxiFactTable:
    """Test a fact table built from a single source with date partitioning."""

    def test_fact_trips_loads_with_partitioning(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.trips (
                trip_id STRING, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP,
                pickup_location_id INT, dropoff_location_id INT,
                passenger_count INT, trip_distance DOUBLE, fare_amount DOUBLE
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.trips VALUES
            ('t1', TIMESTAMP '2024-03-15 08:30:00', TIMESTAMP '2024-03-15 08:45:00', 1, 2, 1, 2.5, 12.0),
            ('t2', TIMESTAMP '2024-03-15 09:00:00', TIMESTAMP '2024-03-15 09:20:00', 2, 1, 2, 4.0, 18.0),
            ('t3', TIMESTAMP '2024-03-16 10:00:00', TIMESTAMP '2024-03-16 10:15:00', 1, 1, 1, 1.2,  7.5)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.fact_trips
table_type: fact
merge_keys: [trip_id]
cluster_by: [pickup_date]
sources:
  - name: {test_db}.trips
    alias: t
    cdc_strategy: full
transformation_sql: |
  SELECT
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    CAST(pickup_datetime AS DATE) AS pickup_date
  FROM t
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        rows = spark.table(f"{test_db}.fact_trips").collect()
        assert len(rows) == 3
        total_fare = sum(r.fare_amount for r in rows)
        assert abs(total_fare - 37.5) < 0.01

        by_date = {
            str(r.pickup_date): len([x for x in rows if x.pickup_date == r.pickup_date])
            for r in rows
        }
        assert by_date.get("2024-03-15") == 2
        assert by_date.get("2024-03-16") == 1

        for t in [f"{test_db}.fact_trips", f"{test_db}.trips"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestNYCTaxiLateArrivingTrip:
    """Test late-arriving trips: a trip logged after midnight was actually yesterday."""

    def test_late_arriving_trip_is_inserted(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.trips (
                trip_id STRING, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP,
                pickup_location_id INT, fare_amount DOUBLE
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.trips VALUES
            ('t1', TIMESTAMP '2024-03-15 08:30:00', TIMESTAMP '2024-03-15 08:45:00', 1, 12.0)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.fact_trips
table_type: fact
merge_keys: [trip_id]
sources:
  - name: {test_db}.trips
    alias: t
    cdc_strategy: full
transformation_sql: |
  SELECT trip_id, pickup_datetime, dropoff_datetime, pickup_location_id, fare_amount FROM t
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"
        assert spark.table(f"{test_db}.fact_trips").count() == 1

        spark.sql(f"""
            INSERT INTO {test_db}.trips VALUES
            ('t2', TIMESTAMP '2024-03-15 23:50:00', TIMESTAMP '2024-03-16 00:10:00', 2, 15.0)
        """)

        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"
        assert spark.table(f"{test_db}.fact_trips").count() == 2

        for t in [f"{test_db}.fact_trips", f"{test_db}.trips"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
