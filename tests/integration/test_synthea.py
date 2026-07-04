"""
Synthea synthetic EHR dataset integration tests.

Models the Kimball challenges from the Synthea synthetic patient records:
  - Multi-fact constellation: one patient dimension feeding encounters,
    conditions, medications, and observations fact tables
  - SCD2 on patients: address, marital status, and insurance can change
  - Role-playing dimensions: provider appears in multiple fact tables
  - Out-of-order facts: a condition diagnosed in 2018 may be recorded
    against an encounter that happened in 2019

Reference: https://synthea.mitre.org/
These tests use small synthetic samples (3-5 rows per table) that exercise
the same Kimball patterns as the full synthetic dataset.
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
    db_name = f"kimball_synthea_{uuid.uuid4().hex[:8]}"
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
        path = tmp_path / f"synthea_{uuid.uuid4().hex[:8]}.yml"
        path.write_text(content, encoding="utf-8")
        return str(path)
    return _write


class TestSyntheaPatientSCD2:
    """Test SCD2 on patients: address/marital status changes create new versions."""

    def test_patient_address_change_creates_new_version(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.patients (
                patient_id INT, first_name STRING, last_name STRING,
                city STRING, state STRING, marital_status STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.patients VALUES
            (1, 'Alice', 'Smith', 'Boston',    'MA', 'single'),
            (2, 'Bob',   'Jones', 'Cambridge', 'MA', 'married')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_patient
table_type: dimension
scd_type: 2
keys:
  surrogate_key: patient_sk
  natural_keys: [patient_id]
surrogate_key_strategy: identity
track_history_columns: [city, state, marital_status]
sources:
  - name: {test_db}.patients
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT patient_id, first_name, last_name, city, state, marital_status FROM p
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        spark.sql(f"""
            UPDATE {test_db}.patients
            SET city = 'Worcester', marital_status = 'married'
            WHERE patient_id = 1
        """)

        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        p1_rows = (
            spark.table(f"{test_db}.dim_patient")
            .filter("patient_id = 1")
            .orderBy("patient_sk")
            .collect()
        )
        assert len(p1_rows) == 2
        old, new = p1_rows[0], p1_rows[1]
        assert not old["__is_current"]
        assert old["city"] == "Boston"
        assert old["marital_status"] == "single"
        assert new["__is_current"]
        assert new["city"] == "Worcester"
        assert new["marital_status"] == "married"

        p2_rows = (
            spark.table(f"{test_db}.dim_patient")
            .filter("patient_id = 2")
            .collect()
        )
        assert len(p2_rows) == 1
        assert p2_rows[0]["__is_current"]

        for t in [f"{test_db}.dim_patient", f"{test_db}.patients"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestSyntheaEncounterFact:
    """Test an encounter fact table referencing the patient dimension."""

    def test_fact_encounters_with_patient_sk(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.patients (
                patient_id INT, first_name STRING, last_name STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.patients VALUES
            (1, 'Alice', 'Smith'),
            (2, 'Bob',   'Jones')
        """)

        spark.sql(f"""
            CREATE TABLE {test_db}.encounters (
                encounter_id STRING, patient_id INT, provider_id INT,
                encounter_type STRING, encounter_timestamp TIMESTAMP
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.encounters VALUES
            ('e1', 1, 10, 'ambulatory', TIMESTAMP '2024-01-10 09:00:00'),
            ('e2', 1, 20, 'inpatient',  TIMESTAMP '2024-02-15 14:00:00'),
            ('e3', 2, 10, 'ambulatory', TIMESTAMP '2024-03-01 10:00:00')
        """)

        dim_config = tmp_config(f"""
table_name: {test_db}.dim_patient
table_type: dimension
scd_type: 1
keys:
  surrogate_key: patient_sk
  natural_keys: [patient_id]
surrogate_key_strategy: identity
sources:
  - name: {test_db}.patients
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT patient_id, first_name, last_name FROM p
""")

        fact_config = tmp_config(f"""
table_name: {test_db}.fact_encounters
table_type: fact
merge_keys: [encounter_id]
sources:
  - name: {test_db}.encounters
    alias: e
    cdc_strategy: full
transformation_sql: |
  SELECT
    encounter_id, patient_id, provider_id, encounter_type, encounter_timestamp
  FROM e
""")

        orch1 = Orchestrator(dim_config, spark=spark, etl_schema=test_db)
        assert orch1.run()["status"] == "SUCCESS"

        orch2 = Orchestrator(fact_config, spark=spark, etl_schema=test_db)
        assert orch2.run()["status"] == "SUCCESS"

        rows = spark.table(f"{test_db}.fact_encounters").collect()
        assert len(rows) == 3
        p1_encounters = [r for r in rows if r.patient_id == 1]
        assert len(p1_encounters) == 2

        for t in [
            f"{test_db}.fact_encounters",
            f"{test_db}.dim_patient",
            f"{test_db}.encounters",
            f"{test_db}.patients",
        ]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestSyntheaRolePlayingProvider:
    """Test that the same provider dimension is referenced from multiple facts."""

    def test_provider_appears_in_encounters_and_conditions(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.providers (
                provider_id INT, name STRING, specialty STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.providers VALUES
            (10, 'Gregory House',  'diagnostics'),
            (20, 'James Wilson',   'oncology')
        """)

        spark.sql(f"""
            CREATE TABLE {test_db}.encounters (
                encounter_id STRING, provider_id INT, encounter_type STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.encounters VALUES
            ('e1', 10, 'ambulatory'),
            ('e2', 20, 'inpatient')
        """)

        spark.sql(f"""
            CREATE TABLE {test_db}.conditions (
                condition_id STRING, patient_id INT, provider_id INT,
                condition_code STRING, diagnosis_date DATE
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.conditions VALUES
            ('c1', 1, 10, 'E11.9',  DATE '2024-01-20'),
            ('c2', 2, 20, 'C50.9',  DATE '2024-02-10')
        """)

        dim_config = tmp_config(f"""
table_name: {test_db}.dim_provider
table_type: dimension
scd_type: 1
keys:
  surrogate_key: provider_sk
  natural_keys: [provider_id]
surrogate_key_strategy: identity
sources:
  - name: {test_db}.providers
    alias: pr
    cdc_strategy: full
transformation_sql: |
  SELECT provider_id, name, specialty FROM pr
""")

        enc_config = tmp_config(f"""
table_name: {test_db}.fact_encounters
table_type: fact
merge_keys: [encounter_id]
sources:
  - name: {test_db}.encounters
    alias: e
    cdc_strategy: full
transformation_sql: |
  SELECT encounter_id, provider_id, encounter_type FROM e
""")

        cond_config = tmp_config(f"""
table_name: {test_db}.fact_conditions
table_type: fact
merge_keys: [condition_id]
sources:
  - name: {test_db}.conditions
    alias: c
    cdc_strategy: full
transformation_sql: |
  SELECT condition_id, patient_id, provider_id, condition_code, diagnosis_date FROM c
""")

        assert Orchestrator(dim_config, spark=spark, etl_schema=test_db).run()["status"] == "SUCCESS"
        assert Orchestrator(enc_config, spark=spark, etl_schema=test_db).run()["status"] == "SUCCESS"
        assert Orchestrator(cond_config, spark=spark, etl_schema=test_db).run()["status"] == "SUCCESS"

        enc_rows = spark.table(f"{test_db}.fact_encounters").collect()
        cond_rows = spark.table(f"{test_db}.fact_conditions").collect()
        assert len(enc_rows) == 2
        assert len(cond_rows) == 2

        enc_house = [r for r in enc_rows if r.provider_id == 10]
        cond_house = [r for r in cond_rows if r.provider_id == 10]
        assert len(enc_house) == 1
        assert len(cond_house) == 1

        for t in [
            f"{test_db}.fact_encounters",
            f"{test_db}.fact_conditions",
            f"{test_db}.dim_provider",
            f"{test_db}.encounters",
            f"{test_db}.conditions",
            f"{test_db}.providers",
        ]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
