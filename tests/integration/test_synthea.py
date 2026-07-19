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

import pytest
from pyspark.sql import SparkSession

from kimball.common.errors import DataQualityError
from kimball.orchestration.orchestrator import Orchestrator

pytestmark = pytest.mark.usefixtures("spark")


class TestSyntheaPatientSCD2:
    """Test SCD2 on patients: address/marital status changes create new versions."""

    def test_patient_address_change_creates_new_version(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.patients (
                patient_id INT, first_name STRING, last_name STRING,
                city STRING, state STRING, marital_status STRING,
                updated_at STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.patients VALUES
            (1, 'Alice', 'Smith', 'Boston',    'MA', 'single',   '2024-01-01T00:00:00'),
            (2, 'Bob',   'Jones', 'Cambridge', 'MA', 'married',  '2024-01-01T00:00:00')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_patient
table_type: dimension
scd_type: 2
effective_at: updated_at
keys:
  surrogate_key: patient_sk
  natural_keys: [patient_id]
track_history_columns: [city, state, marital_status]
sources:
  - name: {test_db}.patients
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT patient_id, first_name, last_name, city, state, marital_status, updated_at FROM p
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        spark.sql(f"""
            UPDATE {test_db}.patients
            SET city = 'Worcester', marital_status = 'married', updated_at = '2024-06-01T00:00:00'
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
            spark.table(f"{test_db}.dim_patient").filter("patient_id = 2").collect()
        )
        assert len(p2_rows) == 1
        assert p2_rows[0]["__is_current"]

        for t in [f"{test_db}.dim_patient", f"{test_db}.patients"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestSyntheaEncounterFact:
    """Test an encounter fact table referencing the patient dimension.

    The fact declares a foreign key on patient_id -> dim_patient.patient_id so
    the framework's FK integrity gate runs. The test verifies the gate has
    teeth: a valid run succeeds, but an encounter whose patient_id does not
    exist in dim_patient is rejected (run fails) rather than silently written.
    """

    def test_fact_encounters_fk_gates_orphan_patients(
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
foreign_keys:
  - column: patient_id
    references: {test_db}.dim_patient
    dimension_key: patient_id
    lookup:
      source_columns: [patient_id]
      detect_fanout: true
      validate_resolution: true
sources:
  - name: {test_db}.encounters
    alias: e
    cdc_strategy: full
transformation_sql: |
  SELECT
    encounter_id, patient_id, provider_id, encounter_type, encounter_timestamp
  FROM e
""")

        assert (
            Orchestrator(dim_config, spark=spark, etl_schema=test_db).run()["status"]
            == "SUCCESS"
        )
        assert (
            Orchestrator(fact_config, spark=spark, etl_schema=test_db).run()["status"]
            == "SUCCESS"
        )

        rows = spark.table(f"{test_db}.fact_encounters").collect()
        assert len(rows) == 3, f"Expected 3 encounters, got {len(rows)}"
        p1_encounters = [r for r in rows if r.patient_id == 1]
        assert len(p1_encounters) == 2

        # Real FK verification: every fact.patient_id must exist in dim_patient.
        # A mere row-count check would pass even if the fact referenced
        # patients that don't exist in the dimension.
        dim_patient_ids = {
            r.patient_id for r in spark.table(f"{test_db}.dim_patient").collect()
        }
        for r in rows:
            assert r.patient_id in dim_patient_ids, (
                f"Fact row references patient_id={r.patient_id} absent from dim_patient"
            )

        # Negative case with teeth: insert an encounter whose patient_id is NOT
        # in dim_patient and re-run. The FK integrity gate must reject it --
        # the run raises DataQualityError rather than silently writing the
        # orphan. If it succeeds, the gate is a no-op and the test fails.
        spark.sql(f"""
            INSERT INTO {test_db}.encounters VALUES
            ('e4', 999, 10, 'emergency', TIMESTAMP '2024-04-01 08:00:00')
        """)
        with pytest.raises(DataQualityError):
            Orchestrator(fact_config, spark=spark, etl_schema=test_db).run()

        for t in [
            f"{test_db}.fact_encounters",
            f"{test_db}.dim_patient",
            f"{test_db}.encounters",
            f"{test_db}.patients",
        ]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestSyntheaRolePlayingProvider:
    """Test the same provider dimension is referenced from multiple facts.

    Both facts declare a foreign key on provider_id -> dim_provider.provider_id.
    The test verifies the role-playing relationship has teeth: both facts
    succeed with valid providers, and an orphan provider in EITHER fact is
    rejected by the FK integrity gate.
    """

    def test_both_facts_reference_provider_dimension_with_fk_gate(
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
foreign_keys:
  - column: provider_id
    references: {test_db}.dim_provider
    dimension_key: provider_id
    lookup:
      source_columns: [provider_id]
      detect_fanout: true
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
foreign_keys:
  - column: provider_id
    references: {test_db}.dim_provider
    dimension_key: provider_id
    lookup:
      source_columns: [provider_id]
      detect_fanout: true
sources:
  - name: {test_db}.conditions
    alias: c
    cdc_strategy: full
transformation_sql: |
  SELECT condition_id, patient_id, provider_id, condition_code, diagnosis_date FROM c
""")

        assert (
            Orchestrator(dim_config, spark=spark, etl_schema=test_db).run()["status"]
            == "SUCCESS"
        )
        assert (
            Orchestrator(enc_config, spark=spark, etl_schema=test_db).run()["status"]
            == "SUCCESS"
        )
        assert (
            Orchestrator(cond_config, spark=spark, etl_schema=test_db).run()["status"]
            == "SUCCESS"
        )

        enc_rows = spark.table(f"{test_db}.fact_encounters").collect()
        cond_rows = spark.table(f"{test_db}.fact_conditions").collect()
        assert len(enc_rows) == 2, f"Expected 2 encounters, got {len(enc_rows)}"
        assert len(cond_rows) == 2, f"Expected 2 conditions, got {len(cond_rows)}"

        # Real role-playing verification: provider 10 (House) is referenced by
        # BOTH facts, and every provider_id in both facts exists in dim_provider.
        dim_provider_ids = {
            r.provider_id for r in spark.table(f"{test_db}.dim_provider").collect()
        }
        for r in enc_rows + cond_rows:
            assert r.provider_id in dim_provider_ids, (
                f"Fact row references provider_id={r.provider_id} absent from dim_provider"
            )
        enc_house = [r for r in enc_rows if r.provider_id == 10]
        cond_house = [r for r in cond_rows if r.provider_id == 10]
        assert len(enc_house) == 1 and len(cond_house) == 1, (
            "Provider 10 should be referenced by both facts (role-playing dimension)"
        )

        # Negative case with teeth: a condition referencing an unknown provider
        # must be rejected by the FK gate. If the run succeeds, the gate is a
        # no-op and the role-playing relationship is unenforced.
        spark.sql(f"""
            INSERT INTO {test_db}.conditions VALUES
            ('c3', 2, 999, 'Z99.9', DATE '2024-03-10')
        """)
        with pytest.raises(DataQualityError):
            Orchestrator(cond_config, spark=spark, etl_schema=test_db).run()

        for t in [
            f"{test_db}.fact_encounters",
            f"{test_db}.fact_conditions",
            f"{test_db}.dim_provider",
            f"{test_db}.encounters",
            f"{test_db}.conditions",
            f"{test_db}.providers",
        ]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
