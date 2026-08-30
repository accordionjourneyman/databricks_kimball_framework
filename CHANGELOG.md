# Changelog

All notable changes to the Kimball Framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `tools/inspect_etl_control.py` â€” read-only diagnostic CLI to query
  the `etl_control` Delta table. Supports `--table`, `--running`,
  `--failed`, `--older-than`, `--json`, `--limit` flags. Unit tests: 15.
- `tests/unit/test_inspect_tool.py` â€” unit tests for inspect tool.
- `kimball query-history` CLI subcommand â€” delegates to
  `tools/query_history.py` to analyze `system.query.history`. Handles
  access-denied and connectivity errors gracefully.

## [0.3.0] - 2026-07-19

### BREAKING
- **REMOVED:** `watermark_database` parameter from `Orchestrator` and
  `PipelineExecutor` (use `etl_schema` instead)
- **REMOVED:** PII strategy `"hash"` (non-cryptographic alias for
  `fast_hash`). Use `"fast_hash"` or `"tokenize"`.
- **REMOVED:** `NullPolicyConfig.mode` â€” the `"legacy"` mode is gone.
  Null policy is always "kimball" (strict null substitution).
- **REMOVED:** Cluster by parameter from
  `table_creator.create_table_with_clustering()` â€” use config-based
  cluster_by only.
- **REMOVED:** Programmatic `partition_by` and the `partition_by` argument to
  `RuntimePolicy.cluster_clause()`; table layout is now config-driven.
- **REMOVED:** `kimball.validation` compatibility re-export and unused
  specialized error aliases. Import validation types from
  `kimball.orchestration.validation`; use `StructuredError` for categorized
  operational failures.
- **REMOVED:** `KIMBALL_TRUST_STORED_HASHDIFF` env var â€” hashdiff is
  always recomputed.
- **REMOVED:** `silver_schema`/`gold_schema` split in `TargetConfig`
  (consolidated into single `etl_schema` field).
- **REMOVED:** `compile_time_sql_check` from `RuntimeOptions`.
- **REMOVED:** Dead methods and functions:
  `build_expire_set`, `build_insert_values`, `_select_payload_columns`,
  `_placeholder_column`, `enable_schema_auto_merge`,
  `default_checkpoint_location`, `LateArrivingDimensionProcessor`,
  `PipelineExecutor._run_parallel`, `PipelineExecutor._run_wave`,
  `PipelineExecutor.dry_run`, `ETLControlManager.update_watermark`,
  `ETLControlManager.batch_start`, `ETLControlManager.get_batch_status`,
  `ETLControlManager.get_config_fingerprint`,
  `ETLControlManager.get_source_schema_fingerprint`,
  `StagingCleanupManager.cleanup_staging_tables`,
  `PipelineCheckpoint` (all methods), `evaluate_contract_changes`,
  `load_contract_directory`, `compile_project`,
  `ValidationReport.all_passed`, `_run_compile_time_sql_check`,
  `checkpoint_manager`.

### Added
- `SECURITY.md` with coordinated vulnerability disclosure process
- `CONTRIBUTING.md` with dev setup and PR expectations
- `CODE_OF_CONDUCT.md` (Contributor Covenant v2.1)
- GitHub issue templates (bug, feature, question)
- Pull request template with production-readiness checklist
- `docs/COMPATIBILITY.md` (Python Ã— PySpark Ã— Delta matrix)
- `kimball --version` CLI flag
- CI-generated JSON Schema for pipeline YAML config
- `validate_resolution` and `detect_fanout` config options on
  `ForeignKeyLookupConfig` for pre-merge resolution validation
  (fanout detection, count matching, resolution rate logging)

### Changed
- `DEFAULT_VALID_TO` is now timezone-aware (`tzinfo=timezone.utc`)
  to prevent PySpark Connect Arrow serialization overflow
- Streaming `.persist()` calls are guarded for serverless compute
  compatibility
- Removed unused runtime options: `optimize_scd2_lazy_eval`,
  `batch_control_writes`, `single_window_scd2`, `trust_stored_hashdiff`

### Fixed
- Streaming checkpoint default uses UC volumes on Databricks
  (DBFS root is disabled on serverless)
- Integration test: `early_arriving_facts` legacy config field
  removed from skeleton generation test
- Documentation review: fixed 25 stale or inconsistent references
  across CHANGELOG, GETTING_STARTED, CONTRIBUTING, CONFIGURATION,
  COMPATIBILITY, KNOWN_LIMITATIONS, BENCHMARKING, and issue templates

---

## [0.2.0] - 2026-01-15

### Breaking Changes

- **REMOVED**: `UniqueKeyGenerator` class - violated deterministic surrogate key requirements
- **REMOVED**: dbt implementation (was experimental, now PySpark-only)
- **DEPRECATED**: `SequenceKeyGenerator` blocked by default (unsafe global sort)

### Changed

- **Repo Structure**: Flattened from `implementations/pyspark/` to root level
- Checkpoint optimization - `checkpoint()` now optional via `enable_lineage_truncation` config

### Added

- YAML Configuration Schema Validation with JSON Schema
- Environment Variables: `KIMBALL_CLEANUP_REGISTRY_TABLE`, `KIMBALL_CHECKPOINT_TABLE`
- Resilient `StagingTableManager` context manager
- Support for Databricks Runtime 13+ exception patterns

### Fixed

- SCD2 intra-batch sequencing prevents history corruption
- System columns always preserved during column pruning
- Atomic cleanup operations prevent race conditions
- Timestamp overflow protection (2099 instead of 9999)

---

## [0.1.1] - 2025-01-06

### Added

- **ETL Control Table**: Unified control table tracking batch_id, source, row counts, status
- **PipelineExecutor**: Wave-based parallel notebook execution
- **Error classification**: DATA_QUALITY, INFRASTRUCTURE, SCHEMA_DRIFT, DEPENDENCY, UNKNOWN
- **CDF deduplication**: Via `primary_keys` config option
- **Foreign key declarations** in fact configs

### Changed

- Renamed `WatermarkManager` â†’ `ETLControlManager`
- Updated orchestrator for batch auditing with row counts

### Fixed

- Orchestrator try block indentation issue
- Spark Connect type inference errors with null values

---

## [0.1.0] - 2025-01-05

### Added

- Initial release of the Kimball Dimensional Modeling Framework
- **Orchestrator**: Main pipeline coordinator with ETL control management
- **Loader**: Multi-format data loading (CSV, Parquet, Delta, JSON, CDF)
- **Merger**: Delta Lake merge operations with SCD1/SCD2 support
- **Table Creator**: Automatic dimension/fact table DDL with liquid clustering
- **Key Generator**: Surrogate key generation (SHA-256 hashing or identity columns)
- **Hashing**: Configurable row hashing for change detection
- **Config**: YAML-based pipeline configuration with Jinja2 templating
- **Bus Matrix**: Documentation generator for dimensional models

### Features

- SCD Type 1 (overwrite) and SCD Type 2 (versioned history)
- Identity column support for surrogate keys (Databricks-native)
- Liquid clustering support for Delta tables
- Soft deletes with `_is_deleted` flag
- Hash-based change detection with `_hash` column
- Watermark-based incremental processing
- Change Data Feed (CDF) integration

---

[0.3.0]: https://github.com/accordionjourneyman/databricks_kimball_framework/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/accordionjourneyman/databricks_kimball_framework/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/accordionjourneyman/databricks_kimball_framework/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/accordionjourneyman/databricks_kimball_framework/releases/tag/v0.1.0
