# Changelog

All notable changes to the Kimball Framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

- Renamed `WatermarkManager` → `ETLControlManager`
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

[0.2.0]: https://github.com/your-username/databricks_kimball_framework/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/your-username/databricks_kimball_framework/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/your-username/databricks_kimball_framework/releases/tag/v0.1.0
