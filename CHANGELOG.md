# Changelog

All notable changes to the Kimball Framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking Changes
- **REMOVED**: `UniqueKeyGenerator` class - violated deterministic surrogate key requirements, used unsafe `monotonically_increasing_id()`
- **DEPRECATED**: `SequenceKeyGenerator` remains deprecated but available for migration period

### Added
- **YAML Configuration Schema Validation**: Added JSON Schema validation for YAML configs to catch configuration errors early with descriptive error messages
- **Enhanced Configuration Robustness**: Automatic validation of required fields, data types, enum values, and Kimball-specific business rules
- **Environment Variables**: Added `KIMBALL_CLEANUP_REGISTRY_TABLE` and `KIMBALL_CHECKPOINT_TABLE` for staging cleanup and pipeline checkpoints

### Fixed
- **BLOCKER**: Replaced unsafe `.collect()` in `StagingCleanupManager` with distributed `foreachPartition()` processing
- **BLOCKER**: Fixed year 9999 timestamp overflow by using 2099-12-31 for `__valid_to` default rows
- **MAJOR**: Resolved dependency version conflict in `requirements.txt` (removed duplicate `databricks-sdk` entries)

### Planned
- Early Arriving Facts support for referential integrity handling
- Advanced SCD patterns (SCD3, SCD4, SCD6)
- Data quality framework integration
- Unity Catalog integration examples
- Performance benchmarking suite

---

## [0.2.0] - 2025-12-26

### Added
- Resilient `StagingTableManager` context manager to prevent workspace pollution from failed ETL operations
- `checkpoint_root` parameter to `Orchestrator` constructor for reliable DataFrame checkpointing
- Support for Databricks Runtime 13+ exception patterns in retry decorator
- `KIMBALL_CHECKPOINT_ROOT` environment variable support
- `scripts/validate_fixes.py` for framework health checks

### Changed
- Shifted DataFrame checkpoints to persistent DBFS storage to support large-scale shuffles and prevent "No such file" errors
- Updated hashing to use vectorized `struct` operations for Photon compatibility
- Improved error handling in metadata lookups to prevent TypeError on empty results
- Removed redundant `.count()` performance anti-patterns in merger operations

### Fixed
- SQL injection vulnerabilities in DDL operations with proper identifier escaping
- Generic exception handling replaced with specific PySpark exception types
- Unsafe metadata access patterns that could fail on empty DataFrames

---

## [0.1.1] - 2025-01-06

### Added
- **ETL Control Table** (`KIMBALL_ETL_CONTROL`): Unified control table replacing simple watermarks, now tracking batch_id, source, row counts, error classifications, and status per run
- **KIMBALL_ETL_SCHEMA environment variable**: Configure where ETL control table is created (e.g., `os.environ["KIMBALL_ETL_SCHEMA"] = "etl_control"`)
- **PipelineExecutor**: New class for wave-based parallel notebook execution (dimensions wave → facts wave)
- **Error classification support**: Categorize failures (DATA_QUALITY, INFRASTRUCTURE, SCHEMA_DRIFT, DEPENDENCY, UNKNOWN)
- **CDF deduplication with `primary_keys`**: Deduplicate Change Data Feed sources using primary keys before merge
- **Foreign key declarations**: Declare FK relationships in fact configs for documentation and future lineage features

### Changed
- **Renamed `WatermarkManager` → `ETLControlManager`**: Better reflects expanded scope (watermarks + audit data). Old name still works as deprecated alias
- **Renamed error classes**: `WatermarkConflictError` → `ETLControlConflictError`, `WatermarkNotFoundError` → `ETLControlNotFoundError`
- **Updated orchestrator**: Now uses `etl_control` attribute, supports batch auditing with row counts
- **Improved createDataFrame**: Explicit schema for ETL control updates to avoid Spark Connect type inference errors

### Fixed
- Orchestrator try block indentation issue causing syntax errors
- Spark Connect `CANNOT_DETERMINE_TYPE` errors when updating control table with null values

### Backward Compatibility
- `WatermarkManager` still works as an alias for `ETLControlManager`
- Old error class names still work as aliases
- Existing watermark-based configs continue to work

---

## [0.1.0] - 2025-01-05

### Added
- Initial release of the Kimball Dimensional Modeling Framework
- **Orchestrator**: Main pipeline coordinator with ETL control management
- **Loader**: Multi-format data loading (CSV, Parquet, Delta, JSON, CDF)
- **Merger**: Delta Lake merge operations with SCD1/SCD2 support
- **Table Creator**: Automatic dimension/fact table DDL generation with liquid clustering
- **Key Generator**: Surrogate key generation (SHA-256 hashing or identity columns)
- **Hashing**: Configurable row hashing for change detection
- **Config**: YAML-based pipeline configuration
- **Bus Matrix**: Documentation generator for dimensional models
- **Skeleton Generator**: Boilerplate YAML config generation

### Features
- SCD Type 1 (overwrite) and SCD Type 2 (versioned history) support
- Identity column support for surrogate keys (Databricks-native)
- Liquid clustering support for Delta tables
- Soft deletes with `_is_deleted` flag
- Automatic `_effective_from`, `_effective_to`, `_is_current` management for SCD2
- Hash-based change detection with `_hash` column
- Watermark-based incremental processing
- Change Data Feed (CDF) integration for incremental loads

### Documentation
- Comprehensive README with quick start guide
- ARCHITECTURE.md explaining the framework design
- CONFIGURATION.md with full YAML reference
- GETTING_STARTED.md tutorial
- LIQUID_CLUSTERING.md for clustering configuration
- Example configurations for dimensions and facts

---

[Unreleased]: https://github.com/your-org/kimball-framework/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/your-org/kimball-framework/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/your-org/kimball-framework/releases/tag/v0.1.0
