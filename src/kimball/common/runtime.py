"""Runtime configuration for Kimball Framework.

This module provides a centralized RuntimeOptions class that encapsulates
all runtime configuration, replacing scattered environment variable checks.

Usage:
    # Create with defaults (reads from environment)
    options = RuntimeOptions.from_environment()

    # Create with explicit values (for testing/DI)
    options = RuntimeOptions(
        etl_schema="gold",
        checkpoint_root="/dbfs/checkpoints",
        mode="lite",
    )

    # Pass to Orchestrator
    orchestrator = Orchestrator(config, runtime_options=options)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass
class RuntimeOptions:
    """Centralized runtime configuration for Kimball pipelines.

    This replaces scattered os.environ.get() calls with a single,
    testable, injectable configuration object.

    Attributes:
        etl_schema: Schema for ETL control tables (e.g., 'gold', 'catalog.schema').
        checkpoint_root: Path for Spark checkpoints. If None, uses local checkpointing.
        mode: 'lite' (default) or 'full'. Full mode enables all resilience features.
        enable_checkpoints: Enable pipeline checkpointing (default: from mode).
        enable_staging_cleanup: Enable staging table cleanup (default: from mode).
        enable_metrics: Enable query metrics collection (default: from mode).
        enable_auto_cluster: Enable auto-clustering (default: from mode).
        spark_session: Optional injected SparkSession for testing.

    JVM Performance Tuning (read this before going to production):
        shuffle_partitions: Number of partitions for shuffles. Spark's default (200)
            is almost always wrong:
            - Too HIGH for small data: creates tiny partitions, namenode pressure,
              excessive task scheduling overhead
            - Too LOW for large data: creates huge partitions that spill to disk
              and cause GC thrashing
            Set to 'auto' (recommended) to let AQE handle it dynamically, or
            calculate: target_partition_size_mb * num_partitions ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â°ÃƒÆ’Ã¢â‚¬Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â  shuffle_data_size
            Rule of thumb: 128-256MB per partition.

        skew_threshold_mb: Partition size threshold for skew detection (default: 256MB).
            If you have dimension defaults like -1 or 'Unknown' with millions of rows,
            those partitions will be skewed. AQE will split them if > this threshold.

    """

    etl_schema: str | None = None
    checkpoint_root: str | None = None
    mode: Literal["lite", "full"] = "lite"

    # Feature flags (None means inherit from mode)
    enable_checkpoints: bool | None = None
    enable_staging_cleanup: bool | None = None
    enable_metrics: bool | None = None
    enable_auto_cluster: bool | None = None

    # Performance Optimization Flags (opt-in; not all are safe as defaults)
    approx_grain_check: bool | None = None
    skip_delete_detection: bool | None = None

    # JVM/Spark Performance Tuning
    # These settings have direct impact on GC pressure and shuffle efficiency
    shuffle_partitions: str | int = "auto"  # 'auto' = let AQE decide, or explicit int
    skew_threshold_mb: int = 256  # Partition size threshold for skew handling
    skew_factor: int = 5  # Partition Nx larger than median = skewed
    use_approximate_unique: bool = False
    """Use HLL-based approx_count_distinct instead of exact groupBy for uniqueness checks.
    O(n) instead of O(n log n) shuffle. Probabilistic (~1.5% error)."""

    # Injected dependencies (for testing)
    spark_session: SparkSession | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Resolve feature flags based on mode if not explicitly set."""
        is_full_mode = self.mode == "full"

        if self.enable_checkpoints is None:
            self.enable_checkpoints = is_full_mode
        if self.enable_staging_cleanup is None:
            self.enable_staging_cleanup = is_full_mode
        if self.enable_metrics is None:
            self.enable_metrics = is_full_mode
        if self.enable_auto_cluster is None:
            self.enable_auto_cluster = is_full_mode

    @classmethod
    def from_environment(cls) -> RuntimeOptions:
        """Create RuntimeOptions from environment variables.

        Environment Variables:
            KIMBALL_ETL_SCHEMA: Schema for ETL control tables.
            KIMBALL_CHECKPOINT_ROOT: Path for Spark checkpoints.
            KIMBALL_MODE: 'lite' or 'full' (default: 'lite').
            KIMBALL_ENABLE_CHECKPOINTS: '1' to enable checkpoints.
            KIMBALL_ENABLE_STAGING_CLEANUP: '1' to enable staging cleanup.
            KIMBALL_ENABLE_METRICS: '1' to enable metrics collection.
            KIMBALL_ENABLE_AUTO_CLUSTER: '1' to enable auto-clustering.

        Returns:
            RuntimeOptions instance configured from environment.
        """
        mode_str = os.environ.get("KIMBALL_MODE", "lite").lower()
        mode: Literal["lite", "full"] = "full" if mode_str == "full" else "lite"
        if "KIMBALL_SKIP_VALIDATION_IF_UNCHANGED" in os.environ:
            raise ValueError(
                "KIMBALL_SKIP_VALIDATION_IF_UNCHANGED was removed because unchanged "
                "schema/configuration does not prove unchanged data. Remove the variable; "
                "data-quality, natural-key, and foreign-key checks now always run."
            )

        def _flag(env_var: str) -> bool | None:
            """Parse feature flag: '1' -> True, '0' -> False, missing -> None."""
            val = os.environ.get(env_var)
            if val == "1":
                return True
            return False if val == "0" else None

        return cls(
            etl_schema=os.environ.get("KIMBALL_ETL_SCHEMA"),
            checkpoint_root=os.environ.get("KIMBALL_CHECKPOINT_ROOT"),
            mode=mode,
            enable_checkpoints=_flag("KIMBALL_ENABLE_CHECKPOINTS"),
            enable_staging_cleanup=_flag("KIMBALL_ENABLE_STAGING_CLEANUP"),
            enable_metrics=_flag("KIMBALL_ENABLE_METRICS"),
            enable_auto_cluster=_flag("KIMBALL_ENABLE_AUTO_CLUSTER"),
            # Performance Optimization Flags
            approx_grain_check=_flag("KIMBALL_APPROX_GRAIN_CHECK"),
            skip_delete_detection=_flag("KIMBALL_SKIP_DELETE_DETECTION"),
            # JVM Performance Tuning
            shuffle_partitions=os.environ.get("KIMBALL_SHUFFLE_PARTITIONS", "auto"),
            skew_threshold_mb=int(os.environ.get("KIMBALL_SKEW_THRESHOLD_MB", "256")),
            skew_factor=int(os.environ.get("KIMBALL_SKEW_FACTOR", "5")),
            use_approximate_unique=os.environ.get("KIMBALL_USE_APPROXIMATE_UNIQUE", "")
            == "1",
        )
