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
    """

    etl_schema: str | None = None
    checkpoint_root: str | None = None
    mode: Literal["lite", "full"] = "lite"

    # Feature flags (None means inherit from mode)
    enable_checkpoints: bool | None = None
    enable_staging_cleanup: bool | None = None
    enable_metrics: bool | None = None
    enable_auto_cluster: bool | None = None

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

        def _flag(env_var: str) -> bool | None:
            """Parse feature flag: '1' -> True, '0' -> False, missing -> None."""
            val = os.environ.get(env_var)
            if val == "1":
                return True
            if val == "0":
                return False
            return None

        return cls(
            etl_schema=os.environ.get("KIMBALL_ETL_SCHEMA"),
            checkpoint_root=os.environ.get("KIMBALL_CHECKPOINT_ROOT"),
            mode=mode,
            enable_checkpoints=_flag("KIMBALL_ENABLE_CHECKPOINTS"),
            enable_staging_cleanup=_flag("KIMBALL_ENABLE_STAGING_CLEANUP"),
            enable_metrics=_flag("KIMBALL_ENABLE_METRICS"),
            enable_auto_cluster=_flag("KIMBALL_ENABLE_AUTO_CLUSTER"),
        )

    def feature_enabled(self, feature: str) -> bool:
        """Check if a feature is enabled.

        Args:
            feature: Feature name (checkpoints, staging_cleanup, metrics, auto_cluster).

        Returns:
            True if feature is enabled, False otherwise.
        """
        feature_map = {
            "checkpoints": self.enable_checkpoints,
            "staging_cleanup": self.enable_staging_cleanup,
            "metrics": self.enable_metrics,
            "auto_cluster": self.enable_auto_cluster,
        }
        return bool(feature_map.get(feature, False))
